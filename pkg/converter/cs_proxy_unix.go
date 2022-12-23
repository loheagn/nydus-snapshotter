//go:build !windows
// +build !windows

/*
 * Copyright (c) 2022. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package converter

import (
	"archive/tar"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/containerd/containerd/content"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type contentStoreProxy struct {
	localAddr string
	listener  net.Listener
	errChan   chan error
}

const (
	// recvBufSize is the size of the buffer used to receive data from the unix socket.
	// The max may like `read;{start};{n}` where `start` is uint64 and n is int64.
	// So 50 is enough.
	recvBufSize = 50

	// reqSep is the separator used to separate the requests.
	reqSep = ";"
)

// the commands used to communicate with the proxy client
const (
	setTypeCommand  = "set_type"
	blobSizeCommand = "blob_size"
	readDataCommand = "read"
)

// This indicates that which part of the blob layer should be returned to the proxy client.
const (
	WholeBlobType = "whole"
	RawBlobType   = "raw"
	BootStrapType = "bootstrap"
)

func newContentStoreProxy(workDir string) (*contentStoreProxy, error) {
	sockP, err := os.CreateTemp(workDir, "nydus-cs-proxy-*.sock")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create unix socket file")
	}
	if err := os.Remove(sockP.Name()); err != nil {
		return nil, err
	}
	return &contentStoreProxy{
		localAddr: sockP.Name(),
		errChan:   make(chan error, 1),
	}, nil
}

func (p *contentStoreProxy) close() error {
	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			return err
		}
	}
	return os.Remove(p.localAddr)
}

func (p *contentStoreProxy) handleConn(c net.Conn, ra content.ReaderAt) {
	var (
		req           []string
		dataReader    io.Reader
		tarHeader     *tar.Header
		currentOffset uint64
		err           error
	)

	recvReq := func() error {
		buf := make([]byte, 0, recvBufSize)
		readBuf := make([]byte, 1)
		for {
			_, err := c.Read(readBuf)
			if err != nil {
				return errors.Wrap(err, "failed to read from connection")
			}
			logrus.Debugf("received data from %s: %s", c.RemoteAddr().String(), readBuf)
			if readBuf[0] == '&' {
				break
			}
			buf = append(buf, readBuf[0])
		}
		reqStr := string(buf)
		logrus.Debugf("received data from %s: %s", c.RemoteAddr().String(), reqStr)
		reqStr = strings.Trim(strings.Trim(reqStr, "\x00"), reqSep)
		req = strings.Split(reqStr, reqSep)
		logrus.Debugf("received data from %s: %s", c.RemoteAddr().String(), req)
		return nil
	}

	sendRespBytes := func(resp []byte) error {
		n, err := c.Write(resp)
		if err != nil {
			return errors.Wrapf(err, "failed to write to connection: %v", err)
		}
		logrus.Debugf("send %d bytes to %s: %s", n, c.RemoteAddr().String(), resp)
		return nil
	}

	sendResp := func(resp string) error {
		return sendRespBytes([]byte(resp))
	}

	setType := func() error {
		if len(req) != 2 {
			return errors.Errorf("invalid request: %s", req)
		}
		handle := func(reader io.Reader, hdr *tar.Header) error {
			dataReader, tarHeader = reader, hdr
			return nil
		}
		switch req[1] {
		case WholeBlobType:
			dataReader = newSeekReader(ra)
		case RawBlobType:
			seekFile(ra, EntryBlob, handle)
		case BootStrapType:
			seekFile(ra, EntryBootstrap, handle)
		default:
			return errors.Wrapf(err, "invalid request: %s", req)
		}
		return sendResp("ok")
	}

	sendBlobSize := func() error {
		if tarHeader == nil {
			return sendResp("0")
		}

		return sendResp(fmt.Sprintf("%d", tarHeader.Size))
	}

	readData := func() error {
		if len(req) != 3 {
			return errors.Errorf("invalid request: %s", req)
		}
		offset, err := strconv.ParseUint(req[1], 10, 64)
		if err != nil || offset != currentOffset {
			return errors.Wrapf(err, "invalid request: %s", req)
		}
		n, err := strconv.ParseInt(req[2], 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid request: %s", req)
		}

		copied, err := io.CopyN(c, dataReader, n)
		if err != nil {
			return errors.Wrap(err, "copy to proxy client")
		}
		currentOffset += uint64(copied)

		return nil
	}

	for {
		err := recvReq()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			p.errChan <- errors.Wrap(err, "failed to receive request")
			break
		}
		switch req[0] {
		case setTypeCommand:
			err = setType()
		case blobSizeCommand:
			err = sendBlobSize()
		case readDataCommand:
			err = readData()
			if errors.Is(err, io.EOF) {
				// client has read all data, close the connection
				break
			}
		default:
			err = errors.Errorf("invalid request: %s", req)
		}
		if err != nil {
			c.Close()
			p.errChan <- err
			break
		}
	}
	c.Close()
	p.errChan <- nil
}

// serve creates a unix socket and listens on it to proxy the the containerd content store to nydus-image and nydusd.
// The function returns the unix socket address, a function to shut down the proxy and an error.
func (p *contentStoreProxy) serve(ra content.ReaderAt) {
	l, err := net.Listen("unix", p.localAddr)
	if err != nil {
		logrus.Debug(err.Error())
		p.errChan <- errors.Wrap(err, "failed to listen unix socket")
		return
	}
	p.listener = l

	for {
		conn, err := l.Accept()
		if err != nil {
			logrus.Error(err, "failed to accept connection")
			p.errChan <- errors.Wrap(err, "failed to accept connection")
			break
		}
		logrus.Debugf("accepting connection from %s", conn.RemoteAddr().String())

		go p.handleConn(conn, ra)
	}
}
