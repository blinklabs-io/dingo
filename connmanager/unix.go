// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connmanager

import (
	"errors"
	"fmt"
	"net"
)

// UnixConn is a wrapper around net.UnixConn that provides a unique remote address
type UnixConn struct {
	*net.UnixConn
	remoteAddr UnixConnAddr
}

func NewUnixConn(conn net.Conn) (*UnixConn, error) {
	uConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, errors.New("connection is not net.UnixConn")
	}
	// Construct address string
	var fdNum int
	// Get raw connection
	rawConn, err := uConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("failed to get raw connection: %w", err)
	}
	// Retrieve socket file descriptor
	if rawConn == nil {
		return nil, errors.New("raw connection returned empty")
	}
	err = rawConn.Control(
		func(fd uintptr) {
			fdNum = int(fd)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get socket file descriptor: %w", err)
	}
	return &UnixConn{
		UnixConn: uConn,
		remoteAddr: UnixConnAddr{
			addr: fmt.Sprintf(
				"unix@%d",
				fdNum,
			),
		},
	}, nil
}

func (u UnixConn) RemoteAddr() net.Addr {
	return u.remoteAddr
}

type UnixConnAddr struct {
	addr string
}

func (UnixConnAddr) Network() string {
	return "unix"
}

func (u UnixConnAddr) String() string {
	return u.addr
}
