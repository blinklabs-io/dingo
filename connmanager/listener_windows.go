//go:build windows

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
	"net"
	"strings"
	"syscall"

	"github.com/Microsoft/go-winio"
)

// socketControl is a no-op on Windows
func socketControl(_network, _address string, _c syscall.RawConn) error {
	return nil
}

type UnixConnAddr struct {
	addr string
}

func (a UnixConnAddr) Network() string { return "pipe" }

func (a UnixConnAddr) String() string { return a.addr }

type UnixConn struct {
	net.Conn
	remoteAddr UnixConnAddr
}

func NewUnixConn(conn net.Conn) (*UnixConn, error) {
	if conn == nil {
		return nil, errors.New("connection is nil")
	}
	if conn.RemoteAddr() == nil {
		return nil, errors.New("connection has no remote address")
	}
	return &UnixConn{
		Conn:       conn,
		remoteAddr: UnixConnAddr{addr: conn.RemoteAddr().String()},
	}, nil
}

func (u *UnixConn) RemoteAddr() net.Addr {
	return u.remoteAddr
}

// createPipeListener creates a named pipe listener on Windows
func createPipeListener(_, address string) (net.Listener, error) {
	// Adjust address to named pipe format if not already
	if !strings.HasPrefix(address, `\\.\pipe\`) {
		address = `\\.\pipe\` + address
	}
	return winio.ListenPipe(address, nil)
}
