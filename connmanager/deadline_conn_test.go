// Copyright 2026 Blink Labs Software
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
	"net"
	"testing"
	"time"
)

func TestWithSocketDeadlinesOnlyWrapsTCP(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	if got := withSocketDeadlines(left); got != left {
		t.Fatal("non-TCP connection should not be wrapped")
	}
}

func TestDeadlineConnRefreshesReadAndWriteDeadlines(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	defer server.Close()

	wrapped := withSocketDeadlines(client)
	if _, ok := wrapped.(*deadlineConn); !ok {
		t.Fatal("TCP connection should be wrapped")
	}

	before := time.Now().Add(socketIdleTimeout)
	if _, err := wrapped.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	if _, err := server.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(socketIdleTimeout)

	deadline, ok := wrapped.(*deadlineConn)
	if !ok {
		t.Fatal("TCP connection should be wrapped")
	}
	if err := deadline.Conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if deadline.timeout <= 0 || !after.After(before) {
		t.Fatal("deadline refresh should use a future idle timeout")
	}
}
