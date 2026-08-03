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
	base := &recordingConn{}
	wrapped := &deadlineConn{Conn: base, timeout: socketIdleTimeout}
	before := time.Now()
	if _, err := wrapped.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if !base.writeDeadline.After(before) || !base.readDeadline.After(before) {
		t.Fatal("read and write deadlines should be refreshed")
	}
}

type recordingConn struct {
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *recordingConn) Read(p []byte) (int, error) {
	return len(p), nil
}

func (c *recordingConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (*recordingConn) Close() error         { return nil }
func (*recordingConn) LocalAddr() net.Addr  { return nil }
func (*recordingConn) RemoteAddr() net.Addr { return nil }
func (c *recordingConn) SetDeadline(t time.Time) error {
	c.readDeadline, c.writeDeadline = t, t
	return nil
}
func (c *recordingConn) SetReadDeadline(t time.Time) error  { c.readDeadline = t; return nil }
func (c *recordingConn) SetWriteDeadline(t time.Time) error { c.writeDeadline = t; return nil }
