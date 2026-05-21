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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnableTCPLingerZeroOnTCP verifies that enableTCPLingerZero
// sets SO_LINGER on a real TCP connection. We can't directly read
// the linger value back through Go's net API, so we drive it through
// a paired listen/dial and assert the helper does not return an error.
// The semantic effect (RST on close) is exercised by integration paths.
func TestEnableTCPLingerZeroOnTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	dialDone := make(chan net.Conn, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			dialDone <- nil
			return
		}
		dialDone <- c
	}()

	srv, err := ln.Accept()
	require.NoError(t, err)
	defer srv.Close()

	cli := <-dialDone
	require.NotNil(t, cli)
	defer cli.Close()

	assert.NoError(t, enableTCPLingerZero(srv))
	assert.NoError(t, enableTCPLingerZero(cli))
}

// TestEnableTCPLingerZeroOnNonTCPIsNoop verifies that calling the
// helper on a non-TCP connection (e.g. unix-socket NtC) is a no-op
// and never returns an error. The listener wires the helper before
// the unix-socket wrapping, so this guard prevents NtC paths from
// erroring on accept. net.Pipe gives us a non-TCP net.Conn pair
// without a filesystem path — important on macOS where t.TempDir
// paths exceed the unix-socket sun_path length limit.
func TestEnableTCPLingerZeroOnNonTCPIsNoop(t *testing.T) {
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	assert.NoError(t, enableTCPLingerZero(srv))
	assert.NoError(t, enableTCPLingerZero(cli))
}

// TestEnableTCPLingerZeroOnNilIsNoop covers the defensive path where
// a caller passes a nil connection (e.g. dial error path).
func TestEnableTCPLingerZeroOnNilIsNoop(t *testing.T) {
	assert.NoError(t, enableTCPLingerZero(nil))
}
