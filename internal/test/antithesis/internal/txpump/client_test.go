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

package txpump

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtoFromAddr_Unix(t *testing.T) {
	cases := []struct {
		addr  string
		proto string
	}{
		{"/ipc/node.socket", "unix"},
		{"/var/run/cardano-node/node.socket", "unix"},
		{"localhost:3001", "tcp"},
		{"127.0.0.1:3001", "tcp"},
		{"node:3001", "tcp"},
	}
	for _, tc := range cases {
		got := protoFromAddr(tc.addr)
		assert.Equal(t, tc.proto, got, "addr=%q", tc.addr)
	}
}

// TestNewNodeClient_ConnectFailure verifies that NewNodeClient returns an
// error when it cannot connect to the specified address.  No live node is
// required because a dynamically allocated port that is immediately closed
// is used, avoiding flakiness from hardcoded port numbers.
func TestNewNodeClient_ConnectFailure(t *testing.T) {
	// Allocate a free port and close it immediately so nothing is listening.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close() //nolint:errcheck
	_, err = NewNodeClient(addr, 42, nil)
	require.Error(t, err, "dial to refused address should fail")
}

// TestNewNodeClient_UnixSocketMissing verifies that dialling a non-existent
// Unix socket returns an error with the path in the message.
func TestNewNodeClient_UnixSocketMissing(t *testing.T) {
	path := "/tmp/txpump_test_nonexistent_socket_12345.sock"
	_, err := NewNodeClient(path, 42, nil)
	require.Error(t, err, "dial to missing socket should fail")
	require.Contains(t, err.Error(), path, "error should mention the socket path")
}

func TestNodeClient_SubmitTxNilConnection(t *testing.T) {
	c := &NodeClient{addr: "/ipc/node.socket"}
	err := c.SubmitTx(conwayEraID, []byte{0x80})
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection is nil")
	require.Contains(t, err.Error(), c.addr)
}
