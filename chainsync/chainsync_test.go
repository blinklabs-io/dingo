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

package chainsync

import (
	"fmt"
	"net"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestState() *State {
	return &State{
		clients:        make(map[ouroboros.ConnectionId]*ChainsyncClientState),
		trackedClients: make(map[ouroboros.ConnectionId]struct{}),
	}
}

func newTestConnectionId(n int) ouroboros.ConnectionId {
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr(
		"tcp",
		fmt.Sprintf("127.0.0.1:%d", n+10000),
	)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

// TestAddClientConnIdRespectsLimit verifies that AddClientConnId enforces
// the maxClients limit, preventing bypass of connection limits.
func TestAddClientConnIdRespectsLimit(t *testing.T) {
	s := newTestState()
	maxClients := 2

	connA := newTestConnectionId(1)
	connB := newTestConnectionId(2)
	connC := newTestConnectionId(3)

	// First two should succeed
	require.True(t, s.AddClientConnId(connA, maxClients))
	require.True(t, s.AddClientConnId(connB, maxClients))

	// Third should be rejected at the limit
	assert.False(
		t,
		s.AddClientConnId(connC, maxClients),
		"AddClientConnId must reject connections at maxClients limit",
	)
	assert.Equal(t, 2, s.ClientConnCount())
}

// TestAddClientConnIdRejectsDuplicate verifies that AddClientConnId
// rejects duplicate connection IDs.
func TestAddClientConnIdRejectsDuplicate(t *testing.T) {
	s := newTestState()

	conn := newTestConnectionId(1)

	require.True(t, s.AddClientConnId(conn, 10))
	assert.False(
		t,
		s.AddClientConnId(conn, 10),
		"AddClientConnId must reject duplicate connection IDs",
	)
	assert.Equal(t, 1, s.ClientConnCount())
}

// TestAddClientConnIdSetsActive verifies that AddClientConnId sets the
// first connection as the active client when none exists.
func TestAddClientConnIdSetsActive(t *testing.T) {
	s := newTestState()

	conn := newTestConnectionId(1)

	require.Nil(t, s.GetClientConnId())
	require.True(t, s.AddClientConnId(conn, 10))
	require.NotNil(t, s.GetClientConnId())
	assert.Equal(t, conn, *s.GetClientConnId())
}

// TestTryAddClientConnIdLimitEnforced verifies TryAddClientConnId
// enforces the maxClients limit.
func TestTryAddClientConnIdLimitEnforced(t *testing.T) {
	s := newTestState()
	maxClients := 2

	connA := newTestConnectionId(1)
	connB := newTestConnectionId(2)
	connC := newTestConnectionId(3)

	require.True(t, s.TryAddClientConnId(connA, maxClients))
	require.True(t, s.TryAddClientConnId(connB, maxClients))

	assert.False(
		t,
		s.TryAddClientConnId(connC, maxClients),
		"TryAddClientConnId should reject when at limit",
	)
	assert.Equal(t, 2, s.ClientConnCount())
}

// TestTryAddClientConnIdDuplicateRejected verifies TryAddClientConnId
// rejects duplicate connections.
func TestTryAddClientConnIdDuplicateRejected(t *testing.T) {
	s := newTestState()

	conn := newTestConnectionId(1)

	require.True(t, s.TryAddClientConnId(conn, 10))
	assert.False(
		t,
		s.TryAddClientConnId(conn, 10),
		"TryAddClientConnId should reject duplicate connection IDs",
	)
}
