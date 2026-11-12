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

package ouroboros

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// The outbound connection handler's start waits for a ChainSync Jumping
// operation holding the connection's lock, so it never runs beside a restart.
func TestOutboundChainsyncStartWaitsForConnectionLock(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	peer := h.addPeer("peer", 100)
	conn := h.cm.GetConnectionById(peer.connId)
	require.NotNil(t, conn)

	unlock := h.o.outboundChainsyncLocks.acquire(peer.connId)
	started := make(chan bool, 1)
	go func() {
		started <- h.o.startOutboundChainsyncLocked(peer.connId, conn)
	}()
	testutil.RequireNoReceive(t, started, 100*time.Millisecond,
		"the start ran while a jump or restart held the connection's lock")
	unlock()
	require.True(t, testutil.RequireReceive(t, started, 5*time.Second,
		"the start did not run once the lock was released"))
}

// A lock entry stays while any caller holds or waits for it, so a reconnect
// reusing the ConnectionId waits for the old operation, and is dropped once
// no caller uses it.
func TestOutboundChainsyncLocksShareInUseEntry(t *testing.T) {
	t.Parallel()
	var locks outboundChainsyncLocks
	connId := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	unlockFirst := locks.acquire(connId)
	second := make(chan func(), 1)
	go func() { second <- locks.acquire(connId) }()
	testutil.RequireNoReceive(t, second, 100*time.Millisecond,
		"a second caller locked the connection while the first held it")
	unlockFirst()
	unlockSecond := testutil.RequireReceive(t, second, 5*time.Second,
		"the second caller did not get the lock")
	unlockSecond()
	locks.mu.Lock()
	defer locks.mu.Unlock()
	require.Empty(t, locks.locks, "an unused lock entry was kept")
}
