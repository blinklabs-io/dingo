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
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// With no best peer, many honest peers can trail the local tip by more than k
// at once. Recycling all of them in one gap leaves no honest peer to select,
// so only one behind-local-tip recycle may be requested per cooldown window
// and the peer whose cursor is at the local tip is never recycled.
func TestMassBehindLocalTipRecycleKeepsSomeHonestPeers(t *testing.T) {
	t.Parallel()

	const (
		tipBlock     = uint64(10000)
		k            = uint64(432)
		honestPeers  = 13
		cursorBehind = uint64(500)
	)
	f := newForecastHorizonFixture(t, "10.0.0.1:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(tipBlock, k)
	f.setBestPeer(false)

	state := f.o.chainsyncState
	// The default client limit is below the peer count under test.
	addClient := func(remote string) ouroboros.ConnectionId {
		t.Helper()
		conn := newTestConnId("127.0.0.1:6020", remote)
		require.True(
			t,
			state.TryAddClientConnIdWithDirection(conn, honestPeers+2, true),
		)
		return conn
	}
	honestConns := make([]ouroboros.ConnectionId, 0, honestPeers)
	for i := range honestPeers {
		honestConns = append(
			honestConns,
			addClient(fmt.Sprintf("10.1.0.%d:3001", i+1)),
		)
	}
	forkConn := addClient("10.2.0.1:3001")

	deliver := func(
		conn ouroboros.ConnectionId,
		block uint64,
		tip ochainsync.Tip,
		b byte,
	) {
		t.Helper()
		require.NoError(t, f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			newTestBlockHeader(block, block, b),
			tip,
		))
	}
	honestTip := peerTipAt(tipBlock + 50000)
	for i, conn := range honestConns {
		deliver(conn, tipBlock-cursorBehind, honestTip, byte(0x10+i))
	}
	deliver(forkConn, tipBlock-1, peerTipAt(tipBlock+1), 0xf0)

	// The first recycle is the one the policy permits; wait for it, then
	// require that nothing else follows.
	first := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"a far-behind honest peer must still be recycled once",
	)
	data, ok := first.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, event.ChainsyncResyncReasonBehindLocalTip, data.Reason)
	require.NotEqual(t, forkConn, data.ConnectionId,
		"the peer whose cursor is at the local tip is not recycled")
	require.Contains(t, honestConns, data.ConnectionId)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		300*time.Millisecond,
		"other honest peers must not be recycled in the same window",
	)
}
