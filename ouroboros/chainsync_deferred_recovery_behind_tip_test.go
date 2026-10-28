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

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A peer whose tip is below the applied local tip serves a chain that can
// never be selected, so re-verifying its deferred headers cannot change
// anything. Re-intersecting it only replays the same headers, and each
// reconnect is a new connection the once-per-connection idle bound does not
// cover. Its deferred header is withheld without parking a recovery.
func TestDeferredHeaderRecoverySkipsPeerBehindLocalTip(t *testing.T) {
	t.Parallel()

	var localTip, forkParent, forkHash gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0] = 0x01, 0x03, 0x04
	f := newDeferredHeaderFixture(t, localTip)
	f.o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return 100, 2160
	}
	stale := nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 99,
		slotNumber:  86400,
	}}

	f.rollForward(t, stale)

	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		300*time.Millisecond,
		"a peer whose tip is below the local tip must not be re-intersected",
	)
	require.NotContains(
		t,
		f.o.deferredHeaderRecovery,
		f.conn,
		"a peer whose tip is below the local tip has nothing to recover",
	)
	tracked := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tracked)
	require.NotEqual(
		t,
		ocommon.NewPoint(stale.SlotNumber(), stale.Hash().Bytes()),
		tracked.Cursor,
		"the deferred header stays withheld",
	)
}

// A peer whose tip equals the local tip can still win the tiebreak, so its
// deferred header keeps the ordinary recovery.
func TestDeferredHeaderRecoveryKeepsPeerAtLocalTip(t *testing.T) {
	t.Parallel()

	var localTip, forkParent, forkHash gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0] = 0x01, 0x03, 0x05
	f := newDeferredHeaderFixture(t, localTip)
	f.o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return 100, 2160
	}
	f.rollForward(t, nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 100,
		slotNumber:  86400,
	}})

	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"a peer at the local tip must still be re-intersected",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		data.Reason,
	)
}

// The applied-block path drops a parked recovery whose peer has since fallen
// below the local tip, instead of re-intersecting it.
func TestDeferredHeaderRecoveryDropsParkedPeerBehindLocalTip(t *testing.T) {
	t.Parallel()

	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6081")
	localTipBlock := uint64(1)
	f.o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return localTipBlock, 2160
	}
	f.deferHeader(t, 0xef, 0x01, 200, 1)
	localTipBlock = 2
	f.applyElsewhere(ocommon.NewPoint(300, []byte("applied-1")))

	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		300*time.Millisecond,
		"a parked peer now below the local tip must not be re-intersected",
	)
	require.NotContains(t, f.o.deferredHeaderRecovery, f.conn)
}
