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

package chainselection

import (
	"fmt"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// denseFork delivers the shared block at slot 1000 and k+1 headers after it, so
// the peer offers more than k headers after the intersection.
func denseFork(t *testing.T, f *gddFixture, conn ouroboros.ConnectionId) {
	t.Helper()
	f.deliver(t, conn, 0, append([]uint64{1000}, slotRange(1001, 1041)...)...)
}

func TestGDDIdlePeerWithoutHeadersAfterIntersection(t *testing.T) {
	t.Parallel()

	t.Run("idle peer is disconnected", func(t *testing.T) {
		t.Parallel()
		f := newGDDFixture(t, true)
		dense, idle := corrConn(1), corrConn(2)
		denseFork(t, f, dense)
		f.deliver(t, idle, 0, 1000)
		f.cs.SetPeerIdling(idle, f.cs.GetPeerTip(idle).ObservedTip.Point)

		f.evaluate()

		assert.Equal(t, []ouroboros.ConnectionId{idle}, f.disconnected())
	})

	t.Run("non-idle peer is kept", func(t *testing.T) {
		t.Parallel()
		f := newGDDFixture(t, true)
		dense, waiting := corrConn(1), corrConn(2)
		denseFork(t, f, dense)
		f.deliver(t, waiting, 0, 1000)

		f.evaluate()

		assert.Empty(t, f.disconnected())
	})

	t.Run("idle peer is kept when rival offers only k headers", func(t *testing.T) {
		t.Parallel()
		f := newGDDFixture(t, true)
		rival, idle := corrConn(1), corrConn(2)
		f.deliver(t, rival, 0, append([]uint64{1000}, slotRange(1001, 1040)...)...)
		f.deliver(t, idle, 0, 1000)
		f.cs.SetPeerIdling(idle, f.cs.GetPeerTip(idle).ObservedTip.Point)

		f.evaluate()

		assert.Empty(t, f.disconnected())
	})
}

// forecastHorizonHeader records a header the peer has sent that waits for the
// ledger forecast horizon, the selector's view of a header past what it can
// validate.
func forecastHorizonHeader(
	t *testing.T,
	f *gddFixture,
	conn ouroboros.ConnectionId,
	slot uint64,
) {
	t.Helper()
	base := f.cs.GetPeerTip(conn).ObservedTip
	f.cs.PrepareForecastHorizonWitness(PeerTipUpdateEvent{
		ConnectionId:     conn,
		Tip:              genesisTip(1_000_000, "adv", base.BlockNumber+3),
		ObservedTip:      genesisTip(slot, fmt.Sprintf("h%d", slot), base.BlockNumber+1),
		AdmissionID:      99,
		ObservedPrevHash: base.Point.Hash,
	})
	f.cs.mutex.RLock()
	defer f.cs.mutex.RUnlock()
	require.Contains(t, f.cs.forecastHeldSlots, conn)
	require.Empty(
		t,
		f.cs.forecastHorizonWitnesses,
		"the default configuration records no corroboration witness",
	)
}

func TestGDDLatestSlotClosesUpperBound(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, latest uint64) *gddFixture {
		f := newGDDFixture(t, true)
		dense, sparse := corrConn(1), corrConn(2)
		denseFork(t, f, dense)
		// One header in the window (1000, 1120]; 110 trailing slots could
		// still hold more unless a later header proves they cannot.
		f.deliver(t, sparse, 0, 1000, 1010)
		forecastHorizonHeader(t, f, sparse, latest)
		return f
	}

	t.Run("latest header past the window closes the bound", func(t *testing.T) {
		t.Parallel()
		f := setup(t, 1200)

		f.evaluate()

		assert.Equal(t, []ouroboros.ConnectionId{corrConn(2)}, f.disconnected())
	})

	t.Run("latest header inside the window leaves the bound open", func(t *testing.T) {
		t.Parallel()
		f := setup(t, 1050)

		f.evaluate()

		assert.Empty(t, f.disconnected())
	})
}

// stageFrontier appends verified headers to a peer's staged frontier, the way
// admission stages them while the ledger has not yet applied their parents.
func stageFrontier(
	t *testing.T,
	f *gddFixture,
	conn ouroboros.ConnectionId,
	slots ...uint64,
) {
	t.Helper()
	f.cs.mutex.Lock()
	defer f.cs.mutex.Unlock()
	frontier := f.cs.clonePeerTipAdmissionFrontier(f.cs.peerTips[conn])
	require.NotNil(t, frontier)
	for _, slot := range slots {
		prev := frontier.ObservedTip
		f.cs.applyPeerTipAdmissionUpdateLocked(frontier, PeerTipUpdateEvent{
			ConnectionId:     conn,
			Tip:              genesisTip(1_000_000, "adv", prev.BlockNumber+2),
			ObservedTip:      genesisTip(slot, fmt.Sprintf("h%d", slot), prev.BlockNumber+1),
			ObservedPrevHash: prev.Point.Hash,
		})
	}
	f.cs.pendingPeerTipFrontiers[conn] = frontier
}

// A peer whose headers sit in the staged frontier has them counted: the
// latest header past the window closes the upper bound at the staged count,
// not at the admitted count.
func TestGDDStagedHeadersCountTowardWindow(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (*gddFixture, ouroboros.ConnectionId) {
		f := newGDDFixture(t, true)
		dense, honest := corrConn(1), corrConn(2)
		denseFork(t, f, dense)
		f.deliver(t, honest, 0, 1000)
		// 50 headers in the window, then one past it.
		stageFrontier(t, f, honest, append(slotRange(1001, 1050), 1200)...)
		return f, honest
	}

	t.Run("staged window count keeps the peer", func(t *testing.T) {
		t.Parallel()
		f, _ := setup(t)

		f.evaluate()

		assert.Empty(t, f.disconnected())
	})

	t.Run("staged count below the rival still disconnects", func(t *testing.T) {
		t.Parallel()
		f := newGDDFixture(t, true)
		dense, sparse := corrConn(1), corrConn(2)
		denseFork(t, f, dense)
		f.deliver(t, sparse, 0, 1000)
		stageFrontier(t, f, sparse, 1010, 1200)

		f.evaluate()

		assert.Equal(t, []ouroboros.ConnectionId{sparse}, f.disconnected())
	})
}

func TestForecastHeldSlotLifecycle(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	conn := corrConn(1)
	f.deliver(t, conn, 0, 1000)
	update := PeerTipUpdateEvent{
		ConnectionId:     conn,
		Tip:              genesisTip(1_000_000, "adv", 3),
		ObservedTip:      genesisTip(1200, "h1200", 2),
		AdmissionID:      7,
		ObservedPrevHash: []byte("h1000"),
	}
	require.Empty(t, f.cs.PrepareForecastHorizonWitness(update))
	require.Contains(t, f.cs.forecastHeldSlots, conn)

	other := update
	other.AdmissionID = 8
	f.cs.RemoveForecastHorizonWitness(other)
	require.Contains(t, f.cs.forecastHeldSlots, conn, "another wait's removal")

	f.cs.RemoveForecastHorizonWitness(update)
	assert.NotContains(t, f.cs.forecastHeldSlots, conn)
}
