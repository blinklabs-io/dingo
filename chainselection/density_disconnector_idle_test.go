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
	require.Contains(t, f.cs.forecastHorizonWitnesses, conn)
}

func TestGDDLatestSlotClosesUpperBound(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, latest uint64) *gddFixture {
		f := newGDDFixture(t, true)
		f.cs.config.MinCorroboratingPeers = 1
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
