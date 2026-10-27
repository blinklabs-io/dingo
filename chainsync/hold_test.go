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

package chainsync_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func holdTip(slot uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte{byte(slot)}),
		BlockNumber: slot / 10,
	}
}

// PeerTipSlot reports the highest tip an eligible client reported, whether it
// arrived as a held header or an admitted one. An observability-only client
// is excluded: it is a random inbound peer whose tip need not be a chain.
func TestHoldPeerTipSlot(t *testing.T) {
	t.Parallel()
	state := chainsync.NewState(nil, nil)
	_, ok := state.PeerTipSlot()
	require.False(t, ok, "no client, no tip")

	eligible := newTestConnId(1)
	other := newTestConnId(2)
	observer := newTestConnId(3)
	require.True(t, state.AddClientConnId(eligible))
	require.True(t, state.AddClientConnId(other))
	require.True(t, state.TryAddObservedClientConnId(observer))
	_, ok = state.PeerTipSlot()
	require.False(t, ok, "a client that reported nothing has no tip")

	state.RecordHeldClientTip(eligible, holdTip(500))
	state.AdmitClientTip(other, holdTip(600).Point, holdTip(600))
	state.RecordHeldClientTip(observer, holdTip(9000))
	slot, ok := state.PeerTipSlot()
	require.True(t, ok)
	assert.Equal(t, uint64(600), slot)

	state.RecordHeldClientTip(eligible, holdTip(700))
	slot, _ = state.PeerTipSlot()
	assert.Equal(t, uint64(700), slot)

	tc := state.GetTrackedClient(eligible)
	require.NotNil(t, tc)
	assert.Equal(
		t,
		ocommon.Point{},
		tc.Cursor,
		"a held tip does not move the admitted cursor",
	)
	assert.False(t, state.RecordHeldClientTip(newTestConnId(4), holdTip(1)))
}

// A hold marks every client tracked while it is held, and the marks outlast
// the release until each client is removed. A client tracked after the
// release is not marked.
func TestHoldMarksClientsUntilRemoved(t *testing.T) {
	t.Parallel()
	state := chainsync.NewState(nil, nil)
	before := newTestConnId(1)
	during := newTestConnId(2)
	after := newTestConnId(3)
	require.True(t, state.AddClientConnId(before))
	assert.False(t, state.ConnectionIngestHeld(before))

	state.SetNetworkIngestHeld(true)
	require.True(t, state.TryAddObservedClientConnId(during))
	state.SetNetworkIngestHeld(false)
	require.True(t, state.AddClientConnId(after))

	assert.True(t, state.ConnectionIngestHeld(before))
	assert.True(t, state.ConnectionIngestHeld(during))
	assert.False(t, state.ConnectionIngestHeld(after))

	state.RemoveClientConnId(before)
	assert.False(t, state.ConnectionIngestHeld(before))
	require.True(t, state.AddClientConnId(before))
	assert.False(t, state.ConnectionIngestHeld(before),
		"a new connection under a reused ID is not marked")
}
