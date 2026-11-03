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

package ledger

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chainselection"
)

// The per-connection history limit counts headers. While Genesis is active it
// must cover the k headers of the local fragment plus every header the
// forecast range can hold, which at full density is one per window slot.
func TestPeerHeaderHistoryLimitGenesisCoversForecastRange(t *testing.T) {
	t.Parallel()

	const k = 600
	window := chainselection.GenesisWindowSlotsForParams(k, big.NewRat(1, 20))
	require.Equal(t, uint64(36000), window)

	testCases := []struct {
		name   string
		active bool
		window uint64
		want   func(floor int) int
	}{
		{
			name:   "window above 4k",
			active: true,
			window: window,
			want:   func(floor int) int { return max(floor, int(window)+k) },
		},
		{
			name:   "genesis inactive",
			active: false,
			window: window,
			want:   func(floor int) int { return floor },
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			fixture.ls.config.CardanoNodeConfig = newTestShelleyGenesisCfgWithSecurityParam(t, k)
			fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
				return tc.active, tc.window
			}
			floor := max(
				maxPeerHeaderHistoryPerConn,
				fixture.ls.allowedQueuedHeaders(),
			)
			assert.Equal(t, tc.want(floor), fixture.ls.peerHeaderHistoryLimit())
		})
	}
}

// A dense candidate can hold more than 4k headers inside the forecast range.
// The fork-path walk must still reach a held ancestor that many linked
// headers back.
func TestFindPeerForkPathGenesisDenseCandidateBeyondFourK(t *testing.T) {
	t.Parallel()

	const k = 600
	const linked = 4*k + 50
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.CardanoNodeConfig = newTestShelleyGenesisCfgWithSecurityParam(t, k)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, chainselection.GenesisWindowSlotsForParams(
			k,
			big.NewRat(1, 20),
		)
	}
	require.Greater(t, linked, fixture.ls.allowedQueuedHeaders())

	stamped := fixture.ls.CurrentHeaderCryptoVerificationGeneration()
	byHash := map[string]ChainsyncEvent{}
	prevOf := map[string][]byte{}
	prevHash := fixture.ancestorTip.Point.Hash
	slot := fixture.ancestorTip.Point.Slot
	var tipEvent ChainsyncEvent
	var tipPrev []byte
	for i := range linked {
		slot++
		hash := testHashBytes(fmt.Sprintf("dense-header-%d", i))
		ev := ChainsyncEvent{
			ConnectionId:                       fixture.connId,
			HeaderCryptoVerificationGeneration: stamped,
			Point:                              ocommon.NewPoint(slot, hash),
			BlockHeader: mockHeader{
				hash:        lcommon.NewBlake2b256(hash),
				prevHash:    lcommon.NewBlake2b256(prevHash),
				blockNumber: fixture.ancestorTip.BlockNumber + uint64(i) + 1,
				slot:        slot,
			},
		}
		if i == linked-1 {
			tipEvent, tipPrev = ev, prevHash
			break
		}
		byHash[hex.EncodeToString(hash)] = ev
		prevOf[hex.EncodeToString(hash)] = prevHash
		prevHash = hash
	}
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		h []byte,
	) (ChainsyncEvent, []byte, bool) {
		k := hex.EncodeToString(h)
		ev, ok := byHash[k]
		return ev, prevOf[k], ok
	}

	point, path, err := fixture.ls.findPeerForkPath(
		tipEvent,
		tipPrev,
		fixture.currentTip.Point.Slot,
	)
	require.NoError(t, err)
	require.NotNil(t, point, "ancestor behind %d linked headers lost", linked)
	assert.Equal(t, fixture.ancestorTip.Point.Slot, point.Slot)
	assert.Len(t, path, linked)
}
