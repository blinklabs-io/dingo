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
	"fmt"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chainselection"
)

// The Genesis window is measured in slots, but the per-connection history
// limit counts headers. While Genesis is active the limit must follow the
// candidate-fragment bound of 4k headers, not the window's slot count.
func TestPeerHeaderHistoryLimitGenesisWindowCountsHeaders(t *testing.T) {
	t.Parallel()

	const k = 2160
	window := chainselection.GenesisWindowSlotsForParams(k, big.NewRat(1, 20))
	require.Greater(t, window, uint64(4*k))

	testCases := []struct {
		name   string
		active bool
		window uint64
		want   func(floor int) int
	}{
		{
			name:   "slot window above candidate bound",
			active: true,
			window: window,
			want:   func(floor int) int { return max(floor, 4*k) },
		},
		{
			name:   "slot window below candidate bound",
			active: true,
			window: 5000,
			want:   func(floor int) int { return max(floor, 5000) },
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
			require.Less(t, floor, 4*k)
			assert.Equal(t, tc.want(floor), fixture.ls.peerHeaderHistoryLimit())
		})
	}
}

// Headers the local chain has already passed are evicted by count. Under
// Genesis that count must stop at the candidate-fragment bound rather than
// retaining one header per slot of the window.
func TestPeerHeaderHistoryGenesisRetentionBoundedByHeaders(t *testing.T) {
	t.Parallel()

	const k = 2160
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.CardanoNodeConfig = newTestShelleyGenesisCfgWithSecurityParam(t, k)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, chainselection.GenesisWindowSlotsForParams(
			k,
			big.NewRat(1, 20),
		)
	}

	const headerCount = 4*k + 100
	slot := fixture.ancestorTip.Point.Slot
	prevHash := fixture.ancestorTip.Point.Hash
	for i := range headerCount {
		hash := testHashBytes(fmt.Sprintf("genesis-retention-%d", i))
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(slot, hash),
			BlockHeader: mockHeader{
				hash:        lcommon.NewBlake2b256(hash),
				prevHash:    lcommon.NewBlake2b256(prevHash),
				blockNumber: uint64(i) + 1,
				slot:        slot,
			},
		})
		prevHash = hash
	}

	history := fixture.ls.peerHeaderHistory[connIdKey(fixture.connId)]
	require.NotNil(t, history)
	assert.Equal(t, 4*k, len(history.order))
}
