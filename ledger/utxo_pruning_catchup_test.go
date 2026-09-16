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
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestUtxoPruningDeferredForCatchup pins both of utxoPruningDeferredForCatchup's
// defer conditions, plus the two cases that must NOT defer (human review,
// Chris Guiney, dingo#4320): "No test references utxoPruningDeferredForCatchup.
// It is the one change here that widens what Acquire accepts, so a mirror
// that drifts from cleanupConsumedUtxos' own two defer conditions fails
// open rather than closed." checkUtxoRetentionWindow calls this to decide
// whether to accept a point below the ordinary stability-window floor, so
// a false positive here (deferring when it shouldn't) would let Acquire
// accept a point cleanupConsumedUtxos might have already pruned.
func TestUtxoPruningDeferredForCatchup(t *testing.T) {
	t.Parallel()

	t.Run("no upstream tracked at all: not deferred", func(t *testing.T) {
		t.Parallel()
		ls := &LedgerState{}
		require.False(t, ls.utxoPruningDeferredForCatchup(1000, 50))
	})

	t.Run("active upstream, target not yet known: deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6101, 3291)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		// UpstreamSyncStatus is (0, true) here: a live active connection
		// with no admitted target yet -- "still syncing," per that
		// function's own doc comment.
		require.True(t, ls.utxoPruningDeferredForCatchup(1000, 50))
	})

	t.Run("active upstream, known target, far behind: deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6102, 3292)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		ls.publishActiveUpstream(connA)
		ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
			ConnectionId:      connA,
			SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
			SyncTargetTrusted: true,
		})
		require.Equal(t, uint64(1000), ls.UpstreamTipSlot())
		// tipSlot 100 is 900 slots behind upstream's 1000, well outside a
		// 50-slot stability window.
		require.True(t, ls.utxoPruningDeferredForCatchup(100, 50))
	})

	t.Run("active upstream, known target, caught up: not deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6103, 3293)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		ls.publishActiveUpstream(connA)
		ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
			ConnectionId:      connA,
			SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
			SyncTargetTrusted: true,
		})
		require.Equal(t, uint64(1000), ls.UpstreamTipSlot())
		// tipSlot 980 is within a 50-slot stability window of upstream's
		// 1000 -- caught up, pruning must proceed normally.
		require.False(t, ls.utxoPruningDeferredForCatchup(980, 50))
	})
}
