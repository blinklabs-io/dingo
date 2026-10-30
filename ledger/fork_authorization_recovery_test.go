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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForkAncestryAuthorizationRecovery(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"stale", "unstamped", "missing", "current"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			bus := newTestEventBus(t, nil)
			t.Cleanup(bus.Stop)
			ls.config.EventBus = bus
			resyncCh := subscribeChainsyncResync(t, bus)
			parent := mockHeader{
				hash: lcommon.NewBlake2b256(
					testHashBytes("authorization-parent"),
				),
				prevHash: lcommon.NewBlake2b256(
					fixture.currentTip.Point.Hash,
				),
				blockNumber: fixture.currentTip.BlockNumber + 1,
				slot:        fixture.currentTip.Point.Slot + 1,
			}
			if state != "missing" {
				stamp := ls.loadConsensusSnapshot().generation
				require.NotZero(t, stamp)
				if state == "unstamped" {
					stamp = 0
				}
				ls.recordPeerHeaderHistory(ChainsyncEvent{
					ConnectionId:                       fixture.connId,
					HeaderCryptoVerificationGeneration: stamp,
					Point: ocommon.NewPoint(
						parent.SlotNumber(),
						parent.Hash().Bytes(),
					),
					BlockHeader: parent,
				})
				if state == "stale" {
					ls.Lock()
					ls.publishSnapshotsLocked()
					ls.Unlock()
					require.NotEqual(
						t,
						stamp,
						ls.loadConsensusSnapshot().generation,
					)
				}
			}
			header := mockHeader{
				hash: lcommon.NewBlake2b256(
					testHashBytes("authorization-child"),
				),
				prevHash:    parent.hash,
				blockNumber: parent.blockNumber + 1,
				slot:        parent.slot + 1,
			}
			err := ls.chain.AddBlockHeader(context.Background(), header)
			var notFit chain.BlockNotFitChainTipError
			require.ErrorAs(t, err, &notFit)
			var pending pendingPublishes
			resolved, err := ls.tryResolveFork(ChainsyncEvent{
				ConnectionId:                       fixture.connId,
				HeaderCryptoVerificationGeneration: ls.loadConsensusSnapshot().generation,
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockHeader: header,
			}, notFit, &pending, ls.loadConsensusSnapshot().generation)
			pending.flush()
			require.NoError(t, err)
			require.True(t, resolved)
			assert.Equal(t, fixture.currentTip, ls.chain.Tip())
			assert.Equal(t, fixture.currentTip, ls.currentTip)
			if state == "current" {
				assert.Equal(t, 2, ls.chain.HeaderCount())
				select {
				case e := <-resyncCh:
					t.Fatalf("current ancestry unexpectedly resynced: %+v", e)
				default:
				}
				return
			}
			assert.Zero(
				t,
				ls.chain.HeaderCount(),
				"unverified ancestry must not be queued",
			)
			resync := testutil.RequireReceive(
				t,
				resyncCh,
				testutil.AsyncWait,
				"ancestry must replay from a fresh intersection",
			)
			assert.Equal(t, fixture.connId, resync.ConnectionId)
			reason := event.ChainsyncResyncReasonRollbackNotFound
			if state == "stale" {
				reason = event.ChainsyncResyncReasonHeaderAuthorizationChanged
			}
			assert.Equal(t, reason, resync.Reason)
			assert.Equal(
				t,
				state == "stale",
				event.IsLocalChainsyncResyncReason(resync.Reason),
			)
		})
	}
}
