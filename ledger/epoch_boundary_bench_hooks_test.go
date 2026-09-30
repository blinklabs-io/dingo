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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/stretchr/testify/require"
)

// epochBoundaryBenchPartialPrecompute commits the first half of the round's
// pool chunks and stops, as a restart between two chunks would.
func epochBoundaryBenchPartialPrecompute(
	b *testing.B,
	f *epochBoundaryBenchFixture,
) {
	epochBoundaryBenchPartialPrecomputeT(b, f)
}

func epochBoundaryBenchPartialPrecomputeT(
	b testing.TB,
	f *epochBoundaryBenchFixture,
) {
	b.Helper()
	evt := epochBoundaryBenchPrecomputeEvent()
	round, ok, err := f.ls.resolveStakeRewardPrecomputeRound(
		evt.NewEpoch+1,
		evt.BoundarySlot,
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1),
	)
	require.NoError(b, err)
	require.True(b, ok)
	chunks := (len(round.poolInputs) + f.ls.rewardPrecomputeChunkSize() - 1) /
		f.ls.rewardPrecomputeChunkSize()
	for range chunks / 2 {
		done, err := f.ls.stakeRewardPrecomputeChunkStep(round)
		require.NoError(b, err)
		require.False(b, done)
	}
}

func (ls *LedgerState) waitEpochBoundaryBenchBackground() {
	ls.deferredStakeInputsWG.Wait()
}

// epochBoundaryBenchWireDeferred mirrors node.go's
// wireDeferredRewardStakeInputs.
func epochBoundaryBenchWireDeferred(ls *LedgerState, mgr *snapshot.Manager) {
	ls.SetEpochBoundaryDeferredStakeInputsHook(
		func(
			txn *database.Txn,
		) (uint64, uint64, []*models.RewardStakeInput, bool) {
			deferred, ok := mgr.TakeDeferredRewardStakeInputs(txn)
			if !ok {
				return 0, 0, nil, false
			}
			return deferred.Epoch, deferred.BoundarySlot, deferred.Inputs, true
		},
	)
	mgr.SetDeferRewardStakeInputs(true)
}
