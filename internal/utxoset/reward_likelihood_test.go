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

package utxoset_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func likelihoodChange(
	epoch, slot, pot uint64,
	weights []byte,
	pools ...byte,
) ledger.RewardInputChange {
	rows := make([]*models.RewardPoolLikelihood, 0, len(pools))
	for _, p := range pools {
		rows = append(rows, &models.RewardPoolLikelihood{
			Epoch: epoch, PoolKeyHash: bytes.Repeat([]byte{p}, 28),
			LogWeights: weights, PerformanceEstimate: float64(p) / 256,
		})
	}
	return ledger.RewardInputChange{
		Kind:  ledger.RewardInputLikelihood,
		Epoch: epoch,
		Likelihood: &models.RewardLikelihoodState{
			Epoch: epoch, RewardPot: types.Uint64(pot),
			CapturedSlot: slot - 1, BoundarySlot: slot, Complete: true,
		},
		LikelihoodPools: rows,
	}
}

// The non-myopic likelihood state survives a reopen, a later save of an epoch
// replaces that epoch's pools, and a rollback below a boundary restores what
// the set held before it.
func TestRewardLikelihoodSurvivesReopenAndRollsBack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	set, err := utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	weights := make([]byte, 400)
	for i := range 100 {
		binary.BigEndian.PutUint32(weights[i*4:], math.Float32bits(float32(i)))
	}
	apply := func(slot uint64, changes ...ledger.RewardInputChange) {
		tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyLedgerChanges(
			&tip, ledger.LedgerChanges{RewardInputs: changes},
		))
	}
	apply(100, likelihoodChange(10, 100, 1_000, weights, 0x10, 0x11))
	apply(200, likelihoodChange(11, 200, 2_000, weights, 0x11))

	require.NoError(t, set.Close())
	set, err = utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	states, err := set.RewardLikelihoodStates(nil)
	require.NoError(t, err)
	require.Len(t, states, 2)
	require.Equal(t, uint64(10), states[0].Epoch)
	require.Equal(t, types.Uint64(1_000), states[0].RewardPot)
	require.Equal(t, uint64(200), states[1].BoundarySlot)
	pools, err := set.RewardPoolLikelihoodRows(10, nil)
	require.NoError(t, err)
	require.Len(t, pools, 2)
	require.Equal(t, bytes.Repeat([]byte{0x10}, 28), pools[0].PoolKeyHash)
	require.Equal(t, weights, pools[0].LogWeights)
	require.InDelta(t, float64(0x10)/256, pools[0].PerformanceEstimate, 0)

	apply(300, likelihoodChange(11, 300, 3_000, weights, 0x12))
	pools, err = set.RewardPoolLikelihoodRows(11, nil)
	require.NoError(t, err)
	require.Len(t, pools, 1, "a save replaces the epoch's pools")
	require.Equal(t, bytes.Repeat([]byte{0x12}, 28), pools[0].PoolKeyHash)

	_, err = set.Rollback(ocommon.NewPoint(200, bytes.Repeat([]byte{200}, 32)))
	require.NoError(t, err)
	states, err = set.RewardLikelihoodStates(nil)
	require.NoError(t, err)
	require.Len(t, states, 2)
	require.Equal(t, types.Uint64(2_000), states[1].RewardPot)
	pools, err = set.RewardPoolLikelihoodRows(11, nil)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, bytes.Repeat([]byte{0x11}, 28), pools[0].PoolKeyHash)

	_, err = set.Rollback(ocommon.NewPoint(100, bytes.Repeat([]byte{100}, 32)))
	require.NoError(t, err)
	states, err = set.RewardLikelihoodStates(nil)
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, uint64(10), states[0].Epoch)
	pools, err = set.RewardPoolLikelihoodRows(11, nil)
	require.NoError(t, err)
	require.Empty(t, pools)
}
