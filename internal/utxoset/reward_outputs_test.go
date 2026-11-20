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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A reward write larger than one store transaction holds is applied in
// chunks, in order: here a wipe of the epoch ahead of 60,000 outputs, each
// of which is a row, an index row and two journal entries.
func TestRewardWriteLargerThanAStoreTransactionIsApplied(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	const outputs = 60_000
	pool := bytes.Repeat([]byte{0xd1}, 28)
	changes := []ledger.RewardOutputChange{{
		Kind: ledger.RewardOutputWipe, Output: models.RewardAccountOutput{Epoch: 5},
	}}
	for i := range outputs {
		key := make([]byte, 28)
		binary.BigEndian.PutUint32(key, uint32(i)) //nolint:gosec // a test index
		changes = append(changes, ledger.RewardOutputChange{
			Kind: ledger.RewardOutputPut, Slot: 10,
			Output: models.RewardAccountOutput{
				Epoch: 5, StakingKey: key, PoolKeyHash: pool, RewardType: "member",
				Amount: types.Uint64(uint64(i) + 1), Spendable: true,
				CapturedSlot: 10, BoundarySlot: 20,
			},
		})
	}
	require.NoError(t, set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		RewardOutputs: changes,
	}))

	epochs, err := set.RewardOutputEpochs(nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, epochs)
	for _, i := range []int{0, outputs / 2, outputs - 1} {
		key := make([]byte, 28)
		binary.BigEndian.PutUint32(key, uint32(i)) //nolint:gosec // a test index
		rows, err := set.CredentialRewardOutputs(0, key, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1, "output %d", i)
		require.Equal(t, types.Uint64(uint64(i)+1), rows[0].Amount)
	}
	got, err := set.EpochRewardOutputs(5, outputs+1, nil)
	require.NoError(t, err)
	require.Len(t, got, outputs, "every output has its index row")
}
