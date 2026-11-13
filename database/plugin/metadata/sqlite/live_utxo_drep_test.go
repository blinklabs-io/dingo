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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestDRepVotingPowerByTypeRefusesIncompleteLiveStake requires a store that
// keeps no utxo rows to refuse a by-type total when a delegator has no
// reward_live_stake row, rather than answer from its empty utxo table.
func TestDRepVotingPowerByTypeRefusesIncompleteLiveStake(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("abstain-staking-key-28-bytes"),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true,
		Reward:     10,
	}))
	// Creating the account may add its stake row; the case is a delegator
	// whose row is missing.
	_, err := writeDB.Exec(`DELETE FROM reward_live_stake`)
	require.NoError(t, err)
	store.SetLiveUtxoSource(emptyLiveSource{})
	t.Cleanup(func() { store.SetLiveUtxoSource(nil) })

	_, err = store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain}, 0, nil,
	)
	require.ErrorContains(t, err, "reward_live_stake is incomplete")
}

// TestGetUtxosRestoredByRollbackBoundaries requires a rollback to slot S to
// restore exactly the outputs created at or before S and spent after S.
func TestGetUtxosRestoredByRollbackBoundaries(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	const slot = 100
	seed := []struct {
		name    string
		added   uint64
		deleted uint64
		want    bool
	}{
		{"live", 10, 0, false},
		{"spent after", 20, 150, true},
		{"created after", 150, 200, false},
		{"created at, spent after", slot, slot + 1, true},
		{"spent at", 30, slot, false},
	}
	for i, row := range seed {
		txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId:       txID,
			StakingKey: []byte("restored-by-rollback-staking-k1"),
			Amount:     1,
		}))
		_, err := writeDB.Exec(
			`UPDATE utxo SET added_slot = ?, deleted_slot = ?
WHERE id = (SELECT MAX(id) FROM utxo)`,
			row.added, row.deleted,
		)
		require.NoError(t, err)
	}
	got, err := store.GetUtxosRestoredByRollback(slot, nil)
	require.NoError(t, err)
	gotAdded := map[uint64]bool{}
	for _, u := range got {
		gotAdded[u.AddedSlot] = true
	}
	var want []uint64
	for _, row := range seed {
		if row.want {
			want = append(want, row.added)
		}
	}
	require.Len(t, got, len(want))
	for _, key := range want {
		require.True(t, gotAdded[key], "missing output added at slot %d", key)
	}
}
