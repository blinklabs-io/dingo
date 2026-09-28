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

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// populateRewardLiveStakeBatchFixture writes stake keys under both
// credential tags, including keys that are byte prefixes of one another, so
// that batch boundaries fall between tags and between a key and its
// extensions. Some keys have an account only, some a live UTxO only, and
// some both; every third account delegates, some more than once. It writes
// only through dialect-translated statements, so it runs on every dialect.
func populateRewardLiveStakeBatchFixture(t testing.TB, store *Store) int {
	t.Helper()
	keys := make([]models.StakeCredentialRef, 0)
	for tag := range uint8(2) {
		for index := range 12 {
			base := []byte{byte(index / 3)}
			for extension := range index % 3 {
				base = append(base, byte(extension))
			}
			keys = append(keys, models.NewStakeCredentialRef(tag, base))
		}
	}
	db := store.instrumentedQueryer(store.writeDB)
	var utxos []models.Utxo
	for index, ref := range keys {
		hasAccount := index%4 != 3
		hasUtxo := index%4 != 0
		pooled := hasAccount && index%3 == 0
		if hasAccount {
			account := &models.Account{
				StakingKey: ref.Key, CredentialTag: ref.Tag,
				AddedSlot: uint64(10 + index), CreatedSlot: uint64(10 + index),
				Reward: types.Uint64(uint64(index)), Active: index%5 != 0,
			}
			if pooled {
				account.Pool = []byte{0xa0, byte(index % 2)}
			}
			require.NoError(t, store.ImportAccount(account, nil))
		}
		if hasUtxo {
			for output := range 1 + index%3 {
				txID := make([]byte, 32)
				txID[0], txID[1], txID[2] = ref.Tag, byte(index), byte(output)
				utxos = append(utxos, models.Utxo{
					TxId: txID, OutputIdx: uint32(output),
					StakingKey: ref.Key, CredentialTag: ref.Tag,
					AddedSlot: uint64(100 + index),
					Amount:    types.Uint64(uint64(1000*index + output)),
				})
			}
		}
		if !pooled {
			continue
		}
		assignments := []struct {
			pool []byte
			slot int
		}{{[]byte{0xa0, byte(index % 2)}, 20 + index}}
		if index%2 == 0 {
			assignments = append(assignments, struct {
				pool []byte
				slot int
			}{[]byte{0xa0, 0x09}, 30 + index})
		}
		for _, assignment := range assignments {
			_, err := db.ExecContext(t.Context(), `
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, ?, ?, NULL, ?)`,
				ref.Key, ref.Tag, assignment.pool, assignment.slot,
			)
			require.NoError(t, err)
		}
	}
	require.NoError(t, store.ImportUtxos(utxos, nil))
	return len(keys)
}

// testRewardLiveStakeBatchBoundaries proves that splitting the rebuild into
// key-range batches neither drops nor alters a row at any boundary. Both
// rebuilds are idempotent over unchanged inputs, so one store is rebuilt as a
// single batch to establish the expected table and then again at each batch
// size.
func testRewardLiveStakeBatchBoundaries(t *testing.T, store *Store) {
	keys := populateRewardLiveStakeBatchFixture(t, store)
	for _, fromRunningTotals := range []bool{false, true} {
		rebuild := func(batch int) map[string]rewardLiveStakeSnapshotRow {
			store.rewardLiveStakeBatchSize = batch
			if fromRunningTotals {
				require.NoError(
					t,
					store.RebuildRewardLiveStakeFromRunningTotals(500, nil),
				)
			} else {
				require.NoError(t, store.RebuildRewardLiveStake(500, nil))
			}
			return readRewardLiveStakeSnapshot(t, store)
		}
		want := rebuild(keys + 1)
		require.Len(t, want, keys)
		for _, batch := range []int{1, 2, 3, 5, 7, keys - 1, keys} {
			require.Equal(
				t,
				want,
				rebuild(batch),
				"running=%t batch=%d",
				fromRunningTotals,
				batch,
			)
		}
	}
}

func TestRebuildRewardLiveStakeBatchBoundaries(t *testing.T) {
	t.Parallel()
	testRewardLiveStakeBatchBoundaries(t, newMigratedSQLiteStore(t))
}
