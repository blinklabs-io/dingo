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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type rewardLiveStakeSnapshotRow struct {
	tag, key, pool                                  string
	utxoStake, rewardStake, totalStake              string
	registered                                      bool
	delegationSlot, delegationBlock, delegationCert int64
	updatedSlot, calculationVersion                 int64
}

func populateRewardLiveStakeRebuildFixture(t testing.TB, store *Store) {
	t.Helper()
	delegated := models.NewStakeCredentialRef(0, []byte{0x10, 0x11})
	accountOnly := models.NewStakeCredentialRef(0, []byte{0x20, 0x21})
	scriptDelegated := models.NewStakeCredentialRef(1, []byte{0x30, 0x31})
	for _, account := range []*models.Account{
		{
			StakingKey: delegated.Key, CredentialTag: delegated.Tag,
			Pool: []byte{0xa0, 0xa1}, AddedSlot: 10, CreatedSlot: 10,
			Reward: types.Uint64(100), Active: true,
		},
		{
			StakingKey: accountOnly.Key, CredentialTag: accountOnly.Tag,
			AddedSlot: 20, CreatedSlot: 20, Reward: types.Uint64(7), Active: true,
		},
		{
			StakingKey: scriptDelegated.Key, CredentialTag: scriptDelegated.Tag,
			Pool: []byte{0xb0, 0xb1}, AddedSlot: 30, CreatedSlot: 30,
			Reward: types.Uint64(11), Active: true,
		},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	utxos := []models.Utxo{
		{
			TxId: bytesForRebuildTest(0x41), StakingKey: delegated.Key,
			CredentialTag: delegated.Tag, AddedSlot: 11, Amount: types.Uint64(50),
		},
		{
			TxId: bytesForRebuildTest(0x42), StakingKey: delegated.Key,
			CredentialTag: delegated.Tag, AddedSlot: 12, Amount: types.Uint64(75),
		},
		{
			TxId: bytesForRebuildTest(0x43), StakingKey: scriptDelegated.Key,
			CredentialTag: scriptDelegated.Tag, AddedSlot: 31, Amount: types.Uint64(9),
		},
	}
	require.NoError(t, store.ImportUtxos(utxos, nil))
}

func readRewardLiveStakeSnapshot(
	t testing.TB,
	store *Store,
) map[string]rewardLiveStakeSnapshotRow {
	t.Helper()
	rows, err := store.writeDB.Query(`
SELECT credential_tag, staking_key, pool_key_hash,
       utxo_stake, reward_stake, total_stake, registered,
       pool_delegation_slot, pool_delegation_block_index,
       pool_delegation_cert_index, updated_slot, calculation_version
FROM reward_live_stake ORDER BY credential_tag, staking_key`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	ret := make(map[string]rewardLiveStakeSnapshotRow)
	for rows.Next() {
		var row rewardLiveStakeSnapshotRow
		var tag int64
		var key, pool []byte
		require.NoError(t, rows.Scan(
			&tag, &key, &pool, &row.utxoStake, &row.rewardStake,
			&row.totalStake, &row.registered, &row.delegationSlot,
			&row.delegationBlock, &row.delegationCert, &row.updatedSlot,
			&row.calculationVersion,
		))
		row.tag = fmt.Sprintf("%d", tag)
		row.key = string(key)
		row.pool = string(pool)
		ret[row.tag+":"+row.key] = row
	}
	require.NoError(t, rows.Err())
	return ret
}

// TestRebuildRewardLiveStakeFromRunningTotalsMatchesAuthoritativeRebuild
// compares every aggregate column on one fixture, including an account with
// no UTxO and key/script credentials delegated to pools.
func TestRebuildRewardLiveStakeFromRunningTotalsMatchesAuthoritativeRebuild(
	t *testing.T,
) {
	t.Parallel()
	authoritative := newMigratedSQLiteStore(t)
	runningTotals := newMigratedSQLiteStore(t)
	populateRewardLiveStakeRebuildFixture(t, authoritative)
	populateRewardLiveStakeRebuildFixture(t, runningTotals)
	_, err := runningTotals.writeDB.Exec(`
INSERT INTO reward_live_stake
    (credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
     registered, updated_slot, calculation_version)
VALUES (0, ?, '999', '999', '1998', false, 1, 0)`, []byte{0xee, 0xef})
	require.NoError(t, err)
	require.NoError(t, authoritative.RebuildRewardLiveStake(100, nil))
	require.NoError(t, runningTotals.RebuildRewardLiveStakeFromRunningTotals(100, nil))
	require.Equal(
		t,
		readRewardLiveStakeSnapshot(t, authoritative),
		readRewardLiveStakeSnapshot(t, runningTotals),
	)
}

// TestRebuildRewardLiveStakeFromRunningTotalsUsesImportedTotals proves the
// API-backfill finalization path does not read the live UTxO table again. The
// deliberately malformed amount would make the authoritative scan fail, but
// the running total established by the importer remains usable.
func TestRebuildRewardLiveStakeFromRunningTotalsUsesImportedTotals(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, []byte{0x01, 0x02})
	reward := types.Uint64(100)
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     10,
		CreatedSlot:   10,
		Reward:        reward,
		Active:        true,
	}, nil))
	accountOnly := models.NewStakeCredentialRef(0, []byte{0x03, 0x04})
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    accountOnly.Key,
		CredentialTag: accountOnly.Tag,
		AddedSlot:     12,
		CreatedSlot:   12,
		Reward:        types.Uint64(7),
		Active:        true,
	}, nil))
	utxo := models.Utxo{
		TxId:          bytesForRebuildTest(0x11),
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     11,
		Amount:        types.Uint64(50),
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{utxo}, nil))

	var before string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`, ref.Tag, ref.Key).
		Scan(&before))
	require.Equal(t, "50", before)

	_, err := store.writeDB.Exec(
		`UPDATE utxo SET amount = 'not-a-lovelace' WHERE tx_id = ?`,
		utxo.TxId,
	)
	require.NoError(t, err)

	require.NoError(t, store.RebuildRewardLiveStakeFromRunningTotals(100, nil))
	var gotUtxo, gotReward, gotTotal string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake, reward_stake, total_stake
FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`, ref.Tag, ref.Key).
		Scan(&gotUtxo, &gotReward, &gotTotal))
	require.Equal(t, "50", gotUtxo)
	require.Equal(t, "100", gotReward)
	require.Equal(t, "150", gotTotal)
	var accountOnlyTotal string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT total_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		accountOnly.Tag, accountOnly.Key).Scan(&accountOnlyTotal))
	require.Equal(t, "7", accountOnlyTotal)
}

func TestRebuildRewardLiveStakeFromRunningTotalsRejectsMissingUtxoTotal(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, []byte{0x51, 0x52})
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:          bytesForRebuildTest(0x53),
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     11,
		Amount:        types.Uint64(50),
	}}, nil))
	_, err := store.writeDB.Exec(
		`DELETE FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag,
		ref.Key,
	)
	require.NoError(t, err)
	err = store.RebuildRewardLiveStakeFromRunningTotals(100, nil)
	require.ErrorContains(t, err, "missing reward live stake running total")
}

func bytesForRebuildTest(seed byte) []byte {
	ret := make([]byte, 32)
	ret[0] = seed
	return ret
}

// BenchmarkRebuildRewardLiveStakeFinalizers compares the two finalization
// paths on a 20,000-live-UTxO fixture. Reproduce with:
//
//	go test ./database/plugin/metadata/sqlstore -run '^$' \
//	  -bench BenchmarkRebuildRewardLiveStakeFinalizers -benchtime=1x -count=1
func BenchmarkRebuildRewardLiveStakeFinalizers(b *testing.B) {
	for _, path := range []struct {
		name string
		fast bool
	}{
		{name: "authoritative"},
		{name: "running_totals", fast: true},
	} {
		b.Run(path.name, func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			ref := models.NewStakeCredentialRef(0, []byte{1, 2, 3})
			amounts := make([]uint64, 20_000)
			for i := range amounts {
				amounts[i] = uint64(i + 1)
			}
			seedCredentialUtxos(b, store, 99, ref, amounts, nil)
			require.NoError(b, store.ImportAccount(&models.Account{
				StakingKey: ref.Key, CredentialTag: ref.Tag,
				AddedSlot: 1, CreatedSlot: 1, Reward: types.Uint64(100), Active: true,
			}, nil))
			require.NoError(b, store.RebuildRewardLiveStake(100, nil))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if path.fast {
					err = store.RebuildRewardLiveStakeFromRunningTotals(100, nil)
				} else {
					err = store.RebuildRewardLiveStake(100, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
