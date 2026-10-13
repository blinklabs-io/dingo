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
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var migratedStoreSequence atomic.Uint64

// newMigratedTestStore opens an in-memory SQLite store carrying the checked-in
// schema, so a test exercises the real column types rather than a hand-written
// approximation of them.
func newMigratedTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_mir_%d?mode=memory&cache=shared",
			migratedStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func mirTestCredential(seed byte) *lcommon.Credential {
	var hash lcommon.Blake2b224
	for i := range hash {
		hash[i] = seed
	}
	return &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
}

// TestGetAccountSumsByCredentialSumsSignedMIRDeltas proves the reserves and
// treasury aggregate reads sum signed values. Summing them through the coin
// helper would reject the negative row outright; ignoring the sign would report
// a total larger than the account ever received.
func TestGetAccountSumsByCredentialSumsSignedMIRDeltas(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	credential := mirTestCredential(0x23).Credential[:]
	seedMIRRewardRow(t, store, 0, credential, "1000")
	seedMIRRewardRow(t, store, 0, credential, "-250")
	seedMIRRewardRow(t, store, 1, credential, "700")
	seedMIRRewardRow(t, store, 1, credential, "-900")

	sums, err := store.GetAccountSumsByCredential(0, credential, nil)
	require.NoError(t, err)
	require.NotNil(t, sums.ReservesSum)
	require.NotNil(t, sums.TreasurySum)
	assert.Equal(t, "750", sums.ReservesSum.String())
	assert.Equal(t, "-200", sums.TreasurySum.String())
}

// TestGetAccountSumsByCredentialWithoutMIRHistory pins the zero value the
// aggregate reads return when there is nothing to sum, so the signed totals are
// never handed to a caller as nil. The empty-credential case never reaches a
// query, so it is the one that depends on the returned value being initialized.
func TestGetAccountSumsByCredentialWithoutMIRHistory(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	for _, test := range []struct {
		name       string
		credential []byte
	}{
		{
			name:       "known credential with no MIR rows",
			credential: mirTestCredential(0x24).Credential[:],
		},
		{
			name:       "empty credential short-circuits the query",
			credential: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sums, err := store.GetAccountSumsByCredential(
				0,
				test.credential,
				nil,
			)
			require.NoError(t, err)
			require.NotNil(t, sums.ReservesSum)
			require.NotNil(t, sums.TreasurySum)
			assert.Equal(t, "0", sums.ReservesSum.String())
			assert.Equal(t, "0", sums.TreasurySum.String())
		})
	}
}

// insertLatestDelegationRow writes one stake_delegation row together with the
// certs and transaction rows the finalizer's ranking reads block_index and
// cert_index from.
func insertLatestDelegationRow(
	t testing.TB,
	store *Store,
	tag uint8,
	key, pool []byte,
	addedSlot, blockIndex, certIndex int64,
) {
	t.Helper()
	txResult, err := store.writeDB.Exec(
		"INSERT INTO \"transaction\" (slot, block_index) VALUES (?, ?)",
		addedSlot, blockIndex,
	)
	require.NoError(t, err)
	txID, err := txResult.LastInsertId()
	require.NoError(t, err)
	certResult, err := store.writeDB.Exec(
		"INSERT INTO certs (transaction_id, slot, cert_index) VALUES (?, ?, ?)",
		txID, addedSlot, certIndex,
	)
	require.NoError(t, err)
	certID, err := certResult.LastInsertId()
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, ?, ?, ?, ?)`, key, tag, pool, certID, addedSlot)
	require.NoError(t, err)
}

// snapshotRow returns one key's aggregate row, failing when the rebuild
// wrote none: every assertion below would otherwise hold for the zero value.
func snapshotRow(
	t testing.TB,
	snapshot map[string]rewardLiveStakeSnapshotRow,
	tag uint8,
	key []byte,
) rewardLiveStakeSnapshotRow {
	t.Helper()
	row, ok := snapshot[fmt.Sprintf("%d:%s", tag, key)]
	require.True(t, ok, "no reward live stake row for %d:%x", tag, key)
	return row
}

// TestRebuildRewardLiveStakeLatestDelegationSelection pins the delegation
// columns for the credential shapes the finalizer's latest-assignment
// pruning has to leave alone. The pruning keeps every credential whose
// account row is active with a non-NULL pool and ranks that credential's
// whole history, so a credential whose newest assignment is to some other
// pool must still fall back to the account's own added_slot rather than
// silently promoting the older matching assignment.
func TestRebuildRewardLiveStakeLatestDelegationSelection(t *testing.T) {
	t.Parallel()
	poolA := []byte{0xa0, 0xa1}
	poolB := []byte{0xb0, 0xb1}

	matching := models.NewStakeCredentialRef(0, []byte{0x01})
	superseded := models.NewStakeCredentialRef(0, []byte{0x02})
	inactive := models.NewStakeCredentialRef(0, []byte{0x03})
	noPool := models.NewStakeCredentialRef(0, []byte{0x04})

	store := newMigratedSQLiteStore(t)
	for _, account := range []*models.Account{
		{
			StakingKey: matching.Key, CredentialTag: matching.Tag,
			Pool: poolA, AddedSlot: 10, CreatedSlot: 10, Active: true,
		},
		{
			StakingKey: superseded.Key, CredentialTag: superseded.Tag,
			Pool: poolA, AddedSlot: 20, CreatedSlot: 20, Active: true,
		},
		{
			StakingKey: inactive.Key, CredentialTag: inactive.Tag,
			Pool: poolA, AddedSlot: 30, CreatedSlot: 30, Active: false,
		},
		{
			StakingKey: noPool.Key, CredentialTag: noPool.Tag,
			AddedSlot: 40, CreatedSlot: 40, Active: true,
		},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{
		{
			TxId: bytesForRebuildTest(0x51), StakingKey: matching.Key,
			CredentialTag: matching.Tag, AddedSlot: 11, Amount: types.Uint64(5),
		},
		{
			TxId: bytesForRebuildTest(0x52), StakingKey: superseded.Key,
			CredentialTag: superseded.Tag, AddedSlot: 21, Amount: types.Uint64(6),
		},
		{
			TxId: bytesForRebuildTest(0x53), StakingKey: inactive.Key,
			CredentialTag: inactive.Tag, AddedSlot: 31, Amount: types.Uint64(7),
		},
		{
			TxId: bytesForRebuildTest(0x54), StakingKey: noPool.Key,
			CredentialTag: noPool.Tag, AddedSlot: 41, Amount: types.Uint64(8),
		},
	}, nil))

	insertLatestDelegationRow(t, store, matching.Tag, matching.Key, poolA, 12, 3, 4)
	// superseded delegated to poolA first and to poolB later; the account
	// still names poolA, so the newest assignment does not match it.
	insertLatestDelegationRow(t, store, superseded.Tag, superseded.Key, poolA, 22, 1, 1)
	insertLatestDelegationRow(t, store, superseded.Tag, superseded.Key, poolB, 23, 2, 2)
	insertLatestDelegationRow(t, store, inactive.Tag, inactive.Key, poolA, 32, 5, 6)
	insertLatestDelegationRow(t, store, noPool.Tag, noPool.Key, poolA, 42, 7, 8)

	require.NoError(t, store.RebuildRewardLiveStake(100, nil))
	snapshot := readRewardLiveStakeSnapshot(t, store)

	matched := snapshotRow(t, snapshot, 0, matching.Key)
	require.Equal(t, string(poolA), matched.pool)
	require.Equal(t, int64(12), matched.delegationSlot)
	require.Equal(t, int64(3), matched.delegationBlock)
	require.Equal(t, int64(4), matched.delegationCert)

	// No latest_delegation match, so the account's own added_slot stands and
	// the older poolA assignment at slot 22 is not promoted.
	stale := snapshotRow(t, snapshot, 0, superseded.Key)
	require.Equal(t, string(poolA), stale.pool)
	require.Equal(t, int64(20), stale.delegationSlot)
	require.Equal(t, int64(0), stale.delegationBlock)
	require.Equal(t, int64(0), stale.delegationCert)

	unregistered := snapshotRow(t, snapshot, 0, inactive.Key)
	require.False(t, unregistered.registered)
	require.Empty(t, unregistered.pool)
	require.Equal(t, int64(0), unregistered.delegationSlot)

	undelegated := snapshotRow(t, snapshot, 0, noPool.Key)
	require.True(t, undelegated.registered)
	require.Empty(t, undelegated.pool)
	require.Equal(t, int64(0), undelegated.delegationSlot)
}

type rewardLiveStakeSnapshotRow struct {
	tag, key, pool                                  string
	utxoStake, rewardStake, totalStake              string
	registered                                      bool
	delegationSlot, delegationBlock, delegationCert int64
	updatedSlot, calculationVersion                 int64
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

func bytesForRebuildTest(seed byte) []byte {
	ret := make([]byte, 32)
	ret[0] = seed
	return ret
}
