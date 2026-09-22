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

	matched := snapshot["0:"+string(matching.Key)]
	require.Equal(t, string(poolA), matched.pool)
	require.Equal(t, int64(12), matched.delegationSlot)
	require.Equal(t, int64(3), matched.delegationBlock)
	require.Equal(t, int64(4), matched.delegationCert)

	// No latest_delegation match, so the account's own added_slot stands and
	// the older poolA assignment at slot 22 is not promoted.
	stale := snapshot["0:"+string(superseded.Key)]
	require.Equal(t, string(poolA), stale.pool)
	require.Equal(t, int64(20), stale.delegationSlot)
	require.Equal(t, int64(0), stale.delegationBlock)
	require.Equal(t, int64(0), stale.delegationCert)

	require.Contains(t, snapshot, "0:"+string(inactive.Key))
	unregistered := snapshot["0:"+string(inactive.Key)]
	require.False(t, unregistered.registered)
	require.Empty(t, unregistered.pool)
	require.Equal(t, int64(0), unregistered.delegationSlot)

	undelegated := snapshot["0:"+string(noPool.Key)]
	require.True(t, undelegated.registered)
	require.Empty(t, undelegated.pool)
	require.Equal(t, int64(0), undelegated.delegationSlot)
}

// TestRebuildRewardLiveStakeFromRunningTotalsLatestDelegationSelection runs
// the same shapes through the Mithril finalization path, which reaches the
// same ranked query with the running-total join attached.
func TestRebuildRewardLiveStakeFromRunningTotalsLatestDelegationSelection(
	t *testing.T,
) {
	t.Parallel()
	authoritative := newMigratedSQLiteStore(t)
	runningTotals := newMigratedSQLiteStore(t)
	for _, store := range []*Store{authoritative, runningTotals} {
		populateRewardLiveStakeRebuildFixture(t, store)
		insertLatestDelegationRow(
			t, store, 0, []byte{0x10, 0x11}, []byte{0xa0, 0xa1}, 12, 3, 4,
		)
		insertLatestDelegationRow(
			t, store, 0, []byte{0x10, 0x11}, []byte{0xc0, 0xc1}, 13, 4, 5,
		)
		insertLatestDelegationRow(
			t, store, 1, []byte{0x30, 0x31}, []byte{0xb0, 0xb1}, 32, 6, 7,
		)
	}
	require.NoError(t, authoritative.RebuildRewardLiveStake(100, nil))
	require.NoError(
		t,
		runningTotals.RebuildRewardLiveStakeFromRunningTotals(100, nil),
	)
	require.Equal(
		t,
		readRewardLiveStakeSnapshot(t, authoritative),
		readRewardLiveStakeSnapshot(t, runningTotals),
	)
	snapshot := readRewardLiveStakeSnapshot(t, runningTotals)
	require.Equal(t, int64(32), snapshot["1:"+string([]byte{0x30, 0x31})].delegationSlot)
	require.Equal(t, int64(10), snapshot["0:"+string([]byte{0x10, 0x11})].delegationSlot)
}
