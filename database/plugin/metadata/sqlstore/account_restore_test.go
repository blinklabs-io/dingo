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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// snapshotStakingKey returns a distinct 28-byte stake credential.
func snapshotStakingKey(marker byte) []byte {
	key := make([]byte, 28)
	key[0] = marker
	return key
}

// importSnapshotAccount writes the row shape a Mithril snapshot import
// produces: added_slot at the snapshot slot, created_slot 0, active, and no
// certificate history anywhere.
func importSnapshotAccount(
	t *testing.T,
	store *Store,
	key []byte,
	pool []byte,
	drep []byte,
	drepType uint64,
	snapshotSlot uint64,
) {
	t.Helper()
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		Pool:          pool,
		Drep:          drep,
		DrepType:      drepType,
		AddedSlot:     snapshotSlot,
		CreatedSlot:   0,
		Active:        true,
	}, nil))
}

func execAccountSQL(t *testing.T, store *Store, query string, args ...any) {
	t.Helper()
	_, err := store.writeDB.ExecContext(context.Background(), query, args...)
	require.NoError(t, err)
}

// A deregistration applied after a snapshot import and then rolled back must
// not survive: the account is active at the rollback slot, and leaving it
// inactive makes every later delegation certificate for the credential fail
// Conway rule 45 forever.
func TestSnapshotAccountRollbackLosesActive(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x6a)
	importSnapshotAccount(t, store, key, nil, nil, 0, 100)

	// A deregistration certificate at slot 200, as block application writes it.
	execAccountSQL(t, store, `
INSERT INTO deregistration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 200)`, key)
	execAccountSQL(t, store, `
UPDATE account SET active = 0, added_slot = 200
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Active)
}

// A pool delegation applied after a snapshot import and then rolled back must
// not survive; the stake distribution and leader schedule read this column.
func TestSnapshotAccountRollbackKeepsStaleDelegation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x6b)
	poolA := []byte{0xaa, 0xaa}
	poolB := []byte{0xbb, 0xbb}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)

	execAccountSQL(t, store, `
INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, 200)`, key, poolB)
	execAccountSQL(t, store, `
UPDATE account SET pool = ?, added_slot = 200
WHERE credential_tag = 0 AND staking_key = ?`, poolB, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolA, got.Pool)
	require.True(t, got.Active)
}

// The same for a vote delegation, which feeds the DRep tally.
func TestSnapshotAccountRollbackKeepsStaleVoteDelegation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x6c)
	drepA := []byte{0xa1, 0xa1}
	importSnapshotAccount(t, store, key, nil, drepA, 1, 100)

	// Vote delegation to AlwaysAbstain (drep_type 2, no credential bytes).
	execAccountSQL(t, store, `
INSERT INTO vote_delegation (staking_key, credential_tag, drep, drep_type, added_slot)
VALUES (?, 0, NULL, 2, 200)`, key)
	execAccountSQL(t, store, `
UPDATE account SET drep = NULL, drep_type = 2, added_slot = 200
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, drepA, got.Drep)
	require.Equal(t, uint64(models.DrepTypeScriptHash), got.DrepType)
}

// A delegation certificate that survives the rollback still wins over the
// imported baseline.
func TestSnapshotAccountRollbackKeepsSurvivingDelegation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x6d)
	poolA := []byte{0xaa, 0xaa}
	poolB := []byte{0xbb, 0xbb}
	poolC := []byte{0xcc, 0xcc}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)

	execAccountSQL(t, store, `
INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, 200)`, key, poolB)
	execAccountSQL(t, store, `
INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, 300)`, key, poolC)
	execAccountSQL(t, store, `
UPDATE account SET pool = ?, added_slot = 300
WHERE credential_tag = 0 AND staking_key = ?`, poolC, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(250, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolB, got.Pool)
	require.Equal(t, uint64(200), got.AddedSlot)
	require.True(t, got.Active)
}

// A deregistration that is still in effect at the rollback slot must not be
// undone by the baseline: restoring "active" here would make a replayed
// re-registration certificate fail Conway rule 44 instead.
func TestSnapshotAccountRollbackHonorsSurvivingDeregistration(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x6e)
	poolA := []byte{0xaa, 0xaa}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)

	execAccountSQL(t, store, `
INSERT INTO deregistration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 200)`, key)
	execAccountSQL(t, store, `
INSERT INTO registration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 300)`, key)
	execAccountSQL(t, store, `
UPDATE account SET active = 1, pool = NULL, added_slot = 300
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(250, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Active)
	require.Empty(t, got.Pool)
}

// The existing delete path must not regress: an account whose row was created
// after the rollback slot has no state at that slot and is removed.
func TestAccountRollbackDeletesAccountCreatedAfterSlot(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x6f)
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		AddedSlot:     200,
		CreatedSlot:   200,
		Active:        true,
	}, nil))

	require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

// A database bootstrapped before the baseline table existed, whose backfill an
// operator lost, still must not stay wedged: with no registration and no
// surviving deregistration certificate the account was registered at the
// rollback slot, and pool/DRep are left alone because nothing can derive them.
func TestSnapshotAccountRollbackWithoutBaselineDerivesActive(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x70)
	poolA := []byte{0xaa, 0xaa}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)
	execAccountSQL(t, store, `DELETE FROM account_import_baseline`)

	execAccountSQL(t, store, `
INSERT INTO deregistration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 200)`, key)
	execAccountSQL(t, store, `
UPDATE account SET active = 0, pool = NULL, added_slot = 200
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Active)
}

// The same shape with the deregistration still in effect at the rollback slot
// stays deregistered, and its delegation is cleared.
func TestSnapshotAccountRollbackWithoutBaselineKeepsDeregistration(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x71)
	poolA := []byte{0xaa, 0xaa}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)
	execAccountSQL(t, store, `DELETE FROM account_import_baseline`)

	execAccountSQL(t, store, `
INSERT INTO deregistration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 200)`, key)
	execAccountSQL(t, store, `
INSERT INTO registration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 300)`, key)
	execAccountSQL(t, store, `
UPDATE account SET active = 1, added_slot = 300
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(250, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Active)
	require.Empty(t, got.Pool)
}

// Re-importing a credential replaces its baseline, so a second bootstrap does
// not restore to the older snapshot's delegation.
func TestImportAccountReplacesBaseline(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x72)
	poolA := []byte{0xaa, 0xaa}
	poolB := []byte{0xbb, 0xbb}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)
	importSnapshotAccount(t, store, key, poolB, nil, 0, 400)

	execAccountSQL(t, store, `
UPDATE account SET pool = NULL, added_slot = 500
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(450, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolB, got.Pool)
	require.Equal(t, uint64(400), got.AddedSlot)
}

// A rollback to before the snapshot slot must not leave added_slot ahead of
// the rollback target, which would make the row invisible to the next
// rollback's added_slot > slot scan while still holding future state.
func TestSnapshotAccountRollbackClampsAddedSlotBelowBaseline(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x73)
	poolA := []byte{0xaa, 0xaa}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)

	require.NoError(t, store.RestoreAccountStateAtSlot(50, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(50), got.AddedSlot)
	require.Equal(t, poolA, got.Pool)
	require.True(t, got.Active)
}

// Mithril reconciliation deactivating a credential absent from a newer
// snapshot is a statement about the baseline, so a later rollback must not
// restore the account it tombstoned.
func TestDeactivateAccountsClearsBaseline(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x83)
	poolA := []byte{0xaa, 0xaa}
	importSnapshotAccount(t, store, key, poolA, nil, 0, 100)

	require.NoError(t, store.DeactivateAccounts(nil, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, key),
	}))
	execAccountSQL(t, store, `
UPDATE account SET added_slot = 200
WHERE credential_tag = 0 AND staking_key = ?`, key)

	require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Active)
	require.Empty(t, got.Pool)
}

// A failing baseline write must not leave the account row behind. The pair is
// what RestoreAccountStateAtSlot reads, and an account row committed without
// its baseline keeps deriving the pre-fix state for that credential, because
// nothing rewrites the baseline unless the account is imported again.
func TestImportAccountBaselineFailureRollsBackAccountRow(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x81)
	// Removing the baseline table is the second write failing on a handle the
	// account upsert already succeeded on.
	execAccountSQL(t, store, `DROP TABLE account_import_baseline`)

	require.Error(t, store.ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		AddedSlot:     100,
		CreatedSlot:   0,
		Active:        true,
	}, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

// The tombstone and the baseline it contradicts must fail or land together: an
// account left inactive while its baseline stays active is exactly the state a
// later rollback reads to restore the account reconciliation just removed.
func TestDeactivateAccountsBaselineFailureKeepsAccountActive(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := snapshotStakingKey(0x82)
	importSnapshotAccount(t, store, key, nil, nil, 0, 100)
	execAccountSQL(t, store, `DROP TABLE account_import_baseline`)

	require.Error(t, store.DeactivateAccounts(nil, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, key),
	}))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Active)
}

// A baseline write on the autocommit handle can only be a split pair, so the
// helper refuses it instead of committing one half.
func TestWriteAccountImportBaselineRequiresTransaction(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	require.Error(t, writeAccountImportBaseline(
		t.Context(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		&models.Account{
			StakingKey:    snapshotStakingKey(0x74),
			CredentialTag: 0,
			AddedSlot:     100,
			Active:        true,
		},
	))
}
