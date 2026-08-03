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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newReconcileTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "", // in-memory
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

func b28(fill byte) []byte { return bytes.Repeat([]byte{fill}, 28) }
func b32(fill byte) []byte { return bytes.Repeat([]byte{fill}, 32) }

// TestReconcileStaleLedgerState verifies that, given the live key set of a newer
// snapshot, the reconcile marks every live database row absent from that set
// inactive (never deletes): UTxOs get a DeletedSlot tombstone, accounts and
// DReps go Active=false, and pools get a retirement. Rows present in the
// snapshot are untouched.
func TestReconcileStaleLedgerState(t *testing.T) {
	db := newReconcileTestDB(t)
	store := db.Metadata()

	const tipSlot = uint64(5000)
	const tipEpoch = uint64(42)

	keepTxId, staleTxId := b32(0x11), b32(0x22)
	keepStake, staleStake := b28(0xaa), b28(0xbb)
	keepPool, stalePool := b28(0xcc), b28(0xdd)
	keepDrep, staleDrep := b28(0xee), b28(0xff)

	// Seed two live UTxOs, two active accounts, two pools, two DReps.
	txn := db.MetadataTxn(true)
	require.NoError(t, store.CreateUtxo(txn.Metadata(), &models.Utxo{
		TxId: keepTxId, OutputIdx: 0, AddedSlot: 100, DeletedSlot: 0,
	}))
	require.NoError(t, store.CreateUtxo(txn.Metadata(), &models.Utxo{
		TxId: staleTxId, OutputIdx: 0, AddedSlot: 100, DeletedSlot: 0,
	}))
	require.NoError(t, store.CreateAccount(txn.Metadata(), &models.Account{
		StakingKey: keepStake, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.CreateAccount(txn.Metadata(), &models.Account{
		StakingKey: staleStake, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.CreateDrep(txn.Metadata(), &models.Drep{
		Credential: keepDrep, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.CreateDrep(txn.Metadata(), &models.Drep{
		Credential: staleDrep, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	for _, pkh := range [][]byte{keepPool, stalePool} {
		require.NoError(t, store.ImportPool(
			&models.Pool{PoolKeyHash: pkh, VrfKeyHash: b32(0x01)},
			&models.PoolRegistration{PoolKeyHash: pkh, AddedSlot: 100},
			txn.Metadata(),
		))
	}
	require.NoError(t, txn.Commit())

	// The pool reconcile lists active pools via GetActivePoolKeyHashes, which
	// reads the chain tip and the epoch at that slot — exactly as the real
	// flow does after importTip. Seed both so the pool path is exercised.
	require.NoError(t, db.SetEpoch(0, tipEpoch, nil, nil, nil, nil, 6, 1, 10000, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 4000, Hash: b32(0x01)},
	}, nil))

	// The newer snapshot keeps only the "keep" rows.
	keys := newReconcileKeys()
	require.NoError(t, keys.addUtxo(keepTxId, 0))
	keys.addAccount(0, keepStake)
	keys.addPool(keepPool)
	keys.addDrep(0, keepDrep)
	keys.markUtxosComplete()
	keys.markCertStateComplete()

	require.NoError(t, reconcileStaleLedgerState(
		context.Background(), db, keys, tipSlot, tipEpoch,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	read := db.MetadataTxn(false)
	defer read.Release()
	rstore := db.Metadata()

	// UTxO: keep is still live; stale is tombstoned at tipSlot.
	keepU, err := rstore.GetUtxo(keepTxId, 0, read.Metadata())
	require.NoError(t, err)
	require.NotNil(t, keepU)
	staleU, err := rstore.GetUtxo(staleTxId, 0, read.Metadata())
	require.NoError(t, err)
	require.Nil(t, staleU, "stale UTxO should no longer be live")
	staleUSpent, err := rstore.GetUtxoIncludingSpent(
		staleTxId, 0, read.Metadata(),
	)
	require.NoError(t, err)
	require.NotNil(t, staleUSpent, "stale UTxO should be tombstoned, not deleted")
	require.EqualValues(
		t, tipSlot, staleUSpent.DeletedSlot,
		"stale UTxO should be tombstoned at the reconcile tip slot",
	)

	// Account: keep active, stale inactive.
	keepA, err := rstore.GetAccountByCredential(0, keepStake, true, read.Metadata())
	require.NoError(t, err)
	require.True(t, keepA.Active)
	staleA, err := rstore.GetAccountByCredential(0, staleStake, true, read.Metadata())
	require.NoError(t, err)
	require.False(t, staleA.Active, "stale account should be inactive")

	// DRep: keep active, stale inactive.
	keepD, err := rstore.GetDrepByCredential(0, keepDrep, true, read.Metadata())
	require.NoError(t, err)
	require.True(t, keepD.Active)
	staleD, err := rstore.GetDrepByCredential(0, staleDrep, true, read.Metadata())
	require.NoError(t, err)
	require.False(t, staleD.Active, "stale DRep should be inactive")

	// Pool: stale gets a retirement row; keep does not.
	staleP, err := rstore.GetPool(lcommon.PoolKeyHash(stalePool), true, read.Metadata())
	require.NoError(t, err)
	require.NotEmpty(t, staleP.Retirement, "stale pool should be retired")
	keepP, err := rstore.GetPool(lcommon.PoolKeyHash(keepPool), true, read.Metadata())
	require.NoError(t, err)
	require.Empty(t, keepP.Retirement, "kept pool should not be retired")
}

// TestReconcileSkipsEntitiesWithIncompleteRecording pins the recording-complete
// guard: an entity whose snapshot key recording did not complete is skipped
// rather than treated as authoritative, so a parse gap or bypassed import path
// cannot mass-deactivate live state. Completed entities still reconcile in the
// same pass.
func TestReconcileSkipsEntitiesWithIncompleteRecording(t *testing.T) {
	db := newReconcileTestDB(t)
	store := db.Metadata()

	const tipSlot = uint64(5000)
	const tipEpoch = uint64(42)

	liveTxId := b32(0x11)
	liveStake, staleStake := b28(0xaa), b28(0xbb)
	livePool := b28(0xcc)
	liveDrep := b28(0xee)

	txn := db.MetadataTxn(true)
	require.NoError(t, store.CreateUtxo(txn.Metadata(), &models.Utxo{
		TxId: liveTxId, OutputIdx: 0, AddedSlot: 100, DeletedSlot: 0,
	}))
	require.NoError(t, store.CreateAccount(txn.Metadata(), &models.Account{
		StakingKey: liveStake, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.CreateAccount(txn.Metadata(), &models.Account{
		StakingKey: staleStake, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.CreateDrep(txn.Metadata(), &models.Drep{
		Credential: liveDrep, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: livePool, VrfKeyHash: b32(0x01)},
		&models.PoolRegistration{PoolKeyHash: livePool, AddedSlot: 100},
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())

	require.NoError(t, db.SetEpoch(0, tipEpoch, nil, nil, nil, nil, 6, 1, 10000, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 4000, Hash: b32(0x01)},
	}, nil))

	// Only account recording completed (keeping liveStake only). The UTxO,
	// pool, and DRep reconciles must be skipped; the account reconcile must
	// still run and deactivate the stale account.
	keys := newReconcileKeys()
	keys.addAccount(0, liveStake)
	keys.accountsComplete = true

	require.NoError(t, reconcileStaleLedgerState(
		context.Background(), db, keys, tipSlot, tipEpoch,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	read := db.MetadataTxn(false)
	defer read.Release()

	u, err := store.GetUtxo(liveTxId, 0, read.Metadata())
	require.NoError(t, err)
	require.NotNil(t, u, "UTxO must stay live when the UTxO key set is empty")

	keepA, err := store.GetAccountByCredential(0, liveStake, true, read.Metadata())
	require.NoError(t, err)
	require.True(t, keepA.Active)
	staleA, err := store.GetAccountByCredential(0, staleStake, true, read.Metadata())
	require.NoError(t, err)
	require.False(t, staleA.Active,
		"a non-empty account key set must still reconcile")

	d, err := store.GetDrepByCredential(0, liveDrep, true, read.Metadata())
	require.NoError(t, err)
	require.True(t, d.Active,
		"DRep must stay active when the DRep key set is empty")

	p, err := store.GetPool(lcommon.PoolKeyHash(livePool), true, read.Metadata())
	require.NoError(t, err)
	require.Empty(t, p.Retirement,
		"pool must not be retired when pool recording is incomplete")
}

// TestReconcileCompletedEmptyKeySetsAreAuthoritative verifies that a complete
// import that records zero live keys for an entity still reconciles that entity.
// This is the small/private-chain case where all rows of that type were spent,
// deregistered, or retired by the snapshot tip.
func TestReconcileCompletedEmptyKeySetsAreAuthoritative(t *testing.T) {
	db := newReconcileTestDB(t)
	store := db.Metadata()

	const tipSlot = uint64(5000)
	const tipEpoch = uint64(42)

	liveTxId := b32(0x11)
	liveStake := b28(0xaa)
	livePool := b28(0xcc)
	liveDrep := b28(0xee)

	txn := db.MetadataTxn(true)
	require.NoError(t, store.CreateUtxo(txn.Metadata(), &models.Utxo{
		TxId: liveTxId, OutputIdx: 0, AddedSlot: 100, DeletedSlot: 0,
	}))
	require.NoError(t, store.CreateAccount(txn.Metadata(), &models.Account{
		StakingKey: liveStake, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.CreateDrep(txn.Metadata(), &models.Drep{
		Credential: liveDrep, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: livePool, VrfKeyHash: b32(0x01)},
		&models.PoolRegistration{PoolKeyHash: livePool, AddedSlot: 100},
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())

	require.NoError(t, db.SetEpoch(0, tipEpoch, nil, nil, nil, nil, 6, 1, 10000, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 4000, Hash: b32(0x01)},
	}, nil))

	keys := newReconcileKeys()
	keys.markUtxosComplete()
	keys.markCertStateComplete()

	require.NoError(t, reconcileStaleLedgerState(
		context.Background(), db, keys, tipSlot, tipEpoch,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	read := db.MetadataTxn(false)
	defer read.Release()

	u, err := store.GetUtxo(liveTxId, 0, read.Metadata())
	require.NoError(t, err)
	require.Nil(t, u, "completed empty UTxO set must tombstone stale UTxOs")

	acct, err := store.GetAccountByCredential(0, liveStake, true, read.Metadata())
	require.NoError(t, err)
	require.False(t, acct.Active,
		"completed empty account set must deactivate stale accounts")

	drep, err := store.GetDrepByCredential(0, liveDrep, true, read.Metadata())
	require.NoError(t, err)
	require.False(t, drep.Active,
		"completed empty DRep set must deactivate stale DReps")

	pool, err := store.GetPool(lcommon.PoolKeyHash(livePool), true, read.Metadata())
	require.NoError(t, err)
	require.NotEmpty(t, pool.Retirement,
		"completed empty pool set must retire stale pools")
}

// TestReconcileStaleLedgerStateEmptyIsNoop ensures reconcile against an empty
// database (the fresh-bootstrap case) does nothing and does not error.
func TestReconcileStaleLedgerStateEmptyIsNoop(t *testing.T) {
	db := newReconcileTestDB(t)
	keys := newReconcileKeys()
	require.NoError(t, reconcileStaleLedgerState(
		context.Background(), db, keys, 1, 1,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
}

func TestReconcileKeysRejectsMalformedUtxoKey(t *testing.T) {
	keys := newReconcileKeys()
	err := keys.addUtxo(bytes.Repeat([]byte{0x11}, 31), 0)
	require.ErrorContains(t, err, "malformed UTxO reconcile key")
	require.Empty(t, keys.utxos)
}

func TestReconcileStaleLedgerStateCanceledBeforeMarkersDoesNotMutate(
	t *testing.T,
) {
	db := newReconcileTestDB(t)
	store := db.Metadata()
	staleStake := b28(0xbb)

	txn := db.MetadataTxn(true)
	require.NoError(t, store.CreateAccount(txn.Metadata(), &models.Account{
		StakingKey: staleStake, CredentialTag: 0, AddedSlot: 100, Active: true,
	}))
	require.NoError(t, txn.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := reconcileStaleLedgerState(
		ctx, db, newReconcileKeys(), 5000, 42,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.ErrorIs(t, err, context.Canceled)

	read := db.MetadataTxn(false)
	defer read.Release()
	acct, err := store.GetAccountByCredential(
		0, staleStake, true, read.Metadata(),
	)
	require.NoError(t, err)
	require.True(t, acct.Active)
}

func TestValidateReconcileImportConfigRequiresCompleteInputs(t *testing.T) {
	base := ImportConfig{
		State: &RawLedgerState{
			Tip:           &SnapshotTip{Slot: 1, BlockHash: b32(0x01)},
			UTxOData:      []byte{0xa0},
			CertStateData: []byte{0x83, 0xa0, 0xa0, 0xa0},
		},
	}
	require.NoError(t, validateReconcileImportConfig(base))

	withImportKey := base
	withImportKey.ImportKey = "digest:1"
	require.ErrorContains(
		t,
		validateReconcileImportConfig(withImportKey),
		"ImportKey",
	)

	missingUTxO := base
	missingUTxO.State = &RawLedgerState{
		Tip:           base.State.Tip,
		CertStateData: base.State.CertStateData,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingUTxO),
		"UTxO",
	)

	missingCertState := base
	missingCertState.State = &RawLedgerState{
		Tip:      base.State.Tip,
		UTxOData: base.State.UTxOData,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingCertState),
		"cert-state",
	)

	missingGovState := base
	missingGovState.State = &RawLedgerState{
		Tip:           base.State.Tip,
		UTxOData:      base.State.UTxOData,
		CertStateData: base.State.CertStateData,
		EraIndex:      EraConway,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingGovState),
		"Conway governance state",
	)

	withGovState := missingGovState
	withGovState.State.GovStateData = []byte{0x87}
	require.NoError(t, validateReconcileImportConfig(withGovState))

	missingUTxOHDTable := base
	missingUTxOHDTable.State = &RawLedgerState{
		Tip:           base.State.Tip,
		UTxOHD:        true,
		UTxOData:      base.State.UTxOData,
		CertStateData: base.State.CertStateData,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingUTxOHDTable),
		"UTxO-HD",
	)
}
