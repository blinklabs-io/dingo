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

package database

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func testTip(slot uint64, fill byte) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{fill}, 32),
		},
		BlockNumber: slot / 20,
	}
}

func requireNoMetadataTip(t *testing.T, db *Database) {
	t.Helper()
	require.Nil(t, db.Metadata(), "the ledger's database keeps no tip row")
}

func TestTipIsKeptInTheBlockStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)

	got, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ochainsync.Tip{}, got)

	want := testTip(4242, 0xab)
	require.NoError(t, db.SetTip(want, nil))
	got, err = db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	requireNoMetadataTip(t, db)

	require.NoError(t, closeTestDatabase(db))
	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	got, err = reopened.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// A tip set back to origin is a stored value, not an absent one.
	require.NoError(t, reopened.SetTip(ochainsync.Tip{}, nil))
	got, err = reopened.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ochainsync.Tip{}, got)
	raw, err := reopened.GetChainState(LedgerTipKey, nil)
	require.NoError(t, err)
	require.Equal(t, "0:0:", raw)
}

func TestTipSetInATransactionFollowsItsCommit(t *testing.T) {
	t.Parallel()

	for _, open := range []struct {
		name string
		txn  func(*Database) *Txn
	}{
		{"combined", func(db *Database) *Txn {
			return db.Transaction(t.Context(), true)
		}},
		{"blob", func(db *Database) *Txn { return db.BlobTxn(true) }},
	} {
		t.Run(open.name, func(t *testing.T) {
			t.Parallel()

			db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
			require.NoError(t, err)
			before := testTip(100, 0x01)
			require.NoError(t, db.SetTip(before, nil))

			rolledBack := open.txn(db)
			require.NoError(t, db.SetTip(testTip(120, 0x02), rolledBack))
			require.NoError(t, rolledBack.Rollback())
			got, err := db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, before, got, "a rolled back tip was kept")

			after := testTip(140, 0x03)
			txn := open.txn(db)
			require.NoError(t, db.SetTip(after, txn))
			got, err = db.GetTip(txn)
			require.NoError(t, err)
			require.Equal(t, after, got, "the transaction reads its own tip")
			got, err = db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, before, got, "the tip moved before the commit")
			require.NoError(t, txn.Commit())
			got, err = db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, after, got)
			requireNoMetadataTip(t, db)
		})
	}
}

// A metadata-only transaction on the ledger's database holds no handle the
// tip could be written through, so the write is refused rather than lost.
func TestTipNeedsABlobTransaction(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	txn := db.MetadataTxn(t.Context(), true)
	defer txn.Rollback() //nolint:errcheck
	require.ErrorIs(t, db.SetTip(testTip(120, 0x02), txn), types.ErrNilTxn)
}

// timestampOnlyMetadata accepts the commit timestamp of a transaction whose
// metadata handle is a test double the embedded store cannot write through.
type timestampOnlyMetadata struct {
	metadata.MetadataStore
}

// failedMetadataCommit commits a transaction that set the tip to next over a
// database whose tip is before, with the metadata commit failing after the
// blob commit, and returns the tip left behind.
func failedMetadataCommitDatabase(
	t *testing.T,
	before, next ochainsync.Tip,
) *Database {
	t.Helper()
	real, err := newTestDatabase(t, &Config{DataDir: t.TempDir(), Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, real.SetTip(before, nil))

	db := &Database{
		config:   &Config{},
		logger:   real.logger,
		metadata: timestampOnlyMetadata{real.Metadata()},
		blobRef:  newBlobStoreRef(real.Blob()),
	}
	commitErr := errors.New("metadata commit refused")
	txn := &Txn{
		db:          db,
		readWrite:   true,
		metadataTxn: &commitFailingTxn{err: commitErr},
		blobStore:   real.Blob(),
		blobTxn:     real.Blob().NewTransaction(true),
	}
	require.NoError(t, db.SetTip(next, txn))
	var partial PartialCommitError
	require.ErrorAs(t, txn.Commit(), &partial)
	require.ErrorIs(t, partial.MetadataErr, commitErr)

	return real
}

func failedMetadataCommit(
	t *testing.T,
	before, next ochainsync.Tip,
) ochainsync.Tip {
	t.Helper()
	got, err := failedMetadataCommitDatabase(t, before, next).GetTip(nil)
	require.NoError(t, err)
	return got
}

// A tip must never name a block the committed metadata does not reach. An
// advancing tip therefore stays where it was when its metadata commit fails,
// and a retreating tip has already moved: the metadata rows above it may be
// gone, and reverting them again from the lower tip is safe.
func TestTipNeverLeadsTheCommittedMetadata(t *testing.T) {
	t.Parallel()

	lower, higher := testTip(100, 0x01), testTip(120, 0x02)
	t.Run("advance", func(t *testing.T) {
		t.Parallel()
		require.Equal(
			t, lower, failedMetadataCommit(t, lower, higher),
			"the tip advanced without its metadata commit",
		)
	})
	t.Run("retreat", func(t *testing.T) {
		t.Parallel()
		require.Equal(
			t, lower, failedMetadataCommit(t, higher, lower),
			"the tip stayed above a metadata revert that may have happened",
		)
	})
	t.Run("same slot, other block", func(t *testing.T) {
		t.Parallel()
		other := testTip(120, 0x03)
		require.Equal(
			t, other, failedMetadataCommit(t, higher, other),
			"the tip kept naming a block the revert removed",
		)
	})
}

// A retreating tip is written before the metadata commit that reverts to it.
// Until that commit succeeds the block store marks the revert as pending, so
// a stop in between is not mistaken for a ledger that is simply at the lower
// tip. An advance is never marked.
func TestRevertIsMarkedPendingUntilItsMetadataCommit(t *testing.T) {
	t.Parallel()

	lower, higher := testTip(100, 0x01), testTip(120, 0x02)
	pendingRevert := func(t *testing.T, db *Database) (ochainsync.Tip, bool) {
		t.Helper()
		tip, found, err := db.TipRevertPending()
		require.NoError(t, err)
		return tip, found
	}
	commitTip := func(t *testing.T, db *Database, tip ochainsync.Tip) {
		t.Helper()
		txn := db.Transaction(t.Context(), true)
		require.NoError(t, db.SetTip(tip, txn))
		require.NoError(t, txn.Commit())
	}
	t.Run("failed revert", func(t *testing.T) {
		t.Parallel()
		tip, found := pendingRevert(
			t, failedMetadataCommitDatabase(t, higher, lower),
		)
		require.True(t, found, "nothing marks the unconfirmed revert")
		require.Equal(t, lower, tip)
	})
	t.Run("failed advance", func(t *testing.T) {
		t.Parallel()
		_, found := pendingRevert(
			t, failedMetadataCommitDatabase(t, lower, higher),
		)
		require.False(t, found)
	})
	t.Run("committed revert", func(t *testing.T) {
		t.Parallel()
		db, err := newTestDatabase(t, &Config{DataDir: t.TempDir(), Role: RoleReleased})
		require.NoError(t, err)
		require.NoError(t, db.SetTip(higher, nil))
		commitTip(t, db, lower)
		_, found := pendingRevert(t, db)
		require.False(t, found, "a committed revert is still marked")
	})
	t.Run("set again without a revert", func(t *testing.T) {
		t.Parallel()
		db := failedMetadataCommitDatabase(t, higher, lower)
		commitTip(t, db, lower)
		commitTip(t, db, higher)
		tip, found := pendingRevert(t, db)
		require.True(t, found, "a tip write cleared a revert it did not make")
		require.Equal(t, lower, tip)
	})
	t.Run("repeated revert", func(t *testing.T) {
		t.Parallel()
		db := newReleasedTestDB(t)
		target := testIndexedBlock(1_500, 1, 0x15)
		require.NoError(t, db.BlockCreate(target, nil))
		at := ochainsync.Tip{
			Point:       ocommon.Point{Slot: target.Slot, Hash: target.Hash},
			BlockNumber: target.Number,
		}
		require.NoError(t, db.SetTip(at, nil))
		value, err := encodeTip(at)
		require.NoError(t, err)
		require.NoError(t, db.SetChainState(TipRevertPendingKey, value, nil))

		txn := db.Transaction(t.Context(), true)
		require.NoError(t, txn.Do(func(txn *Txn) error {
			_, _, err := db.RevertStoredLedgerAfterSlot(t.Context(), at.Point, 0, txn)
			return err
		}))
		_, found := pendingRevert(t, db)
		require.False(t, found, "the repeated revert left its mark")
	})
}

// A read-write blob transaction that read the tip would be refused at
// commit whenever the ledger moved the tip meanwhile.
func TestTipReadTakesNoPartInAWriteTransaction(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, db.SetTip(testTip(100, 0x01), nil))

	txn := db.BlobTxn(true)
	defer txn.Release()
	got, err := db.GetTip(txn)
	require.NoError(t, err)
	require.Equal(t, testTip(100, 0x01), got)
	require.NoError(t, db.SetChainState("tip_test_value", "1", txn))

	moved := testTip(120, 0x02)
	require.NoError(t, db.SetTip(moved, nil))
	require.NoError(t, txn.Commit())
	got, err = db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, moved, got)
}

func TestReadSnapshotIsAnchoredAtTheBlockStoreTip(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	want := testTip(100, 0x01)
	require.NoError(t, db.SetTip(want, nil))

	snapshot, anchor, err := NewReadSnapshotContext(t.Context(), db)
	require.NoError(t, err)
	defer snapshot.Release()
	require.Equal(t, want, anchor)

	require.NoError(t, db.SetTip(testTip(120, 0x02), nil))
	got, err := db.GetTip(snapshot)
	require.NoError(t, err)
	require.Equal(t, want, got, "the snapshot's tip moved under it")
}

// An IndexerDB reads the ledger's block store, so a tip it kept there would
// be the ledger's: its own tip stays in its metadata store.
func TestIndexerTipIsNotTheLedgerTip(t *testing.T) {
	t.Parallel()

	ledger, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ledgerTip := testTip(4000, 0x0a)
	require.NoError(t, ledger.SetTip(ledgerTip, nil))

	indexer := &Database{
		config:   &Config{Role: RoleIndexer},
		logger:   ledger.logger,
		metadata: storesWithFreshBlob(t, t.TempDir()).Metadata,
		blobRef:  newBlobStoreRef(ledger.Blob()),
	}
	got, err := indexer.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ochainsync.Tip{}, got, "the IndexerDB read the ledger tip")

	indexerTip := testTip(2000, 0x0b)
	require.NoError(t, indexer.SetTip(indexerTip, nil))
	got, err = indexer.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, indexerTip, got)
	got, err = ledger.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ledgerTip, got, "the IndexerDB moved the ledger tip")

	txn := indexer.MetadataTxn(t.Context(), true)
	require.NoError(t, indexer.SetTip(testTip(2020, 0x0c), txn))
	require.NoError(t, txn.Commit())
	got, err = ledger.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ledgerTip, got, "the IndexerDB moved the ledger tip")
}

func TestOpenCopiesTheMetadataTipIntoTheBlockStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	released := testTip(4242, 0xab)
	require.NoError(t, db.Metadata().SetTip(released, nil))
	require.NoError(t, closeTestDatabase(db))

	db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	got, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, released, got)

	// The row stays for an adoption to read, so the next open finds it
	// again: the tip the ledger has written since must win.
	row, err := db.Metadata().GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, released, row)
	advanced := testTip(4262, 0xcd)
	require.NoError(t, db.SetTip(advanced, nil))
	require.NoError(t, closeTestDatabase(db))

	db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	got, err = db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, advanced, got)
}

func TestOpenLeavesTheMetadataTipOfAnotherBlobStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetTip(testTip(4242, 0xab), nil))
	require.NoError(t, closeTestDatabase(db))

	stores := storesWithFreshBlob(t, dir)
	_, err = New(t.Context(), &Config{DataDir: dir, Role: RoleReleased}, stores)
	require.Error(t, err)
	txn := stores.Blob.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	_, err = stores.Blob.Get(txn, chainStateBlobKey(LedgerTipKey))
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}

func TestStoredTipIsValidated(t *testing.T) {
	t.Parallel()

	_, err := encodeTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: make([]byte, 33)},
	})
	require.ErrorContains(t, err, "33 bytes")

	for _, raw := range []string{
		"1:2",
		"1:2:aa:bb",
		"x:2:aa",
		"1:y:aa",
		"1:2:zz",
		"-1:2:aa",
		"1:2:" + strings.Repeat("ab", 33),
	} {
		_, err := decodeTip(raw)
		require.Error(t, err, raw)
	}

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, db.SetChainState(LedgerTipKey, "1:2", nil))
	_, err = db.GetTip(nil)
	require.ErrorContains(t, err, "stored tip has 2 fields")
}

// syncCountingBlob counts the flushes of the store it wraps.
type syncCountingBlob struct {
	blob.BlobStore
	syncs atomic.Int32
}

func (s *syncCountingBlob) Sync() error {
	s.syncs.Add(1)
	return s.BlobStore.Sync()
}

// A retreating tip is written ahead of a metadata commit the block store
// does not otherwise wait for, so it is flushed before that commit can become
// durable without it. An advancing tip may be lost: the ledger then applies
// from the earlier block.
func TestRetreatingTipIsFlushedBeforeTheMetadataCommit(t *testing.T) {
	t.Parallel()

	real, err := newTestDatabase(t, &Config{DataDir: t.TempDir(), Role: RoleReleased})
	require.NoError(t, err)
	counting := &syncCountingBlob{BlobStore: real.Blob()}
	db := &Database{
		config:   &Config{},
		logger:   real.logger,
		metadata: real.Metadata(),
		blobRef:  newBlobStoreRef(counting),
	}
	set := func(tip ochainsync.Tip) {
		t.Helper()
		txn := db.MetadataTxn(t.Context(), true)
		require.NoError(t, db.SetTip(tip, txn))
		require.NoError(t, txn.Commit())
		got, err := db.GetTip(nil)
		require.NoError(t, err)
		require.Equal(t, tip, got)
	}

	set(testTip(120, 0x02))
	require.Zero(t, counting.syncs.Load(), "an advancing tip was flushed")
	set(testTip(100, 0x01))
	require.Equal(
		t, int32(1), counting.syncs.Load(),
		"a retreating tip was not flushed before its metadata commit",
	)
}

// The LedgerDB is updated from an after-commit callback, and must never be
// ahead of the tip when a stop falls between the two.
func TestTipIsWrittenBeforeAfterCommitCallbacks(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	want := testTip(140, 0x03)
	var seen ochainsync.Tip
	var seenErr error
	txn := db.Transaction(t.Context(), true)
	require.NoError(t, db.SetTip(want, txn))
	txn.AfterCommit(func() { seen, seenErr = db.GetTip(nil) })
	require.NoError(t, txn.Commit())
	require.NoError(t, seenErr)
	require.Equal(t, want, seen, "the callback ran ahead of the tip")
}

// A v0.94.2 database keeps a tip row in its metadata store that adopting its
// API rows as an IndexerDB reads. A revert of the metadata store moves the
// row down with the rows, and a database that never had one gets none.
func TestTruncateLowersAReleasedMetadataTipRow(t *testing.T) {
	t.Parallel()

	for _, released := range []bool{true, false} {
		name := "fresh"
		if released {
			name = "released"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := newReleasedTestDB(t)
			const targetSlot = uint64(1_500)
			target := testIndexedBlock(targetSlot, 1, 0x15)
			require.NoError(t, db.BlockCreate(target, nil))
			old := testTip(2_000, 0x20)
			if released {
				require.NoError(t, db.Metadata().SetTip(old, nil))
			}
			require.NoError(t, db.SetTip(old, nil))

			point := ocommon.Point{Slot: targetSlot, Hash: target.Hash}
			txn := db.Transaction(t.Context(), true)
			require.NoError(t, txn.Do(func(txn *Txn) error {
				_, _, err := db.RevertStoredLedgerAfterSlot(t.Context(), point, 0, txn)
				return err
			}))

			tip, err := db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, point, tip.Point)
			row, err := db.Metadata().GetTip(nil)
			require.NoError(t, err)
			if released {
				require.Equal(t, tip, row, "the row stayed above the rows")
			} else {
				require.Equal(t, ochainsync.Tip{}, row, "a tip row was written")
			}
		})
	}
}
