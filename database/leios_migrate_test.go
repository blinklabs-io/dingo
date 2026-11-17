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
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// writeCurrentLeiosEB writes the hash-first, slot-suffixed key format the
// shared blob store used before the LeiosDB.
func writeCurrentLeiosEB(
	t *testing.T,
	d *Database,
	slot uint64,
	hash []byte,
	manifest []byte,
	txs []cbor.RawMessage,
	certified bool,
) {
	t.Helper()
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blob, blobTxn := txn.BlobStore(), txn.Blob()
	require.NoError(
		t,
		blob.Set(blobTxn, types.LeiosEBManifestKey(hash, slot), manifest),
	)
	if txs != nil {
		val, err := cbor.Encode(txs)
		require.NoError(t, err)
		require.NoError(
			t,
			blob.Set(blobTxn, types.LeiosEBTxsKey(hash, slot), val),
		)
	}
	if certified {
		require.NoError(
			t,
			blob.Set(blobTxn, types.LeiosEBCertifiedKey(hash, slot), []byte{1}),
		)
	}
	require.NoError(t, txn.Commit())
}

func legacyLeiosKeysRemaining(t *testing.T, d *Database) int {
	t.Helper()
	keys, err := d.legacyLeiosKeys(context.Background(), 1<<20)
	require.NoError(t, err)
	return len(keys)
}

func TestMigrateLeiosStoreClassifiesCurrentAndLegacyRecords(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	txs := []cbor.RawMessage{{0x41, 0x01}}
	var (
		oldCertified   = randomHash(t)
		oldUncertified = randomHash(t)
		atTip          = randomHash(t)
		newer          = randomHash(t)
		markerless     = randomHash(t)
		legacyOld      = randomHash(t)
		legacyNew      = randomHash(t)
		malformed      = randomHash(t)
	)
	writeCurrentLeiosEB(t, d, 10, oldCertified, []byte("m1"), txs, true)
	writeCurrentLeiosEB(t, d, 11, oldUncertified, []byte("m2"), txs, false)
	writeCurrentLeiosEB(t, d, 100, atTip, []byte("m3"), txs, false)
	writeCurrentLeiosEB(t, d, 120, newer, []byte("m4"), nil, false)
	writeCurrentLeiosEB(t, d, 12, markerless, []byte("m5"), txs, false)
	writeLegacyLeiosEB(t, d, 13, legacyOld, []byte("m6"), txs)
	writeLegacyLeiosEB(t, d, 130, legacyNew, []byte("m7"), txs)
	// A legacy manifest whose value is too short to carry a slot.
	func() {
		txn := d.BlobTxn(true)
		defer txn.Rollback() //nolint:errcheck
		require.NoError(t, txn.BlobStore().Set(
			txn.Blob(),
			types.LegacyLeiosEBManifestKey(malformed), []byte{1, 2, 3},
		),
		)
		require.NoError(t, txn.Commit())
	}()

	immutable := leiosdb.Ref{Slot: 100, Hash: randomHash(t)}
	certifier := leiosdb.Ref{Slot: 13, Hash: randomHash(t)}
	res, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{
		ImmutablePoint: immutable,
		ImmutableKnown: true,
		CertifiedByChain: func(_ context.Context, eb leiosdb.Ref) (leiosdb.Ref, bool, error) {
			// The chain certifies the marker-less record and the old legacy
			// one, and only those.
			switch string(eb.Hash) {
			case string(markerless), string(legacyOld):
				return certifier, true, nil
			}
			return leiosdb.Ref{}, false, nil
		},
	})
	require.NoError(t, err)
	require.Equal(
		t,
		3,
		res.Pinned,
		"marker, chain-derived and legacy chain-derived",
	)
	require.Equal(
		t,
		1,
		res.Dropped,
		"the uncertified record below the immutable point",
	)
	require.Equal(
		t,
		3,
		res.Volatile,
		"at the point, above it, and a legacy record above it",
	)
	require.Equal(t, 1, res.Malformed)
	require.Zero(t, legacyLeiosKeysRemaining(t, d))

	store, err := d.LeiosDB()
	require.NoError(t, err)
	pinned := func(slot uint64, hash []byte) bool {
		ok, err := store.IsPinned(leiosdb.Ref{Slot: slot, Hash: hash})
		require.NoError(t, err)
		return ok
	}
	require.True(t, pinned(10, oldCertified))
	require.True(t, pinned(12, markerless))
	require.True(t, pinned(13, legacyOld))
	require.False(
		t,
		pinned(100, atTip),
		"at the immutable slot the certifier may still be volatile",
	)
	got, err := d.GetLeiosEBManifest(oldCertified, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("m1"), got)
	gotTxs, err := d.GetLeiosEBTxs(oldCertified, 10)
	require.NoError(t, err)
	require.Equal(t, txs, gotTxs)
	got, err = d.GetLeiosEBManifest(legacyOld, 13)
	require.NoError(t, err)
	require.Equal(t, []byte("m6"), got)
	_, err = d.GetLeiosEBManifest(oldUncertified, 11)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	got, err = d.GetLeiosEBManifest(legacyNew, 130)
	require.NoError(t, err)
	require.Equal(t, []byte("m7"), got)
	_, err = d.GetLeiosEBTxs(newer, 120)
	require.ErrorIs(
		t,
		err,
		types.ErrBlobKeyNotFound,
		"manifest-only stays manifest-only",
	)

	wm, err := store.Watermarks()
	require.NoError(t, err)
	require.True(t, wm.PinnedThroughSet)
	require.Equal(t, immutable.Slot, wm.PinnedThrough.Slot)
	state, err := store.MigrationState()
	require.NoError(t, err)
	require.Equal(t, leiosdb.MigrationDone, state)

	// A second run is a no-op.
	again, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{})
	require.NoError(t, err)
	require.True(t, again.AlreadyDone)
}

func TestMigrateLeiosStoreKeepsEverythingVolatileWithoutImmutablePoint(
	t *testing.T,
) {
	t.Parallel()

	d := newTestDB(t)
	hash := randomHash(t)
	writeCurrentLeiosEB(t, d, 10, hash, []byte("m"), nil, true)
	res, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, res.Volatile)
	require.Zero(t, res.Pinned)
	store, err := d.LeiosDB()
	require.NoError(t, err)
	wm, err := store.Watermarks()
	require.NoError(t, err)
	require.False(
		t,
		wm.PinnedThroughSet,
		"the pin walk starts from the chain's beginning",
	)
}

func TestMigrateLeiosStoreReportsChainDerivationFailure(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	writeCurrentLeiosEB(t, d, 10, randomHash(t), []byte("m"), nil, false)
	boom := errors.New("chain unavailable")
	_, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{
		ImmutablePoint: leiosdb.Ref{Slot: 50},
		ImmutableKnown: true,
		CertifiedByChain: func(context.Context, leiosdb.Ref) (leiosdb.Ref, bool, error) {
			return leiosdb.Ref{}, false, boom
		},
	})
	require.ErrorIs(t, err, boom)
	require.Equal(
		t,
		1,
		legacyLeiosKeysRemaining(t, d),
		"a failed pass deletes nothing",
	)
	store, err := d.LeiosDB()
	require.NoError(t, err)
	state, err := store.MigrationState()
	require.NoError(t, err)
	require.Equal(t, leiosdb.MigrationNone, state)
}

// A crash after every record is copied and before the sources are deleted
// leaves both copies; the next run deletes the sources without recopying and
// reads are served throughout.
func TestMigrateLeiosStoreCrashAfterCopyBeforeDelete(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := randomHash(t)
	writeCurrentLeiosEB(t, d, 10, hash, []byte("m"), []cbor.RawMessage{}, false)
	crash := errors.New("simulated crash")
	_, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{
		Hook: func(point string) error {
			if point == LeiosMigrationHookCopied {
				return crash
			}
			return nil
		},
	})
	require.ErrorIs(t, err, crash)
	store, err := d.LeiosDB()
	require.NoError(t, err)
	state, err := store.MigrationState()
	require.NoError(t, err)
	require.Equal(t, leiosdb.MigrationCopied, state)
	require.Equal(t, 2, legacyLeiosKeysRemaining(t, d))
	got, err := d.GetLeiosEBManifest(hash, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("m"), got)

	res, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, res.Deleted)
	require.Zero(t, res.Volatile, "copied records are not copied again")
	require.Zero(t, legacyLeiosKeysRemaining(t, d))
	got, err = d.GetLeiosEBManifest(hash, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("m"), got)
}

// A crash between delete batches resumes from the rescan.
func TestMigrateLeiosStoreCrashBetweenDeleteBatches(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	for i := range leiosMigrationDeleteBatch + 5 {
		writeCurrentLeiosEB(
			t,
			d,
			uint64(10+i),
			randomHash(t),
			[]byte("m"),
			nil,
			false,
		)
	}
	crash := errors.New("simulated crash")
	_, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{
		Hook: func(point string) error {
			if point == LeiosMigrationHookDeleteBatch {
				return crash
			}
			return nil
		},
	})
	require.ErrorIs(t, err, crash)
	require.Equal(t, 5, legacyLeiosKeysRemaining(t, d))

	res, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{})
	require.NoError(t, err)
	require.Equal(t, 5, res.Deleted)
	require.Zero(t, legacyLeiosKeysRemaining(t, d))
}

func TestMigrateLeiosStoreStopsOnCancelledContext(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	writeCurrentLeiosEB(t, d, 10, randomHash(t), []byte("m"), nil, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := d.MigrateLeiosStore(ctx, LeiosMigrationOptions{})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, legacyLeiosKeysRemaining(t, d))
}

// The migration loads the record values in bounded batches, in slot order, so
// a large store is never held in memory at once.
func TestMigrateLeiosStoreLoadsValuesInBoundedBatches(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	const records = 5
	for i := range records {
		writeCurrentLeiosEB(
			t, d, uint64(100+i), randomHash(t),
			[]byte("manifest"), []cbor.RawMessage{{0x41, 0x01}}, false,
		)
	}
	var batches []int
	res, err := d.MigrateLeiosStore(context.Background(), LeiosMigrationOptions{
		BatchEntries: 2,
		OnBatch:      func(entries int) { batches = append(batches, entries) },
	})
	require.NoError(t, err)
	require.Equal(t, records, res.Volatile)
	require.Equal(t, []int{2, 2, 1}, batches)

	store, err := d.LeiosDB()
	require.NoError(t, err)
	slot, found, err := store.MaxSlot()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(100+records-1), slot)
}

func TestMigrateLeiosStorePrefersCurrentManifestOverLegacyKey(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := randomHash(t)
	// Both key forms hold one occurrence; the legacy value carries a slot
	// prefix and the current one does not.
	writeLegacyLeiosEB(t, d, 10, hash, []byte("legacy-manifest"), nil)
	writeCurrentLeiosEB(
		t,
		d,
		10,
		hash,
		[]byte("current-manifest-bytes"),
		nil,
		false,
	)

	_, err := d.MigrateLeiosStore(t.Context(), LeiosMigrationOptions{})
	require.NoError(t, err)
	got, err := d.GetLeiosEBManifest(hash, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("current-manifest-bytes"), got)
	require.Zero(t, legacyLeiosKeysRemaining(t, d))
}
