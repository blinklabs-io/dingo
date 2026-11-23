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
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// writeLegacyLeiosEB writes a manifest (and, if txsRaw is non-nil, a
// transaction list) directly under the legacy hash-only blob keys,
// bypassing SetLeiosEB entirely -- simulating data persisted by a node
// running before the key format changed to (slot, hash).
func writeLegacyLeiosEB(
	t *testing.T,
	d *Database,
	slot uint64,
	hash []byte,
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) {
	t.Helper()
	blob := d.Blob()
	require.NotNil(t, blob)
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blobTxn := txn.Blob()
	require.NotNil(t, blobTxn)

	manifestVal := make([]byte, 8+len(manifestRaw))
	binary.BigEndian.PutUint64(manifestVal[:8], slot)
	copy(manifestVal[8:], manifestRaw)
	require.NoError(
		t,
		blob.Set(blobTxn, types.LegacyLeiosEBManifestKey(hash), manifestVal),
	)
	if txsRaw != nil {
		txsVal, err := cbor.Encode(txsRaw)
		require.NoError(t, err)
		require.NoError(
			t,
			blob.Set(blobTxn, types.LegacyLeiosEBTxsKey(hash), txsVal),
		)
	}
	require.NoError(t, txn.Commit())
}

// TestGetLeiosEBManifestFallsBackToLegacyKey verifies that a node upgrading
// from before the blob key format changed to (slot, hash) can still read
// manifests it persisted under the old hash-only key,
// rather than that data becoming silently unreachable.
func TestGetLeiosEBManifestFallsBackToLegacyKey(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("0123456789abcdef0123456789abcdef")[:32]
	slot := uint64(12345)
	manifestRaw := []byte("legacy manifest bytes")

	writeLegacyLeiosEB(t, d, slot, hash, manifestRaw, nil)

	got, err := d.GetLeiosEBManifest(hash, slot)
	require.NoError(t, err)
	require.Equal(t, manifestRaw, got)

	// A request for a different slot must not match the legacy record: its
	// embedded slot is validated, not just its presence under the hash.
	_, err = d.GetLeiosEBManifest(hash, slot+1)
	require.Error(t, err)
}

// TestGetLeiosEBTxsFallsBackToLegacyKey is the transaction-body half of the
// same regression: legacy "et" data (paired 1:1 with a legacy "em" record,
// since the old format could only ever track one occurrence per hash) must
// still be readable after upgrading, gated on the legacy manifest's own
// embedded slot matching the request.
func TestGetLeiosEBTxsFallsBackToLegacyKey(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("fedcba9876543210fedcba9876543210")[:32]
	slot := uint64(54321)
	manifestRaw := []byte("legacy manifest bytes 2")
	txsRaw := []cbor.RawMessage{mustCborForLeiosTest(t, "tx0")}

	writeLegacyLeiosEB(t, d, slot, hash, manifestRaw, txsRaw)

	got, err := d.GetLeiosEBTxs(hash, slot)
	require.NoError(t, err)
	require.Equal(t, txsRaw, got)

	// A different slot must not pick up the legacy txs either.
	_, err = d.GetLeiosEBTxs(hash, slot+1)
	require.Error(t, err)
}

// TestGetLeiosEBManifestPropagatesRealLegacyReadError verifies that a genuine
// failure reading the legacy fallback key (storage,
// network, auth) must surface as-is, not collapse into the exact-key's
// ordinary ErrBlobKeyNotFound and look like the manifest was simply never
// persisted.
func TestGetLeiosEBManifestPropagatesRealLegacyReadError(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("0123456789abcdef0123456789abcdef")[:32]
	slot := uint64(111)
	readErr := errors.New("legacy read: storage unavailable")

	d.SetBlobStore(&mockBlobStore{
		getErrs: map[string]error{
			string(types.LegacyLeiosEBManifestKey(hash)): readErr,
		},
	})

	_, err := d.GetLeiosEBManifest(hash, slot)
	require.ErrorIs(t, err, readErr)
	require.NotErrorIs(t, err, types.ErrBlobKeyNotFound)
}

// TestGetLeiosEBTxsPropagatesRealLegacyReadError is the transaction-body
// half of the same regression.
func TestGetLeiosEBTxsPropagatesRealLegacyReadError(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("fedcba9876543210fedcba9876543210")[:32]
	slot := uint64(222)
	readErr := errors.New("legacy read: storage unavailable")

	d.SetBlobStore(&mockBlobStore{
		getErrs: map[string]error{
			string(types.LegacyLeiosEBManifestKey(hash)): readErr,
		},
	})

	_, err := d.GetLeiosEBTxs(hash, slot)
	require.ErrorIs(t, err, readErr)
	require.NotErrorIs(t, err, types.ErrBlobKeyNotFound)
}

func mustCborForLeiosTest(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	data, err := cbor.Encode(value)
	require.NoError(t, err)
	return cbor.RawMessage(data)
}

// MaxLeiosEBSlot answers from the slot-ordered keys, so the highest slot is
// found whichever order records were written in, and a block pinned into the
// immutable partition still counts.
func TestMaxLeiosEBSlotSpansBothPartitions(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	manifestRaw := []byte("manifest bytes")

	got, err := d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(0), got, "no manifests is not an error")

	require.NoError(t, d.SetLeiosEB(700, randomHash(t), manifestRaw, nil))
	require.NoError(t, d.SetLeiosEB(200, randomHash(t), manifestRaw, nil))
	got, err = d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(700), got)

	store, err := d.LeiosDB()
	require.NoError(t, err)
	pinned := leiosdb.Ref{Slot: 900, Hash: randomHash(t)}
	require.NoError(t, d.SetLeiosEB(900, pinned.Hash, manifestRaw, nil))
	_, err = store.Pin(pinned, leiosdb.Ref{Slot: 901, Hash: randomHash(t)})
	require.NoError(t, err)
	got, err = d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(900), got)
}

// A write below the collection watermark is refused rather than stored for the
// next collection to delete, and a pinned block stays writable so a
// content-pending pin can be repaired.
func TestSetLeiosEBRefusesBehindCollectionWatermark(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	store, err := d.LeiosDB()
	require.NoError(t, err)
	_, err = store.GarbageCollect(t.Context(), 100)
	require.NoError(t, err)

	hash := randomHash(t)
	err = d.SetLeiosEB(99, hash, []byte("m"), nil)
	require.ErrorIs(t, err, leiosdb.ErrBehindImmutableTip)
	_, err = d.GetLeiosEBManifest(hash, 99)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)

	require.NoError(t, d.SetLeiosEB(100, hash, []byte("m"), nil))
	pinnedHash := randomHash(t)
	_, err = store.Pin(
		leiosdb.Ref{Slot: 50, Hash: pinnedHash},
		leiosdb.Ref{Slot: 51, Hash: randomHash(t)},
	)
	require.NoError(t, err)
	require.NoError(t, d.SetLeiosEB(50, pinnedHash, []byte("m"), []cbor.RawMessage{}))
}

// The endorser block records no longer touch the shared blob store.
func TestSetLeiosEBLeavesSharedBlobStoreUntouched(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	require.NoError(t, d.SetLeiosEB(
		10, randomHash(t), []byte("m"), []cbor.RawMessage{{0x01}},
	))
	require.NoError(t, d.PutLeiosClosure(10, randomHash(t), []byte("closure")))

	txn := d.BlobTxn(false)
	defer txn.Release()
	blob := txn.BlobStore()
	it := blob.NewIterator(txn.Blob(), types.BlobIteratorOptions{Prefix: []byte("e")})
	defer it.Close()
	for it.Seek([]byte("e")); it.ValidForPrefix([]byte("e")); it.Next() {
		t.Errorf("shared blob store holds endorser-block key %x", it.Item().Key())
	}
	bit := blob.NewIterator(txn.Blob(), types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	})
	defer bit.Close()
	for bit.Seek([]byte(types.BlockBlobKeyPrefix)); bit.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)); bit.Next() {
		t.Errorf("shared blob store holds a closure block %x", bit.Item().Key())
	}
}

// A rollback removes the pins certified by the removed ranking blocks and
// lowers the watermarks so the re-applied chain can pin and store again;
// pins certified at or before the rollback point stay.
func TestTruncateAfterSlotRollsBackLeiosPins(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	const rollbackSlot = uint64(1_500)
	target := testIndexedBlock(rollbackSlot, 1, 0x15)
	require.NoError(t, d.BlockCreate(target, nil))
	store, err := d.LeiosDB()
	require.NoError(t, err)

	type pin struct {
		eb, cert leiosdb.Ref
		kept     bool
	}
	pins := []pin{
		{eb: leiosdb.Ref{Slot: 1498, Hash: randomHash(t)}, cert: leiosdb.Ref{Slot: 1499, Hash: randomHash(t)}, kept: true},
		{eb: leiosdb.Ref{Slot: 1499, Hash: randomHash(t)}, cert: leiosdb.Ref{Slot: 1500, Hash: target.Hash}, kept: true},
		{eb: leiosdb.Ref{Slot: 1500, Hash: randomHash(t)}, cert: leiosdb.Ref{Slot: 1501, Hash: randomHash(t)}},
		{eb: leiosdb.Ref{Slot: 1510, Hash: randomHash(t)}, cert: leiosdb.Ref{Slot: 1511, Hash: randomHash(t)}},
	}
	for _, p := range pins {
		require.NoError(t, d.SetLeiosEB(p.eb.Slot, p.eb.Hash, []byte("m"), []cbor.RawMessage{}))
		_, err := store.Pin(p.eb, p.cert)
		require.NoError(t, err)
	}
	require.NoError(t, store.AdvancePinnedThrough(pins[3].cert))
	_, err = store.GarbageCollect(t.Context(), 1_505)
	require.NoError(t, err)

	_, _, err = d.TruncateAfterSlot(
		t.Context(),
		ocommon.Point{Slot: rollbackSlot, Hash: target.Hash},
		0,
		nil,
	)
	require.NoError(t, err)

	for _, p := range pins {
		pinned, err := d.IsLeiosEBPinned(p.eb.Slot, p.eb.Hash)
		require.NoError(t, err)
		require.Equal(t, p.kept, pinned, "eb slot %d", p.eb.Slot)
	}
	wm, err := store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, rollbackSlot, wm.PinnedThrough.Slot)
	require.Equal(t, rollbackSlot, wm.GCBefore)
}

func TestIsLeiosEBPinnedIsPerOccurrence(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := randomHash(t)
	pinned, err := d.IsLeiosEBPinned(10, hash)
	require.NoError(t, err)
	require.False(t, pinned)

	store, err := d.LeiosDB()
	require.NoError(t, err)
	_, err = store.Pin(
		leiosdb.Ref{Slot: 10, Hash: hash},
		leiosdb.Ref{Slot: 11, Hash: randomHash(t)},
	)
	require.NoError(t, err)
	pinned, err = d.IsLeiosEBPinned(10, hash)
	require.NoError(t, err)
	require.True(t, pinned)
	pinned, err = d.IsLeiosEBPinned(11, hash)
	require.NoError(t, err)
	require.False(t, pinned)
}

// An offset into a certified closure resolves from the LeiosDB when the blob
// store has no block at the closure's (slot, hash), and a legacy synthetic
// block in the blob store still wins.
func TestOffsetResolutionFallsBackToLeiosClosure(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	cache := d.CborCache()
	store, err := d.LeiosDB()
	require.NoError(t, err)
	hash := randomHash(t)
	closure := []byte("0123456789")
	require.NoError(t, store.PutClosure(leiosdb.Ref{Slot: 12, Hash: hash}, closure))
	canUse, err := d.CanUseLeiosTransactionBacking(12, hash)
	require.NoError(t, err)
	require.False(t, canUse)

	txn := d.BlobTxn(false)
	defer txn.Release()
	got, err := cache.getBlockCbor(d.Blob(), txn.Blob(), 12, hash)
	require.NoError(t, err)
	require.Equal(t, closure, got)

	legacyHash := randomHash(t)
	require.NoError(t, d.SetGenesisCbor(13, legacyHash, []byte("legacy"), nil))
	canUse, err = d.CanUseLeiosTransactionBacking(13, legacyHash)
	require.NoError(t, err)
	require.False(t, canUse)
	require.NoError(t, store.PutClosure(leiosdb.Ref{Slot: 13, Hash: legacyHash}, []byte("leiosdb")))
	got, err = cache.getBlockCbor(d.Blob(), d.BlobTxn(false).Blob(), 13, legacyHash)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy"), got)

	canUse, err = d.CanUseLeiosTransactionBacking(15, randomHash(t))
	require.NoError(t, err)
	require.True(t, canUse)

	_, err = cache.getBlockCbor(d.Blob(), d.BlobTxn(false).Blob(), 14, randomHash(t))
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}

// A node that never handles an endorser block must not grow a Leios store
// from an offset read that misses the shared blob store.
func TestLeiosClosureFallbackDoesNotCreateStore(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.leiosClosure(7, make([]byte, 32))
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	require.NoDirExists(t, filepath.Join(dataDir, leiosdb.SubdirName))

	// Once the store is open the same read finds a stored closure.
	store, err := db.LeiosDB()
	require.NoError(t, err)
	ref := leiosdb.Ref{Slot: 7, Hash: make([]byte, 32)}
	require.NoError(t, store.PutClosure(ref, []byte("closure")))
	got, err := db.leiosClosure(7, make([]byte, 32))
	require.NoError(t, err)
	require.Equal(t, []byte("closure"), got)
}

// A chain rollback on a node that never handled an endorser block has no pins
// to demote and must not create the Leios store.
func TestRollbackLeiosPinsDoesNotCreateStore(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	point := ocommon.Point{Slot: 40, Hash: make([]byte, 32)}

	n, err := db.RollbackLeiosPins(point)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoDirExists(t, filepath.Join(dataDir, leiosdb.SubdirName))

	// With a store present the rollback demotes the pins above the point.
	store, err := db.LeiosDB()
	require.NoError(t, err)
	eb := leiosdb.Ref{Slot: 50, Hash: randomHash(t)}
	require.NoError(t, store.PutVolatile(
		eb, leiosdb.Content{Manifest: []byte("manifest")},
	))
	_, err = store.Pin(eb, leiosdb.Ref{Slot: 51, Hash: randomHash(t)})
	require.NoError(t, err)
	n, err = db.RollbackLeiosPins(point)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

// A closed Database does not reopen its Leios store behind the caller's back.
func TestLeiosDBDoesNotReopenAfterClose(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	_, err = db.LeiosDB()
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = db.LeiosDB()
	require.ErrorIs(t, err, leiosdb.ErrClosed)
	_, err = db.leiosClosure(7, make([]byte, 32))
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}

// Close returns only once the owned Leios store has flushed: records written
// before it are readable by a fresh store opened on the same data directory.
func TestDatabaseCloseFlushesLeiosDB(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	store, err := db.LeiosDB()
	require.NoError(t, err)
	eb := leiosdb.Ref{Slot: 77, Hash: randomHash(t)}
	require.NoError(t, store.PutVolatile(
		eb, leiosdb.Content{Manifest: []byte("manifest")},
	))
	require.NoError(t, store.PutClosure(eb, []byte("closure")))
	require.NoError(t, db.Close())
	_, err = store.Get(eb)
	require.ErrorIs(t, err, leiosdb.ErrClosed)

	reopened, err := leiosdb.Open(leiosdb.Options{DataDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	content, err := reopened.Get(eb)
	require.NoError(t, err)
	require.Equal(t, []byte("manifest"), content.Manifest)
	closure, err := reopened.GetClosure(eb)
	require.NoError(t, err)
	require.Equal(t, []byte("closure"), closure)
}
