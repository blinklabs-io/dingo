//go:build dingo_extra_plugins

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

package gcs

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blobbackup"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/blockverify"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// fakeCloser stands in for the real *storage.Writer, which needs a live GCS
// bucket (or credentials) to construct at all. joinCloseErr is tested against
// this controllable io.Closer instead of standing up real cloud storage.
type fakeCloser struct {
	err error
}

func (f *fakeCloser) Close() error {
	return f.err
}

// When the write already failed and the writer also fails to close, both
// errors matter: the close failure can be the only signal that the partial
// upload was never aborted cleanly. Neither may be dropped in favor of the
// other.
func TestJoinCloseErrJoinsOnCloseFailure(t *testing.T) {
	writeErr := errors.New("write failed")
	closeErr := errors.New("upload aborted")

	got := joinCloseErr(writeErr, &fakeCloser{err: closeErr})

	if !errors.Is(got, writeErr) {
		t.Fatalf("expected joined error to wrap the write error: %v", got)
	}
	if !errors.Is(got, closeErr) {
		t.Fatalf("expected joined error to wrap the close error: %v", got)
	}
}

// A clean close must not manufacture a joined error out of nothing -- the
// caller's original write error should come back unwrapped.
func TestJoinCloseErrReturnsOriginalOnCloseSuccess(t *testing.T) {
	writeErr := errors.New("write failed")

	got := joinCloseErr(writeErr, &fakeCloser{err: nil})

	if got != writeErr {
		t.Fatalf("expected original error identity, got: %v", got)
	}
}

// hasGCSCredentials is defined in backup_test.go and shared across this
// package's test files.

func TestBlobStoreConformance(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
		store := newTestGCSStore(t)
		// RunBlobStoreConformance's subtests (KVRoundTrip, BlockRoundTrip,
		// ...) commit real, persistent keys; newTestGCSStore only
		// registers Stop, which does not touch bucket contents, so without
		// this the first successful run would poison the bucket for every
		// later run's "must start empty" check.
		t.Cleanup(func() { cleanupTestGCSStore(t, store) })
		return store
	})
}

func TestBlobStoreResourceCleanup(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if bucket == "" {
		t.Skip("DINGO_TEST_GCS_BUCKET not set")
	}

	// The same guards newTestGCSStore applies (backup_test.go), checked
	// once up front rather than via newTestGCSStore itself: GCS has no
	// prefix option, so this test's "k" key would otherwise silently
	// coexist with (or collide with) whatever an arbitrary, unconfigured
	// bucket already has in it.
	probe, err := NewWithOptions(WithBucket(bucket))
	require.NoError(t, err)
	require.NoError(t, probe.Start())
	empty, err := blobbackup.IsEmpty(context.Background(), probe)
	require.NoError(t, err)
	require.NoError(t, probe.Stop())
	if !empty {
		t.Skip(
			"DINGO_TEST_GCS_BUCKET is not empty -- use a bucket dedicated " +
				"to this test",
		)
	}

	// Deliberately not newTestGCSStore for the cycles below: that defers
	// Stop via t.Cleanup, which only fires once at the end of the whole
	// test, but this check's entire point is that each of the 5 cycles
	// fully stops before the next one starts (see
	// AssertRepeatedLifecycleIsSafe's doc comment) -- t.Cleanup timing
	// would silently turn that into "construct 5, then stop all 5," never
	// exercising Stop-then-reopen at all.
	storagetest.AssertRepeatedLifecycleIsSafe(t, 5, func(t *testing.T) {
		store, err := NewWithOptions(WithBucket(bucket))
		require.NoError(t, err)
		require.NoError(t, store.Start())
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, []byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		// GCS has no prefix option to isolate this key the way aws's
		// equivalent test does (see newTestS3Store's WithPrefix in
		// database/plugin/blob/aws/backup_test.go): delete it explicitly
		// so 5 cycles against a real, persistent bucket don't leave 5
		// copies of the same key behind, and so it can never collide with
		// another test/run sharing the bucket.
		deleteTxn := store.NewTransaction(true)
		require.NoError(t, store.Delete(deleteTxn, []byte("k")))
		require.NoError(t, deleteTxn.Commit())
		require.NoError(t, store.Stop())
	})
}

// TestBlobStoreBadCredentialsFailsCleanly needs no real credentials or
// network access: unlike the AWS SDK (which only builds a client in Start
// and defers all validation to first use, see
// aws.TestBlobStoreUnreachableEndpointFailsWithoutHanging), GCS's
// storage.NewGRPCClient loads and parses the credentials file eagerly, so
// pointing GOOGLE_APPLICATION_CREDENTIALS at a nonexistent file makes Start
// itself fail immediately. t.Setenv scopes the override to this test only.
func TestBlobStoreBadCredentialsFailsCleanly(t *testing.T) {
	// Not t.Parallel: changes process-wide credential and emulator settings.
	// Emulator clients bypass authentication, so this test must disable them.
	t.Setenv("STORAGE_EMULATOR_HOST_GRPC", "")
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Setenv(
		"GOOGLE_APPLICATION_CREDENTIALS",
		filepath.Join(t.TempDir(), "nonexistent-credentials.json"),
	)
	store, err := NewWithOptions(WithBucket("dingo-test"))
	require.NoError(t, err)
	require.Error(t, store.Start())
}

// newTestGCSStore is defined in backup_test.go and shared across this
// package's test files.

// cleanupTestGCSStore deletes every key currently in store. GCS has no
// prefix option to scope a test run the way aws.WithPrefix does (see
// newTestGCSStore's own doc comment in backup_test.go), so the whole
// bucket is the shared scope every test in this package must leave empty
// when it's done: newTestGCSStore requires an empty bucket up front and
// skips otherwise, so a committed key a test never removes poisons every
// subsequent run against the same dedicated bucket.
func cleanupTestGCSStore(t *testing.T, store *BlobStoreGCS) {
	t.Helper()
	txn := store.NewTransaction(true)
	defer txn.Rollback() //nolint:errcheck
	it := store.NewIterator(txn, types.BlobIteratorOptions{})
	require.NotNil(t, it)
	defer it.Close()
	for it.Valid() {
		if item := it.Item(); item != nil {
			// Unlike the S3 equivalent this is mirrored from (scoped to
			// one test's own unique prefix), GCS has no prefix option at
			// all, so the whole bucket is the shared scope every test in
			// this package depends on staying empty: a silently-ignored
			// Delete failure here would poison every later run against
			// the same dedicated bucket rather than just this one test.
			require.NoError(t, store.Delete(txn, item.Key()))
		}
		it.Next()
	}
	require.NoError(t, txn.Commit())
}

// TestBlobStoreGetBlockURLSignsCommittedBlock exercises the happy path no
// existing test in this package covers: a committed block's GetBlockURL
// returns a usable, parseable, non-expired presigned URL and the block's
// metadata. Note this needs signing capability beyond plain bucket access
// (a service account key, or IAM SignBlob permission for ADC
// impersonation) -- a plain user-account ADC that hasGCSCredentials accepts
// for the rest of this file's tests may not have it, in which case this
// fails with "gcs: failed to sign URL" rather than skipping.
func TestBlobStoreGetBlockURLSignsCommittedBlock(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	store := newTestGCSStore(t)
	// Registered after (so it runs before, via t.Cleanup's LIFO order,
	// while the store is still started) newTestGCSStore's own Stop
	// cleanup -- this test commits a block, unlike
	// TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock below, which
	// only stages and rolls back and so never touches the bucket.
	t.Cleanup(func() { cleanupTestGCSStore(t, store) })
	slot := uint64(500)
	hash := []byte("block-url-committed")

	writeTxn := store.NewTransaction(true)
	require.NoError(t, store.SetBlock(
		writeTxn, slot, hash, []byte{0x82, 0x01, 0x02}, 1, 0, 7, nil,
	))
	require.NoError(t, writeTxn.Commit())

	readTxn := store.NewTransaction(false)
	defer func() { require.NoError(t, readTxn.Rollback()) }()
	signed, meta, err := store.GetBlockURL(
		t.Context(),
		readTxn,
		ocommon.Point{Slot: slot, Hash: hash},
	)
	require.NoError(t, err)
	require.NotEmpty(t, signed.URL.String())
	require.True(t, signed.Expires.After(time.Now()))
	require.Equal(t, uint64(7), meta.Height)
}

// TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock exercises the
// documented contract in DATABASE.md's Cross-Store Durability Contract: "a
// block staged but not yet committed is reported as not found rather than
// signed into a URL that would 404." The staging check itself runs before
// any network call, but constructing a real GCS client still needs valid
// credentials, so this is gated the same as every other test here.
func TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	store := newTestGCSStore(t)
	slot := uint64(600)
	hash := []byte("block-url-staged")

	txn := store.NewTransaction(true)
	defer func() { require.NoError(t, txn.Rollback()) }()
	require.NoError(t, store.SetBlock(
		txn, slot, hash, []byte{0x82, 0x03, 0x04}, 2, 0, 8, nil,
	))

	_, _, err := store.GetBlockURL(
		t.Context(),
		txn,
		ocommon.Point{Slot: slot, Hash: hash},
	)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}

// TestGetBlockVerifiesContent proves GetBlock re-derives the block's hash
// from the returned bytes rather than trusting the (slot, hash) key alone:
// a genuine block round-trips, but content stored under a hash it does not
// actually hash to -- as a corrupted object, an eventual-consistency stale
// read, or a misdirected request could produce -- is rejected instead of
// being handed back to the caller as though it were the requested block.
func TestGetBlockVerifiesContent(t *testing.T) {
	blocks, err := fixtures.GenerateConwayChain(
		1, lcommon.Blake2b256{}, 1000, 10, 2,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	realBlock, otherBlock := blocks[0], blocks[1]

	store, err := NewWithOptions()
	require.NoError(t, err)
	store.client = new(storage.Client)
	store.bucket = new(storage.BucketHandle)

	t.Run("genuine content round-trips", func(t *testing.T) {
		hash := realBlock.Hash()
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 1000, hash[:], realBlock.Cbor(),
			1, uint(gledger.BlockTypeConway), 1, nil,
		))

		gotCbor, meta, err := store.GetBlock(txn, 1000, hash[:])
		require.NoError(t, err)
		require.Equal(t, realBlock.Cbor(), gotCbor)
		require.Equal(t, uint(gledger.BlockTypeConway), meta.Type)
	})

	t.Run("content for the wrong block is rejected", func(t *testing.T) {
		requestedHash := realBlock.Hash()
		txn := store.NewTransaction(true)
		// Stage otherBlock's bytes under realBlock's key, simulating a
		// remote store handing back the wrong object for the key.
		require.NoError(t, store.SetBlock(
			txn, 1010, requestedHash[:], otherBlock.Cbor(),
			2, uint(gledger.BlockTypeConway), 1, nil,
		))

		_, _, err := store.GetBlock(txn, 1010, requestedHash[:])
		require.ErrorIs(t, err, blockverify.ErrHashMismatch)
	})

	t.Run("synthetic ID=0 entries skip verification", func(t *testing.T) {
		// Mirrors Database.SetGenesisCbor: genesis UTxO CBOR and Leios
		// endorser-block manifests share this same bp/bp..._metadata key
		// layout with ID=0, but are not decodable ledger blocks, so
		// GetBlock must not attempt to verify them.
		notABlock := []byte("genesis UTxO CBOR, not a block")
		key := []byte("synthetic-key-hash")
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 2000, key, notABlock,
			0, 0, 0, nil,
		))

		gotCbor, meta, err := store.GetBlock(txn, 2000, key)
		require.NoError(t, err)
		require.Equal(t, notABlock, gotCbor)
		require.Equal(t, uint64(0), meta.ID)
	})

	t.Run("ID=0 with a nonzero type is still verified", func(t *testing.T) {
		// Only the exact (ID, Type) == (0, 0) synthetic marker skips
		// verification. A real chain block never has both zero, so this
		// proves an ID==0 row with a nonzero type -- however it got that
		// way -- still gets its content checked rather than silently
		// passing through as though it were a synthetic entry.
		requestedHash := realBlock.Hash()
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 3000, requestedHash[:], otherBlock.Cbor(),
			0, uint(gledger.BlockTypeConway), 1, nil,
		))

		_, _, err := store.GetBlock(txn, 3000, requestedHash[:])
		require.ErrorIs(t, err, blockverify.ErrHashMismatch)
	})
}

// TestStreamIteratorDefersListing pins that neither Rewind nor Seek contacts
// GCS. NewIterator rewinds the iterator it returns and every caller in
// database/ seeks straight afterwards, so a rewind that listed eagerly fetched
// a page from the head of the prefix and threw it away. database's block
// iterator reopens the iterator every batch of block keys, so it paid one such
// page per batch.
//
// A nil bucket is the proof: reaching GCS through it panics, so returning from
// Rewind and Seek is evidence that no listing was opened.
func TestStreamIteratorDefersListing(t *testing.T) {
	store := &BlobStoreGCS{}
	it := &gcsStreamIterator{
		store:  store,
		prefix: []byte(types.BlockBlobKeyPrefix),
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Rewind opened a listing: %v", r)
			}
		}()
		it.Rewind()
	}()
	if it.iter != nil {
		t.Fatal("Rewind opened a listing")
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Seek opened a listing: %v", r)
			}
		}()
		it.Seek([]byte("bp-somewhere"))
	}()
	if it.iter != nil {
		t.Fatal("Seek opened a listing")
	}
	if it.err != nil {
		t.Fatalf("Seek: %v", it.err)
	}
}

// TestStreamIteratorCloseStopsListing pins that a closed iterator stays closed.
// The listing is deferred to the first read, so Valid, Err, and Next after
// Close must not open one. A nil bucket panics if they do.
func TestStreamIteratorCloseStopsListing(t *testing.T) {
	store := &BlobStoreGCS{}
	it := &gcsStreamIterator{
		store:  store,
		prefix: []byte(types.BlockBlobKeyPrefix),
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Rewind opened a listing: %v", r)
			}
		}()
		it.Rewind()
	}()
	it.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("reading a closed iterator opened a listing: %v", r)
		}
	}()
	if it.Valid() {
		t.Fatal("closed iterator reports a valid position")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("closed iterator: %v", err)
	}
	it.Next()
}

func TestStreamIteratorSeekAfterCloseRearmsListing(t *testing.T) {
	it := &gcsStreamIterator{}
	it.Close()
	it.Seek([]byte("bp-resume"))
	if it.closed || it.started || string(it.pendingStart) != "bp-resume" {
		t.Fatalf("Seek after Close did not rearm listing: %+v", it)
	}
}

func TestReadBlobObjectWithLimit(t *testing.T) {
	data, err := readBlobObjectWithLimit(strings.NewReader("123"), 3)
	if err != nil || string(data) != "123" {
		t.Fatalf("read within limit = %q, %v", data, err)
	}
	if _, err := readBlobObjectWithLimit(strings.NewReader("1234"), 3); err == nil {
		t.Fatal("oversized object should be rejected")
	}
}

func TestGcsTransactionStagesAndRollsBack(t *testing.T) {
	txn := &gcsTxn{pending: make(map[string]gcsPendingChange)}
	txn.stageSet([]byte("key"), []byte("value"))
	value, deleted, staged := txn.stagedValue([]byte("key"))
	if !staged || deleted || string(value) != "value" {
		t.Fatalf(
			"staged value = %q, deleted=%v, staged=%v",
			value,
			deleted,
			staged,
		)
	}
	txn.stageDelete([]byte("key"))
	value, deleted, staged = txn.stagedValue([]byte("key"))
	if !staged || !deleted || value != nil {
		t.Fatalf(
			"staged delete = %q, deleted=%v, staged=%v",
			value,
			deleted,
			staged,
		)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if txn.pending != nil {
		t.Fatal("rollback should discard pending changes")
	}
}

func TestReverseKeyFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	f := &reverseKeyFile{file: file}
	for _, key := range []string{"a", "b", "c"} {
		if err := writeReverseKey(file, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"c", "b", "a"} {
		got, valid, err := f.nextReverse()
		if err != nil || !valid || got != want {
			t.Fatalf("reverse key = %q, %v, %v", got, valid, err)
		}
	}
	if _, valid, err := f.nextReverse(); err != nil || valid {
		t.Fatalf(
			"reverse iterator should be exhausted: valid=%v err=%v",
			valid,
			err,
		)
	}
	file.Close()
}

// Corrupt local spool framing must never yield a key to the cloud iterator.
func TestReverseKeyFileRejectsMalformedRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short header", []byte{0, 0, 0}},
		{"missing prefix", []byte{0, 0, 0, 4}},
		{"truncated payload", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 2}},
		{"mismatched lengths", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "reverse-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write(tc.data); err != nil {
				t.Fatal(err)
			}
			f := &reverseKeyFile{file: file}
			key, valid, err := f.nextReverse()
			if err == nil || valid || key != "" {
				t.Fatalf(
					"malformed spool yielded key %q, valid=%v, err=%v",
					key,
					valid,
					err,
				)
			}
		})
	}
}

func TestReverseKeyFileEmptyAndBinaryKeys(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	keys := []string{
		"",
		string(binary.BigEndian.AppendUint32(nil, 0xffffffff)),
		"last",
	}
	for _, key := range keys {
		if err := writeReverseKey(file, key); err != nil {
			t.Fatal(err)
		}
	}
	f := &reverseKeyFile{file: file}
	for i := len(keys) - 1; i >= 0; i-- {
		key, valid, err := f.nextReverse()
		if err != nil || !valid || key != keys[i] {
			t.Fatalf(
				"reverse key = %q, valid=%v, err=%v; want %q",
				key,
				valid,
				err,
				keys[i],
			)
		}
	}
	if _, valid, err := f.nextReverse(); err != nil || valid {
		t.Fatalf("exhausted iterator: valid=%v err=%v", valid, err)
	}
}

func TestReverseIteratorPropagatesSpoolCorruption(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := writeReverseKey(file, "a"); err != nil {
		t.Fatal(err)
	}
	// The trailer still declares one byte, but the prefix declares two.
	if _, err := file.WriteAt([]byte{0, 0, 0, 2}, 0); err != nil {
		t.Fatal(err)
	}
	it := &gcsReverseIterator{keys: &reverseKeyFile{file: file}}
	it.Rewind()
	if it.Err() == nil || it.Valid() || it.Item() != nil {
		t.Fatalf(
			"corrupted spool iterator: valid=%v err=%v",
			it.Valid(),
			it.Err(),
		)
	}
}

// resolveKey is the single read path shared by Get and every typed getter. A
// value staged by this transaction has to win over the bucket, otherwise a
// read-after-write inside one transaction returns pre-transaction state and the
// plugin diverges from badger. The bucket is never consulted for a staged key,
// so a nil client is proof the read was served from the staging map.
func TestResolveKeyServesStagedWrite(t *testing.T) {
	store := &BlobStoreGCS{}
	txn := &gcsTxn{store: store, pending: make(map[string]gcsPendingChange)}
	txn.stageSet([]byte("key"), []byte("staged"))

	value, err := store.resolveKey(context.Background(), txn, []byte("key"))
	if err != nil {
		t.Fatalf("resolveKey on a staged write: %v", err)
	}
	if string(value) != "staged" {
		t.Fatalf("resolveKey = %q, want %q", value, "staged")
	}
}

// A staged delete has to read as missing for the rest of the transaction.
func TestResolveKeyReportsStagedDeleteAsMissing(t *testing.T) {
	store := &BlobStoreGCS{}
	txn := &gcsTxn{store: store, pending: make(map[string]gcsPendingChange)}
	txn.stageDelete([]byte("key"))

	_, err := store.resolveKey(context.Background(), txn, []byte("key"))
	if !errors.Is(err, types.ErrBlobKeyNotFound) {
		t.Fatalf(
			"resolveKey on a staged delete = %v, want ErrBlobKeyNotFound",
			err,
		)
	}
}

// Iterators must not list a key this transaction has staged for deletion: the
// value path resolves staged changes, so listing it would surface a key whose
// value immediately reads back as missing.
func TestStagedDeletedFiltersIteratorKeys(t *testing.T) {
	store := &BlobStoreGCS{}
	txn := &gcsTxn{store: store, pending: make(map[string]gcsPendingChange)}
	txn.stageDelete([]byte("gone"))
	txn.stageSet([]byte("kept"), []byte("value"))

	if !stagedDeleted(txn, "gone") {
		t.Fatal("a staged delete should be filtered from listings")
	}
	if stagedDeleted(txn, "kept") {
		t.Fatal("a staged write must not be filtered from listings")
	}
	if stagedDeleted(txn, "untouched") {
		t.Fatal("an unstaged key must not be filtered from listings")
	}

	// A finished transaction has no staged state to honor, and a foreign txn
	// type must not panic the iterator.
	txn.finished = true
	if stagedDeleted(txn, "gone") {
		t.Fatal("a finished transaction should not filter listings")
	}
	if stagedDeleted(nil, "gone") {
		t.Fatal("a nil transaction should not filter listings")
	}
}

// A zero-length write is a real value, not a deletion. Collapsing the two would
// make Set of an empty blob read back as ErrBlobKeyNotFound until commit.
func TestResolveKeyServesStagedEmptyValue(t *testing.T) {
	st := &BlobStoreGCS{}
	txn := &gcsTxn{store: st, pending: make(map[string]gcsPendingChange)}
	txn.stageSet([]byte("key"), []byte{})

	value, deleted, staged := txn.stagedValue([]byte("key"))
	if !staged || deleted {
		t.Fatalf(
			"empty write should be staged and not deleted, got deleted=%v staged=%v",
			deleted,
			staged,
		)
	}
	if value == nil || len(value) != 0 {
		t.Fatalf("staged empty value = %v, want an empty non-nil slice", value)
	}

	got, err := st.resolveKey(context.Background(), txn, []byte("key"))
	if err != nil {
		t.Fatalf("resolveKey on a staged empty value: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("resolveKey = %q, want empty", got)
	}

	// Iterators must still list it: it is a write, not a delete.
	if stagedDeleted(txn, "key") {
		t.Fatal("a staged empty value must not be filtered from listings")
	}
}

// RollbackIsNoop must report false now that mutations are staged and applied
// only in Commit: Rollback discards the staged work without issuing any GCS
// request. It reported true when Set/Delete wrote through immediately, and
// database/lifecycle/blob_bulk_delete.go reads this flag to decide whether a
// failed batch's deletes are permanent — reporting true made a truncate count
// blocks it never removed.
func TestRollbackIsNoopReportsFalse(t *testing.T) {
	txn := &gcsTxn{pending: make(map[string]gcsPendingChange)}
	if txn.RollbackIsNoop() {
		t.Fatal(
			"staged transactions are reversible: Rollback issues no requests",
		)
	}

	// Rollback really does discard staged work rather than applying it.
	txn.stageSet([]byte("key"), []byte("value"))
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if txn.pending != nil {
		t.Fatal("rollback should discard pending changes")
	}
}

func TestTombstoneBlockRetainsMetadata(t *testing.T) {
	store, err := NewWithOptions()
	require.NoError(t, err)
	store.client = new(storage.Client)
	store.bucket = new(storage.BucketHandle)
	txn := store.NewTransaction(true)
	slot := uint64(42)
	hash := []byte("gcs-tombstone-metadata")

	require.NoError(t, store.SetBlock(
		txn, slot, hash, []byte{0x80}, 7, 1, 6, nil,
	))
	require.NoError(t, store.TombstoneBlock(txn, slot, hash))

	_, metadata, err := store.GetBlock(txn, slot, hash)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
	require.Equal(t, uint64(7), metadata.ID)
}
