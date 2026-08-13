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

//go:build dingo_extra_plugins

package gcs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blobbackup"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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
