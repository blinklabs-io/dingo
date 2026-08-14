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

package aws

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// hasS3Credentials is defined in backup_test.go and shared across this
// package's test files.

func TestBlobStoreConformance(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
		t.Helper()
		opts := []BlobStoreS3OptionFunc{
			WithBucket(bucket),
			WithRegion(region),
			// Isolate this run's keys from any other test/CI run sharing
			// the bucket. t.Name() alone is not enough here -- it is
			// always "TestBlobStoreConformance" on this shared outer test,
			// so every invocation would otherwise write and delete under
			// the exact same prefix; a run interrupted before its own
			// cleanup runs (a killed CI job, a panic, a machine restart)
			// would leave objects the next run then reads, corrupting
			// assertions that expect to start from an empty prefix (e.g.
			// IteratorEnumeratesWrittenKeys). time.Now().UnixNano()
			// matches the per-run suffix already used for the
			// GetBlockURL tests below and benchmark_test.go.
			WithPrefix(fmt.Sprintf(
				"storagetest-conformance-%d/",
				time.Now().UnixNano(),
			)),
		}
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			opts = append(opts, WithEndpoint(endpoint))
		}
		store, err := NewWithOptions(opts...)
		require.NoError(t, err)
		require.NoError(t, store.Start())
		t.Cleanup(func() {
			require.NoError(t, store.Stop())
		})
		// Registered after (so it runs before, via t.Cleanup's LIFO order,
		// while the store is still started) Stop's own cleanup, matching
		// newTestS3Store's convention in backup_test.go: this suite commits
		// real data under the prefix above and would otherwise accumulate
		// every run's objects in a real, persistent bucket.
		t.Cleanup(func() { cleanupTestS3Store(t, store) })
		return store
	})
}

func TestBlobStoreResourceCleanup(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}

	storagetest.AssertRepeatedLifecycleIsSafe(t, 5, func(t *testing.T) {
		// t.Name() is always "TestBlobStoreResourceCleanup" on this shared
		// outer test, not a per-cycle or per-run value -- see the identical
		// fix's comment on TestBlobStoreConformance's prefix above for why
		// that fails to isolate concurrent/interrupted runs.
		opts := []BlobStoreS3OptionFunc{
			WithBucket(bucket),
			WithPrefix(fmt.Sprintf(
				"storagetest-resource-cleanup-%d/",
				time.Now().UnixNano(),
			)),
		}
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			opts = append(opts, WithEndpoint(endpoint))
		}
		store, err := NewWithOptions(opts...)
		require.NoError(t, err)
		require.NoError(t, store.Start())
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, []byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		// Delete what this cycle committed while the store is still
		// started, so 5 cycles against a real, persistent bucket don't
		// accumulate 5 copies of the same key under 5 different prefixes.
		cleanupTestS3Store(t, store)
		require.NoError(t, store.Stop())
	})
}

// TestBlobStoreUnreachableEndpointFailsWithoutHanging needs no credentials
// or running server: it points at a closed local port so the operation
// fails with a connection error rather than reaching any real endpoint.
// Start itself does not probe connectivity (the SDK only builds a client),
// so the failure surfaces on first use; this asserts it surfaces quickly
// and as an error, not a hang or a panic.
func TestBlobStoreUnreachableEndpointFailsWithoutHanging(t *testing.T) {
	store, err := NewWithOptions(
		WithBucket("dingo-test"),
		WithRegion("us-east-1"),
		WithEndpoint("http://127.0.0.1:1/"),
		// Bounds the SDK's own retry/backoff so the test fails fast instead
		// of waiting out the 60s default.
		WithTimeout(3*time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
	})

	start := time.Now()
	txn := store.NewTransaction(false)
	_, err = store.Get(txn, []byte("k"))
	require.Error(t, err)
	require.NoError(t, txn.Rollback())
	require.Less(
		t,
		time.Since(start),
		10*time.Second,
		"an unreachable endpoint should fail within the configured "+
			"operation timeout, not hang",
	)
}

// TestBlobStoreBadCredentialsFailsCleanly is gated on a real, reachable
// endpoint being configured (the same convention as every other test in this
// file) because it needs a server that actually rejects the credentials --
// pointing at nothing would just repeat
// TestBlobStoreUnreachableEndpointFailsWithoutHanging. t.Setenv scopes the
// deliberately wrong credentials to this test only.
func TestBlobStoreBadCredentialsFailsCleanly(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "storagetest-invalid-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "storagetest-invalid-secret-key")

	opts := []BlobStoreS3OptionFunc{
		WithBucket(bucket),
		WithRegion("us-east-1"),
		WithTimeout(10 * time.Second),
	}
	if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}
	store, err := NewWithOptions(opts...)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
	})

	txn := store.NewTransaction(false)
	_, err = store.Get(txn, []byte("k"))
	require.Error(t, err)
	require.NoError(t, txn.Rollback())
}

// newTestS3Store is defined in backup_test.go and shared across this
// package's test files.

// TestBlobStoreGetBlockURLSignsCommittedBlock exercises the happy path no
// existing test in this package covers: a committed block's GetBlockURL
// returns a usable, parseable, non-expired presigned URL and the block's
// metadata.
func TestBlobStoreGetBlockURLSignsCommittedBlock(t *testing.T) {
	store := newTestS3Store(
		t,
		fmt.Sprintf("block-url-signs-%d/", time.Now().UnixNano()),
	)
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
// any network call, but constructing a real S3 client still needs a
// reachable endpoint, so this is gated the same as every other test here.
func TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock(t *testing.T) {
	store := newTestS3Store(
		t,
		fmt.Sprintf("block-url-staged-%d/", time.Now().UnixNano()),
	)
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
