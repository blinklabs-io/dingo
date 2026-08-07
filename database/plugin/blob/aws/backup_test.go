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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blobbackup"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// hasS3Credentials mirrors internal/integration/cloud_test.go's helper of
// the same purpose, scoped locally so this package's tests can skip
// cleanly without a live S3/MinIO backend.
func hasS3Credentials() bool {
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" &&
		os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		if _, err := os.Stat(filepath.Join(home, ".aws", "credentials")); err == nil {
			return true
		}
	}
	return false
}

func newTestS3Store(t *testing.T, prefix string) *BlobStoreS3 {
	t.Helper()
	if !hasS3Credentials() {
		t.Skip(
			"no S3 credentials configured (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or ~/.aws/credentials)",
		)
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	opts := []BlobStoreS3OptionFunc{
		WithBucket(bucket),
		WithRegion(region),
		WithPrefix(prefix),
		WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
	}
	if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}
	store, err := NewWithOptions(opts...)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() { _ = store.Stop() })
	// Registered after (so it runs before, via t.Cleanup's LIFO order,
	// while the store is still started) Stop's own cleanup: CI's MinIO
	// container is destroyed after each job and needs no cleanup, but a
	// developer running these tests locally against a real, persistent
	// bucket (the ~/.aws/credentials fallback above exists for exactly
	// that) would otherwise accumulate every run's objects in it forever,
	// since Stop itself does nothing to the bucket's actual contents.
	t.Cleanup(func() { cleanupTestS3Store(t, store) })
	return store
}

// cleanupTestS3Store deletes every key currently under store's own unique
// prefix -- never anything outside it, so this can't touch another test's
// or another run's keys sharing the same bucket.
func cleanupTestS3Store(t *testing.T, store *BlobStoreS3) {
	t.Helper()
	txn := store.NewTransaction(true)
	defer txn.Rollback() //nolint:errcheck
	it := store.NewIterator(txn, types.BlobIteratorOptions{})
	if it == nil {
		return
	}
	defer it.Close()
	for it.Valid() {
		if item := it.Item(); item != nil {
			_ = store.Delete(txn, item.Key())
		}
		it.Next()
	}
	_ = txn.Commit()
}

// TestBackupRestoreRoundTrip validates the full round trip against a real
// S3-compatible backend: Backup streams every key/value out of a store
// with known data, and Restore into a separate empty store reproduces
// that same data exactly.
func TestBackupRestoreRoundTrip(t *testing.T) {
	src := newTestS3Store(
		t,
		fmt.Sprintf("backup-src-%d/", time.Now().UnixNano()),
	)
	txn := src.NewTransaction(true)
	require.NoError(t, src.Set(txn, []byte("key-a"), []byte("value-a")))
	require.NoError(t, src.Set(txn, []byte("key-b"), []byte("value-b")))
	require.NoError(t, txn.Commit())

	var buf bytes.Buffer
	require.NoError(t, src.Backup(context.Background(), &buf))

	dst := newTestS3Store(
		t,
		fmt.Sprintf("backup-dst-%d/", time.Now().UnixNano()),
	)
	require.NoError(t, dst.Restore(context.Background(), &buf))

	readTxn := dst.NewTransaction(false)
	defer readTxn.Rollback() //nolint:errcheck
	value, err := dst.Get(readTxn, []byte("key-a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), value)
	value, err = dst.Get(readTxn, []byte("key-b"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-b"), value)
}

// TestRestoreRejectsNonEmptyStore validates that Restore refuses to run
// against a real store that already has a key in it, instead of merging
// the backup's contents into whatever is already there.
func TestRestoreRejectsNonEmptyStore(t *testing.T) {
	store := newTestS3Store(
		t,
		fmt.Sprintf("backup-nonempty-%d/", time.Now().UnixNano()),
	)
	txn := store.NewTransaction(true)
	require.NoError(t, store.Set(txn, []byte("existing"), []byte("value")))
	require.NoError(t, txn.Commit())

	var buf bytes.Buffer
	require.NoError(
		t,
		blobbackup.WriteRecord(
			&buf,
			[]byte("k"),
			[]byte("v"),
			maxBlobReadBytes,
		),
	)
	err := store.Restore(context.Background(), bytes.NewReader(
		append(
			append(blobbackup.Magic[:], blobbackup.Version),
			buf.Bytes()...,
		),
	))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "already contains data"))
}

// TestRestoreRejectsUnknownMagic validates that Restore rejects a stream
// that doesn't start with this format's own magic header, rather than
// misinterpreting arbitrary data as backup records.
func TestRestoreRejectsUnknownMagic(t *testing.T) {
	store := newTestS3Store(
		t,
		fmt.Sprintf("backup-badmagic-%d/", time.Now().UnixNano()),
	)
	err := store.Restore(context.Background(), bytes.NewReader([]byte("nope!")))
	require.Error(t, err)
}
