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
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blobbackup"
	"github.com/stretchr/testify/require"
)

// hasGCSCredentials mirrors internal/integration/cloud_test.go's helper of
// the same purpose, scoped locally so this package's tests can skip
// cleanly without real GCS credentials -- there is currently no live GCS
// emulator in CI (unlike S3, which CI covers via a MinIO service), so this
// suite only runs where an operator supplies real credentials manually.
func hasGCSCredentials() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		adcPath := filepath.Join(
			home, ".config", "gcloud", "application_default_credentials.json",
		)
		if _, err := os.Stat(adcPath); err == nil {
			return true
		}
	}
	return false
}

// newTestGCSStore requires DINGO_TEST_GCS_BUCKET to point at a bucket
// dedicated to this test, since the GCS plugin (unlike S3) has no
// configurable key prefix to isolate a test run's keys from anything else
// in the bucket -- this skips rather than risk disrupting an operator's
// real bucket if it isn't already empty.
func newTestGCSStore(t *testing.T) *BlobStoreGCS {
	t.Helper()
	if !hasGCSCredentials() {
		t.Skip(
			"no GCS credentials configured (GOOGLE_APPLICATION_CREDENTIALS or gcloud ADC)",
		)
	}
	bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if bucket == "" {
		t.Skip("DINGO_TEST_GCS_BUCKET not set")
	}
	store, err := NewWithOptions(
		WithBucket(bucket),
		WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
	)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() { _ = store.Stop() })
	empty, err := blobbackup.IsEmpty(context.Background(), store)
	require.NoError(t, err)
	if !empty {
		t.Skip(
			"DINGO_TEST_GCS_BUCKET is not empty -- use a bucket dedicated to this test",
		)
	}
	return store
}

// TestBackupRestoreRoundTrip validates the full round trip against a real
// GCS bucket: Backup streams every key/value out of the store with known
// data, and Restore back into the same (necessarily emptied first, since
// GCS has no key-prefix isolation) bucket reproduces that same data
// exactly.
func TestBackupRestoreRoundTrip(t *testing.T) {
	src := newTestGCSStore(t)
	txn := src.NewTransaction(true)
	require.NoError(t, src.Set(txn, []byte("key-a"), []byte("value-a")))
	require.NoError(t, src.Set(txn, []byte("key-b"), []byte("value-b")))
	require.NoError(t, txn.Commit())
	t.Cleanup(func() {
		cleanup := src.NewTransaction(true)
		_ = src.Delete(cleanup, []byte("key-a"))
		_ = src.Delete(cleanup, []byte("key-b"))
		_ = cleanup.Commit()
	})

	var buf bytes.Buffer
	require.NoError(t, src.Backup(context.Background(), &buf))

	// Delete the source keys before restoring into the same (necessarily
	// shared, since GCS has no key-prefix isolation) bucket, so Restore's
	// own "must be empty" precondition holds.
	del := src.NewTransaction(true)
	require.NoError(t, src.Delete(del, []byte("key-a")))
	require.NoError(t, src.Delete(del, []byte("key-b")))
	require.NoError(t, del.Commit())

	require.NoError(t, src.Restore(context.Background(), &buf))
	t.Cleanup(func() {
		cleanup := src.NewTransaction(true)
		_ = src.Delete(cleanup, []byte("key-a"))
		_ = src.Delete(cleanup, []byte("key-b"))
		_ = cleanup.Commit()
	})

	readTxn := src.NewTransaction(false)
	defer readTxn.Rollback() //nolint:errcheck
	value, err := src.Get(readTxn, []byte("key-a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), value)
	value, err = src.Get(readTxn, []byte("key-b"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-b"), value)
}

// TestRestoreRejectsNonEmptyStore validates that Restore refuses to run
// against a real bucket that already has a key in it, instead of merging
// the backup's contents into whatever is already there.
func TestRestoreRejectsNonEmptyStore(t *testing.T) {
	store := newTestGCSStore(t)
	txn := store.NewTransaction(true)
	require.NoError(t, store.Set(txn, []byte("existing"), []byte("value")))
	require.NoError(t, txn.Commit())
	t.Cleanup(func() {
		cleanup := store.NewTransaction(true)
		_ = store.Delete(cleanup, []byte("existing"))
		_ = cleanup.Commit()
	})

	var buf bytes.Buffer
	require.NoError(
		t,
		blobbackup.WriteRecord(&buf, []byte("k"), []byte("v"), maxBlobReadBytes),
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
	store := newTestGCSStore(t)
	err := store.Restore(context.Background(), bytes.NewReader([]byte("nope!")))
	require.Error(t, err)
}
