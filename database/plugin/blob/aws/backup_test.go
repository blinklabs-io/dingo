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
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWriteReadRecordRoundTrip validates that writeRecord/readRecord's
// length-prefixed framing preserves keys and values exactly (including a
// zero-length value), and that reading past the last record reports a
// clean io.EOF rather than a spurious error.
func TestWriteReadRecordRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(
		t,
		writeRecord(&buf, []byte("key-one"), []byte("value-one")),
	)
	require.NoError(t, writeRecord(&buf, []byte("key-two"), []byte{}))

	key, value, err := readRecord(&buf)
	require.NoError(t, err)
	require.Equal(t, []byte("key-one"), key)
	require.Equal(t, []byte("value-one"), value)

	key, value, err = readRecord(&buf)
	require.NoError(t, err)
	require.Equal(t, []byte("key-two"), key)
	require.Empty(t, value)

	_, _, err = readRecord(&buf)
	require.ErrorIs(t, err, io.EOF)
}

// TestReadRecordRejectsOversizedKeyLength validates that a declared key
// length above maxBackupKeyLen is rejected before any allocation sized by
// that untrusted length, guarding against a corrupted or adversarial
// backup stream driving an oversized allocation.
func TestReadRecordRejectsOversizedKeyLength(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], maxBackupKeyLen+1)
	buf.Write(lenBuf[:])
	_, _, err := readRecord(&buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestReadRecordRejectsOversizedValueLength validates that a declared
// value length above maxBlobReadBytes is rejected before any allocation
// sized by that untrusted length, the value-side counterpart of the
// key-length check above.
func TestReadRecordRejectsOversizedValueLength(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeRecord(io.Discard, []byte("k"), nil))
	var keyLenBuf [4]byte
	binary.BigEndian.PutUint32(keyLenBuf[:], 1)
	buf.Write(keyLenBuf[:])
	buf.WriteByte('k')
	var valLenBuf [8]byte
	binary.BigEndian.PutUint64(valLenBuf[:], uint64(maxBlobReadBytes)+1)
	buf.Write(valLenBuf[:])
	_, _, err := readRecord(&buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestReadRecordRejectsTruncatedStream validates that a record cut short
// mid-value produces a real error, not an io.EOF that could be mistaken
// for a clean end of the backup stream.
func TestReadRecordRejectsTruncatedStream(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeRecord(&buf, []byte("key"), []byte("value")))
	truncated := buf.Bytes()[:buf.Len()-2]
	_, _, err := readRecord(bytes.NewReader(truncated))
	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF)
}

// TestContextWriterStopsOnCancellation validates that contextWriter checks
// ctx before each Write, so cancelling mid-Backup stops further output
// instead of running to completion regardless of cancellation.
func TestContextWriterStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := &contextWriter{ctx: ctx, w: io.Discard}
	_, err := w.Write([]byte("x"))
	require.NoError(t, err)
	cancel()
	_, err = w.Write([]byte("x"))
	require.ErrorIs(t, err, context.Canceled)
}

// TestContextReaderStopsOnCancellation is contextWriter's Restore-side
// counterpart: validates contextReader checks ctx before each Read, so
// cancelling mid-Restore stops further reads instead of consuming the
// entire stream regardless of cancellation.
func TestContextReaderStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &contextReader{ctx: ctx, r: bytes.NewReader([]byte("xxxxxxxx"))}
	buf := make([]byte, 4)
	_, err := r.Read(buf)
	require.NoError(t, err)
	cancel()
	_, err = r.Read(buf)
	require.ErrorIs(t, err, context.Canceled)
}

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
	return store
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
	require.NoError(t, writeRecord(&buf, []byte("k"), []byte("v")))
	err := store.Restore(context.Background(), bytes.NewReader(
		append(append(backupMagic[:], backupVersion), buf.Bytes()...),
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
