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

package blobbackup

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// testMaxValueLen mirrors the 256MiB bound both cloud blob plugins actually
// configure (their own maxBlobReadBytes); kept as a local constant here
// since production limits are each plugin's own concern, not this shared
// package's.
const testMaxValueLen int64 = 256 << 20

// TestWriteReadRecordRoundTrip validates that WriteRecord/ReadRecord's
// length-prefixed framing preserves keys and values exactly (including a
// zero-length value), and that reading past the last record reports a
// clean io.EOF once (and only once) the terminator written by
// writeTerminator has actually been read.
func TestWriteReadRecordRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	checksum := crc32.NewIEEE()
	writeBoth := func(key, value []byte) {
		require.NoError(t, WriteRecord(&buf, key, value, testMaxValueLen))
		require.NoError(t, WriteRecord(checksum, key, value, testMaxValueLen))
	}
	writeBoth([]byte("key-one"), []byte("value-one"))
	writeBoth([]byte("key-two"), []byte{})
	require.NoError(t, writeTerminator(&buf, 2, checksum.Sum32()))

	key, value, err := ReadRecord(&buf, testMaxValueLen)
	require.NoError(t, err)
	require.Equal(t, []byte("key-one"), key)
	require.Equal(t, []byte("value-one"), value)

	key, value, err = ReadRecord(&buf, testMaxValueLen)
	require.NoError(t, err)
	require.Equal(t, []byte("key-two"), key)
	require.Empty(t, value)

	_, _, err = ReadRecord(&buf, testMaxValueLen)
	require.ErrorIs(t, err, io.EOF)
	term, ok := errors.AsType[*ErrTerminator](err)
	require.True(t, ok)
	require.NotNil(t, term)
	require.Equal(t, uint64(2), term.RecordCount)
	require.Equal(t, checksum.Sum32(), term.Checksum)
}

// TestReadRecordRejectsMissingTerminator guards the real gap this format's
// terminator marker exists to close: a backup file truncated exactly at a
// record boundary (a partial copy, a storage-layer truncation) used to read
// back as a clean io.EOF -- indistinguishable from a genuinely complete
// backup -- silently dropping every key after the cut. Without a
// terminator, ReadRecord must now report a real error instead.
func TestReadRecordRejectsMissingTerminator(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(
		t,
		WriteRecord(&buf, []byte("key-one"), []byte("value-one"), testMaxValueLen),
	)
	// No writeTerminator call: this is exactly what a file truncated right
	// after a complete record looks like.

	_, _, err := ReadRecord(&buf, testMaxValueLen)
	require.NoError(t, err, "the one complete record still reads back fine")

	_, _, err = ReadRecord(&buf, testMaxValueLen)
	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF)
	require.Contains(t, err.Error(), "terminator")
}

// TestReadRecordRejectsOversizedKeyLength validates that a declared key
// length above MaxKeyLen is rejected before any allocation sized by that
// untrusted length, guarding against a corrupted or adversarial backup
// stream driving an oversized allocation.
func TestReadRecordRejectsOversizedKeyLength(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], MaxKeyLen+1)
	buf.Write(lenBuf[:])
	_, _, err := ReadRecord(&buf, testMaxValueLen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestReadRecordRejectsOversizedValueLength validates that a declared
// value length above maxValueLen is rejected before any allocation sized
// by that untrusted length, the value-side counterpart of the key-length
// check above.
func TestReadRecordRejectsOversizedValueLength(t *testing.T) {
	var buf bytes.Buffer
	var keyLenBuf [4]byte
	binary.BigEndian.PutUint32(keyLenBuf[:], 1)
	buf.Write(keyLenBuf[:])
	buf.WriteByte('k')
	var valLenBuf [8]byte
	binary.BigEndian.PutUint64(valLenBuf[:], uint64(testMaxValueLen)+1)
	buf.Write(valLenBuf[:])
	_, _, err := ReadRecord(&buf, testMaxValueLen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestWriteRecordRejectsOversizedValue guards a real gap: ReadRecord
// rejects a declared value length over maxValueLen, but WriteRecord itself
// only ever bounded the key -- an oversized value would write
// successfully, producing a backup file that is silently guaranteed to
// fail every future Restore instead of failing loudly at Backup time.
func TestWriteRecordRejectsOversizedValue(t *testing.T) {
	oversized := make([]byte, testMaxValueLen+1)
	err := WriteRecord(io.Discard, []byte("k"), oversized, testMaxValueLen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestReadRecordRejectsTruncatedStream validates that a record cut short
// mid-value produces a real error, not an io.EOF that could be mistaken
// for a clean end of the backup stream.
func TestReadRecordRejectsTruncatedStream(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(
		t,
		WriteRecord(&buf, []byte("key"), []byte("value"), testMaxValueLen),
	)
	truncated := buf.Bytes()[:buf.Len()-2]
	_, _, err := ReadRecord(bytes.NewReader(truncated), testMaxValueLen)
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

// TestPartialDataWarning validates the two states an operator needs to be
// able to tell apart from a failed Restore's error text alone: zero
// committed batches means the store is still untouched (empty string, no
// special handling needed), while any nonzero count means the store now
// holds real, un-undoable data and the message must say so explicitly.
func TestPartialDataWarning(t *testing.T) {
	require.Empty(t, partialDataWarning(0))

	msg := partialDataWarning(3)
	require.Contains(t, msg, "3 batch")
	require.Contains(t, msg, "discarded")
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

// fakeBlobItem/fakeIterator/fakeTxn/fakeStore are a minimal in-memory Store
// implementation, just enough to drive Backup/Restore end to end without a
// real S3/GCS backend, for tests that need to exercise the terminator's
// corruption-detection behavior specifically (a real store isn't available
// in this package's unit tests, and standing one up wouldn't add anything
// -- this failure mode lives entirely in the stream bytes, not the store).
type fakeBlobItem struct {
	key   []byte
	value []byte
}

func (i *fakeBlobItem) Key() []byte { return i.key }

func (i *fakeBlobItem) ValueCopy(dst []byte) ([]byte, error) {
	return append(dst[:0], i.value...), nil
}

type fakeIterator struct {
	items []*fakeBlobItem
	pos   int
}

func (it *fakeIterator) Rewind()                    { it.pos = 0 }
func (it *fakeIterator) Seek([]byte)                {}
func (it *fakeIterator) Valid() bool                { return it.pos < len(it.items) }
func (it *fakeIterator) ValidForPrefix([]byte) bool { return it.Valid() }
func (it *fakeIterator) Next()                      { it.pos++ }
func (it *fakeIterator) Close()                     {}
func (it *fakeIterator) Err() error                 { return nil }

func (it *fakeIterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return it.items[it.pos]
}

type fakeTxn struct {
	store   *fakeStore
	pending map[string][]byte
}

func (t *fakeTxn) Commit() error {
	maps.Copy(t.store.data, t.pending)
	return nil
}

func (t *fakeTxn) Rollback() error { return nil }

type fakeStore struct {
	data map[string][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: map[string][]byte{}}
}

func (s *fakeStore) NewTransaction(bool) types.Txn {
	return &fakeTxn{store: s, pending: map[string][]byte{}}
}

func (s *fakeStore) NewIterator(
	types.Txn,
	types.BlobIteratorOptions,
) types.BlobIterator {
	items := make([]*fakeBlobItem, 0, len(s.data))
	for k, v := range s.data {
		items = append(items, &fakeBlobItem{key: []byte(k), value: v})
	}
	return &fakeIterator{items: items}
}

func (s *fakeStore) Set(txn types.Txn, key, value []byte) error {
	ft, ok := txn.(*fakeTxn)
	if !ok {
		return errors.New("blobbackup test: wrong txn type")
	}
	ft.pending[string(key)] = append([]byte{}, value...)
	return nil
}

// TestBackupRestoreRoundTripFake validates a full Backup-then-Restore round
// trip against the fake store, confirming Restore accepts a genuine
// terminator (correct record count and checksum) produced by a real Backup
// call, not just that ReadRecord's framing parses.
func TestBackupRestoreRoundTripFake(t *testing.T) {
	src := newFakeStore()
	txn := src.NewTransaction(true)
	require.NoError(t, src.Set(txn, []byte("key-a"), []byte("value-a")))
	require.NoError(t, txn.Commit())

	var buf bytes.Buffer
	require.NoError(
		t,
		Backup(context.Background(), src, &buf, testMaxValueLen, "test backup"),
	)

	dst := newFakeStore()
	require.NoError(
		t,
		Restore(context.Background(), dst, &buf, testMaxValueLen, "test restore"),
	)
	require.Equal(t, []byte("value-a"), dst.data["key-a"])
}

// TestRestoreRejectsTerminatorWithMismatchedChecksum guards the exact gap a
// bare out-of-band marker alone leaves open: terminatorMarker's 4 bytes
// (0xFFFFFFFF) can plausibly appear in a corrupted or truncated stream by
// coincidence, not just from a genuine Backup-written footer. A stream
// that produces the marker with a footer whose checksum doesn't match what
// was actually read must be rejected as corrupted, not accepted as a
// clean, complete end.
func TestRestoreRejectsTerminatorWithMismatchedChecksum(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(
		t,
		WriteRecord(&buf, []byte("k1"), []byte("v1"), testMaxValueLen),
	)
	require.NoError(t, writeTerminator(&buf, 1, 0xDEADBEEF))

	stream := append(append(Magic[:], byte(Version)), buf.Bytes()...)
	err := Restore(
		context.Background(),
		newFakeStore(),
		bytes.NewReader(stream),
		testMaxValueLen,
		"test",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")
}

// TestRestoreRejectsTerminatorWithMismatchedCount is the count-side
// counterpart of the checksum test above: a correct checksum for the
// records actually present but a declared count that doesn't match them
// (e.g. a stream truncated right after some records but before others the
// terminator claims) must also be rejected.
func TestRestoreRejectsTerminatorWithMismatchedCount(t *testing.T) {
	var buf bytes.Buffer
	checksum := crc32.NewIEEE()
	require.NoError(
		t,
		WriteRecord(&buf, []byte("k1"), []byte("v1"), testMaxValueLen),
	)
	require.NoError(
		t,
		WriteRecord(checksum, []byte("k1"), []byte("v1"), testMaxValueLen),
	)
	require.NoError(t, writeTerminator(&buf, 5, checksum.Sum32()))

	stream := append(append(Magic[:], byte(Version)), buf.Bytes()...)
	err := Restore(
		context.Background(),
		newFakeStore(),
		bytes.NewReader(stream),
		testMaxValueLen,
		"test",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")
}
