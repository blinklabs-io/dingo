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
	"io"
	"testing"

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
// clean io.EOF rather than a spurious error.
func TestWriteReadRecordRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(
		t,
		WriteRecord(&buf, []byte("key-one"), []byte("value-one"), testMaxValueLen),
	)
	require.NoError(
		t,
		WriteRecord(&buf, []byte("key-two"), []byte{}, testMaxValueLen),
	)

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
