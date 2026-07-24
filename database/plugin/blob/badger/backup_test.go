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

package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupRestoreRoundTrip(t *testing.T) {
	src, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer src.Close()

	txn := src.NewTransaction(true)
	require.NoError(t, src.Set(txn, []byte("key1"), []byte("value1")))
	require.NoError(t, src.Set(txn, []byte("key2"), []byte("value2")))
	require.NoError(t, txn.Commit())

	var buf bytes.Buffer
	require.NoError(t, src.Backup(context.Background(), &buf))
	require.NotZero(t, buf.Len())

	dst, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Restore(context.Background(), &buf))

	readTxn := dst.NewTransaction(false)
	defer readTxn.Rollback() //nolint:errcheck
	val1, err := dst.Get(readTxn, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val1)
	val2, err := dst.Get(readTxn, []byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), val2)
}

func TestBackupClosedStoreErrors(t *testing.T) {
	db, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	var buf bytes.Buffer
	err = db.Backup(context.Background(), &buf)
	require.Error(t, err)
}

// TestRestoreRecoversFromMalformedStreamPanic guards against a real crash
// found via manual live testing (dingo#1651 follow-up): some malformed
// backup streams don't just fail Badger's Load() with a clean error, they
// panic it outright (a corrupted length header producing a negative or
// oversized slice length). Restore must recover that panic and return it
// as a normal error, since an unrecovered panic here would crash the
// entire calling process — including a live node mid-restore.
func TestRestoreRecoversFromMalformedStreamPanic(t *testing.T) {
	db, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	garbage := bytes.NewBufferString("not a valid badger backup stream")
	err = db.Restore(context.Background(), garbage)
	require.Error(t, err)
}

// TestRestoreRejectsOversizedRecordLength verifies that a corrupted or
// malicious length prefix claiming more than maxLoadRecordSize is
// rejected with a clear error before Badger's own Load ever sees it --
// and therefore before Load would trust it for an allocation of that
// size, which is what could exhaust process memory and take down a live
// node before this check existed.
func TestRestoreRejectsOversizedRecordLength(t *testing.T) {
	// A length prefix alone, claiming more than maxLoadRecordSize, with
	// no payload bytes following it at all: if this weren't rejected up
	// front, Badger's Load would try to allocate a buffer of this size
	// before ever noticing the payload is missing.
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint64(maxLoadRecordSize)+1))

	t.Run("seekable input", func(t *testing.T) {
		db, err := New(WithDataDir(t.TempDir()))
		require.NoError(t, err)
		defer db.Close()

		err = db.Restore(context.Background(), bytes.NewReader(buf.Bytes()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds")
	})

	t.Run("non-seekable input", func(t *testing.T) {
		db, err := New(WithDataDir(t.TempDir()))
		require.NoError(t, err)
		defer db.Close()

		// bytes.Buffer (unlike bytes.Reader) does not implement
		// io.Seeker, exercising Restore's spool-to-temp-file fallback.
		nonSeekable := bytes.NewBuffer(buf.Bytes())
		err = db.Restore(context.Background(), nonSeekable)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds")
	})
}

func TestRestoreCanceledContext(t *testing.T) {
	db, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = db.Restore(ctx, bytes.NewReader(nil))
	require.ErrorIs(t, err, context.Canceled)
}

// cancelAfterNWrites forwards every Write to the real underlying writer,
// but calls cancel once count reaches n — used to prove Backup's context
// check happens on every internal Write Badger makes while streaming
// entries out, not just once before the whole operation starts (see
// contextWriter in backup.go).
type cancelAfterNWrites struct {
	w      io.Writer
	n      int
	count  int
	cancel context.CancelFunc
}

func (c *cancelAfterNWrites) Write(p []byte) (int, error) {
	c.count++
	if c.count == c.n {
		c.cancel()
	}
	return c.w.Write(p)
}

// TestBackupCancelledMidStreamStopsBeforeCompleting proves cancellation
// takes effect between two of Badger's internal Write calls, not only at
// Backup's own one-time ctx.Err() check before calling into Badger at
// all: Badger's Stream.Backup writes each chunk as a length-prefixed pair
// (an 8-byte header via one Write, the payload via a second) — cancelling
// after the first of those must cause the second to be rejected by
// contextWriter before it ever reaches the instrumented writer, so
// instrumented.count stops advancing past 1.
func TestBackupCancelledMidStreamStopsBeforeCompleting(t *testing.T) {
	db, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	for i := range 50 {
		require.NoError(t, db.Set(
			txn,
			fmt.Appendf(nil, "key%04d", i),
			bytes.Repeat([]byte("x"), 256),
		))
	}
	require.NoError(t, txn.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	instrumented := &cancelAfterNWrites{w: io.Discard, n: 1, cancel: cancel}
	err = db.Backup(ctx, instrumented)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(
		t, 1, instrumented.count,
		"a second Write must never reach the instrumented writer once ctx is cancelled",
	)
}

// cancelAfterNReads is TestRestoreCancelledMidStreamStopsBeforeCompleting's
// counterpart to cancelAfterNWrites.
type cancelAfterNReads struct {
	r      io.Reader
	n      int
	count  int
	cancel context.CancelFunc
}

func (c *cancelAfterNReads) Read(p []byte) (int, error) {
	c.count++
	if c.count == c.n {
		c.cancel()
	}
	return c.r.Read(p)
}

// TestRestoreCancelledMidStreamStopsBeforeCompleting mirrors the Backup
// test above for Restore/Load. Badger's Load wraps r in a 16KB
// bufio.Reader, so the backup stream must be comfortably larger than
// that for the buffered reader to actually need more than one
// underlying Read call — otherwise cancelling after the first Read
// would just be indistinguishable from cancelling before Restore was
// ever called.
func TestRestoreCancelledMidStreamStopsBeforeCompleting(t *testing.T) {
	src, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer src.Close()

	txn := src.NewTransaction(true)
	for i := range 200 {
		require.NoError(t, src.Set(
			txn,
			fmt.Appendf(nil, "key%04d", i),
			bytes.Repeat([]byte("x"), 1024),
		))
	}
	require.NoError(t, txn.Commit())

	var buf bytes.Buffer
	require.NoError(t, src.Backup(context.Background(), &buf))
	require.Greater(t, buf.Len(), 32<<10, "backup must exceed Load's 16KB read-ahead buffer")

	dst, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer dst.Close()

	ctx, cancel := context.WithCancel(context.Background())
	instrumented := &cancelAfterNReads{r: &buf, n: 1, cancel: cancel}
	err = dst.Restore(ctx, instrumented)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(
		t, 1, instrumented.count,
		"a second Read must never reach the instrumented reader once ctx is cancelled",
	)
}
