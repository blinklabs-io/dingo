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
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// defaultLoadMaxPendingWrites bounds in-flight writes during Restore,
// matching Badger's own recommended default for Load().
const defaultLoadMaxPendingWrites = 256

// maxLoadRecordSize bounds a single backup record's declared byte length,
// checked before Badger's own DB.Load is trusted to allocate a buffer of
// that size. Badger's Backup batches entries into one record per ~100MiB
// (its own flushThreshold) plus at most one entry's worth of overrun, so
// 512MiB is generous headroom above anything a real backup produces while
// still being far below what an attacker- or corruption-controlled length
// prefix could otherwise claim (up to 2^64-1, per Load's raw uint64 read).
const maxLoadRecordSize = 512 << 20

// Backup streams a full, MVCC-consistent backup of the store to w. It does
// not block concurrent readers or writers.
//
// Badger's own Backup has no context parameter and, once started, runs
// to completion regardless of ctx — checking ctx.Err() only here, before
// calling it, would let a cancellation request (see bark's
// CancelOperation) sit unnoticed for the entire duration of a large
// backup. Wrapping w in a contextWriter closes that gap: Badger calls
// Write repeatedly as it streams entries out, and each call re-checks
// ctx, so cancellation takes effect within a chunk or two rather than
// only between operations.
func (d *BlobStoreBadger) Backup(ctx context.Context, w io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db := d.DB()
	if db == nil || db.IsClosed() {
		return errors.New("badger backup: store is not open")
	}
	// since=0 requests a full backup of all versions currently retained.
	if _, err := db.Backup(&contextWriter{ctx: ctx, w: w}, 0); err != nil {
		return fmt.Errorf("badger backup: %w", err)
	}
	return nil
}

// Restore replaces the store's contents by loading a backup stream
// produced by Backup. It must only be called against a freshly opened,
// empty store. See Backup's doc comment for why r is wrapped the same
// way w is there: Badger's Load has no context parameter of its own.
//
// Some malformed inputs make Badger's Load return a clean error (e.g. a
// proto-unmarshal failure), but others make it panic outright (observed
// with arbitrary garbage bytes: a corrupted length header produces a
// negative/oversized slice length inside Load, panicking on the
// allocation) — a real, previously-unhandled crash risk for a live node
// restoring an untrusted or corrupted snapshot. The recover below covers
// that, but not every failure mode: Load's length prefix is an
// unvalidated raw uint64 that it trusts unconditionally before
// allocating a buffer of that size, and a single-allocation failure
// large enough can produce a fatal, unrecoverable runtime OOM rather
// than a regular panic recover can catch — no defer can stop that from
// taking the whole process down. validateLoadRecordSizes below makes a
// cheap, allocation-bounded pass over the same record framing Load uses
// first, so an oversized declared length is rejected as a normal error
// before Load ever sees it, rather than only after it's already been
// trusted for the allocation.
func (d *BlobStoreBadger) Restore(ctx context.Context, r io.Reader) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	db := d.DB()
	if db == nil || db.IsClosed() {
		return errors.New("badger restore: store is not open")
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf(
				"badger restore: panic while loading backup stream (corrupted or invalid data): %v",
				p,
			)
		}
	}()

	seeker, ok := r.(io.ReadSeeker)
	if !ok {
		// r isn't seekable (e.g. a network stream), but the validation
		// pass below and the real Load pass afterward each need to read
		// the stream from the start. Spool it to a temp file rather
		// than buffering it in memory: a restore's backup file is
		// exactly the kind of large payload this whole check exists to
		// avoid trusting the size of.
		tmp, tmpErr := os.CreateTemp("", "dingo-badger-restore-*")
		if tmpErr != nil {
			return fmt.Errorf(
				"badger restore: create validation temp file: %w",
				tmpErr,
			)
		}
		defer func() {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}()
		if _, copyErr := io.Copy(tmp, &contextReader{ctx: ctx, r: r}); copyErr != nil {
			return fmt.Errorf(
				"badger restore: buffer backup stream: %w",
				copyErr,
			)
		}
		// Writing left tmp's offset at EOF; both the validation pass and
		// the real Load pass below need to read from the start.
		if _, seekErr := tmp.Seek(0, io.SeekStart); seekErr != nil {
			return fmt.Errorf(
				"badger restore: rewind validation temp file: %w",
				seekErr,
			)
		}
		seeker = tmp
	}

	// Wrapped the same way the real Load call below is: a seekable input
	// (e.g. a local snapshot file) can be large enough that this
	// validation pass alone takes real time, and without this it ignored
	// cancellation entirely (unlike the unseekable branch above, whose
	// buffering io.Copy already went through a contextReader) -- a
	// cancelled live restore could sit unresponsive through this whole
	// pass before Load's own cancellation check ever got a chance to run.
	if err := validateLoadRecordSizes(&contextReader{ctx: ctx, r: seeker}, maxLoadRecordSize); err != nil {
		return fmt.Errorf("badger restore: %w", err)
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("badger restore: rewind backup stream: %w", err)
	}

	if loadErr := db.Load(&contextReader{ctx: ctx, r: seeker}, defaultLoadMaxPendingWrites); loadErr != nil {
		return fmt.Errorf("badger restore: %w", loadErr)
	}
	return nil
}

// validateLoadRecordSizes reads r using the exact same record framing
// Badger's own DB.Load does -- a repeating [8-byte little-endian
// length][that many payload bytes] stream -- but only to check each
// declared length against maxRecordSize, discarding the payload bytes
// without ever allocating a buffer sized by the untrusted length itself
// (io.CopyN streams through a small fixed-size internal buffer
// regardless of n). r's position is left at EOF; callers must seek back
// to the start before the real Load call.
func validateLoadRecordSizes(r io.Reader, maxRecordSize uint64) error {
	br := bufio.NewReader(r)
	for {
		var sz uint64
		if err := binary.Read(br, binary.LittleEndian, &sz); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read record length: %w", err)
		}
		if sz > maxRecordSize {
			return fmt.Errorf(
				"backup record size %d exceeds %d byte limit (corrupted or invalid backup)",
				sz, maxRecordSize,
			)
		}
		if _, err := io.CopyN(io.Discard, br, int64(sz)); err != nil {
			return fmt.Errorf("skip record payload: %w", err)
		}
	}
}

// contextWriter wraps an io.Writer, checking ctx before each Write so a
// long-running Backup can be cancelled mid-transfer instead of only
// before it starts.
type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (cw *contextWriter) Write(p []byte) (int, error) {
	if err := cw.ctx.Err(); err != nil {
		return 0, err
	}
	return cw.w.Write(p)
}

// contextReader is contextWriter's Restore-side counterpart.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr *contextReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}
