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
	"context"
	"errors"
	"fmt"
	"io"
)

// defaultLoadMaxPendingWrites bounds in-flight writes during Restore,
// matching Badger's own recommended default for Load().
const defaultLoadMaxPendingWrites = 256

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
// restoring an untrusted or corrupted snapshot. The recover below ensures
// Restore always returns a normal error either way, never brings down the
// calling process.
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
	if loadErr := db.Load(&contextReader{ctx: ctx, r: r}, defaultLoadMaxPendingWrites); loadErr != nil {
		return fmt.Errorf("badger restore: %w", loadErr)
	}
	return nil
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
