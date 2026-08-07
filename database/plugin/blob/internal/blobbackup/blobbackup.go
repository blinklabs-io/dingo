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

// Package blobbackup implements the shared backup/restore stream format used
// by cloud blob store plugins (s3, gcs) that have no native point-in-time
// snapshot primitive of their own -- a plain length-prefixed key/value
// stream produced by walking the store's existing Get/Set/NewIterator
// interface, distinct from badger's own native Backup/Load format. Both
// plugins are otherwise near-identical siblings (see their database.go
// files), so this one shared implementation replaces what would otherwise
// be two copies that could silently drift apart on a framing or version
// change.
package blobbackup

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/blinklabs-io/dingo/database/types"
)

// Magic/Version identify this backup framing. Every batch is keyed only by
// content, so a version byte lets a future framing change be detected up
// front rather than misparsed.
var Magic = [4]byte{'D', 'B', 'L', 'B'}

// Version 2 added the mandatory terminatorMarker (see its own doc comment)
// -- a version 1 stream (this whole cloud-backup mechanism's first cut,
// never released) had no way to distinguish a truncated file from a
// complete one, so there is no migration path from it and none is needed:
// nothing has shipped a version 1 backup for this to stay compatible with.
const Version = 2

// terminatorMarker is an out-of-band declared key length -- larger than any
// real record's, which WriteRecord bounds to MaxKeyLen -- that Backup
// writes once after the last real record, and ReadRecord requires seeing
// before treating end-of-stream as clean. Without an explicit terminator, a
// backup file truncated exactly at a record boundary (a partial copy, a
// storage-layer truncation, a cut-short upload) reads back identically to a
// complete one: io.ReadFull simply reports a clean io.EOF either way, so
// Restore would have no way to tell "every key was captured" from "the
// stream stopped early but happened to stop cleanly" -- a silent partial
// restore that looks like a successful complete one.
//
// The marker's 4 bytes alone are not sufficient proof of a clean end,
// though: 0xFFFFFFFF ("all bits set") is a realistic corruption pattern
// for some storage/flash failure modes, not just an arbitrary value picked
// at random, so a truncated or corrupted stream that happens to place
// those exact 4 bytes at a key-length read position would otherwise be
// silently accepted as complete -- the same class of bug this marker
// exists to close, just moved to a different trigger. The terminator
// therefore carries a 12-byte footer after the marker (an 8-byte record
// count and a 4-byte CRC32 checksum, both computed over every record
// actually written), and ReadRecord's caller must verify both match what
// it actually read before treating the terminator as genuine -- see
// Restore.
const terminatorMarker uint32 = 0xFFFFFFFF

// DefaultRestoreBatchRecords/DefaultRestoreBatchBytes bound how many records
// Restore accumulates per write transaction. A cloud store's transaction
// commit applies from an in-memory pending map, so a single transaction
// spanning an entire large store would hold the whole restored dataset in
// memory before ever issuing a write; batching bounds that regardless of how
// large the store being restored is.
const (
	DefaultRestoreBatchRecords = 1000
	DefaultRestoreBatchBytes   = 32 << 20
)

// MaxKeyLen bounds a single record's declared key length, checked before
// allocating a buffer of that size -- blob keys are always small (a few
// dozen bytes), so this is generous headroom while still far below what a
// corrupted or adversarial stream's raw length prefix could otherwise claim
// (up to 2^32-1).
const MaxKeyLen = 64 << 10

// Store is the subset of a cloud blob store's own interface Backup/Restore
// need: a plain key-iteration walk and batched writes over the store's
// existing transaction/iterator machinery, since neither S3 nor GCS has a
// native snapshot mechanism of its own.
type Store interface {
	NewTransaction(readWrite bool) types.Txn
	NewIterator(txn types.Txn, opts types.BlobIteratorOptions) types.BlobIterator
	Set(txn types.Txn, key, value []byte) error
}

// Backup streams every key/value currently in store to w, using the store's
// own read-only transaction and forward iterator rather than any cloud-native
// mechanism (neither S3 nor GCS has one). database/lifecycle.Snapshot already
// holds Database.PauseCommitsContext for the full duration of this call, so
// this needs no consistency mechanism of its own -- for a very large
// cloud-backed store this does mean the existing write-pause lasts as long as
// the full bucket walk, not just a brief lock, an inherent tradeoff of this
// backend family (see DATABASE.md). maxValueLen bounds a single value's
// length (mirroring the calling plugin's own read-side limit); errPrefix
// names the calling plugin (e.g. "s3 backup") in every returned error.
func Backup(
	ctx context.Context,
	store Store,
	w io.Writer,
	maxValueLen int64,
	errPrefix string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cw := &contextWriter{ctx: ctx, w: w}
	if _, err := cw.Write(Magic[:]); err != nil {
		return fmt.Errorf("%s: write header: %w", errPrefix, err)
	}
	if _, err := cw.Write([]byte{Version}); err != nil {
		return fmt.Errorf("%s: write header: %w", errPrefix, err)
	}

	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	// NewIterator opens its own network-call context internally (via
	// opContext) rather than accepting one, so there is nothing here for
	// contextcheck to actually flag -- ctx cancellation is still honored via
	// contextWriter on every Write below.
	it := store.NewIterator(txn, types.BlobIteratorOptions{}) //nolint:contextcheck
	if it == nil {
		return fmt.Errorf("%s: blob iterator is nil", errPrefix)
	}
	defer it.Close()
	if err := it.Err(); err != nil {
		return fmt.Errorf("%s: blob iterator: %w", errPrefix, err)
	}
	it.Rewind()
	checksum := crc32.NewIEEE()
	var recordCount uint64
	for it.Valid() {
		// The per-key work below (a potentially large ValueCopy network
		// read, or the next iterator page fetch) isn't itself ctx-aware --
		// NewIterator manages its own internal call context rather than
		// accepting this one (see the comment above) -- so an in-flight
		// operation can't be preempted mid-call. Checking here bounds how
		// long a cancellation takes to actually stop this loop to "at most
		// one more per-key operation," instead of running to the end of
		// the walk regardless of ctx.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", errPrefix, err)
		}
		item := it.Item()
		if item == nil {
			it.Next()
			continue
		}
		key := item.Key()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf(
				"%s: read value for key %x: %w", errPrefix, key, err,
			)
		}
		if err := WriteRecord(cw, key, value, maxValueLen); err != nil {
			return fmt.Errorf("%s: %w", errPrefix, err)
		}
		// Never fails: hash.Hash's Write always reports success, per its
		// io.Writer contract. Feeds the checksum the exact same bytes just
		// written above, so the terminator's footer can prove to Restore
		// that every record was actually captured, not just that
		// something ending in terminatorMarker's bytes was seen -- see
		// terminatorMarker's own doc comment.
		_ = WriteRecord(checksum, key, value, maxValueLen)
		recordCount++
		it.Next()
	}
	// A cloud iterator can fail mid-walk (a paginator error partway through
	// listing), which Valid() reports identically to "no more keys" -- so the
	// loop above exiting cleanly is not proof every key was seen.
	if err := it.Err(); err != nil {
		return fmt.Errorf("%s: blob iterator: %w", errPrefix, err)
	}
	// Recorded once the walk is confirmed complete (the it.Err() check just
	// above), so its presence in a backup file is itself proof every key was
	// captured -- see terminatorMarker's own doc comment.
	if err := writeTerminator(cw, recordCount, checksum.Sum32()); err != nil {
		return fmt.Errorf("%s: %w", errPrefix, err)
	}
	return nil
}

// writeTerminator writes terminatorMarker's 4 bytes, followed by recordCount
// (8 bytes) and checksum (4 bytes), bypassing WriteRecord's normal
// per-record framing (there is no key or value to follow it).
func writeTerminator(w io.Writer, recordCount uint64, checksum uint32) error {
	var buf [16]byte
	binary.BigEndian.PutUint32(buf[0:4], terminatorMarker)
	binary.BigEndian.PutUint64(buf[4:12], recordCount)
	binary.BigEndian.PutUint32(buf[12:16], checksum)
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("write terminator: %w", err)
	}
	return nil
}

// Restore replaces store's contents by loading a backup stream produced by
// Backup. It must only be called against a freshly created, empty store,
// enforced below -- matching blob.Restorer's documented contract. A restore
// that fails partway through (a batch commit error, a malformed record) can
// leave some already-committed batches in the store: a cloud store's
// transaction commit applies and durably commits each batch independently,
// so those earlier batches are not retroactively undone by a later batch's
// failure. A failed Restore must not be retried against the same store --
// IsEmpty's precondition check below will (correctly) refuse it as no longer
// empty; discard the store and start over instead. Every error return below
// that can only occur once at least one earlier batch already committed
// (see partialDataWarning) says so explicitly, so an operator reading the
// failure doesn't have to already know this internal batching detail to
// realize the store can't just be retried against.
func Restore(
	ctx context.Context,
	store Store,
	r io.Reader,
	maxValueLen int64,
	errPrefix string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	empty, err := IsEmpty(ctx, store)
	if err != nil {
		return fmt.Errorf("%s: check store is empty: %w", errPrefix, err)
	}
	if !empty {
		return fmt.Errorf(
			"%s: store already contains data -- Restore must only be "+
				"called against a freshly created, empty store",
			errPrefix,
		)
	}

	cr := &contextReader{ctx: ctx, r: bufio.NewReader(r)}
	var header [5]byte
	if _, err := io.ReadFull(cr, header[:]); err != nil {
		return fmt.Errorf("%s: read header: %w", errPrefix, err)
	}
	if [4]byte(header[:4]) != Magic {
		return fmt.Errorf("%s: not a recognized backup stream", errPrefix)
	}
	if header[4] != Version {
		return fmt.Errorf(
			"%s: unsupported backup version %d", errPrefix, header[4],
		)
	}

	txn := store.NewTransaction(true)
	batchRecords := 0
	batchBytes := 0
	// committedBatches tracks how many earlier batches have already been
	// durably committed at the point any later step fails, so every error
	// path below can tell the caller whether this store is still untouched
	// (safe to just discard and retry with a fresh one -- no different from
	// any other Restore failure) or already holds real, un-undoable partial
	// data (per this function's own doc comment) that must not be mistaken
	// for an empty, retry-ready store.
	committedBatches := 0
	flush := func() error {
		if batchRecords == 0 {
			return nil
		}
		if err := txn.Commit(); err != nil {
			// The store's own transaction Commit already marks itself
			// finished (and attempts its own compensating undo) on every
			// error path, so this Rollback is a defensive no-op today --
			// kept so a future change to that internal contract can't
			// silently leave txn open here.
			_ = txn.Rollback()
			return err
		}
		committedBatches++
		txn = store.NewTransaction(true)
		batchRecords = 0
		batchBytes = 0
		return nil
	}
	checksum := crc32.NewIEEE()
	var recordCount uint64
	for {
		key, value, err := ReadRecord(cr, maxValueLen)
		if err != nil {
			if term, ok := errors.AsType[*ErrTerminator](err); ok {
				// The marker's mere presence isn't proof of a clean end --
				// see terminatorMarker's doc comment -- so a mismatch here
				// means the stream is corrupted (or a coincidental
				// corruption pattern happened to resemble the marker
				// itself), not that Restore actually captured everything.
				if term.RecordCount != recordCount ||
					term.Checksum != checksum.Sum32() {
					_ = txn.Rollback()
					return fmt.Errorf(
						"%s: backup stream is corrupted or truncated -- "+
							"terminator declares %d record(s) (checksum "+
							"%08x), but %d record(s) (checksum %08x) were "+
							"actually read%s",
						errPrefix, term.RecordCount, term.Checksum,
						recordCount, checksum.Sum32(),
						partialDataWarning(committedBatches),
					)
				}
				break
			}
			_ = txn.Rollback()
			return fmt.Errorf(
				"%s: %w%s", errPrefix, err, partialDataWarning(committedBatches),
			)
		}
		// Never fails: hash.Hash's Write always reports success. Mirrors
		// Backup's own checksum update so the two sides compare directly.
		_ = WriteRecord(checksum, key, value, maxValueLen)
		recordCount++
		if err := store.Set(txn, key, value); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf(
				"%s: set key %x: %w%s",
				errPrefix, key, err, partialDataWarning(committedBatches),
			)
		}
		batchRecords++
		batchBytes += len(key) + len(value)
		if batchRecords >= DefaultRestoreBatchRecords ||
			batchBytes >= DefaultRestoreBatchBytes {
			if err := flush(); err != nil {
				return fmt.Errorf(
					"%s: commit batch: %w%s",
					errPrefix, err, partialDataWarning(committedBatches),
				)
			}
		}
	}
	if err := flush(); err != nil {
		return fmt.Errorf(
			"%s: commit final batch: %w%s",
			errPrefix, err, partialDataWarning(committedBatches),
		)
	}
	return nil
}

// partialDataWarning returns an empty string if committedBatches is zero
// (the store is still untouched -- a failure here is no different from any
// other Restore failure), or an explicit suffix describing how many
// batches are already durably committed and un-undoable, so an operator
// reading a failed Restore's error output knows the store must be
// discarded rather than assumed safe to retry against.
func partialDataWarning(committedBatches int) string {
	if committedBatches == 0 {
		return ""
	}
	return fmt.Sprintf(
		" (%d batch(es) already committed to the store -- it now contains "+
			"partial data and must be discarded; do not retry Restore "+
			"against it)",
		committedBatches,
	)
}

// IsEmpty reports whether store has no keys at all.
func IsEmpty(ctx context.Context, store Store) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	// See the identical comment on the NewIterator call in Backup above.
	it := store.NewIterator(txn, types.BlobIteratorOptions{}) //nolint:contextcheck
	if it == nil {
		return false, errors.New("blob iterator is nil")
	}
	defer it.Close()
	if err := it.Err(); err != nil {
		return false, err
	}
	it.Rewind()
	empty := !it.Valid()
	// Rewind can itself fail mid-listing (a paginator error on the very
	// first page), which Valid() reports identically to "no keys" -- check
	// again so a transient S3/GCS listing failure can't be misreported as
	// an empty store, letting Restore wrongly merge data into a bucket
	// that was never actually confirmed empty.
	if err := it.Err(); err != nil {
		return false, err
	}
	return empty, nil
}

// WriteRecord frames key/value as [4-byte BE key length][key][8-byte BE
// value length][value].
func WriteRecord(w io.Writer, key, value []byte, maxValueLen int64) error {
	if len(key) > MaxKeyLen {
		return fmt.Errorf(
			"key length %d exceeds %d byte limit",
			len(key), MaxKeyLen,
		)
	}
	// ReadRecord rejects a declared value length over maxValueLen on the way
	// back in; checking the same bound here means an oversized value fails
	// loudly at backup time instead of producing a snapshot file that is
	// silently guaranteed to fail every future restore.
	if int64(len(value)) > maxValueLen {
		return fmt.Errorf(
			"value length %d exceeds %d byte limit",
			len(value), maxValueLen,
		)
	}
	var lenBuf [8]byte
	// Bounded by the MaxKeyLen check above.
	keyLen := uint32(len(key)) //nolint:gosec // G115
	binary.BigEndian.PutUint32(lenBuf[:4], keyLen)
	if _, err := w.Write(lenBuf[:4]); err != nil {
		return fmt.Errorf("write key length: %w", err)
	}
	if _, err := w.Write(key); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(value)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write value length: %w", err)
	}
	if _, err := w.Write(value); err != nil {
		return fmt.Errorf("write value: %w", err)
	}
	return nil
}

// ErrTerminator is returned by ReadRecord (wrapping io.EOF, so an existing
// errors.Is(err, io.EOF) check still recognizes it as end-of-stream)
// instead of a plain io.EOF whenever it reads terminatorMarker in a
// key-length position. Its RecordCount/Checksum are the footer's own
// declared values -- see terminatorMarker's doc comment for why a caller
// (Restore) must use errors.As to recover them and compare against what it
// actually read before accepting this as a genuine, uncorrupted end, rather
// than treating the marker's mere presence as sufficient proof on its own.
type ErrTerminator struct {
	RecordCount uint64
	Checksum    uint32
}

func (e *ErrTerminator) Error() string { return "end of stream (terminator)" }
func (e *ErrTerminator) Unwrap() error { return io.EOF }

// ReadRecord reads one record written by WriteRecord. Returns an
// *ErrTerminator (which wraps io.EOF) only once it reads terminatorMarker
// in the key-length position, and has also read the fixed-size footer that
// follows it; a plain end-of-file at the key-length read itself (the
// stream simply having no more bytes) means the backup was truncated
// before ever reaching its terminator, and is a real error, not a clean
// end. Any other read failure, including a partial record, is also a real
// error. Declared lengths are validated against sane bounds before
// allocating a buffer of that size, so a corrupted or adversarial stream
// can only ever produce a normal error, not an attempted multi-gigabyte
// allocation -- mirrors badger's Restore validating its own
// length-prefixed framing the same way (see validateLoadRecordSizes in the
// badger plugin).
func ReadRecord(
	r io.Reader,
	maxValueLen int64,
) (key, value []byte, err error) {
	var keyLenBuf [4]byte
	if _, err := io.ReadFull(r, keyLenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			// Deliberately not %w-wrapping io.EOF: this must NOT satisfy
			// errors.Is(_, io.EOF), or Restore's loop would treat a
			// truncated-before-the-terminator stream as a clean, complete
			// end instead of the data-loss bug it actually is.
			return nil, nil, errors.New(
				"unexpected end of stream: missing terminator " +
					"(backup is truncated)",
			)
		}
		return nil, nil, fmt.Errorf("read key length: %w", err)
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
	if keyLen == terminatorMarker {
		var footer [12]byte
		if _, err := io.ReadFull(r, footer[:]); err != nil {
			return nil, nil, fmt.Errorf("read terminator footer: %w", err)
		}
		return nil, nil, &ErrTerminator{
			RecordCount: binary.BigEndian.Uint64(footer[:8]),
			Checksum:    binary.BigEndian.Uint32(footer[8:12]),
		}
	}
	if keyLen > MaxKeyLen {
		return nil, nil, fmt.Errorf(
			"key length %d exceeds %d byte limit (corrupted or invalid backup)",
			keyLen, MaxKeyLen,
		)
	}
	key = make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, nil, fmt.Errorf("read key: %w", err)
	}
	var valLenBuf [8]byte
	if _, err := io.ReadFull(r, valLenBuf[:]); err != nil {
		return nil, nil, fmt.Errorf("read value length: %w", err)
	}
	valLen := binary.BigEndian.Uint64(valLenBuf[:])
	// maxValueLen is always a positive, caller-supplied byte-count bound
	// (each cloud plugin's own maxBlobReadBytes constant), never negative.
	if valLen > uint64(maxValueLen) { //nolint:gosec // G115
		return nil, nil, fmt.Errorf(
			"value length %d exceeds %d byte limit (corrupted or invalid backup)",
			valLen,
			maxValueLen,
		)
	}
	value = make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, nil, fmt.Errorf("read value: %w", err)
	}
	return key, value, nil
}

// contextWriter wraps an io.Writer, checking ctx before each Write so a
// long-running Backup can be cancelled mid-transfer instead of only before
// it starts.
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
