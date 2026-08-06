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
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/blinklabs-io/dingo/database/types"
)

// backupMagic/backupVersion identify this store's own backup framing (a
// plain length-prefixed key/value stream over the store's existing
// Get/Set/NewIterator interface), distinct from badger's native
// Backup/Load format -- GCS has no equivalent point-in-time snapshot
// primitive, so this is a full key-iteration dump instead. Every batch is
// keyed only by content, so a version byte lets a future framing change be
// detected up front rather than misparsed.
var backupMagic = [4]byte{'D', 'B', 'L', 'B'}

const backupVersion = 1

// defaultRestoreBatchRecords/defaultRestoreBatchBytes bound how many
// records Restore accumulates per write transaction. gcsTxn.Commit applies
// from an in-memory "pending" map (see stageSet), so a single transaction
// spanning an entire large store would hold the whole restored dataset in
// memory before ever issuing a write; batching bounds that regardless of
// how large the store being restored is.
const (
	defaultRestoreBatchRecords = 1000
	defaultRestoreBatchBytes   = 32 << 20
)

// Backup streams every key/value currently in the store to w, using the
// store's own read-only transaction and forward iterator rather than any
// GCS-native mechanism (GCS has none). database/lifecycle.Snapshot already
// holds Database.PauseCommitsContext for the full duration of this call, so
// this needs no consistency mechanism of its own -- for a very large
// GCS-backed store this does mean the existing write-pause lasts as long as
// the full bucket walk, not just a brief lock, an inherent tradeoff of this
// backend family (see DATABASE.md).
func (d *BlobStoreGCS) Backup(ctx context.Context, w io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cw := &contextWriter{ctx: ctx, w: w}
	if _, err := cw.Write(backupMagic[:]); err != nil {
		return fmt.Errorf("gcs backup: write header: %w", err)
	}
	if _, err := cw.Write([]byte{backupVersion}); err != nil {
		return fmt.Errorf("gcs backup: write header: %w", err)
	}

	txn := d.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	// NewIterator opens its own network-call context internally
	// (via opContext) rather than accepting one, so there is nothing
	// here for contextcheck to actually flag -- ctx cancellation is
	// still honored via contextWriter on every Write below.
	it := d.NewIterator(txn, types.BlobIteratorOptions{}) //nolint:contextcheck
	if it == nil {
		return errors.New("gcs backup: blob iterator is nil")
	}
	defer it.Close()
	if err := it.Err(); err != nil {
		return fmt.Errorf("gcs backup: blob iterator: %w", err)
	}
	it.Rewind()
	for it.Valid() {
		item := it.Item()
		if item == nil {
			it.Next()
			continue
		}
		key := item.Key()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("gcs backup: read value for key %x: %w", key, err)
		}
		if err := writeRecord(cw, key, value); err != nil {
			return fmt.Errorf("gcs backup: %w", err)
		}
		it.Next()
	}
	// A cloud iterator can fail mid-walk (a paginator error partway through
	// listing), which Valid() reports identically to "no more keys" -- so
	// the loop above exiting cleanly is not proof every key was seen.
	if err := it.Err(); err != nil {
		return fmt.Errorf("gcs backup: blob iterator: %w", err)
	}
	return nil
}

// Restore replaces the store's contents by loading a backup stream produced
// by Backup. It must only be called against a freshly created, empty store,
// enforced below -- matching blob.Restorer's documented contract.
func (d *BlobStoreGCS) Restore(ctx context.Context, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	empty, err := d.isEmpty(ctx)
	if err != nil {
		return fmt.Errorf("gcs restore: check store is empty: %w", err)
	}
	if !empty {
		return errors.New(
			"gcs restore: store already contains data -- Restore must only be called against a freshly created, empty store",
		)
	}

	cr := &contextReader{ctx: ctx, r: bufio.NewReader(r)}
	var header [5]byte
	if _, err := io.ReadFull(cr, header[:]); err != nil {
		return fmt.Errorf("gcs restore: read header: %w", err)
	}
	if [4]byte(header[:4]) != backupMagic {
		return errors.New("gcs restore: not a gcs backup stream")
	}
	if header[4] != backupVersion {
		return fmt.Errorf(
			"gcs restore: unsupported backup version %d",
			header[4],
		)
	}

	txn := d.NewTransaction(true)
	batchRecords := 0
	batchBytes := 0
	flush := func() error {
		if batchRecords == 0 {
			return nil
		}
		if err := txn.Commit(); err != nil {
			return err
		}
		txn = d.NewTransaction(true)
		batchRecords = 0
		batchBytes = 0
		return nil
	}
	for {
		key, value, err := readRecord(cr)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("gcs restore: %w", err)
		}
		if err := d.Set(txn, key, value); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("gcs restore: set key %x: %w", key, err)
		}
		batchRecords++
		batchBytes += len(key) + len(value)
		if batchRecords >= defaultRestoreBatchRecords ||
			batchBytes >= defaultRestoreBatchBytes {
			if err := flush(); err != nil {
				return fmt.Errorf("gcs restore: commit batch: %w", err)
			}
		}
	}
	if err := flush(); err != nil {
		return fmt.Errorf("gcs restore: commit final batch: %w", err)
	}
	return nil
}

// isEmpty reports whether the store has no keys at all.
func (d *BlobStoreGCS) isEmpty(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	txn := d.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	// See the identical comment on the NewIterator call in Backup above.
	it := d.NewIterator(txn, types.BlobIteratorOptions{}) //nolint:contextcheck
	if it == nil {
		return false, errors.New("blob iterator is nil")
	}
	defer it.Close()
	if err := it.Err(); err != nil {
		return false, err
	}
	it.Rewind()
	return !it.Valid(), nil
}

// writeRecord frames key/value as [4-byte BE key length][key][8-byte BE
// value length][value].
func writeRecord(w io.Writer, key, value []byte) error {
	if len(key) > maxBackupKeyLen {
		return fmt.Errorf(
			"key length %d exceeds %d byte limit",
			len(key), maxBackupKeyLen,
		)
	}
	var lenBuf [8]byte
	// Bounded by the maxBackupKeyLen check above.
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

// maxBackupKeyLen bounds a single record's declared key length, checked
// before allocating a buffer of that size -- blob keys are always small
// (a few dozen bytes), so this is generous headroom while still far below
// what a corrupted or adversarial stream's raw length prefix could
// otherwise claim (up to 2^32-1).
const maxBackupKeyLen = 64 << 10

// readRecord reads one record written by writeRecord. Returns io.EOF (and
// no other data) only when the stream ends cleanly at a record boundary;
// any other read failure, including a partial record, is a real error.
// Declared lengths are validated against sane bounds before allocating a
// buffer of that size, so a corrupted or adversarial stream can only ever
// produce a normal error, not an attempted multi-gigabyte allocation --
// mirrors badger's Restore validating its own length-prefixed framing the
// same way (see validateLoadRecordSizes in the badger plugin).
func readRecord(r io.Reader) (key, value []byte, err error) {
	var keyLenBuf [4]byte
	if _, err := io.ReadFull(r, keyLenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, io.EOF
		}
		return nil, nil, fmt.Errorf("read key length: %w", err)
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
	if keyLen > maxBackupKeyLen {
		return nil, nil, fmt.Errorf(
			"key length %d exceeds %d byte limit (corrupted or invalid backup)",
			keyLen, maxBackupKeyLen,
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
	if valLen > uint64(maxBlobReadBytes) {
		return nil, nil, fmt.Errorf(
			"value length %d exceeds %d byte limit (corrupted or invalid backup)",
			valLen,
			maxBlobReadBytes,
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
