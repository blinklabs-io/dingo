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

package leiosdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
)

// SubdirName is the directory under the data directory that holds the store.
const SubdirName = "leios"

// Crash points a Hook can interrupt. A hook that returns an error makes the
// operation stop at that point exactly as a crash would, leaving the records
// already committed.
const (
	// HookPinCopied fires in Pin after the content is copied into the
	// immutable partition and before the pin record is written.
	HookPinCopied = "pin-copied"
	// HookPinCopy fires in Pin after each individual record copy commits, so
	// a test can interrupt between the manifest and the transactions.
	HookPinCopy = "pin-copy"
	// HookPinRecorded fires in Pin after the pin record is committed.
	HookPinRecorded = "pin-recorded"
	// HookGCWatermark fires in GarbageCollect after the watermark is
	// persisted and before the first delete.
	HookGCWatermark = "gc-watermark"
	// HookGCBatch fires in GarbageCollect after each committed delete batch.
	HookGCBatch = "gc-batch"
)

// gcBatchKeys bounds one garbage-collection delete transaction. Deletes carry
// no value, so the bound is the transaction entry budget, not memory.
const gcBatchKeys = 256

// Options configures Open.
type Options struct {
	Logger *slog.Logger
	// DataDir is the node data directory; the store lives in DataDir/leios.
	// An empty DataDir opens an in-memory store.
	DataDir string
	// Hook, when set, is called at each crash point (see HookPinCopied).
	// It exists so tests can interrupt an operation between its commits.
	Hook func(point string) error
	// ValueLogFileSize and MemTableSize bound the on-disk store's file
	// reservation. Zero selects a default.
	ValueLogFileSize int64
	MemTableSize     int64
}

// BadgerStore implements Store over a badger blob store.
type BadgerStore struct {
	bs   blob.BlobStore
	hook func(string) error
	// mu serializes every write so a partition move, a collection and a
	// rollback never interleave. Reads take the read side.
	mu     sync.RWMutex
	closed bool
	owns   bool
}

var _ Store = (*BadgerStore)(nil)

// Open opens the store described by opts.
func Open(opts Options) (*BadgerStore, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	bopts := []badger.BlobStoreBadgerOptionFunc{
		badger.WithLogger(logger),
		badger.WithSubdir(SubdirName),
		badger.WithCompressionEnabled(false),
		badger.WithBlockCacheSize(0),
		badger.WithIndexCacheSize(0),
		badger.WithValueThreshold(badger.DefaultCoreLeiosValueThreshold),
	}
	if opts.DataDir != "" {
		vlog := opts.ValueLogFileSize
		if vlog == 0 {
			vlog = 128 << 20
		}
		mem := opts.MemTableSize
		if mem == 0 {
			mem = 32 << 20
		}
		bopts = append(
			bopts,
			badger.WithDataDir(opts.DataDir),
			badger.WithValueLogFileSize(vlog),
			badger.WithMemTableSize(mem),
		)
	}
	bs, err := badger.New(bopts...)
	if err != nil {
		return nil, fmt.Errorf("open leios store: %w", err)
	}
	return &BadgerStore{bs: bs, hook: opts.Hook, owns: true}, nil
}

// Wrap builds a store over an existing blob store. The caller keeps ownership
// of bs; Close does not close it.
func Wrap(bs blob.BlobStore) *BadgerStore {
	return &BadgerStore{bs: bs}
}

// BlobStore returns the underlying key-value store, for lifecycle operations
// that back it up as a unit.
func (s *BadgerStore) BlobStore() blob.BlobStore {
	return s.bs
}

func (s *BadgerStore) fire(point string) error {
	if s.hook == nil {
		return nil
	}
	return s.hook(point)
}

func (s *BadgerStore) update(fn func(txn types.Txn) error) error {
	txn := s.bs.NewTransaction(true)
	if err := fn(txn); err != nil {
		_ = txn.Rollback()
		return err
	}
	return txn.Commit()
}

// commit runs update and then flushes the store to disk. It is for the
// writes a commit in another store depends on: the blob and metadata stores
// are made durable in step by database.Txn, but this store is separate, so a
// closure or manifest it acknowledged could otherwise be lost on an unclean
// shutdown while the offsets that point into it survive. Internal moves
// (collection, demotion, watermarks) use update, because the store orders its
// own commits and recovers by repeating them. One flush covers every commit
// before it, so Pin flushes once after its last commit rather than per copy.
func (s *BadgerStore) commit(fn func(txn types.Txn) error) error {
	if err := s.update(fn); err != nil {
		return err
	}
	if err := s.bs.Sync(); err != nil {
		return fmt.Errorf("leiosdb: sync: %w", err)
	}
	return nil
}

func (s *BadgerStore) view(fn func(txn types.Txn) error) error {
	txn := s.bs.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	return fn(txn)
}

func (s *BadgerStore) get(txn types.Txn, key []byte) ([]byte, error) {
	val, err := s.bs.Get(txn, key)
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, ErrNotFound
	}
	return val, err
}

func (s *BadgerStore) has(txn types.Txn, key []byte) (bool, error) {
	_, err := s.get(txn, key)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

func (s *BadgerStore) readWatermarks(txn types.Txn) (Watermarks, error) {
	var wm Watermarks
	val, err := s.get(txn, keyPinnedThrough)
	switch {
	case err == nil:
		ref, derr := decodeRef(val)
		if derr != nil {
			return wm, fmt.Errorf("decode pin watermark: %w", derr)
		}
		wm.PinnedThrough, wm.PinnedThroughSet = ref, true
	case !errors.Is(err, ErrNotFound):
		return wm, err
	}
	val, err = s.get(txn, keyGCBefore)
	switch {
	case err == nil:
		if len(val) != 8 {
			return wm, errors.New("decode collection watermark: bad length")
		}
		wm.GCBefore = binary.BigEndian.Uint64(val)
	case !errors.Is(err, ErrNotFound):
		return wm, err
	}
	return wm, nil
}

// PutVolatile implements Store.
func (s *BadgerStore) PutVolatile(eb Ref, content Content) error {
	if len(eb.Hash) == 0 {
		return errors.New("leiosdb: empty endorser block hash")
	}
	var txsVal []byte
	if content.Txs != nil {
		var err error
		txsVal, err = cbor.Encode(content.Txs)
		if err != nil {
			return fmt.Errorf("leiosdb: encode transactions: %w", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.commit(func(txn types.Txn) error {
		pinned, err := s.has(txn, recordKey(partImmutable, eb, kindPin))
		if err != nil {
			return err
		}
		part := byte(partVolatile)
		if pinned {
			part = partImmutable
		} else {
			wm, err := s.readWatermarks(txn)
			if err != nil {
				return err
			}
			if eb.Slot < wm.GCBefore {
				return ErrBehindImmutableTip
			}
		}
		if err := s.bs.Set(
			txn, recordKey(part, eb, kindManifest), content.Manifest,
		); err != nil {
			return err
		}
		if content.Txs != nil {
			if err := s.bs.Set(
				txn, recordKey(part, eb, kindTxs), txsVal,
			); err != nil {
				return err
			}
		}
		if !pinned {
			return nil
		}
		return s.settlePending(txn, eb)
	})
}

// settlePending drops the pending-pin index entry once the immutable
// partition holds both the manifest and the transaction list.
func (s *BadgerStore) settlePending(txn types.Txn, eb Ref) error {
	hasM, err := s.has(txn, recordKey(partImmutable, eb, kindManifest))
	if err != nil {
		return err
	}
	hasT, err := s.has(txn, recordKey(partImmutable, eb, kindTxs))
	if err != nil {
		return err
	}
	if hasM && hasT {
		return s.deleteIfPresent(txn, pendingKey(eb))
	}
	return nil
}

func (s *BadgerStore) deleteIfPresent(txn types.Txn, key []byte) error {
	err := s.bs.Delete(txn, key)
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil
	}
	return err
}

// PutClosure implements Store.
func (s *BadgerStore) PutClosure(eb Ref, closure []byte) error {
	if len(eb.Hash) == 0 {
		return errors.New("leiosdb: empty endorser block hash")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.commit(func(txn types.Txn) error {
		pinned, err := s.has(txn, recordKey(partImmutable, eb, kindPin))
		if err != nil {
			return err
		}
		part := byte(partVolatile)
		if pinned {
			part = partImmutable
		}
		return s.bs.Set(txn, recordKey(part, eb, kindClosure), closure)
	})
}

// readOrder returns the partitions to read for the block, in order. The
// immutable partition is consulted only once the block has a pin record: Pin
// copies its records one commit at a time and writes the pin record last, so
// an interrupted pin leaves a partial immutable copy that must stay invisible
// while the volatile copy is the complete one.
func (s *BadgerStore) readOrder(txn types.Txn, eb Ref) ([]byte, error) {
	pinned, err := s.has(txn, recordKey(partImmutable, eb, kindPin))
	if err != nil {
		return nil, err
	}
	if pinned {
		return []byte{partImmutable, partVolatile}, nil
	}
	return []byte{partVolatile}, nil
}

// Get implements Store.
func (s *BadgerStore) Get(eb Ref) (Content, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Content{}, ErrClosed
	}
	var out Content
	err := s.view(func(txn types.Txn) error {
		parts, err := s.readOrder(txn, eb)
		if err != nil {
			return err
		}
		for _, part := range parts {
			manifest, err := s.get(txn, recordKey(part, eb, kindManifest))
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			out.Manifest = manifest
			// The transactions come from the same partition as the
			// manifest they complete. A pinned block can hold a partial
			// immutable record while its volatile copy is already gone,
			// but never the reverse, because Pin moves both together.
			txsRaw, err := s.get(txn, recordKey(part, eb, kindTxs))
			switch {
			case err == nil:
				txs, derr := decodeTxs(txsRaw)
				if derr != nil {
					return fmt.Errorf("decode transactions: %w", derr)
				}
				out.Txs = txs
			case !errors.Is(err, ErrNotFound):
				return err
			}
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

type txView []byte

func (v *txView) UnmarshalCBOR(data []byte) error {
	*v = slices.Clip(data)
	return nil
}

// decodeTxs retains views into the store-owned value rather than copying each
// transaction body; the clip stops an append from running into the next one.
func decodeTxs(val []byte) ([]cbor.RawMessage, error) {
	var views []txView
	if _, err := cbor.Decode(val, &views); err != nil {
		return nil, err
	}
	txs := make([]cbor.RawMessage, len(views))
	for i, view := range views {
		txs[i] = cbor.RawMessage(view)
	}
	return txs, nil
}

// GetClosure implements Store.
func (s *BadgerStore) GetClosure(eb Ref) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var out []byte
	err := s.view(func(txn types.Txn) error {
		parts, err := s.readOrder(txn, eb)
		if err != nil {
			return err
		}
		for _, part := range parts {
			val, err := s.get(txn, recordKey(part, eb, kindClosure))
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			out = val
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

// IsPinned implements Store.
func (s *BadgerStore) IsPinned(eb Ref) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	var pinned bool
	err := s.view(func(txn types.Txn) error {
		var err error
		pinned, err = s.has(txn, recordKey(partImmutable, eb, kindPin))
		return err
	})
	return pinned, err
}

// Pin implements Store.
func (s *BadgerStore) Pin(eb, certifier Ref) (PinResult, error) {
	if len(eb.Hash) == 0 {
		return PinResult{}, errors.New("leiosdb: empty endorser block hash")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return PinResult{}, ErrClosed
	}
	var (
		result PinResult
		vals   = map[byte][]byte{}
	)
	pinKey := recordKey(partImmutable, eb, kindPin)
	err := s.view(func(txn types.Txn) error {
		pinned, err := s.has(txn, pinKey)
		if err != nil || pinned {
			result.AlreadyPinned = pinned
			return err
		}
		for _, kind := range []byte{kindManifest, kindTxs, kindClosure} {
			val, err := s.get(txn, recordKey(partVolatile, eb, kind))
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			vals[kind] = val
		}
		return nil
	})
	if err != nil || result.AlreadyPinned {
		return result, err
	}
	// One transaction per copied value: a closure or a dense transaction list
	// can be large, and the pin must not be refused for the size of the
	// transaction that carries it. A copy left behind by an interruption is
	// overwritten by the retry, and is unreachable until the pin record
	// exists because reads consult the immutable partition only for a block
	// that has a pin record.
	for _, kind := range []byte{kindManifest, kindTxs, kindClosure} {
		val, ok := vals[kind]
		if !ok {
			continue
		}
		if err := s.update(func(txn types.Txn) error {
			return s.bs.Set(txn, recordKey(partImmutable, eb, kind), val)
		}); err != nil {
			return result, fmt.Errorf("copy %c record: %w", kind, err)
		}
		if err := s.fire(HookPinCopy); err != nil {
			return result, err
		}
	}
	if err := s.fire(HookPinCopied); err != nil {
		return result, err
	}
	_, hasM := vals[kindManifest]
	_, hasT := vals[kindTxs]
	result.ContentPending = !hasM || !hasT
	if err := s.commit(func(txn types.Txn) error {
		if err := s.bs.Set(txn, pinKey, encodeRef(certifier)); err != nil {
			return err
		}
		for kind := range vals {
			if err := s.deleteIfPresent(
				txn, recordKey(partVolatile, eb, kind),
			); err != nil {
				return err
			}
		}
		if result.ContentPending {
			return s.bs.Set(txn, pendingKey(eb), encodeRef(certifier))
		}
		return nil
	}); err != nil {
		return result, fmt.Errorf("record pin: %w", err)
	}
	if err := s.fire(HookPinRecorded); err != nil {
		return result, err
	}
	return result, nil
}

// AdvancePinnedThrough implements Store.
func (s *BadgerStore) AdvancePinnedThrough(through Ref) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.update(func(txn types.Txn) error {
		wm, err := s.readWatermarks(txn)
		if err != nil {
			return err
		}
		if wm.PinnedThroughSet && through.Slot < wm.PinnedThrough.Slot {
			return nil
		}
		return s.bs.Set(txn, keyPinnedThrough, encodeRef(through))
	})
}

// GarbageCollect implements Store.
func (s *BadgerStore) GarbageCollect(
	ctx context.Context,
	beforeSlot uint64,
) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var effective uint64
	if err := s.update(func(txn types.Txn) error {
		wm, err := s.readWatermarks(txn)
		if err != nil {
			return err
		}
		// A caller computes its bound from a pin watermark it read earlier,
		// and RollbackPins can lower that watermark and demote records above
		// the target before this call takes the lock. The bound is applied
		// against the watermark as it stands now so a demoted record is not
		// collected on the strength of a stale one.
		if wm.PinnedThroughSet {
			beforeSlot = min(beforeSlot, wm.PinnedThrough.Slot)
		}
		effective = max(beforeSlot, wm.GCBefore)
		if effective == wm.GCBefore {
			return nil
		}
		return s.bs.Set(
			txn, keyGCBefore, binary.BigEndian.AppendUint64(nil, effective),
		)
	}); err != nil {
		return 0, err
	}
	if effective == 0 {
		return 0, nil
	}
	if err := s.fire(HookGCWatermark); err != nil {
		return 0, err
	}
	deleted := 0
	for {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		keys, err := s.volatileKeysBelow(effective, gcBatchKeys)
		if err != nil {
			return deleted, err
		}
		if len(keys) == 0 {
			return deleted, nil
		}
		if err := s.update(func(txn types.Txn) error {
			for _, key := range keys {
				if err := s.deleteIfPresent(txn, key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return deleted, err
		}
		deleted += len(keys)
		if err := s.fire(HookGCBatch); err != nil {
			return deleted, err
		}
	}
}

// volatileKeysBelow lists up to limit volatile keys with a slot below slot.
// A pinned block has no volatile keys, so nothing here needs a pin check.
func (s *BadgerStore) volatileKeysBelow(
	slot uint64,
	limit int,
) ([][]byte, error) {
	var keys [][]byte
	err := s.view(func(txn types.Txn) error {
		prefix := []byte{partVolatile}
		it := s.bs.NewIterator(txn, types.BlobIteratorOptions{Prefix: prefix})
		if it == nil {
			return types.ErrBlobStoreUnavailable
		}
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			key := item.Key()
			if len(key) < 9 || binary.BigEndian.Uint64(key[1:9]) >= slot {
				break
			}
			keys = append(keys, bytes.Clone(key))
			if len(keys) >= limit {
				break
			}
		}
		return it.Err()
	})
	return keys, err
}

// Watermarks implements Store.
func (s *BadgerStore) Watermarks() (Watermarks, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Watermarks{}, ErrClosed
	}
	var wm Watermarks
	err := s.view(func(txn types.Txn) error {
		var err error
		wm, err = s.readWatermarks(txn)
		return err
	})
	return wm, err
}

// MaxSlot implements Store.
func (s *BadgerStore) MaxSlot() (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, false, ErrClosed
	}
	var (
		maxSlot uint64
		found   bool
	)
	err := s.view(func(txn types.Txn) error {
		for _, part := range []byte{partVolatile, partImmutable} {
			slot, ok, err := s.maxManifestSlot(txn, part)
			if err != nil {
				return err
			}
			if ok && (!found || slot > maxSlot) {
				maxSlot, found = slot, true
			}
		}
		return nil
	})
	return maxSlot, found, err
}

// maxManifestSlot is a reverse seek from the end of the partition. A slot's
// transaction, closure and pin records can sort after its manifest, so the
// walk skips them until it meets a manifest; it stops after the first group
// of records that contains none only when the partition is exhausted.
func (s *BadgerStore) maxManifestSlot(
	txn types.Txn,
	part byte,
) (uint64, bool, error) {
	prefix := []byte{part}
	it := s.bs.NewIterator(
		txn, types.BlobIteratorOptions{Prefix: prefix, Reverse: true},
	)
	if it == nil {
		return 0, false, types.ErrBlobStoreUnavailable
	}
	defer it.Close()
	// Reverse seek lands on the greatest key not above the target, so the
	// target is the partition prefix padded past any possible key.
	end := append([]byte{part}, bytes.Repeat([]byte{0xff}, 8+64+1)...)
	for it.Seek(end); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if len(key) > 0 && key[len(key)-1] == kindManifest && len(key) >= 11 {
			return binary.BigEndian.Uint64(key[1:9]), true, it.Err()
		}
	}
	return 0, false, it.Err()
}

// PendingPins implements Store.
func (s *BadgerStore) PendingPins() ([]PendingPin, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var out []PendingPin
	err := s.view(func(txn types.Txn) error {
		prefix := []byte{partPending}
		it := s.bs.NewIterator(txn, types.BlobIteratorOptions{Prefix: prefix})
		if it == nil {
			return types.ErrBlobStoreUnavailable
		}
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			key := item.Key()
			if len(key) < 10 {
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			cert, err := decodeRef(val)
			if err != nil {
				return err
			}
			out = append(out, PendingPin{
				EB: Ref{
					Slot: binary.BigEndian.Uint64(key[1:9]),
					Hash: bytes.Clone(key[9:]),
				},
				Certifier: cert,
			})
		}
		return it.Err()
	})
	return out, err
}

// demote moves a pinned endorser block back to the volatile partition and
// removes its pin. The records are copied before the pin is removed, one
// transaction per value, so an interruption leaves the block pinned and the
// retry repeats the copy. The records are kept rather than deleted because
// the chain may still reference them: transaction offsets resolve into the
// closure, and a certification that survives the rollback pins the block
// again without needing its content refetched.
func (s *BadgerStore) demote(eb Ref) error {
	vals := map[byte][]byte{}
	if err := s.view(func(txn types.Txn) error {
		for _, kind := range []byte{kindManifest, kindTxs, kindClosure} {
			val, err := s.get(txn, recordKey(partImmutable, eb, kind))
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			vals[kind] = val
		}
		return nil
	}); err != nil {
		return err
	}
	for _, kind := range []byte{kindManifest, kindTxs, kindClosure} {
		val, ok := vals[kind]
		if !ok {
			continue
		}
		if err := s.update(func(txn types.Txn) error {
			return s.bs.Set(txn, recordKey(partVolatile, eb, kind), val)
		}); err != nil {
			return fmt.Errorf("demote %c record: %w", kind, err)
		}
	}
	return s.update(func(txn types.Txn) error {
		for _, kind := range []byte{
			kindManifest, kindTxs, kindClosure, kindPin,
		} {
			if err := s.deleteIfPresent(
				txn, recordKey(partImmutable, eb, kind),
			); err != nil {
				return err
			}
		}
		return s.deleteIfPresent(txn, pendingKey(eb))
	})
}

// RollbackPins implements Store.
func (s *BadgerStore) RollbackPins(target Ref) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	var doomed []Ref
	err := s.view(func(txn types.Txn) error {
		prefix := []byte{partImmutable}
		it := s.bs.NewIterator(txn, types.BlobIteratorOptions{Prefix: prefix})
		if it == nil {
			return types.ErrBlobStoreUnavailable
		}
		defer it.Close()
		// A certifying block is the child of the block that announced the
		// endorser block, so a pin certified above the target is announced at
		// or after the target's slot.
		for it.Seek(slotPrefix(partImmutable, target.Slot)); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			key := item.Key()
			if len(key) < 11 || key[len(key)-1] != kindPin {
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			cert, err := decodeRef(val)
			if err != nil {
				return err
			}
			if isZeroRef(cert) || cert.Slot <= target.Slot {
				continue
			}
			eb, _, err := parseRecordKey(key)
			if err != nil {
				return err
			}
			doomed = append(doomed, eb)
		}
		return it.Err()
	})
	if err != nil {
		return 0, err
	}
	for _, eb := range doomed {
		if err := s.demote(eb); err != nil {
			return 0, err
		}
	}
	err = s.update(func(txn types.Txn) error {
		wm, err := s.readWatermarks(txn)
		if err != nil {
			return err
		}
		// A watermark at the target's slot with another hash names a block
		// the chain no longer holds, so it moves to the target as well.
		if wm.PinnedThroughSet && (target.Slot < wm.PinnedThrough.Slot ||
			(target.Slot == wm.PinnedThrough.Slot &&
				!bytes.Equal(target.Hash, wm.PinnedThrough.Hash))) {
			if err := s.bs.Set(
				txn, keyPinnedThrough, encodeRef(target),
			); err != nil {
				return err
			}
		}
		if target.Slot < wm.GCBefore {
			return s.bs.Set(
				txn,
				keyGCBefore,
				binary.BigEndian.AppendUint64(nil, target.Slot),
			)
		}
		return nil
	})
	return len(doomed), err
}

// MigrationState implements Store.
func (s *BadgerStore) MigrationState() (MigrationState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return MigrationNone, ErrClosed
	}
	state := MigrationNone
	err := s.view(func(txn types.Txn) error {
		val, err := s.get(txn, keyMigration)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(val) != 1 {
			return errors.New("decode migration state: bad length")
		}
		state = MigrationState(val[0])
		return nil
	})
	return state, err
}

// SetMigrationState implements Store.
func (s *BadgerStore) SetMigrationState(state MigrationState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.commit(func(txn types.Txn) error {
		return s.bs.Set(txn, keyMigration, []byte{byte(state)})
	})
}

// Close implements Store.
func (s *BadgerStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if !s.owns {
		return nil
	}
	return s.bs.Close()
}
