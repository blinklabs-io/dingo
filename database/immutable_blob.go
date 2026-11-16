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

package database

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
)

// immutableBlobStore resolves promoted-block markers. Once a block body moves
// to the ImmutableDB chunk files its bp value is a marker; every read of a bp
// value goes through this wrapper so callers keep seeing block CBOR. Keys and
// metadata are untouched, so the block index and CBOR offsets resolve as they
// always did.
type immutableBlobStore struct {
	blob.BlobStore
	dir string
}

var (
	_ blob.BlobStore             = (*immutableBlobStore)(nil)
	_ blob.TxnBudget             = (*immutableBlobStore)(nil)
	_ blob.SpaceReclaimer        = (*immutableBlobStore)(nil)
	_ blob.WriteConflictDetector = (*immutableBlobStore)(nil)
	_ blob.Backuper              = (*immutableBlobStore)(nil)
	_ blob.Restorer              = (*immutableBlobStore)(nil)
	_ blob.Resettable            = (*immutableBlobStore)(nil)
)

// ErrBlobCapabilityUnsupported reports a lifecycle operation the store beneath
// the marker wrapper does not provide.
var ErrBlobCapabilityUnsupported = errors.New(
	"blob store does not support this operation",
)

// Backup streams the raw store, markers included. The chunk files they point
// into are captured separately by the snapshot.
func (s *immutableBlobStore) Backup(ctx context.Context, w io.Writer) error {
	b, ok := s.BlobStore.(blob.Backuper)
	if !ok {
		return fmt.Errorf("backup: %w", ErrBlobCapabilityUnsupported)
	}
	return b.Backup(ctx, w)
}

// Restore replaces the raw store from a backup.
func (s *immutableBlobStore) Restore(ctx context.Context, r io.Reader) error {
	b, ok := s.BlobStore.(blob.Restorer)
	if !ok {
		return fmt.Errorf("restore: %w", ErrBlobCapabilityUnsupported)
	}
	return b.Restore(ctx, r)
}

// Reset empties the raw store.
func (s *immutableBlobStore) Reset(ctx context.Context) error {
	b, ok := s.BlobStore.(blob.Resettable)
	if !ok {
		return fmt.Errorf("reset: %w", ErrBlobCapabilityUnsupported)
	}
	return b.Reset(ctx)
}

// Unwrap returns the store the wrapper reads markers from.
func (s *immutableBlobStore) Unwrap() blob.BlobStore { return s.BlobStore }

func isBlockBodyKey(key []byte) bool {
	return len(key) == types.BlockBlobKeySize &&
		strings.HasPrefix(string(key), types.BlockBlobKeyPrefix)
}

func (s *immutableBlobStore) resolve(value []byte) ([]byte, error) {
	if !types.IsImmutableMarker(value) {
		return value, nil
	}
	marker, err := types.DecodeImmutableMarker(value)
	if err != nil {
		return nil, err
	}
	_, inner, err := immutable.ReadBlockAt(s.dir, immutable.Location{
		Chunk:  marker.Chunk,
		Offset: marker.Offset,
		Size:   marker.Size,
		CRC:    marker.CRC,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve immutable block marker: %w", err)
	}
	return inner, nil
}

func (s *immutableBlobStore) Get(txn types.Txn, key []byte) ([]byte, error) {
	value, err := s.BlobStore.Get(txn, key)
	if err != nil || !isBlockBodyKey(key) {
		return value, err
	}
	return s.resolve(value)
}

type contextLimitedGetter interface {
	GetContextLimited(
		context.Context, types.Txn, []byte, int64,
	) ([]byte, error)
}

type contextGetter interface {
	GetContext(context.Context, types.Txn, []byte) ([]byte, error)
}

// GetContext forwards a cancellable read.
func (s *immutableBlobStore) GetContext(
	ctx context.Context,
	txn types.Txn,
	key []byte,
) ([]byte, error) {
	var (
		value []byte
		err   error
	)
	if getter, ok := s.BlobStore.(contextGetter); ok {
		value, err = getter.GetContext(ctx, txn, key)
	} else {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err = s.BlobStore.Get(txn, key)
	}
	if err != nil || !isBlockBodyKey(key) {
		return value, err
	}
	return s.resolve(value)
}

// GetContextLimited forwards a cancellable, bounded read. The bound applies to
// the value the caller receives: a marker is small, so the limit is checked
// after resolution.
func (s *immutableBlobStore) GetContextLimited(
	ctx context.Context,
	txn types.Txn,
	key []byte,
	maxBytes int64,
) ([]byte, error) {
	if !isBlockBodyKey(key) {
		if getter, ok := s.BlobStore.(contextLimitedGetter); ok {
			return getter.GetContextLimited(ctx, txn, key, maxBytes)
		}
		return s.GetContext(ctx, txn, key)
	}
	value, err := s.GetContext(ctx, txn, key)
	if err != nil {
		return nil, err
	}
	if int64(len(value)) > maxBytes {
		return nil, fmt.Errorf(
			"blob object exceeds maximum size of %d bytes", maxBytes,
		)
	}
	return value, nil
}

func (s *immutableBlobStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	value, md, err := s.BlobStore.GetBlock(txn, slot, hash)
	if err != nil {
		return value, md, err
	}
	inner, err := s.resolve(value)
	return inner, md, err
}

// GetBlockLocal exists so a wrapper stacked above this one that probes for
// LocalBlockReader keeps a local-only read local.
func (s *immutableBlobStore) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	if reader, ok := s.BlobStore.(blob.LocalBlockReader); ok {
		value, md, err := reader.GetBlockLocal(txn, slot, hash)
		if err != nil {
			return value, md, err
		}
		inner, err := s.resolve(value)
		return inner, md, err
	}
	return s.GetBlock(txn, slot, hash)
}

func (s *immutableBlobStore) RemainingTxnEntries(
	txn types.Txn,
	entryBytes int,
) (int, bool) {
	if budget, ok := s.BlobStore.(blob.TxnBudget); ok {
		return budget.RemainingTxnEntries(txn, entryBytes)
	}
	return 0, false
}

// ReclaimSpace forwards to the store beneath the wrapper. A store that keeps
// no superseded versions has nothing to reclaim.
func (s *immutableBlobStore) ReclaimSpace() error {
	if r, ok := s.BlobStore.(blob.SpaceReclaimer); ok {
		return r.ReclaimSpace()
	}
	return nil
}

func (s *immutableBlobStore) DetectsWriteConflicts() bool {
	if det, ok := s.BlobStore.(blob.WriteConflictDetector); ok {
		return det.DetectsWriteConflicts()
	}
	return false
}

func (s *immutableBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	return &immutableIterator{
		BlobIterator: s.BlobStore.NewIterator(txn, opts),
		store:        s,
	}
}

type immutableIterator struct {
	types.BlobIterator
	store *immutableBlobStore
}

func (it *immutableIterator) Item() types.BlobItem {
	item := it.BlobIterator.Item()
	if item == nil || !isBlockBodyKey(item.Key()) {
		return item
	}
	return &immutableItem{BlobItem: item, store: it.store}
}

type immutableItem struct {
	types.BlobItem
	store *immutableBlobStore
}

func (i *immutableItem) ValueCopy(dst []byte) ([]byte, error) {
	value, err := i.BlobItem.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	inner, err := i.store.resolve(value)
	if err != nil {
		return nil, err
	}
	return append(dst[:0], inner...), nil
}

// RawValueCopy returns the stored value, a marker for a promoted block.
func (i *immutableItem) RawValueCopy(dst []byte) ([]byte, error) {
	return i.BlobItem.ValueCopy(dst)
}

// rawBlobStore returns the store beneath the marker-resolving wrapper, for
// callers that only classify a block value (present, expired, promoted) and
// must not read a promoted body out of the chunk files to do it. It follows
// Unwrap through any wrappers stacked above the marker wrapper, such as the
// Bark archive store, and returns store unchanged when there is no marker
// wrapper.
func rawBlobStore(store blob.BlobStore) blob.BlobStore {
	for cur := store; cur != nil; {
		if wrapper, ok := cur.(*immutableBlobStore); ok {
			return wrapper.BlobStore
		}
		u, ok := cur.(interface{ Unwrap() blob.BlobStore })
		if !ok {
			break
		}
		cur = u.Unwrap()
	}
	return store
}
