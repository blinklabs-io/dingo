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
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosStoreState lazily opens the LeiosDB. A node that never touches an
// endorser block never creates the store or its directory.
type leiosStoreState struct {
	store leiosdb.Store
	// owned is set when the Database opened the store itself and so closes it.
	owned bool
	// closed is set by Close so a later read cannot reopen the store.
	closed bool
	// peer is the database whose LeiosDB this one reads, as the IndexerDB
	// reads the ledger's: both open the same data directory, and one store
	// cannot be opened twice.
	peer *Database
	// migrated caches a recorded MigrationDone so the read path stops
	// consulting the shared blob store's legacy keys.
	migrated atomic.Bool
}

// LeiosDB returns the endorser-block store, opening it under the data
// directory on first use. The store is in memory when the data directory is
// empty. It creates the directory, so only paths that handle an endorser
// block call it; chain-wide paths such as a rollback use existingLeiosDB.
func (d *Database) LeiosDB() (leiosdb.Store, error) {
	d.leiosMu.Lock()
	defer d.leiosMu.Unlock()
	if d.leios.store == nil && d.leios.peer != nil {
		return d.leios.peer.LeiosDB()
	}
	return d.openLeiosDBLocked()
}

func (d *Database) openLeiosDBLocked() (leiosdb.Store, error) {
	if d.leios.store != nil {
		return d.leios.store, nil
	}
	if d.leios.closed {
		return nil, leiosdb.ErrClosed
	}
	store, err := leiosdb.Open(leiosdb.Options{
		Logger:  d.logger,
		DataDir: d.config.DataDir,
	})
	if err != nil {
		return nil, err
	}
	d.leios.store = store
	d.leios.owned = true
	return store, nil
}

// SetLeiosDB installs an externally owned endorser-block store. The caller
// closes it.
func (d *Database) SetLeiosDB(store leiosdb.Store) {
	d.leiosMu.Lock()
	defer d.leiosMu.Unlock()
	d.leios.store = store
	d.leios.owned = false
	d.leios.migrated.Store(false)
}

// LeiosBackupStore returns the blob store behind the LeiosDB for a snapshot to
// back up. It reports false with no error when the node has no LeiosDB, open
// or on disk, so a node that never used one snapshots nothing for it and never
// creates its directory. An offline snapshot opens a fresh Database whose
// LeiosDB is not yet open, so the on-disk directory counts. A LeiosDB that
// exists but cannot be opened, or one that cannot be backed up, is an error:
// treating it as absent would let the snapshot succeed without the certified
// closures that no other store keeps.
func (d *Database) LeiosBackupStore() (blob.BlobStore, bool, error) {
	store, err := d.existingLeiosDB()
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open Leios store for backup: %w", err)
	}
	if store == nil {
		return nil, false, nil
	}
	holder, ok := store.(interface{ BlobStore() blob.BlobStore })
	if !ok {
		return nil, false, errors.New(
			"the Leios store does not expose a blob store to back up",
		)
	}
	return holder.BlobStore(), true, nil
}

func (d *Database) closeLeiosDB() error {
	d.leiosMu.Lock()
	defer d.leiosMu.Unlock()
	d.leios.closed = true
	if d.leios.store == nil || !d.leios.owned {
		return nil
	}
	err := d.leios.store.Close()
	d.leios.store = nil
	return err
}

// leiosRef builds the store reference for an endorser-block occurrence.
func leiosRef(slot uint64, hash []byte) leiosdb.Ref {
	return leiosdb.Ref{Slot: slot, Hash: hash}
}

// SetLeiosEB persists an endorser block's manifest and, when txsRaw is
// non-nil, its transaction bodies as one record write in the LeiosDB's
// volatile partition, or in the immutable partition when the block is already
// pinned. Pass txsRaw==nil to write only the manifest (an incomplete endorser
// block); an empty non-nil slice stores an empty transaction list.
//
// It returns leiosdb.ErrBehindImmutableTip for an unpinned block whose slot
// the garbage collector has passed.
func (d *Database) SetLeiosEB(
	slot uint64,
	hash []byte,
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) error {
	store, err := d.LeiosDB()
	if err != nil {
		return err
	}
	return store.PutVolatile(
		leiosRef(slot, hash),
		leiosdb.Content{Manifest: manifestRaw, Txs: txsRaw},
	)
}

// GetLeiosEBManifest retrieves the raw endorser-block manifest CBOR for the
// exact (slot, hash) occurrence named, from the LeiosDB. It returns
// ErrBlobKeyNotFound when the occurrence is not stored, including when the
// same hash is stored under a different slot.
//
// Until the migration of the shared blob store's legacy records has finished,
// a miss falls back to those records.
func (d *Database) GetLeiosEBManifest(
	hash []byte,
	slot uint64,
) (manifestRaw []byte, err error) {
	store, err := d.LeiosDB()
	if err != nil {
		return nil, err
	}
	content, err := store.Get(leiosRef(slot, hash))
	if err == nil {
		return content.Manifest, nil
	}
	if !errors.Is(err, leiosdb.ErrNotFound) {
		return nil, err
	}
	return d.legacyLeiosEBManifest(store, hash, slot, err)
}

// GetLeiosEBTxs retrieves the raw transaction bodies for the exact (slot,
// hash) occurrence named. It returns ErrBlobKeyNotFound when no complete
// transaction list is stored for that occurrence. The returned slice holds
// the block_transaction CBOR values exactly as SetLeiosEB stored them.
func (d *Database) GetLeiosEBTxs(
	hash []byte,
	slot uint64,
) ([]cbor.RawMessage, error) {
	store, err := d.LeiosDB()
	if err != nil {
		return nil, err
	}
	content, err := store.Get(leiosRef(slot, hash))
	switch {
	case err == nil && content.Complete():
		return content.Txs, nil
	case err == nil:
		err = leiosdb.ErrNotFound
	case !errors.Is(err, leiosdb.ErrNotFound):
		return nil, err
	}
	return d.legacyLeiosEBTxs(store, hash, slot, err)
}

// MaxLeiosEBSlot returns the highest slot holding a persisted endorser-block
// manifest, or zero when there is none. It is a reverse seek, not a scan.
func (d *Database) MaxLeiosEBSlot() (uint64, error) {
	return d.MaxLeiosEBSlotContext(context.Background())
}

// MaxLeiosEBSlotContext is MaxLeiosEBSlot honouring a cancelled context.
func (d *Database) MaxLeiosEBSlotContext(
	ctx context.Context, //nolint:contextcheck // Preserve nil-context compatibility, matching other database context methods.
) (uint64, error) {
	if ctx == nil {
		ctx = context.Background() //nolint:contextcheck // Preserve literal-nil compatibility for this public context method.
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store, err := d.LeiosDB()
	if err != nil {
		return 0, err
	}
	slot, _, err := store.MaxSlot()
	return slot, err
}

// IsLeiosEBPinned reports whether the endorser block occurrence has been
// pinned into the immutable partition, which happens when the ranking block
// that certifies it becomes immutable.
func (d *Database) IsLeiosEBPinned(slot uint64, hash []byte) (bool, error) {
	store, err := d.LeiosDB()
	if err != nil {
		return false, err
	}
	return store.IsPinned(leiosRef(slot, hash))
}

// PutLeiosClosure stores the certified closure CBOR that transaction offsets
// for the endorser block resolve into, and makes it resolvable from memory.
func (d *Database) PutLeiosClosure(
	slot uint64,
	hash []byte,
	closure []byte,
) error {
	store, err := d.LeiosDB()
	if err != nil {
		return err
	}
	if err := store.PutClosure(leiosRef(slot, hash), closure); err != nil {
		return fmt.Errorf("store Leios closure: %w", err)
	}
	if len(hash) == 32 && d.cborCache != nil {
		var blockHash [32]byte
		copy(blockHash[:], hash)
		d.cborCache.cacheBlockCbor(slot, blockHash, closure)
	}
	return nil
}

// PutLeiosTransactionBacking stores the encoded endorser-block transaction
// list as the backing that transaction offsets resolve into, and makes it
// resolvable from memory.
func (d *Database) PutLeiosTransactionBacking(
	slot uint64,
	hash []byte,
	transactions []byte,
) error {
	store, err := d.LeiosDB()
	if err != nil {
		return err
	}
	ref := leiosRef(slot, hash)
	var writeErr error
	if backingStore, ok := store.(leiosdb.TransactionBackingStore); ok {
		writeErr = backingStore.PutTransactionBacking(ref, transactions)
	} else {
		writeErr = store.PutClosure(ref, transactions)
	}
	if writeErr != nil {
		return fmt.Errorf("store Leios transaction backing: %w", writeErr)
	}
	if len(hash) == 32 && d.cborCache != nil {
		var blockHash [32]byte
		copy(blockHash[:], hash)
		d.cborCache.cacheBlockCbor(slot, blockHash, transactions)
	}
	return nil
}

// CanUseLeiosTransactionBacking reports whether new offsets for an endorser
// block may target its transaction-list record. Historical standalone backing
// keeps its original byte layout so already stored offsets remain valid.
func (d *Database) CanUseLeiosTransactionBacking(
	slot uint64,
	hash []byte,
) (bool, error) {
	blobStore, release := d.PinBlob()
	defer release()
	if blobStore == nil {
		return false, types.ErrBlobStoreUnavailable
	}
	txn := blobStore.NewTransaction(false)
	if txn == nil {
		return false, types.ErrBlobStoreUnavailable
	}
	defer txn.Rollback() //nolint:errcheck
	_, _, err := blobStore.GetBlock(txn, slot, hash)
	switch {
	case err == nil:
		return false, nil
	case !errors.Is(err, types.ErrBlobKeyNotFound):
		return false, err
	}
	store, err := d.LeiosDB()
	if err != nil {
		return false, err
	}
	backingStore, ok := store.(leiosdb.TransactionBackingStore)
	if !ok {
		return false, nil
	}
	standalone, err := backingStore.HasStandaloneClosure(
		leiosRef(slot, hash),
	)
	return !standalone, err
}

// RollbackLeiosPins removes the endorser-block pins certified by ranking
// blocks above point and lowers the store's watermarks to it. It returns the
// number of pins removed.
func (d *Database) RollbackLeiosPins(point ocommon.Point) (int, error) {
	store, err := d.existingLeiosDB()
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		// No store means no pins to demote; a chain rollback on a node that
		// never handled an endorser block must not create one.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return store.RollbackPins(leiosRef(point.Slot, point.Hash))
}

// leiosClosure returns the closure CBOR stored for an endorser block, for the
// offset resolver to fall back to when the shared blob store has no block at
// the same (slot, hash).
//
// It opens only a store that already exists, so a node that never handled an
// endorser block does not create the Leios directory by missing a block read.
func (d *Database) leiosClosure(slot uint64, hash []byte) ([]byte, error) {
	store, err := d.existingLeiosDB()
	if err != nil {
		return nil, err
	}
	return store.GetClosure(leiosRef(slot, hash))
}

// existingLeiosDB returns the Leios store when it is open or present on disk,
// and types.ErrBlobKeyNotFound when this node has none.
func (d *Database) existingLeiosDB() (leiosdb.Store, error) {
	d.leiosMu.Lock()
	defer d.leiosMu.Unlock()
	if d.leios.store != nil {
		return d.leios.store, nil
	}
	if d.leios.peer != nil {
		return d.leios.peer.existingLeiosDB()
	}
	if d.leios.closed || d.config.DataDir == "" {
		return nil, types.ErrBlobKeyNotFound
	}
	if _, err := os.Stat(
		filepath.Join(d.config.DataDir, leiosdb.SubdirName),
	); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, err
	}
	return d.openLeiosDBLocked()
}

// The blob read owns val. Retaining views of it avoids another payload copy
// per transaction while keeping separate database reads independent.
func decodeLeiosEBTxs(val []byte) ([]cbor.RawMessage, error) {
	var views []leiosTxView
	if _, err := cbor.Decode(val, &views); err != nil {
		return nil, err
	}
	if views == nil {
		return nil, nil
	}
	txs := make([]cbor.RawMessage, len(views))
	for i, view := range views {
		txs[i] = cbor.RawMessage(view)
	}
	return txs, nil
}

type leiosTxView []byte

func (view *leiosTxView) UnmarshalCBOR(data []byte) error {
	// Decode reads the caller-owned blob directly without reusing its buffer.
	// Cap each view so append cannot overwrite the following transaction.
	*view = slices.Clip(data)
	return nil
}
