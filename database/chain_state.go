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
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/types"
)

// Chain state describes the blocks the block store holds, so it is kept with
// them: one value per name under types.ChainStateKeyPrefix. A value read or
// written through a transaction that carries a blob handle shares that
// transaction; otherwise the call owns a blob transaction of its own.

// DeferredHeaderFloorKey names the slot below which evicted deferred-header
// markers no longer exist, so apply verifies such a block's header in full.
const DeferredHeaderFloorKey = "deferred_header_validation_floor"

// legacyDeferredHeaderMarkerSyncPrefix starts the sync_state rows that held
// deferred-header markers before the blob marker existed, and
// legacyDeferredHeaderMarkerSyncValue is the only value they were written
// with.
const (
	legacyDeferredHeaderMarkerSyncPrefix = "deferred_header_validation:"
	legacyDeferredHeaderMarkerSyncValue  = "true"
)

// ConsensusProviderIdentityKey names the versioned consensus semantics the
// stored chain was created under.
const ConsensusProviderIdentityKey = "consensus_provider_identity"

// ImmutableUtxoOffsetsTipKey names the slot up to which an immutable block
// copy stored produced-UTxO offset references.
const ImmutableUtxoOffsetsTipKey = "immutable_utxo_offsets_tip"

// PendingTruncateKey names the record of a truncate that has begun deleting
// blocks and has not finished.
const PendingTruncateKey = "database_lifecycle_truncate_pending"

// ErrBlockStoreChainState refuses a sync_state read or write of a name the
// block store keeps: a row written there would be read by nothing.
var ErrBlockStoreChainState = errors.New(
	"chain state is kept in the block store, not in sync_state",
)

// blockStoreChainState reports whether the block store keeps the value a
// sync_state key names.
func blockStoreChainState(key string) bool {
	switch key {
	case DeferredHeaderFloorKey, HistoryExpiryCursorKey, HistoryExpiryHeldKey,
		ConsensusProviderIdentityKey, ImmutableUtxoOffsetsTipKey,
		RollbackHighWaterKey, PendingTruncateKey, RollbackIntentKey:
		return true
	}
	return strings.HasPrefix(key, legacyDeferredHeaderMarkerSyncPrefix) ||
		strings.HasPrefix(key, types.ForgeFenceChainStatePrefix) ||
		strings.HasPrefix(key, types.LeaderScheduleChainStatePrefix) ||
		importProgressChainState(key)
}

// refuseBlockStoreChainState returns ErrBlockStoreChainState for a sync_state
// key the ledger's block store keeps. An IndexerDB has no block store of its
// own and keeps its sync_state as it is.
func (d *Database) refuseBlockStoreChainState(key string) error {
	if d.isIndexer() || !blockStoreChainState(key) {
		return nil
	}
	return fmt.Errorf("%w: %q", ErrBlockStoreChainState, key)
}

func chainStateBlobKey(name string) []byte {
	return []byte(types.ChainStateKeyPrefix + name)
}

// chainStateTxn returns the transaction a chain state call runs in and
// whether the call owns it.
func (d *Database) chainStateTxn(txn *Txn, readWrite bool) (*Txn, bool, error) {
	if txn == nil {
		return d.BlobTxn(readWrite), true, nil
	}
	if txn.Blob() == nil {
		return nil, false, fmt.Errorf(
			"%w: chain state needs a blob transaction", types.ErrNilTxn,
		)
	}
	return txn, false, nil
}

// GetChainState returns the chain state value name, or the empty string when
// it is not set.
func (d *Database) GetChainState(name string, txn *Txn) (string, error) {
	txn, owned, err := d.chainStateTxn(txn, false)
	if err != nil {
		return "", err
	}
	if owned {
		defer txn.Rollback() //nolint:errcheck
	}
	store := txn.BlobStore()
	if store == nil {
		return "", types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return "", types.ErrNilTxn
	}
	raw, err := store.Get(blobTxn, chainStateBlobKey(name))
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("GetChainState(%q): %w", name, err)
	}
	return string(raw), nil
}

// SetChainState stores the chain state value name.
func (d *Database) SetChainState(name, value string, txn *Txn) error {
	return d.writeChainState(
		name,
		txn,
		func(store chainStateWriter, blobTxn types.Txn) error {
			return store.Set(blobTxn, chainStateBlobKey(name), []byte(value))
		},
	)
}

// DeleteChainState removes the chain state value name. Removing an absent
// value is not an error.
func (d *Database) DeleteChainState(name string, txn *Txn) error {
	return d.writeChainState(
		name,
		txn,
		func(store chainStateWriter, blobTxn types.Txn) error {
			err := store.Delete(blobTxn, chainStateBlobKey(name))
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				return nil
			}
			return err
		},
	)
}

type chainStateWriter interface {
	Set(txn types.Txn, key, val []byte) error
	Delete(txn types.Txn, key []byte) error
}

func (d *Database) writeChainState(
	name string,
	txn *Txn,
	write func(chainStateWriter, types.Txn) error,
) error {
	txn, owned, err := d.chainStateTxn(txn, true)
	if err != nil {
		return err
	}
	if owned {
		defer txn.Rollback() //nolint:errcheck
	}
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	if err := write(store, blobTxn); err != nil {
		return fmt.Errorf("write chain state %q: %w", name, err)
	}
	if !owned {
		return nil
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("write chain state %q: commit: %w", name, err)
	}
	return nil
}

// ChainStateValue returns the chain state value name through a transaction
// of its own.
func (d *Database) ChainStateValue(name string) (string, error) {
	return d.GetChainState(name, nil)
}

// SetChainStateSynced stores the chain state value name and returns once the
// block store has flushed it, so the value also survives an unclean host
// shutdown. It is for a value whose loss cannot be repaired from the chain.
func (d *Database) SetChainStateSynced(name, value string) error {
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if err := d.SetChainState(name, value, nil); err != nil {
		return err
	}
	if err := store.Sync(); err != nil {
		return fmt.Errorf("sync chain state %q: %w", name, err)
	}
	return nil
}

// ClearImportedChainState removes the chain state that describes the chain
// as it was before a snapshot import replaced it, or the import itself.
func (d *Database) ClearImportedChainState() error {
	for _, name := range []string{
		DeferredHeaderFloorKey,
		ImmutableUtxoOffsetsTipKey,
		RollbackHighWaterKey,
		PendingTruncateKey,
		RollbackIntentKey,
		TipRevertPendingKey,
	} {
		if err := d.DeleteChainState(name, nil); err != nil {
			return err
		}
	}
	return nil
}

// DeferredHeaderFloor returns the slot below which evicted deferred-header
// markers no longer exist, and whether one is recorded.
func (d *Database) DeferredHeaderFloor() (uint64, bool, error) {
	raw, err := d.GetChainState(DeferredHeaderFloorKey, nil)
	if err != nil || raw == "" {
		return 0, false, err
	}
	floor, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse deferred header floor: %w", err)
	}
	return floor, true, nil
}

// SetDeferredHeaderFloor records slot as the deferred-header floor.
func (d *Database) SetDeferredHeaderFloor(slot uint64) error {
	return d.SetChainState(
		DeferredHeaderFloorKey, strconv.FormatUint(slot, 10), nil,
	)
}

// movePairedChainStateToBlockStore runs the open-time move ahead of the node
// settings check when the node settings already name this blob store as
// its pair, which every database holding rows to move does: that check reads
// the tip from the block store. A database with no pairing recorded, or
// paired with another blob store, is left for the gates to judge; nothing is
// copied into a store that may not be its own.
func (d *Database) movePairedChainStateToBlockStore(ctx context.Context) error {
	if d.isIndexer() || isNilStore(d.metadata) {
		return nil
	}
	gates, err := d.ledgerGateValues()
	if err != nil {
		return fmt.Errorf("read the blob store pairing: %w", err)
	}
	paired := gates["blob_store_id"]
	if paired == "" {
		return nil
	}
	held, err := d.recordedBlobStoreID()
	if err != nil {
		if errors.Is(err, types.ErrBlobStoreUnavailable) {
			return nil
		}
		return err
	}
	if held != paired {
		return nil
	}
	return d.moveChainStateToBlockStore(ctx)
}

// moveChainStateToBlockStore copies into the block store the chain state a
// v0.94.2 database holds in sync_state, and syncs each copy. The rows stay:
// the migration is one-way, and once it finishes nothing opens the store
// again. An open that repeats the copy, after a stop before the migration
// finished, finds each value already held and leaves it; a marker is written
// only where none exists and the floor only ever rises.
func (d *Database) moveChainStateToBlockStore(ctx context.Context) error {
	if d.isIndexer() || isNilStore(d.metadata) {
		return nil
	}
	store, release := d.PinBlob()
	release()
	if store == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.copyTipToBlockStore(); err != nil {
		return err
	}
	if err := d.moveLegacyDeferredHeaderMarkers(); err != nil {
		return err
	}
	if err := d.moveDeferredHeaderFloor(); err != nil {
		return err
	}
	if err := d.moveSyncStateValue(HistoryExpiryCursorKey); err != nil {
		return err
	}
	if err := d.moveSyncStatePrefix(
		types.ForgeFenceChainStatePrefix,
	); err != nil {
		return err
	}
	if err := d.moveSyncStatePrefix(
		types.LeaderScheduleChainStatePrefix,
	); err != nil {
		return err
	}
	for _, name := range []string{
		ConsensusProviderIdentityKey,
		ImmutableUtxoOffsetsTipKey,
		RollbackHighWaterKey,
		PendingTruncateKey,
	} {
		if err := d.moveSyncStateValueIfAbsent(name); err != nil {
			return err
		}
	}
	if err := d.moveReleasedRollbackIntent(); err != nil {
		return err
	}
	return d.moveImportProgress()
}

// copyTipToBlockStore copies the tip a v0.94.2 database holds in its metadata
// store into the block store, unless the block store already holds one: once
// copied, the tip is written only there, so a held value is the newer.
//
// The row is left in place. Adopting an API database as an IndexerDB reads
// the tip from the metadata store, and the ledger's database never reads the
// row again.
func (d *Database) copyTipToBlockStore() error {
	held, err := d.GetChainState(LedgerTipKey, nil)
	if err != nil || held != "" {
		return err
	}
	tip, err := d.metadata.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read the metadata tip: %w", err)
	}
	if tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 &&
		tip.BlockNumber == 0 {
		return nil
	}
	value, err := encodeTip(tip)
	if err != nil {
		return fmt.Errorf("copy the metadata tip: %w", err)
	}
	return d.SetChainStateSynced(LedgerTipKey, value)
}

// moveSyncStatePrefix moves every sync_state row whose key begins with
// prefix.
func (d *Database) moveSyncStatePrefix(prefix string) error {
	rows, err := d.metadata.ListSyncStateKeysByPrefix(prefix, nil)
	if err != nil {
		return fmt.Errorf("list %s rows: %w", prefix, err)
	}
	for _, row := range rows {
		if err := d.moveSyncStateValueIfAbsent(row); err != nil {
			return err
		}
	}
	return nil
}

// moveSyncStateValueIfAbsent copies the sync_state row name into the block
// store unless the block store already holds a value under that name.
//
// The value is written only through the block store once it has moved, so a
// value already held there is at least as new as the row.
func (d *Database) moveSyncStateValueIfAbsent(name string) error {
	value, err := d.metadata.GetSyncState(name, nil)
	if err != nil {
		return fmt.Errorf("read %s row: %w", name, err)
	}
	if value == "" {
		return nil
	}
	held, err := d.GetChainState(name, nil)
	if err != nil {
		return err
	}
	if held != "" {
		return nil
	}
	return d.SetChainStateSynced(name, value)
}

// moveSyncStateValue copies the sync_state row name into the block store
// under the same name.
func (d *Database) moveSyncStateValue(name string) error {
	value, err := d.metadata.GetSyncState(name, nil)
	if err != nil {
		return fmt.Errorf("read %s row: %w", name, err)
	}
	if value == "" {
		return nil
	}
	return d.SetChainStateSynced(name, value)
}

func (d *Database) moveLegacyDeferredHeaderMarkers() error {
	rows, err := d.metadata.ListSyncStateKeysByPrefix(
		legacyDeferredHeaderMarkerSyncPrefix, nil,
	)
	if err != nil {
		return fmt.Errorf("list deferred-header marker rows: %w", err)
	}
	for _, row := range rows {
		value, err := d.metadata.GetSyncState(row, nil)
		if err != nil {
			return fmt.Errorf("read deferred-header marker %q: %w", row, err)
		}
		// The rows were only ever written with the bare value. Anything
		// else is a format this release cannot attribute, and dropping it
		// would skip a header's stateful check.
		if value != "" && value != legacyDeferredHeaderMarkerSyncValue {
			return fmt.Errorf(
				"decode deferred-header marker %q: invalid legacy value %q",
				row, value,
			)
		}
		key := strings.TrimPrefix(row, legacyDeferredHeaderMarkerSyncPrefix)
		if value != "" && key != "" {
			held, err := d.HasDeferredHeaderMarker(key)
			if err != nil {
				return err
			}
			if !held {
				if err := d.SetDeferredHeaderMarker(key); err != nil {
					return err
				}
			}
		}
	}
	if len(rows) == 0 {
		return nil
	}
	return d.syncBlockStore("deferred-header markers")
}

func (d *Database) moveDeferredHeaderFloor() error {
	raw, err := d.metadata.GetSyncState(DeferredHeaderFloorKey, nil)
	if err != nil {
		return fmt.Errorf("read deferred header floor row: %w", err)
	}
	if raw == "" {
		return nil
	}
	floor, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("parse deferred header floor row: %w", err)
	}
	held, found, err := d.DeferredHeaderFloor()
	if err != nil {
		return err
	}
	if found && held >= floor {
		return nil
	}
	return d.SetChainStateSynced(
		DeferredHeaderFloorKey, strconv.FormatUint(floor, 10),
	)
}

// syncBlockStore flushes the block store, so the chain state copied into it
// survives a host crash.
func (d *Database) syncBlockStore(what string) error {
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if err := store.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", what, err)
	}
	return nil
}
