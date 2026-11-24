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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// LedgerTipKey names the chain state value holding the point the ledger has
// applied the chain up to, with that block's number.
const LedgerTipKey = "tip"

// TipRevertPendingKey names the chain state value that marks a metadata
// revert whose commit has not been confirmed. It holds the tip the revert
// lowered the stored tip to, in the tip's own encoding.
const TipRevertPendingKey = "tip_revert_pending"

// maxTipHashBytes bounds the block hash a stored tip may carry.
const maxTipHashBytes = 32

func encodeTip(tip ochainsync.Tip) (string, error) {
	if len(tip.Point.Hash) > maxTipHashBytes {
		return "", fmt.Errorf(
			"tip hash is %d bytes, more than %d",
			len(tip.Point.Hash), maxTipHashBytes,
		)
	}
	return strconv.FormatUint(tip.Point.Slot, 10) + ":" +
		strconv.FormatUint(tip.BlockNumber, 10) + ":" +
		hex.EncodeToString(tip.Point.Hash), nil
}

func decodeTip(raw string) (ochainsync.Tip, error) {
	var tip ochainsync.Tip
	if raw == "" {
		return tip, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return tip, fmt.Errorf("stored tip has %d fields, not 3", len(parts))
	}
	slot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return tip, fmt.Errorf("parse stored tip slot: %w", err)
	}
	number, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return tip, fmt.Errorf("parse stored tip block number: %w", err)
	}
	if len(parts[2]) > 2*maxTipHashBytes {
		return tip, fmt.Errorf(
			"stored tip hash is %d hex digits, more than %d",
			len(parts[2]), 2*maxTipHashBytes,
		)
	}
	hash, err := hex.DecodeString(parts[2])
	if err != nil {
		return tip, fmt.Errorf("parse stored tip hash: %w", err)
	}
	tip.Point.Slot = slot
	tip.BlockNumber = number
	if len(hash) > 0 {
		tip.Point.Hash = hash
	}
	return tip, nil
}

// GetTip returns the point the ledger has applied the chain up to. The
// ledger's database keeps it in the block store; an IndexerDB keeps the
// point its own rows reach in its metadata store.
//
// A transaction that has set the tip reads back what it set. A read-only
// transaction with a blob handle reads the tip of its snapshot. Any other
// call reads the committed tip, and takes no part in a read-write blob
// transaction: a read there would make the block store refuse that
// transaction's commit whenever the ledger moved the tip meanwhile.
func (d *Database) GetTip(txn *Txn) (ochainsync.Tip, error) {
	if d.isIndexer() {
		if txn == nil {
			return d.metadata.GetTip(nil)
		}
		return d.metadata.GetTip(txn.Metadata())
	}
	if txn != nil {
		if tip, staged := txn.stagedTip(); staged {
			return tip, nil
		}
		if txn.readWrite || txn.Blob() == nil {
			txn = nil
		}
	}
	raw, err := d.GetChainState(LedgerTipKey, txn)
	if err != nil {
		return ochainsync.Tip{}, fmt.Errorf("read tip: %w", err)
	}
	return decodeTip(raw)
}

// SetTip records the point the ledger has applied the chain up to.
//
// Given a transaction with a metadata handle, the stored tip is kept at or
// below the block the committed metadata reaches, through a blob transaction
// of its own at commit. A tip that advances is written once the metadata
// commit has succeeded, so it never names a block whose metadata writes were
// lost. A tip that retreats is written and flushed before the metadata commit
// that reverts to it, so it never names a block whose metadata rows are gone.
// A stop between the two writes of an advance leaves the tip at the lower
// block, from which the ledger applies again. The retreating write also
// records TipRevertPendingKey, which the commit of that transaction, or of
// a later TruncateAfterSlot, removes once its metadata commit has succeeded, so a stop between the two leaves a mark that the
// rows above the tip were not reverted (TipRevertPending) and the ledger
// repeats the revert. A transaction with only a blob handle carries the write
// itself.
func (d *Database) SetTip(tip ochainsync.Tip, txn *Txn) error {
	if d.isIndexer() {
		if txn == nil {
			return d.metadata.SetTip(tip, nil)
		}
		return d.metadata.SetTip(tip, txn.Metadata())
	}
	value, err := encodeTip(tip)
	if err != nil {
		return err
	}
	if txn == nil {
		return d.SetChainState(LedgerTipKey, value, nil)
	}
	if !txn.readWrite {
		return errors.New("set tip: the transaction is read-only")
	}
	if txn.Metadata() == nil {
		if err := d.SetChainState(LedgerTipKey, value, txn); err != nil {
			return err
		}
	}
	txn.stageTip(tip, value)
	return nil
}

// tipRetreats reports whether moving the tip from current to next leaves the
// block current names without advancing past it.
func tipRetreats(current, next ochainsync.Tip) bool {
	if next.Point.Slot != current.Point.Slot {
		return next.Point.Slot < current.Point.Slot
	}
	return !bytes.Equal(next.Point.Hash, current.Point.Hash)
}

// committedTip reads the stored tip from the store the transaction is pinned
// to.
func (t *Txn) committedTip() (ochainsync.Tip, error) {
	store := t.blobStore
	if store == nil {
		return ochainsync.Tip{}, types.ErrBlobStoreUnavailable
	}
	blobTxn := store.NewTransaction(false)
	if blobTxn == nil {
		return ochainsync.Tip{}, types.ErrNilTxn
	}
	defer blobTxn.Rollback() //nolint:errcheck
	raw, err := store.Get(blobTxn, chainStateBlobKey(LedgerTipKey))
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		return ochainsync.Tip{}, nil
	}
	if err != nil {
		return ochainsync.Tip{}, err
	}
	return decodeTip(string(raw))
}

// lowerMetadataTipRow moves the tip row a v0.94.2 database still holds in its
// metadata store down to tip, in txn, when the row is above it. The ledger
// does not read the row. Adopting the store's API rows as an IndexerDB does,
// and those rows end where a revert of the metadata store leaves them. A
// store without a row gets none.
func (d *Database) lowerMetadataTipRow(tip ochainsync.Tip, txn *Txn) error {
	if d.isIndexer() || txn == nil || txn.Metadata() == nil {
		return nil
	}
	row, err := d.metadata.GetTip(txn.Metadata())
	if err != nil {
		return fmt.Errorf("read the metadata tip row: %w", err)
	}
	if row.Point.Slot == 0 && len(row.Point.Hash) == 0 &&
		row.BlockNumber == 0 {
		return nil
	}
	if !tipRetreats(row, tip) {
		return nil
	}
	if err := d.metadata.SetTip(tip, txn.Metadata()); err != nil {
		return fmt.Errorf("lower the metadata tip row: %w", err)
	}
	return nil
}

// TipRevertPending returns the tip a metadata revert lowered the stored tip
// to when that revert's commit was never confirmed: the metadata store may
// still hold the rows above it.
func (d *Database) TipRevertPending() (ochainsync.Tip, bool, error) {
	if d.isIndexer() {
		return ochainsync.Tip{}, false, nil
	}
	raw, err := d.GetChainState(TipRevertPendingKey, nil)
	if err != nil {
		return ochainsync.Tip{}, false, fmt.Errorf(
			"read pending tip revert: %w", err,
		)
	}
	if raw == "" {
		return ochainsync.Tip{}, false, nil
	}
	tip, err := decodeTip(raw)
	if err != nil {
		return ochainsync.Tip{}, false, fmt.Errorf(
			"read pending tip revert: %w", err,
		)
	}
	return tip, true, nil
}

// ClearTipRevertPending removes the mark TipRevertPending reads. A caller
// uses it for a mark that no longer names the stored tip; a revert that
// commits removes its own.
func (d *Database) ClearTipRevertPending() error {
	if d.isIndexer() {
		return nil
	}
	return d.DeleteChainState(TipRevertPendingKey, nil)
}

// writeStagedTip stores the tip the transaction set, through a blob
// transaction of its own on the store the transaction is pinned to. A
// retreating tip is written with the pending-revert mark and flushed.
func (t *Txn) writeStagedTip(retreat bool) error {
	store := t.blobStore
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := store.NewTransaction(true)
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	if err := store.Set(
		blobTxn, chainStateBlobKey(LedgerTipKey), []byte(t.tipValue),
	); err != nil {
		_ = blobTxn.Rollback()
		return err
	}
	if retreat {
		if err := store.Set(
			blobTxn,
			chainStateBlobKey(TipRevertPendingKey),
			[]byte(t.tipValue),
		); err != nil {
			_ = blobTxn.Rollback()
			return err
		}
	}
	if err := blobTxn.Commit(); err != nil {
		return err
	}
	if !retreat {
		return nil
	}
	return store.Sync()
}

// writeRetreatingTip writes the staged tip ahead of the metadata commit when
// it retreats from the stored one, and reports whether it did.
func (t *Txn) writeRetreatingTip() (bool, error) {
	next, staged := t.stagedTip()
	if !staged || t.metadataTxn == nil || t.sharedMetadata {
		return false, nil
	}
	current, err := t.committedTip()
	if err != nil || !tipRetreats(current, next) {
		return false, err
	}
	if err := t.writeStagedTip(true); err != nil {
		return false, err
	}
	return true, nil
}

// markTipRevert records that the transaction reverts the metadata store to
// the tip it staged, whether or not the stored tip has to move: a revert
// repeated after its commit failed finds the tip already lowered.
func (t *Txn) markTipRevert() {
	t.tipMu.Lock()
	defer t.tipMu.Unlock()
	t.tipRevert = true
}

func (t *Txn) revertsTip() bool {
	t.tipMu.Lock()
	defer t.tipMu.Unlock()
	return t.tipStaged && t.tipRevert
}

// clearTipRevertPending removes the pending-revert mark after the metadata
// commit of a revert. It is not flushed: losing the removal repeats a revert
// that already happened, which changes nothing.
func (t *Txn) clearTipRevertPending() error {
	store := t.blobStore
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := store.NewTransaction(true)
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	key := chainStateBlobKey(TipRevertPendingKey)
	if _, err := store.Get(blobTxn, key); err != nil {
		_ = blobTxn.Rollback()
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil
		}
		return err
	}
	if err := store.Delete(blobTxn, key); err != nil {
		_ = blobTxn.Rollback()
		return err
	}
	return blobTxn.Commit()
}

func (t *Txn) stageTip(tip ochainsync.Tip, value string) {
	t.tipMu.Lock()
	defer t.tipMu.Unlock()
	t.tip = tip
	t.tipValue = value
	t.tipStaged = true
}

func (t *Txn) stagedTip() (ochainsync.Tip, bool) {
	t.tipMu.Lock()
	defer t.tipMu.Unlock()
	return t.tip, t.tipStaged
}
