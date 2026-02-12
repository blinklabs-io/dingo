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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var ErrUtxoNotFound = errors.New("utxo not found")

// deleteUtxoBlobs attempts to delete blob data for the given UTxOs.
// This is a best-effort operation; metadata remains the source of truth. If the
// provided transaction does not include a blob handle, a temporary blob-only
// transaction is used instead.
func deleteUtxoBlobs(d *Database, utxos []models.Utxo, txn *Txn) error {
	blob := d.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}

	useTxn := txn
	owned := false
	if useTxn == nil || useTxn.Blob() == nil {
		useTxn = NewBlobOnlyTxn(d, true)
		owned = true
		defer func() {
			if owned {
				useTxn.Rollback() //nolint:errcheck
			}
		}()
	}

	var deleteErrors int
	for _, utxo := range utxos {
		if err := blob.DeleteUtxo(useTxn.Blob(), utxo.TxId, utxo.OutputIdx); err != nil {
			deleteErrors++
			d.logger.Debug(
				"failed to delete UTxO blob data",
				"txid", hex.EncodeToString(utxo.TxId),
				"output_idx", utxo.OutputIdx,
				"error", err,
			)
		}
	}
	if deleteErrors > 0 {
		d.logger.Debug(
			"blob deletion completed with errors",
			"failed",
			deleteErrors,
			"total",
			len(utxos),
		)
	}

	if owned {
		owned = false // prevent deferred rollback
		if err := useTxn.Commit(); err != nil {
			_ = useTxn.Rollback() // explicit rollback on commit failure
			d.logger.Debug("blob delete commit failed", "error", err)
		}
	}

	return nil
}

func loadCbor(u *models.Utxo, txn *Txn) error {
	db := txn.DB()
	// Use tiered cache if available
	if db.cborCache != nil {
		// Pass the blob transaction so we can see uncommitted writes
		// (important for intra-batch UTxO lookups during validation)
		blobTxn := txn.Blob()
		cbor, err := db.cborCache.ResolveUtxoCbor(u.TxId, u.OutputIdx, blobTxn)
		if err != nil {
			// Map blob-key-not-found to ErrUtxoNotFound
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				return ErrUtxoNotFound
			}
			return fmt.Errorf(
				"resolve UTxO cbor tx=%x idx=%d: %w",
				u.TxId[:8],
				u.OutputIdx,
				err,
			)
		}
		u.Cbor = cbor
		return nil
	}

	// Fallback: direct blob access (for tests without cache)
	blob := db.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	val, err := blob.GetUtxo(txn.Blob(), u.TxId, u.OutputIdx)
	if err != nil {
		// Map blob-key-not-found to ErrUtxoNotFound since it means the UTxO's blob is missing
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return ErrUtxoNotFound
		}
		return fmt.Errorf(
			"resolve UTxO cbor tx=%x idx=%d: %w",
			u.TxId[:8],
			u.OutputIdx,
			err,
		)
	}

	// Check if this is offset-based storage
	if IsUtxoOffsetStorage(val) {
		// Decode the offset reference
		offset, err := DecodeUtxoOffset(val)
		if err != nil {
			return fmt.Errorf("decode utxo offset: %w", err)
		}

		// Get the block CBOR from blob store
		blockCbor, _, err := blob.GetBlock(txn.Blob(), offset.BlockSlot, offset.BlockHash[:])
		if err != nil {
			return fmt.Errorf("get block for utxo extraction: %w", err)
		}

		// Extract the UTxO CBOR from the block
		end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
		if end > uint64(len(blockCbor)) {
			return fmt.Errorf(
				"utxo offset out of bounds: offset=%d, length=%d, block_size=%d",
				offset.ByteOffset,
				offset.ByteLength,
				len(blockCbor),
			)
		}
		u.Cbor = blockCbor[offset.ByteOffset:end]
		return nil
	}

	// Legacy format: raw CBOR data
	u.Cbor = val
	return nil
}

func (d *Database) UtxoByRef(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxo, err := d.metadata.GetUtxo(txId, outputIdx, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if utxo == nil {
		return nil, ErrUtxoNotFound
	}
	if err := loadCbor(utxo, txn); err != nil {
		return nil, err
	}
	return utxo, nil
}

// UtxoByRefIncludingSpent returns a Utxo by reference,
// including spent (consumed) UTxOs.
func (d *Database) UtxoByRefIncludingSpent(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxo, err := d.metadata.GetUtxoIncludingSpent(
		txId,
		outputIdx,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	if utxo == nil {
		return nil, nil
	}
	if err := loadCbor(utxo, txn); err != nil {
		return nil, err
	}
	return utxo, nil
}

func (d *Database) UtxosByAddress(
	addr ledger.Address,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAddress(addr, txn.Metadata())
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

func (d *Database) UtxosByAddressAtSlot(
	addr lcommon.Address,
	slot uint64,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAddressAtSlot(
		addr,
		slot,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

// UtxosByAssets returns UTxOs that contain the specified assets
// policyId: the policy ID of the asset (required)
// assetName: the asset name (pass nil to match all assets under the policy, or empty []byte{} to match assets with empty names)
func (d *Database) UtxosByAssets(
	policyId []byte,
	assetName []byte,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAssets(
		policyId,
		assetName,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

func (d *Database) UtxosDeleteConsumed(
	slot uint64,
	limit int,
	txn *Txn,
) (int, error) {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	// Get UTxOs that are marked as deleted and older than our slot window
	utxos, err := d.metadata.GetUtxosDeletedBeforeSlot(
		slot,
		limit,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"failed to query consumed UTxOs during cleanup: %w",
			err,
		)
	}
	utxoCount := len(utxos)
	deleteUtxos := make([]models.UtxoId, utxoCount)
	for idx, utxo := range utxos {
		deleteUtxos[idx] = models.UtxoId{Hash: utxo.TxId, Idx: utxo.OutputIdx}
	}

	// Delete blob data first (best effort)
	_ = deleteUtxoBlobs(d, utxos, txn)

	// Then delete metadata (source of truth)
	err = d.metadata.DeleteUtxos(deleteUtxos, txn.Metadata())
	if err != nil {
		return 0, err
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return 0, err
		}
		owned = false
	}

	return utxoCount, nil
}

func (d *Database) UtxosDeleteRolledback(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	utxos, err := d.metadata.GetUtxosAddedAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	// Delete blob data first (best effort)
	_ = deleteUtxoBlobs(d, utxos, txn)

	// Then delete metadata (source of truth)
	err = d.metadata.DeleteUtxosAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
		owned = false
	}

	return nil
}

func (d *Database) UtxosUnspend(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = NewMetadataOnlyTxn(d, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.SetUtxosNotDeletedAfterSlot(slot, txn.Metadata()); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
		owned = false
	}
	return nil
}
