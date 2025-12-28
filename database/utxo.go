// Copyright 2025 Blink Labs Software
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
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
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
	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	val, err := blob.GetUtxo(txn.Blob(), u.TxId, u.OutputIdx)
	if err != nil {
		// Map blob-key-not-found to ErrUtxoNotFound since it means the UTxO's blob is missing
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return ErrUtxoNotFound
		}
		return err
	}
	u.Cbor = val
	return nil
}

func (d *Database) AddUtxos(
	utxos []models.UtxoSlot,
	txn *Txn,
) error {
	owned := txn == nil
	if owned {
		txn = d.Transaction(true)
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	for _, utxoSlot := range utxos {
		// Add UTxO to blob DB
		txId := utxoSlot.Utxo.Id.Id().Bytes()
		outputIdx := utxoSlot.Utxo.Id.Index()
		utxoCbor := utxoSlot.Utxo.Output.Cbor()
		// Encode output to CBOR if stored CBOR is empty
		if len(utxoCbor) == 0 {
			var err error
			utxoCbor, err = cbor.Encode(utxoSlot.Utxo.Output)
			if err != nil {
				return err
			}
		}
		err := blob.SetUtxo(blobTxn, txId, outputIdx, utxoCbor)
		if err != nil {
			return err
		}
	}
	err := d.metadata.AddUtxos(
		utxos,
		txn.Metadata(),
	)
	if err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
		owned = false // prevent deferred rollback after successful commit
		return nil
	}
	return nil
}

func (d *Database) UtxoByRef(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
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

func (d *Database) UtxosByAddress(
	addr ledger.Address,
	txn *Txn,
) ([]models.Utxo, error) {
	ret := []models.Utxo{}
	tmpUtxo := models.Utxo{}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	utxos, err := d.metadata.GetUtxosByAddress(addr, txn.Metadata())
	if err != nil {
		return ret, err
	}
	for _, utxo := range utxos {
		tmpUtxo = models.Utxo{
			ID:          utxo.ID,
			TxId:        utxo.TxId,
			OutputIdx:   utxo.OutputIdx,
			AddedSlot:   utxo.AddedSlot,
			DeletedSlot: utxo.DeletedSlot,
			PaymentKey:  utxo.PaymentKey,
			StakingKey:  utxo.StakingKey,
			Amount:      utxo.Amount,
			Assets:      utxo.Assets,
		}
		if err := loadCbor(&tmpUtxo, txn); err != nil {
			return ret, err
		}
		ret = append(ret, tmpUtxo)
	}
	return ret, nil
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
