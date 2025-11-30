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
	"errors"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/dgraph-io/badger/v4"
)

var ErrUtxoNotFound = errors.New("utxo not found")

func loadCbor(u *models.Utxo, txn *Txn) error {
	key := UtxoBlobKey(u.TxId, u.OutputIdx)
	item, err := txn.Blob().Get(key)
	if err != nil {
		return err
	}
	u.Cbor, err = item.ValueCopy(nil)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func (d *Database) AddUtxos(
	utxos []models.UtxoSlot,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	for _, utxoSlot := range utxos {
		// Add UTxO to blob DB
		key := UtxoBlobKey(
			utxoSlot.Utxo.Id.Id().Bytes(),
			utxoSlot.Utxo.Id.Index(),
		)
		utxoCbor := utxoSlot.Utxo.Output.Cbor()
		// Encode output to CBOR if stored CBOR is empty
		if len(utxoCbor) == 0 {
			var err error
			utxoCbor, err = cbor.Encode(utxoSlot.Utxo.Output)
			if err != nil {
				return err
			}
		}
		err := txn.Blob().Set(key, utxoCbor)
		if err != nil {
			return err
		}
	}
	return d.metadata.AddUtxos(
		utxos,
		txn.Metadata(),
	)
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
	var ret error
	if txn == nil {
		txn = d.Transaction(true)
		defer txn.Commit() //nolint:errcheck
	}
	// Get UTxOs that are marked as deleted and older than our slot window
	utxos, err := d.metadata.GetUtxosDeletedBeforeSlot(
		slot,
		limit,
		txn.Metadata(),
	)
	if err != nil {
		return 0, errors.New("failed to query consumed UTxOs during cleanup")
	}
	utxoCount := len(utxos)
	deleteUtxos := make([]any, utxoCount)
	for idx, utxo := range utxos {
		deleteUtxos[idx] = utxo
	}
	err = d.metadata.DeleteUtxos(deleteUtxos, txn.Metadata())
	if err != nil {
		return 0, err
	}

	// Loop through UTxOs and delete, with a new transaction each loop
	for ret == nil {
		// short-circuit loop

		batchSize := min(1000, len(utxos))
		if batchSize == 0 {
			break
		}
		loopTxn := NewBlobOnlyTxn(d, true)
		err := loopTxn.Do(func(txn *Txn) error {
			// Remove from blob DB
			for _, utxo := range utxos[0:batchSize] {
				key := UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
				err := txn.Blob().Delete(key)
				if err != nil {
					ret = err
					return err
				}
			}
			return nil
		})
		if err != nil {
			ret = err
			break
		}
		// Remove batch
		utxos = slices.Delete(utxos, 0, batchSize)
	}
	return utxoCount, ret
}

func (d *Database) UtxosDeleteRolledback(
	slot uint64,
	txn *Txn,
) error {
	var ret error
	if txn == nil {
		txn = d.Transaction(true)
		defer txn.Commit() //nolint:errcheck
	}
	utxos, err := d.metadata.GetUtxosAddedAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}
	err = d.metadata.DeleteUtxosAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	// Loop through UTxOs and delete, reusing our transaction
	for ret == nil {
		// short-circuit loop

		batchSize := min(1000, len(utxos))
		if batchSize == 0 {
			break
		}
		loopTxn := NewBlobOnlyTxn(d, true)
		err := loopTxn.Do(func(txn *Txn) error {
			// Remove from blob DB
			for _, utxo := range utxos[0:batchSize] {
				key := UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
				err := txn.Blob().Delete(key)
				if err != nil {
					ret = err
					return err
				}
			}
			return nil
		})
		if err != nil {
			ret = err
			break
		}
		// Remove batch
		utxos = slices.Delete(utxos, 0, batchSize)
	}
	return ret
}

func (d *Database) UtxosUnspend(
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		txn = NewMetadataOnlyTxn(d, true)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.SetUtxosNotDeletedAfterSlot(slot, txn.Metadata())
}

func UtxoBlobKey(txId []byte, outputIdx uint32) []byte {
	key := []byte("u")
	key = append(key, txId...)
	// Convert index to bytes
	idxBytes := make([]byte, 4)
	new(big.Int).SetUint64(uint64(outputIdx)).FillBytes(idxBytes)
	key = append(key, idxBytes...)
	return key
}
