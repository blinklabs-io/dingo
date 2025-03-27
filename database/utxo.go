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

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/dgraph-io/badger/v4"
)

type Utxo struct {
	ID          uint   `gorm:"primarykey"`
	TxId        []byte `gorm:"index:tx_id_output_idx"`
	OutputIdx   uint32 `gorm:"index:tx_id_output_idx"`
	AddedSlot   uint64 `gorm:"index"`
	DeletedSlot uint64 `gorm:"index"`
	PaymentKey  []byte `gorm:"index"`
	StakingKey  []byte `gorm:"index"`
	Cbor        []byte `gorm:"-"` // This is not represented in the metadata DB
}

func (u *Utxo) TableName() string {
	return "utxo"
}

func (u *Utxo) Decode() (ledger.TransactionOutput, error) {
	return ledger.NewTransactionOutputFromCbor(u.Cbor)
}

func (u *Utxo) loadCbor(txn *Txn) error {
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

func (d *Database) NewUtxo(
	txId []byte,
	outputIdx uint32,
	slot uint64,
	paymentKey, stakeKey, cbor []byte,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	// Add UTxO to blob DB
	key := UtxoBlobKey(txId, outputIdx)
	err := txn.Blob().Set(key, cbor)
	if err != nil {
		return err
	}
	return d.metadata.SetUtxo(txId, outputIdx, slot, paymentKey, stakeKey, txn.Metadata())
}

func (d *Database) UtxoByRef(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (Utxo, error) {
	tmpUtxo := Utxo{}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	utxo, err := d.metadata.GetUtxo(txId, outputIdx, txn.Metadata())
	if err != nil {
		return tmpUtxo, err
	}
	tmpUtxo = Utxo(utxo)
	if err := tmpUtxo.loadCbor(txn); err != nil {
		return tmpUtxo, err
	}
	return tmpUtxo, nil
}

func (d *Database) UtxoConsume(
	utxoId ledger.TransactionInput,
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		txn = NewMetadataOnlyTxn(d, true)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.SetUtxoDeletedAtSlot(utxoId, slot, txn.Metadata())
}

func (d *Database) UtxosByAddress(
	addr ledger.Address,
	txn *Txn,
) ([]Utxo, error) {
	ret := []Utxo{}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	utxos, err := d.metadata.GetUtxosByAddress(addr, txn.Metadata())
	if err != nil {
		return ret, err
	}
	for _, utxo := range utxos {
		tmpUtxo := Utxo(utxo)
		if err := tmpUtxo.loadCbor(txn); err != nil {
			return ret, err
		}
		ret = append(ret, tmpUtxo)
	}
	return ret, nil
}

func (d *Database) UtxosDeleteConsumed(
	slot uint64,
	txn *Txn,
) error {
	var ret error
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	// Get UTxOs that are marked as deleted and older than our slot window
	utxos, err := d.metadata.GetUtxosDeletedBeforeSlot(slot, txn.Metadata())
	if err != nil {
		return errors.New("failed to query consumed UTxOs during cleanup")
	}
	err = d.metadata.DeleteUtxosBeforeSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	// Loop through UTxOs and delete, with a new transaction each loop
	for {
		// short-circuit loop
		if ret != nil {
			break
		}
		batchSize := min(1000, len(utxos))
		if batchSize == 0 {
			break
		}
		// Remove from blob DB
		for _, utxo := range utxos[0:batchSize] {
			key := UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
			err := txn.Blob().Delete(key)
			if err != nil {
				ret = err
				break
			}
		}
		// Remove batch
		utxos = slices.Delete(utxos, 0, batchSize)
	}
	return ret
}

func (d *Database) UtxosDeleteRolledback(
	slot uint64,
	txn *Txn,
) error {
	var ret error
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	utxos, err := d.metadata.GetUtxosDeletedBeforeSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}
	err = d.metadata.DeleteUtxosAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	// Loop through UTxOs and delete, reusing our transaction
	for {
		// short-circuit loop
		if ret != nil {
			break
		}
		batchSize := min(1000, len(utxos))
		if batchSize == 0 {
			break
		}
		// Remove from blob DB
		for _, utxo := range utxos[0:batchSize] {
			key := UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
			err := txn.Blob().Delete(key)
			if err != nil {
				ret = err
				break
			}
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
		txn = NewMetadataOnlyTxn(d, false)
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
