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

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/dgraph-io/badger/v4"
)

type Utxo struct {
	TxId        []byte
	OutputIdx   uint32
	AddedSlot   uint64
	DeletedSlot uint64
	PaymentKey  []byte
	StakingKey  []byte
	Cbor        []byte
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

func UtxoByRef(
	db Database,
	txId []byte,
	outputIdx uint32,
) (Utxo, error) {
	return db.(*BaseDatabase).UtxoByRef(txId, outputIdx, nil)
}

func (d *BaseDatabase) UtxoByRef(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (Utxo, error) {
	tmpUtxo := Utxo{}
	if txn == nil {
		txn = d.Transaction(false)
	}
	utxo, err := txn.DB().Metadata().GetUtxo(txId, outputIdx, txn.Metadata())
	if err != nil {
		return tmpUtxo, err
	}
	tmpUtxo.TxId = utxo.TxId
	tmpUtxo.OutputIdx = utxo.OutputIdx
	tmpUtxo.AddedSlot = utxo.AddedSlot
	tmpUtxo.DeletedSlot = utxo.DeletedSlot
	tmpUtxo.PaymentKey = utxo.PaymentKey
	tmpUtxo.StakingKey = utxo.StakingKey
	if err := tmpUtxo.loadCbor(txn); err != nil {
		return tmpUtxo, err
	}
	return tmpUtxo, nil
}

func UtxosByAddress(
	db Database,
	addr ledger.Address,
) ([]Utxo, error) {
	return db.(*BaseDatabase).UtxosByAddress(addr, nil)
}

func (d *BaseDatabase) UtxosByAddress(
	addr ledger.Address,
	txn *Txn,
) ([]Utxo, error) {
	ret := []Utxo{}
	if txn == nil {
		txn = d.Transaction(false)
	}
	utxos, err := txn.DB().Metadata().GetUtxosByAddress(addr, txn.Metadata())
	if err != nil {
		return ret, err
	}
	for _, utxo := range utxos {
		tmpUtxo := Utxo{
			TxId:        utxo.TxId,
			OutputIdx:   utxo.OutputIdx,
			AddedSlot:   utxo.AddedSlot,
			DeletedSlot: utxo.DeletedSlot,
			PaymentKey:  utxo.PaymentKey,
			StakingKey:  utxo.StakingKey,
		}
		if err := tmpUtxo.loadCbor(txn); err != nil {
			return ret, err
		}
		ret = append(ret, tmpUtxo)
	}
	return ret, nil
}

func UtxoDelete(
	db Database,
	utxo Utxo,
) error {
	return db.(*BaseDatabase).UtxoDelete(utxo, nil)
}

func (d *BaseDatabase) UtxoDelete(
	utxo Utxo,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
	}
	// Remove from metadata DB
	err := txn.DB().Metadata().DeleteUtxo(utxo, txn.Metadata())
	if err != nil {
		return err
	}
	// Remove from blob DB
	key := UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
	err = txn.Blob().Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func UtxosDelete(
	db Database,
	utxos []Utxo,
) error {
	return db.(*BaseDatabase).UtxosDelete(utxos, nil)
}

func (d *BaseDatabase) UtxosDelete(
	utxos []Utxo,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
	}
	// Remove from metadata DB
	err := txn.DB().Metadata().DeleteUtxos([]any{utxos}, txn.Metadata())
	if err != nil {
		return err
	}
	// Remove from blob DB
	for _, utxo := range utxos {
		key := UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
		err := txn.Blob().Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
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
