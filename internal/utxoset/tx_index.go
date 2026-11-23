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

package utxoset

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
)

// txIndexFixedLen is the slot, block index, era id and validity flag that
// start an index row; the block hash follows.
const txIndexFixedLen = 8 + 4 + 1 + 1

// txIndexHashLen is the length of a transaction hash and of a block hash.
const txIndexHashLen = 32

// encodeTxIndexEntry refuses an entry whose hashes are not hash-sized, so a
// row is always one of two fixed lengths: with its block hash, or without
// one for a transaction recorded under no block.
func encodeTxIndexEntry(e ledger.TxIndexEntry) ([]byte, error) {
	if len(e.Hash) != txIndexHashLen {
		return nil, fmt.Errorf(
			"utxoset: transaction index hash of %d bytes", len(e.Hash),
		)
	}
	if len(e.BlockHash) != 0 && len(e.BlockHash) != txIndexHashLen {
		return nil, fmt.Errorf(
			"utxoset: transaction index block hash of %d bytes",
			len(e.BlockHash),
		)
	}
	v := make([]byte, 0, txIndexFixedLen+txIndexHashLen)
	v = binary.BigEndian.AppendUint64(v, e.Slot)
	v = binary.BigEndian.AppendUint32(v, e.BlockIndex)
	v = append(v, e.Type)
	valid := byte(0)
	if e.Valid {
		valid = 1
	}
	v = append(v, valid)
	return append(v, e.BlockHash...), nil
}

func decodeTxIndexEntry(hash, v []byte) (*ledger.TxIndexEntry, error) {
	if len(hash) != txIndexHashLen ||
		(len(v) != txIndexFixedLen &&
			len(v) != txIndexFixedLen+txIndexHashLen) {
		return nil, fmt.Errorf(
			"utxoset: malformed transaction index row: key of %d bytes, value of %d",
			len(hash),
			len(v),
		)
	}
	return &ledger.TxIndexEntry{
		Hash:       bytes.Clone(hash),
		Slot:       binary.BigEndian.Uint64(v),
		BlockIndex: binary.BigEndian.Uint32(v[8:]),
		Type:       v[12],
		Valid:      v[13] == 1,
		BlockHash:  bytes.Clone(v[txIndexFixedLen:]),
	}, nil
}

func txIndexRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.TxIndex))
	for _, e := range lc.TxIndex {
		v, err := encodeTxIndexEntry(e)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  e.Slot,
			Key:   badgerutxo.TxIndexKey(e.Hash),
			Value: v,
		})
	}
	return out, nil
}

// ensureTxIndex loads the metadata store's applied transactions into a set
// from before it kept the index. The rows stream into the set, so the load
// holds no more than one in memory; a load that stops partway leaves no
// marker and starts over.
func (s *Set) ensureTxIndex(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.TxIndexLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the transaction index marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	n := 0
	err = s.store.LoadTxIndex(func(put func(k, v []byte) error) error {
		return ldb.IterateAppliedTransactions(
			txn,
			func(tx models.AppliedTransaction) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				v, err := encodeTxIndexEntry(ledger.TxIndexEntry{
					Hash: tx.Hash, Slot: tx.Slot, BlockHash: tx.BlockHash,
					BlockIndex: tx.BlockIndex, Type: tx.Type,
					Valid: tx.Valid,
				})
				if err != nil {
					return err
				}
				n++
				return put(badgerutxo.TxIndexKey(tx.Hash), v)
			},
		)
	})
	if err != nil {
		return fmt.Errorf("utxoset: load the transaction index: %w", err)
	}
	s.logger.Info(
		"loaded the transaction index into the LedgerDB", "transactions", n,
	)
	return nil
}

// TxIndexLoaded reports whether the set holds the transaction index.
func (s *Set) TxIndexLoaded() (bool, error) {
	return s.store.TxIndexLoaded()
}

// TxIndexEntry returns the index row of the transaction with hash, or nil
// when the ledger has applied none.
func (s *Set) TxIndexEntry(hash []byte) (*ledger.TxIndexEntry, error) {
	if len(hash) != txIndexHashLen {
		return nil, nil
	}
	v, ok, err := s.store.Row(badgerutxo.TxIndexKey(hash))
	if err != nil || !ok {
		return nil, err
	}
	return decodeTxIndexEntry(hash, v)
}

// TxIndexEntryAt returns the index row of the transaction with hash as a
// rollback to slot would leave it.
func (s *Set) TxIndexEntryAt(
	hash []byte,
	slot uint64,
) (*ledger.TxIndexEntry, error) {
	if len(hash) != txIndexHashLen {
		return nil, nil
	}
	v, ok, err := s.store.RowAt(badgerutxo.TxIndexKey(hash), slot)
	if err != nil || !ok {
		return nil, err
	}
	return decodeTxIndexEntry(hash, v)
}

// EachTxIndexEntry calls fn with every row of the transaction index, in hash
// order.
func (s *Set) EachTxIndexEntry(fn func(ledger.TxIndexEntry) error) error {
	return s.store.Rows(
		[]byte{badgerutxo.TxIndexTag},
		func(k, v []byte) error {
			e, err := decodeTxIndexEntry(k[1:], v)
			if err != nil {
				return err
			}
			return fn(*e)
		},
	)
}

// TxHashesAfterSlot lists, over the set's committed state, the hash of every
// transaction the index gained after slot, as far back as the rollback
// journal reaches.
func (s *Set) TxHashesAfterSlot(_ types.Txn, slot uint64) ([][]byte, error) {
	keys, err := s.store.RowKeysChangedAfter(
		[]byte{badgerutxo.TxIndexTag}, slot,
	)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		out = append(out, k[1:])
	}
	return out, nil
}
