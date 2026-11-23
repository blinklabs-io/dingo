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

package ledger

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TxIndexEntry locates an applied transaction on the ledger's chain: the
// block it was applied in, its position there, its era and its phase-2
// validity. A Leios closure transaction is located by the ranking block that
// certified its endorser block.
type TxIndexEntry struct {
	Hash       []byte
	BlockHash  []byte
	Slot       uint64
	BlockIndex uint32
	Type       uint8
	Valid      bool
}

// TxIndexReader serves the transaction index from the LedgerDB.
type TxIndexReader interface {
	TxIndexLoaded() (bool, error)
	TxIndexEntry(hash []byte) (*TxIndexEntry, error)
	TxIndexEntryAt(hash []byte, slot uint64) (*TxIndexEntry, error)
	EachTxIndexEntry(fn func(TxIndexEntry) error) error
}

// txIndexChanges lists the transactions the LedgerDB's index gained after a
// slot, as far back as its rollback journal reaches.
type txIndexChanges interface {
	TxHashesAfterSlot(txn types.Txn, slot uint64) ([][]byte, error)
}

// errTxIndexUnavailable is the error of a transaction lookup the LedgerDB
// cannot answer. A lookup never reports such a transaction as absent: an
// absent transaction is one the ledger may apply again.
var errTxIndexUnavailable = errors.New(
	"the LedgerDB transaction index is not available",
)

func (ls *LedgerState) txIndexReader(set UtxoSetWriter) (TxIndexReader, error) {
	r, ok := set.(TxIndexReader)
	if !ok || ls.utxoSetFailed.Load() {
		return nil, errTxIndexUnavailable
	}
	if ls.txIndexSetLoaded.Load() {
		return r, nil
	}
	loaded, err := r.TxIndexLoaded()
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, errTxIndexUnavailable
	}
	ls.txIndexSetLoaded.Store(true)
	return r, nil
}

// txIndexView returns the index entries of hashes as txn sees the LedgerDB,
// keyed by hash; a transaction not applied has no entry.
func (ls *LedgerState) txIndexView(
	txn ledgerdb.Txn,
	hashes [][]byte,
) (map[string]TxIndexEntry, error) {
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	return ls.txIndexViewOf(ls.readSet(txn), ls.pinOf(txn), own, hashes)
}

// txIndexViewOf is txIndexView over set, for a reader pinned to pin whose own
// staged batch is own. A staged entry stands unless a batch staged after it
// rolls back below its slot, and the LedgerDB's own rows are read as the
// lowest staged rollback would leave them.
func (ls *LedgerState) txIndexViewOf(
	set UtxoSetWriter,
	pin *utxoSetPin,
	own *utxoSetBatch,
	hashes [][]byte,
) (map[string]TxIndexEntry, error) {
	r, err := ls.txIndexReader(set)
	if err != nil {
		return nil, err
	}
	// Held across the LedgerDB reads so no batch moves from staged to
	// applied between the two halves of the answer.
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	batches := ls.utxoSetBatchesSeenBy(pin, own)
	found := make(map[string]TxIndexEntry, len(hashes))
	ceilings := make(map[string]uint64, len(hashes))
	ls.utxoSetMu.Lock()
	for _, hash := range hashes {
		key := string(hash)
		if _, done := ceilings[key]; done {
			continue
		}
		ceiling := uint64(math.MaxUint64)
		for _, b := range slices.Backward(batches) {
			if at, ok := b.txIndexAt[key]; ok {
				if e := b.txIndex[at]; e.Slot <= ceiling {
					found[key] = e
					break
				}
			}
			if b.rollbackTo != nil {
				ceiling = min(ceiling, b.rollbackTo.Slot)
			}
		}
		ceilings[key] = ceiling
	}
	ls.utxoSetMu.Unlock()
	for key, ceiling := range ceilings {
		if _, staged := found[key]; staged {
			continue
		}
		var e *TxIndexEntry
		if ceiling == math.MaxUint64 {
			e, err = r.TxIndexEntry([]byte(key))
		} else {
			e, err = r.TxIndexEntryAt([]byte(key), ceiling)
		}
		if err != nil {
			return nil, err
		}
		if e != nil {
			found[key] = *e
		}
	}
	return found, nil
}

// TxLocation implements types.TxIndexSource: where the transaction with hash
// was applied, as the ledger transaction writing in meta sees the index.
func (ls *LedgerState) TxLocation(
	meta types.Txn,
	hash []byte,
) (types.TxLocation, bool, error) {
	view, err := ls.txIndexViewOf(
		ls.readSetMeta(meta),
		ls.pinOfMeta(meta),
		ls.utxoSetBatchFor(meta),
		[][]byte{hash},
	)
	if err != nil {
		return types.TxLocation{}, false, err
	}
	e, ok := view[string(hash)]
	if !ok {
		return types.TxLocation{}, false, nil
	}
	return types.TxLocation{
		BlockHash:  e.BlockHash,
		Slot:       e.Slot,
		BlockIndex: e.BlockIndex,
		Type:       e.Type,
		Valid:      e.Valid,
	}, true, nil
}

// stageTxIndex records that the transaction with hash was applied in the
// block at point, for the LedgerDB to index once txn commits.
func (ls *LedgerState) stageTxIndex(
	txn ledgerdb.Txn,
	hash []byte,
	point ocommon.Point,
	blockIndex uint32,
	era uint8,
	valid bool,
) {
	ls.stageLedgerChanges(txn, LedgerChanges{TxIndex: []TxIndexEntry{{
		Hash:       bytes.Clone(hash),
		BlockHash:  bytes.Clone(point.Hash),
		Slot:       point.Slot,
		BlockIndex: blockIndex,
		Type:       era,
		Valid:      valid,
	}}})
}

func txIndexTransaction(e TxIndexEntry) models.Transaction {
	return models.Transaction{
		Hash:       e.Hash,
		BlockHash:  e.BlockHash,
		Slot:       e.Slot,
		BlockIndex: e.BlockIndex,
		Type:       int(e.Type),
		Valid:      e.Valid,
	}
}

// GetTransactionByHash returns where the ledger applied the transaction with
// hash, or nil when it applied none. Only the fields the index holds are set.
func (s stagingLedgerDB) GetTransactionByHash(
	_ context.Context,
	hash []byte,
	txn ledgerdb.Txn,
) (*models.Transaction, error) {
	view, err := s.ls.txIndexView(txn, [][]byte{hash})
	if err != nil {
		return nil, err
	}
	e, ok := view[string(hash)]
	if !ok {
		return nil, nil
	}
	tx := txIndexTransaction(e)
	return &tx, nil
}

// GetTransactionsByHashes is GetTransactionByHash for several hashes, in the
// order given, each applied transaction once.
func (s stagingLedgerDB) GetTransactionsByHashes(
	_ context.Context,
	hashes [][]byte,
	txn ledgerdb.Txn,
) ([]models.Transaction, error) {
	view, err := s.ls.txIndexView(txn, hashes)
	if err != nil {
		return nil, err
	}
	ret := make([]models.Transaction, 0, len(view))
	for _, hash := range hashes {
		e, ok := view[string(hash)]
		if !ok {
			continue
		}
		delete(view, string(hash))
		ret = append(ret, txIndexTransaction(e))
	}
	return ret, nil
}

func (s stagingLedgerDB) SetGenesisTransaction(
	ctx context.Context,
	txHash []byte,
	blockHash []byte,
	outputs []lcommon.Utxo,
	offsets map[database.UtxoRef]database.CborOffset,
	txn ledgerdb.Txn,
) error {
	if err := s.LedgerDB.SetGenesisTransaction(
		ctx, txHash, blockHash, outputs, offsets, txn,
	); err != nil {
		return err
	}
	s.ls.stageTxIndex( //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		txn,
		txHash,
		ocommon.Point{Hash: blockHash},
		0,
		0,
		true,
	)
	return nil
}

// TxHashesAfterSlot implements types.TxIndexSource: the hash of every
// transaction applied after slot, as the ledger transaction writing in meta
// sees the index.
func (ls *LedgerState) TxHashesAfterSlot(
	meta types.Txn,
	slot uint64,
) ([][]byte, error) {
	src, ok := ls.readSetMeta(meta).(txIndexChanges)
	if !ok || ls.utxoSetFailed.Load() {
		return nil, errTxIndexUnavailable
	}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	hashes, err := src.TxHashesAfterSlot(nil, slot)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		seen[string(h)] = struct{}{}
	}
	batches := ls.utxoSetBatchesSeenBy(
		ls.pinOfMeta(meta), ls.utxoSetBatchFor(meta),
	)
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	for _, b := range batches {
		for _, e := range b.txIndex {
			if e.Slot <= slot {
				continue
			}
			if _, dup := seen[string(e.Hash)]; dup {
				continue
			}
			seen[string(e.Hash)] = struct{}{}
			hashes = append(hashes, e.Hash)
		}
	}
	return hashes, nil
}
