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

package blob

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/types"
)

// IndexerKeyPrefix prefixes every key the IndexerDB writes into a blob store
// it shares with the ledger. Offsets are located by transaction and output
// identity, which are the same in both stores, so without a prefix the two
// writers would overwrite each other's entries.
const IndexerKeyPrefix = "ix:"

// ErrIndexerReadOnly is returned for a write that is not an IndexerDB offset
// entry: blocks, their indexes and the commit timestamp belong to the ledger.
var ErrIndexerReadOnly = errors.New(
	"blob: the indexer view of the shared blob store cannot write this key",
)

// indexerStore is a BlobStore view for the IndexerDB. Reads of blocks and
// block indexes pass through to the shared store. UTxO and transaction offset
// entries live under IndexerKeyPrefix, for both reads and writes, so the
// IndexerDB neither sees nor replaces the ledger's entries. Every other write
// is refused.
type indexerStore struct {
	BlobStore
}

// NewIndexerStore wraps shared as the IndexerDB's view of it. Closing the view
// does not close shared, which the ledger's database owns.
func NewIndexerStore(shared BlobStore) BlobStore {
	return &indexerStore{BlobStore: shared}
}

func indexerKey(key []byte) []byte {
	out := make([]byte, 0, len(IndexerKeyPrefix)+len(key))
	out = append(out, IndexerKeyPrefix...)
	return append(out, key...)
}

// Close releases nothing: the shared store outlives this view.
func (s *indexerStore) Close() error { return nil }

func (s *indexerStore) SetUtxo(
	txn types.Txn, txId []byte, outputIdx uint32, cbor []byte,
) error {
	return s.BlobStore.Set(
		txn,
		indexerKey(types.UtxoBlobKey(txId, outputIdx)),
		cbor,
	)
}

func (s *indexerStore) GetUtxo(
	txn types.Txn, txId []byte, outputIdx uint32,
) ([]byte, error) {
	return s.Get(txn, indexerKey(types.UtxoBlobKey(txId, outputIdx)))
}

func (s *indexerStore) DeleteUtxo(
	txn types.Txn, txId []byte, outputIdx uint32,
) error {
	return s.BlobStore.Delete(
		txn,
		indexerKey(types.UtxoBlobKey(txId, outputIdx)),
	)
}

func (s *indexerStore) SetTx(txn types.Txn, txHash, offsetData []byte) error {
	return s.BlobStore.Set(txn, indexerKey(types.TxBlobKey(txHash)), offsetData)
}

func (s *indexerStore) GetTx(txn types.Txn, txHash []byte) ([]byte, error) {
	return s.Get(txn, indexerKey(types.TxBlobKey(txHash)))
}

func (s *indexerStore) DeleteTx(txn types.Txn, txHash []byte) error {
	return s.BlobStore.Delete(txn, indexerKey(types.TxBlobKey(txHash)))
}

func hasIndexerPrefix(key []byte) bool {
	return len(key) >= len(IndexerKeyPrefix) &&
		string(key[:len(IndexerKeyPrefix)]) == IndexerKeyPrefix
}

func (s *indexerStore) Set(txn types.Txn, key, val []byte) error {
	if !hasIndexerPrefix(key) {
		return ErrIndexerReadOnly
	}
	return s.BlobStore.Set(txn, key, val)
}

func (s *indexerStore) Delete(txn types.Txn, key []byte) error {
	if !hasIndexerPrefix(key) {
		return ErrIndexerReadOnly
	}
	return s.BlobStore.Delete(txn, key)
}

func (s *indexerStore) SetBlock(
	types.Txn, uint64, []byte, []byte, uint64, uint, uint64, []byte,
) error {
	return ErrIndexerReadOnly
}

func (s *indexerStore) DeleteBlock(types.Txn, uint64, []byte, uint64) error {
	return ErrIndexerReadOnly
}

func (s *indexerStore) TombstoneBlock(types.Txn, uint64, []byte) error {
	return ErrIndexerReadOnly
}

// The wrapped store's optional capabilities are forwarded: hiding them would
// make the IndexerDB's batches look unbounded to a store that bounds its
// transactions.

// RemainingTxnEntries forwards to the shared store when it can answer.
func (s *indexerStore) RemainingTxnEntries(
	txn types.Txn,
	entryBytes int,
) (int, bool) {
	if b, ok := s.BlobStore.(TxnBudget); ok {
		return b.RemainingTxnEntries(txn, entryBytes+len(IndexerKeyPrefix))
	}
	return 0, false
}

// DetectsWriteConflicts forwards to the shared store.
func (s *indexerStore) DetectsWriteConflicts() bool {
	d, ok := s.BlobStore.(WriteConflictDetector)
	return ok && d.DetectsWriteConflicts()
}

// GetBlockLocal forwards to the shared store, or reads the block when it
// has no remote fallback to bypass.
func (s *indexerStore) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	if r, ok := s.BlobStore.(LocalBlockReader); ok {
		return r.GetBlockLocal(txn, slot, hash)
	}
	return s.GetBlock(txn, slot, hash)
}

var (
	_ TxnBudget             = (*indexerStore)(nil)
	_ WriteConflictDetector = (*indexerStore)(nil)
	_ LocalBlockReader      = (*indexerStore)(nil)
)
