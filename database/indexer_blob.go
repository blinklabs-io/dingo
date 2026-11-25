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

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrIndexerBlocksReadOnly is returned for a write an IndexerDB makes to the
// block store it reads: blocks and their indexes belong to the ledger.
var ErrIndexerBlocksReadOnly = errors.New(
	"database: the IndexerDB reads the ledger's block store and cannot write it",
)

// indexerBlobStore is the blob store of an IndexerDB. Block reads pass
// through to the ledger's block store, which it opens only read-only. UTxO
// and transaction CBOR entries live in the IndexerDB's own metadata store.
type indexerBlobStore struct {
	blocks  blob.BlobStore
	offsets metadata.IndexerOffsetStore
}

// indexerBlobTxn pairs a read transaction on the ledger's block store with
// the IndexerDB metadata transaction its CBOR entries are written in.
type indexerBlobTxn struct {
	blocks types.Txn
	// meta is the metadata transaction of the Txn this handle belongs to.
	// The handle does not own it: the Txn commits and rolls it back. A
	// handle with no metadata transaction runs each entry call on its own.
	meta types.Txn
	// metaWritable reports that meta is a write transaction.
	metaWritable bool
}

func (t *indexerBlobTxn) Commit() error { return t.blocks.Rollback() }

func (t *indexerBlobTxn) Rollback() error { return t.blocks.Rollback() }

// NewIndexerBlobStore returns the blob store of an IndexerDB whose metadata
// store is store: blocks are read from the ledger's block store blocks, and
// UTxO and transaction CBOR entries are kept in store. Closing it does not
// close blocks, which the ledger's database owns.
func NewIndexerBlobStore(
	blocks blob.BlobStore,
	store metadata.MetadataStore,
) (blob.BlobStore, error) {
	if blocks == nil || isNilStore(blocks) {
		return nil, errors.New("the IndexerDB needs the ledger's block store")
	}
	offsets, ok := store.(metadata.IndexerOffsetStore)
	if !ok {
		return nil, errors.New(
			"the IndexerDB metadata store cannot keep CBOR offsets",
		)
	}
	return &indexerBlobStore{blocks: blocks, offsets: offsets}, nil
}

// bindIndexerMetadata gives an IndexerDB blob handle the metadata transaction
// it writes its CBOR entries in, so they commit and roll back with the rows
// they belong to. writable reports that metadataTxn is a write transaction.
func bindIndexerMetadata(blobTxn, metadataTxn types.Txn, writable bool) {
	if t, ok := blobTxn.(*indexerBlobTxn); ok && metadataTxn != nil {
		t.meta = metadataTxn
		t.metaWritable = writable
	}
}

// indexerEntryWriter returns a handle that writes an IndexerDB's CBOR entries
// in txn's own metadata write transaction, and false when txn has none or is
// not an IndexerDB transaction. A write outside that transaction would wait
// behind it on a store with one writer, so every entry write of a
// transaction that holds the writer goes through here.
func indexerEntryWriter(txn *Txn) (blob.BlobStore, types.Txn, func(), bool) {
	if txn == nil {
		return nil, nil, nil, false
	}
	store, ok := txn.BlobStore().(*indexerBlobStore)
	if !ok {
		return nil, nil, nil, false
	}
	if h, ok := txn.Blob().(*indexerBlobTxn); ok {
		if !h.metaWritable {
			return nil, nil, nil, false
		}
		return store, h, func() {}, true
	}
	if txn.Metadata() == nil || !txn.IsReadWrite() || txn.staged {
		return nil, nil, nil, false
	}
	h := &indexerBlobTxn{
		blocks:       store.blocks.NewTransaction(false),
		meta:         txn.Metadata(),
		metaWritable: true,
	}
	return store, h, func() { _ = h.Rollback() }, true
}

func (s *indexerBlobStore) unwrap(txn types.Txn) (*indexerBlobTxn, error) {
	t, ok := txn.(*indexerBlobTxn)
	if !ok || t == nil {
		return nil, types.ErrNilTxn
	}
	return t, nil
}

func (s *indexerBlobStore) blockTxn(txn types.Txn) (types.Txn, error) {
	t, err := s.unwrap(txn)
	if err != nil {
		return nil, err
	}
	return t.blocks, nil
}

// Close releases nothing: the ledger's database owns the block store.
func (s *indexerBlobStore) Close() error { return nil }

// NewTransaction opens a read transaction on the block store whatever
// readWrite asks for: the IndexerDB never writes it.
func (s *indexerBlobStore) NewTransaction(bool) types.Txn {
	return &indexerBlobTxn{blocks: s.blocks.NewTransaction(false)}
}

func (s *indexerBlobStore) Get(txn types.Txn, key []byte) ([]byte, error) {
	b, err := s.blockTxn(txn)
	if err != nil {
		return nil, err
	}
	return s.blocks.Get(b, key)
}

func (s *indexerBlobStore) Set(types.Txn, []byte, []byte) error {
	return ErrIndexerBlocksReadOnly
}

func (s *indexerBlobStore) Delete(types.Txn, []byte) error {
	return ErrIndexerBlocksReadOnly
}

func (s *indexerBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	b, err := s.blockTxn(txn)
	if err != nil {
		// The store's own iterator reports a nil transaction through Err.
		return s.blocks.NewIterator(nil, opts)
	}
	return s.blocks.NewIterator(b, opts)
}

func (s *indexerBlobStore) SetBlock(
	types.Txn, uint64, []byte, []byte, uint64, uint, uint64, []byte,
) error {
	return ErrIndexerBlocksReadOnly
}

func (s *indexerBlobStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	b, err := s.blockTxn(txn)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	return s.blocks.GetBlock(b, slot, hash)
}

// GetBlockLocal reads the block without a remote archive fallback when the
// block store has one to bypass.
func (s *indexerBlobStore) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	b, err := s.blockTxn(txn)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	if r, ok := s.blocks.(blob.LocalBlockReader); ok {
		return r.GetBlockLocal(b, slot, hash)
	}
	return s.blocks.GetBlock(b, slot, hash)
}

func (s *indexerBlobStore) DeleteBlock(types.Txn, uint64, []byte, uint64) error {
	return ErrIndexerBlocksReadOnly
}

func (s *indexerBlobStore) TombstoneBlock(types.Txn, uint64, []byte) error {
	return ErrIndexerBlocksReadOnly
}

func (s *indexerBlobStore) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	b, err := s.blockTxn(txn)
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{}, err
	}
	return s.blocks.GetBlockURL(ctx, b, point)
}

func (s *indexerBlobStore) SetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	cbor []byte,
) error {
	t, err := s.unwrap(txn)
	if err != nil {
		return err
	}
	return s.offsets.SetUtxoOffset(t.meta, txId, outputIdx, cbor)
}

func (s *indexerBlobStore) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	t, err := s.unwrap(txn)
	if err != nil {
		return nil, err
	}
	data, ok, err := s.offsets.GetUtxoOffset(t.meta, txId, outputIdx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, types.ErrBlobKeyNotFound
	}
	return data, nil
}

func (s *indexerBlobStore) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	t, err := s.unwrap(txn)
	if err != nil {
		return err
	}
	return s.offsets.DeleteUtxoOffset(t.meta, txId, outputIdx)
}

func (s *indexerBlobStore) SetTx(
	txn types.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	t, err := s.unwrap(txn)
	if err != nil {
		return err
	}
	return s.offsets.SetTransactionOffset(t.meta, txHash, offsetData)
}

func (s *indexerBlobStore) GetTx(txn types.Txn, txHash []byte) ([]byte, error) {
	t, err := s.unwrap(txn)
	if err != nil {
		return nil, err
	}
	data, ok, err := s.offsets.GetTransactionOffset(t.meta, txHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, types.ErrBlobKeyNotFound
	}
	return data, nil
}

func (s *indexerBlobStore) DeleteTx(txn types.Txn, txHash []byte) error {
	t, err := s.unwrap(txn)
	if err != nil {
		return err
	}
	return s.offsets.DeleteTransactionOffset(t.meta, txHash)
}

// DiskSize is zero: the block store's bytes are the ledger's, and the CBOR
// entries are counted with the metadata store.
func (s *indexerBlobStore) DiskSize() (int64, error) { return 0, nil }

// Sync has nothing to flush: the CBOR entries are durable with the metadata
// commit they are written in.
func (s *indexerBlobStore) Sync() error { return nil }

var _ blob.LocalBlockReader = (*indexerBlobStore)(nil)
