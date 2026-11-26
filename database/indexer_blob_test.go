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
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// writeGuardStore records every write that reaches the ledger's block store,
// including the opening of a write transaction.
type writeGuardStore struct {
	blob.BlobStore
	mu     sync.Mutex
	writes []string
}

func (g *writeGuardStore) record(format string, args ...any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writes = append(g.writes, fmt.Sprintf(format, args...))
}

func (g *writeGuardStore) recorded() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.writes...)
}

func (g *writeGuardStore) NewTransaction(readWrite bool) types.Txn {
	if readWrite {
		g.record("open a write transaction")
	}
	return g.BlobStore.NewTransaction(readWrite)
}

func (g *writeGuardStore) Set(txn types.Txn, key, val []byte) error {
	g.record("set %q", key)
	return g.BlobStore.Set(txn, key, val)
}

func (g *writeGuardStore) Delete(txn types.Txn, key []byte) error {
	g.record("delete %q", key)
	return g.BlobStore.Delete(txn, key)
}

func (g *writeGuardStore) SetUtxo(
	txn types.Txn, txId []byte, idx uint32, cbor []byte,
) error {
	g.record("set utxo %x#%d", txId, idx)
	return g.BlobStore.SetUtxo(txn, txId, idx, cbor)
}

func (g *writeGuardStore) DeleteUtxo(
	txn types.Txn, txId []byte, idx uint32,
) error {
	g.record("delete utxo %x#%d", txId, idx)
	return g.BlobStore.DeleteUtxo(txn, txId, idx)
}

func (g *writeGuardStore) SetTx(txn types.Txn, hash, data []byte) error {
	g.record("set tx %x", hash)
	return g.BlobStore.SetTx(txn, hash, data)
}

func (g *writeGuardStore) DeleteTx(txn types.Txn, hash []byte) error {
	g.record("delete tx %x", hash)
	return g.BlobStore.DeleteTx(txn, hash)
}

func (g *writeGuardStore) SetBlock(
	txn types.Txn, slot uint64, hash, cbor []byte, id uint64, blockType uint,
	height uint64, prevHash []byte,
) error {
	g.record("set block %d", slot)
	return g.BlobStore.SetBlock(
		txn, slot, hash, cbor, id, blockType, height, prevHash,
	)
}

func (g *writeGuardStore) DeleteBlock(
	txn types.Txn, slot uint64, hash []byte, id uint64,
) error {
	g.record("delete block %d", slot)
	return g.BlobStore.DeleteBlock(txn, slot, hash, id)
}

func (g *writeGuardStore) TombstoneBlock(
	txn types.Txn, slot uint64, hash []byte,
) error {
	g.record("tombstone block %d", slot)
	return g.BlobStore.TombstoneBlock(txn, slot, hash)
}

// indexedConsumer stores the blocks of a produce-then-consume pair in the
// ledger's block store and indexes the consuming transaction in the
// IndexerDB, which reads the ledger's block store through guard.
func indexedConsumer(t *testing.T) (
	indexerTestDB,
	*writeGuardStore,
	gapRollbackCandidate,
) {
	t.Helper()
	guard := &writeGuardStore{}
	fixture := newIndexerTestDBOver(t, nil, func(
		ledger blob.BlobStore,
	) blob.BlobStore {
		guard.BlobStore = ledger
		return guard
	})
	candidate := findGapRollbackCandidateWithoutCertificates(t)
	ledgerTxn := fixture.ledger.Transaction(context.Background(), true)
	require.NoError(t, ledgerTxn.Do(func(txn *Txn) error {
		for _, block := range candidate.producerBlocks {
			if err := fixture.ledger.BlockCreate(block, txn); err != nil {
				return err
			}
		}
		return fixture.ledger.BlockCreate(candidate.consumerBlock, txn)
	}))
	require.NoError(t, fixture.indexer.SetTransaction(
		context.Background(),
		candidate.consumerTx,
		candidate.consumerPoint,
		0, 0, nil, nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		nil, 0,
	))
	return fixture, guard, candidate
}

func indexerOffsets(t *testing.T, db *Database) metadata.IndexerOffsetStore {
	t.Helper()
	offsets, ok := db.Metadata().(metadata.IndexerOffsetStore)
	require.True(t, ok)
	return offsets
}

func TestIndexerDBResolvesCborThroughItsOwnOffsets(t *testing.T) {
	t.Parallel()
	fixture, _, candidate := indexedConsumer(t)
	tx := candidate.consumerTx
	txHash := tx.Hash().Bytes()
	offsets := indexerOffsets(t, fixture.indexer)

	data, ok, err := offsets.GetTransactionOffset(nil, txHash)
	require.NoError(t, err)
	require.True(t, ok, "the transaction's offset is in the IndexerDB")
	require.True(t, IsTxOffsetStorage(data))
	txOffset, err := DecodeTxOffset(data)
	require.NoError(t, err)
	block := candidate.consumerBlock.Cbor
	wantTx := block[txOffset.ByteOffset : txOffset.ByteOffset+txOffset.ByteLength]
	data, ok, err = offsets.GetUtxoOffset(nil, txHash, 0)
	require.NoError(t, err)
	require.True(t, ok, "the output's offset is in the IndexerDB")
	require.True(t, IsUtxoOffsetStorage(data))

	// A cache of its own has nothing hot, so both reads resolve the offsets
	// against the ledger's block.
	cache := NewTieredCborCache(fixture.indexer.config.CacheConfig, fixture.indexer)
	txn := fixture.indexer.Transaction(context.Background(), false)
	defer txn.Release()
	gotTx, err := cache.ResolveTxCbor(txn, txHash)
	require.NoError(t, err)
	require.Equal(t, wantTx, gotTx)
	gotUtxo, err := cache.ResolveUtxoCbor(txHash, 0, txn)
	require.NoError(t, err)
	require.Equal(t, tx.Produced()[0].Output.Cbor(), gotUtxo)
	utxo, err := fixture.indexer.UtxoByRef(
		context.Background(), txHash, 0, txn,
	)
	require.NoError(t, err)
	require.Equal(t, tx.Produced()[0].Output.Cbor(), utxo.Cbor)
}

func TestIndexerDBWritesNothingToTheLedgerBlockStore(t *testing.T) {
	t.Parallel()
	fixture, guard, candidate := indexedConsumer(t)
	require.NoError(t, fixture.indexer.RollbackMetadataAfterSlot(
		context.Background(),
		ocommon.NewPoint(candidate.consumerPoint.Slot-1, nil),
		0, nil,
	))
	require.Empty(t, guard.recorded(),
		"the IndexerDB indexes and rolls back without writing the ledger's store")
	txn := fixture.indexer.Transaction(context.Background(), true)
	defer txn.Release()
	require.ErrorIs(t,
		txn.BlobStore().Set(txn.Blob(), []byte("k"), []byte("v")),
		ErrIndexerBlocksReadOnly)
	require.Empty(t, guard.recorded())
}

func TestIndexerDBRollbackRemovesItsOffsets(t *testing.T) {
	t.Parallel()
	fixture, _, candidate := indexedConsumer(t)
	txHash := candidate.consumerTx.Hash().Bytes()
	offsets := indexerOffsets(t, fixture.indexer)
	_, ok, err := offsets.GetUtxoOffset(nil, txHash, 0)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, fixture.indexer.RollbackMetadataAfterSlot(
		context.Background(),
		ocommon.NewPoint(candidate.consumerPoint.Slot-1, nil),
		0, nil,
	))
	for idx := range candidate.consumerTx.Produced() {
		_, ok, err := offsets.GetUtxoOffset(nil, txHash, uint32(idx)) // #nosec G115
		require.NoError(t, err)
		require.False(t, ok, "the rolled-back output %d keeps no offset", idx)
	}
	_, ok, err = offsets.GetTransactionOffset(nil, txHash)
	require.NoError(t, err)
	require.False(t, ok, "the rolled-back transaction keeps no offset")
}
