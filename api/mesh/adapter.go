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

package mesh

import (
	"context"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// meshDatabaseAdapter adapts *database.Database to MeshDatabase.
// BlockByHash is a package-level function in the database package rather than
// a method, so this adapter bridges the gap while keeping the Mesh server free
// of a direct *database.Database dependency.
type meshDatabaseAdapter struct {
	db *database.Database
}

// NewMeshDatabase wraps a *database.Database as a MeshDatabase for use in
// the Mesh server config.
func NewMeshDatabase(db *database.Database) MeshDatabase {
	return &meshDatabaseAdapter{db: db}
}

func (a *meshDatabaseAdapter) BlockByHash(ctx context.Context,
	hash []byte,
) (models.Block, error) {
	return database.BlockByHash(ctx, a.db, hash)
}

// BlockByIndex resolves the Cardano block height exposed by Mesh.
func (a *meshDatabaseAdapter) BlockByIndex(ctx context.Context,
	height uint64,
) (models.Block, error) {
	// Mesh block indexes are signed 64-bit, so a larger height cannot be
	// represented in any response.
	if height > math.MaxInt64 {
		return models.Block{}, fmt.Errorf(
			"block height %d exceeds the Mesh index range: %w",
			height,
			models.ErrBlockNotFound,
		)
	}
	if err := ctx.Err(); err != nil {
		return models.Block{}, err
	}
	block, err := database.BlockByNumber(ctx, a.db, height)
	if cancelErr := ctx.Err(); cancelErr != nil {
		return models.Block{}, cancelErr
	}
	return block, err
}

func (a *meshDatabaseAdapter) GetTransactionByHash(ctx context.Context,
	hash []byte,
) (*models.Transaction, error) {
	return a.db.GetTransactionByHash(ctx, hash, nil)
}

func (a *meshDatabaseAdapter) GetTransactionsByBlockHash(ctx context.Context,
	hash []byte,
) ([]models.Transaction, error) {
	return a.db.GetTransactionsByBlockHash(ctx, hash, nil)
}

func (a *meshDatabaseAdapter) UtxosByAddress(
	ctx context.Context,
	addrs []lcommon.Address,
) ([]models.Utxo, error) {
	return a.db.UtxosByAddress(ctx, addrs, 0, nil)
}

func (a *meshDatabaseAdapter) ScanUtxosByAddress(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
	visit func([]models.UtxoWithOrdering) error,
) (ochainsync.Tip, error) {
	return a.db.ScanUtxosByAddress(ctx, q, maxCandidates, maxBytes, visit)
}

func (a *meshDatabaseAdapter) UtxoBalanceByAddressAtSlot(
	ctx context.Context,
	addr lcommon.Address,
	slot uint64,
) (models.AddressBalance, error) {
	txn := database.NewTxnContext(ctx, a.db, false)
	defer txn.Release()
	return a.db.UtxoBalanceByAddressAtSlot(ctx, addr, slot, txn)
}

// boundedDatabase hides the blocks above a tip: with an IndexerDB the
// ledger's block store is ahead of the IndexerDB that holds their
// transactions, and a block answered from the store alone would be empty.
type boundedDatabase struct {
	MeshDatabase
	tip func() (ochainsync.Tip, error)
}

// NewBoundedDatabase makes db answer only for blocks at or below the tip
// that tip returns. A block above it is reported as not found.
func NewBoundedDatabase(
	db MeshDatabase,
	tip func() (ochainsync.Tip, error),
) MeshDatabase {
	return &boundedDatabase{MeshDatabase: db, tip: tip}
}

func (b *boundedDatabase) bound(block models.Block) (models.Block, error) {
	tip, err := b.tip()
	if err != nil {
		return models.Block{}, err
	}
	if block.Slot > tip.Point.Slot {
		return models.Block{}, models.ErrBlockNotFound
	}
	return block, nil
}

func (b *boundedDatabase) BlockByHash(
	ctx context.Context,
	hash []byte,
) (models.Block, error) {
	block, err := b.MeshDatabase.BlockByHash(ctx, hash)
	if err != nil {
		return block, err
	}
	return b.bound(block)
}

func (b *boundedDatabase) BlockByIndex(
	ctx context.Context,
	height uint64,
) (models.Block, error) {
	block, err := b.MeshDatabase.BlockByIndex(ctx, height)
	if err != nil {
		return block, err
	}
	return b.bound(block)
}
