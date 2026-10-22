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

package server

import (
	"context"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// databaseAdapter adapts *database.Database to MidnightDatabase.
// BlockByHash, BlocksRecent, and BlockBeforeSlot are package-level functions
// in the database package rather than methods, so this adapter bridges the
// gap while keeping the rest of this package free of a direct
// *database.Database dependency (mirrors api/mesh's meshDatabaseAdapter).
type databaseAdapter struct {
	db *database.Database
}

// NewDatabase wraps a *database.Database as a MidnightDatabase for use in
// the Midnight gRPC server config.
func NewDatabase(db *database.Database) MidnightDatabase {
	return &databaseAdapter{db: db}
}

func (a *databaseAdapter) GetLatestMidnightGovernanceDatum(
	ctx context.Context,
	datumType string,
	blockNumber uint64,
) (*models.MidnightGovernanceDatum, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.Metadata().GetLatestMidnightGovernanceDatum(
		datumType, blockNumber, txn.Metadata(),
	)
}

func (a *databaseAdapter) GetMidnightAriadneParamsAtOrBeforeEpoch(
	ctx context.Context,
	epoch uint64,
) (*models.MidnightAriadneParams, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.Metadata().GetMidnightAriadneParamsAtOrBeforeEpoch(
		epoch, txn.Metadata(),
	)
}

func (a *databaseAdapter) GetMidnightEpochCandidatesByEpoch(
	ctx context.Context,
	epoch uint64,
) (*models.MidnightEpochCandidates, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.Metadata().GetMidnightEpochCandidatesByEpoch(
		epoch, txn.Metadata(),
	)
}

func (a *databaseAdapter) GetMidnightCommitteeCandidateRegistrationsByTxHashes(
	ctx context.Context,
	txHashes [][]byte,
) ([]models.MidnightCommitteeCandidateRegistration, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.Metadata().GetMidnightCommitteeCandidateRegistrationsByTxHashes(
		txHashes, txn.Metadata(),
	)
}

func (a *databaseAdapter) GetPoolStakeSnapshotsByEpoch(
	ctx context.Context,
	epoch uint64,
	snapshotType string,
) ([]*models.PoolStakeSnapshot, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.GetPoolStakeSnapshotsByEpoch(epoch, snapshotType, txn)
}

func (a *databaseAdapter) GetEpoch(
	ctx context.Context,
	epochId uint64,
) (*models.Epoch, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.GetEpoch(epochId, txn)
}

func (a *databaseAdapter) GetEpochBySlot(
	ctx context.Context,
	slot uint64,
) (*models.Epoch, error) {
	txn, err := a.readTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Release()
	return a.db.GetEpochBySlot(slot, txn)
}

func (a *databaseAdapter) readTransaction(
	ctx context.Context,
) (*database.Txn, error) {
	txn := a.db.TransactionContext(ctx, false)
	if txn.Metadata() == nil {
		txn.Release()
		return nil, types.ErrNilTxn
	}
	return txn, nil
}

func (a *databaseAdapter) BlockByHash(ctx context.Context, hash []byte) (models.Block, error) {
	return database.BlockByHash(ctx, a.db, hash)
}

func (a *databaseAdapter) BlockByNumber(ctx context.Context, number uint64) (models.Block, error) {
	// Cardano block numbers are 0-based; the blob store's internal index is
	// 1-based (database.BlockInitialIndex). Translate here, the same way
	// api/blockfrost's block-by-height lookup does, so callers deal only in
	// consensus block numbers. Guard the addition first: no real chain gets
	// anywhere near math.MaxUint64, but without the check that value would
	// wrap to internal index 0 instead of failing.
	if number > math.MaxUint64-database.BlockInitialIndex {
		return models.Block{}, fmt.Errorf(
			"block number %d overflows internal index: %w",
			number,
			models.ErrBlockNotFound,
		)
	}
	if err := ctx.Err(); err != nil {
		return models.Block{}, err
	}
	block, err := a.db.BlockByIndex(number+database.BlockInitialIndex, nil)
	if cancelErr := ctx.Err(); cancelErr != nil {
		return models.Block{}, cancelErr
	}
	return block, err
}

func (a *databaseAdapter) BlocksRecent(ctx context.Context, count int) ([]models.Block, error) {
	return database.BlocksRecent(ctx, a.db, count)
}

func (a *databaseAdapter) BlockBeforeSlot(ctx context.Context, slot uint64) (models.Block, error) {
	return database.BlockBeforeSlot(ctx, a.db, slot)
}
