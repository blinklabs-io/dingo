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
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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
	datumType string,
	blockNumber uint64,
) (*models.MidnightGovernanceDatum, error) {
	return a.db.GetLatestMidnightGovernanceDatum(datumType, blockNumber)
}

func (a *databaseAdapter) GetMidnightAriadneParamsAtOrBeforeEpoch(
	epoch uint64,
) (*models.MidnightAriadneParams, error) {
	return a.db.GetMidnightAriadneParamsAtOrBeforeEpoch(epoch)
}

func (a *databaseAdapter) GetMidnightEpochCandidatesByEpoch(
	epoch uint64,
) (*models.MidnightEpochCandidates, error) {
	return a.db.GetMidnightEpochCandidatesByEpoch(epoch)
}

func (a *databaseAdapter) GetPoolStakeSnapshotsByEpoch(
	epoch uint64,
	snapshotType string,
) ([]*models.PoolStakeSnapshot, error) {
	return a.db.GetPoolStakeSnapshotsByEpoch(epoch, snapshotType, nil)
}

func (a *databaseAdapter) GetEpoch(epochId uint64) (*models.Epoch, error) {
	return a.db.GetEpoch(epochId, nil)
}

func (a *databaseAdapter) GetEpochBySlot(slot uint64) (*models.Epoch, error) {
	return a.db.GetEpochBySlot(slot, nil)
}

func (a *databaseAdapter) BlockByHash(hash []byte) (models.Block, error) {
	return database.BlockByHash(a.db, hash)
}

func (a *databaseAdapter) BlockByNumber(number uint64) (models.Block, error) {
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
	return a.db.BlockByIndex(number+database.BlockInitialIndex, nil)
}

func (a *databaseAdapter) BlocksRecent(count int) ([]models.Block, error) {
	return database.BlocksRecent(a.db, count)
}

func (a *databaseAdapter) BlockBeforeSlot(slot uint64) (models.Block, error) {
	return database.BlockBeforeSlot(a.db, slot)
}
