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
