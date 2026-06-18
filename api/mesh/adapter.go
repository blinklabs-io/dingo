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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
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

func (a *meshDatabaseAdapter) BlockByHash(
	hash []byte,
) (models.Block, error) {
	return database.BlockByHash(a.db, hash)
}

func (a *meshDatabaseAdapter) BlockByIndex(
	idx uint64,
) (models.Block, error) {
	return a.db.BlockByIndex(idx, nil)
}

func (a *meshDatabaseAdapter) GetTransactionByHash(
	hash []byte,
) (*models.Transaction, error) {
	return a.db.GetTransactionByHash(hash, nil)
}

func (a *meshDatabaseAdapter) GetTransactionsByBlockHash(
	hash []byte,
) ([]models.Transaction, error) {
	return a.db.GetTransactionsByBlockHash(hash, nil)
}
