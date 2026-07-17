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

//go:build dingo_extra_plugins

package conformance

import (
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// conformanceSchema is the dedicated Postgres schema the conformance suite
// migrates into. database/plugin/metadata/postgres's own tests connect to
// the same dingo_test database and migrate models.MigrateModels into the
// default "public" schema; go test runs different packages as separate,
// concurrent binaries, so sharing "public" would let the two suites race on
// the same tables (one DELETEing/AutoMigrating rows the other is mid-vector
// on). A dedicated schema gives this suite its own copy of every table
// without requiring a second CI service or database.
const conformanceSchema = "conformance"

// NewDingoPostgresStateManager creates a new DingoStateManager backed by a
// real Postgres database instead of the default in-memory SQLite one used
// by NewDingoStateManager. DingoStateManager's methods only ever go through
// *gorm.DB, so no other state-manager code needs to change to support this
// backend.
func NewDingoPostgresStateManager(dsn string) (*DingoStateManager, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
		// models.MigrateModels lists Asset before Utxo, but Utxo.Assets
		// declares the FK ("gorm:foreignKey:UtxoID;constraint:..."). A
		// single batched AutoMigrate(MigrateModels...) call pre-parses
		// every model's schema up front, so it embeds that constraint into
		// Asset's CREATE TABLE before Utxo's table exists. SQLite (the
		// default backend here) never validates REFERENCES targets at DDL
		// time, so this was always latent; Postgres validates immediately
		// and fails with "relation utxo does not exist". Disabling FK
		// constraint creation during migration costs nothing for this
		// harness specifically: DingoStateProvider (state_provider.go)
		// never queries these rows back through SQL, only through
		// DingoStateManager's in-memory maps.
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	// SET search_path is a session-level setting; a connection pool would
	// hand later queries a different pooled connection where search_path
	// is back to the server default, silently routing AutoMigrate/writes
	// to "public" instead of our dedicated schema. Pin to a single
	// connection so every statement on this *gorm.DB shares the one
	// session the search_path was set on (same fix already used by
	// database/plugin/metadata/postgres's SetBulkLoadPragmas).
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB handle: %w", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	if err := db.Exec(
		"CREATE SCHEMA IF NOT EXISTS " + conformanceSchema,
	).Error; err != nil {
		return nil, fmt.Errorf("failed to create conformance schema: %w", err)
	}
	if err := db.Exec(
		"SET search_path TO " + conformanceSchema,
	).Error; err != nil {
		return nil, fmt.Errorf("failed to set search_path: %w", err)
	}

	// Run migrations for all models
	if err := db.AutoMigrate(models.MigrateModels...); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return &DingoStateManager{
		db:                   db,
		govState:             conformance.NewGovernanceState(),
		utxos:                make(map[string]common.Utxo),
		stakeRegistrations:   make(map[common.Blake2b224]uint64),
		poolRegistrations:    make(map[common.Blake2b224]bool),
		drepRegistrations:    make(map[common.Blake2b224]bool),
		committeeMembers:     make(map[common.Blake2b224]uint64),
		hotKeyAuthorizations: make(map[common.Blake2b224]common.Blake2b224),
		committeeRemovals:    make(map[string]map[common.Blake2b224]struct{}),
		committeeQuorums:     make(map[string]*big.Rat),
	}, nil
}
