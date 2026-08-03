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
	mysqldriver "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// conformanceMysqlDatabase is the dedicated MySQL database the conformance
// suite migrates into. MySQL has no schema/database distinction the way
// Postgres does, so a dedicated database is the direct equivalent of
// state_manager_postgres.go's dedicated "conformance" schema:
// database/plugin/metadata/mysql's own tests connect to the shared
// dingo_test database, and go test runs different packages as separate,
// concurrent binaries, so sharing it would let the two suites race on the
// same tables.
const conformanceMysqlDatabase = "dingo_conformance_test"

// NewDingoMysqlStateManager creates a new DingoStateManager backed by a
// real MySQL database instead of the default in-memory SQLite one used by
// NewDingoStateManager. rootDSN must authenticate as a user allowed to
// create databases -- database/plugin/metadata/mysql's own test user is
// deliberately granted access only to dingo_test by the official mysql
// image's bootstrap, so it cannot be reused here; a root DSN is required
// to create conformanceMysqlDatabase.
func NewDingoMysqlStateManager(rootDSN string) (*DingoStateManager, error) {
	adminDB, err := gorm.Open(gormmysql.Open(rootDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql admin connection: %w", err)
	}
	if err := adminDB.Exec(
		"CREATE DATABASE IF NOT EXISTS " + conformanceMysqlDatabase,
	).Error; err != nil {
		return nil, fmt.Errorf("failed to create conformance database: %w", err)
	}
	if adminSQLDB, err := adminDB.DB(); err == nil {
		adminSQLDB.Close()
	}

	dsn, err := dsnWithDatabase(rootDSN, conformanceMysqlDatabase)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to build conformance database dsn: %w",
			err,
		)
	}

	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
		// models.MigrateModels lists Asset before Utxo, but Utxo.Assets
		// declares the FK ("gorm:foreignKey:UtxoID;constraint:..."). A
		// single batched AutoMigrate(MigrateModels...) call pre-parses
		// every model's schema up front, so it embeds that constraint into
		// Asset's CREATE TABLE before Utxo's table exists. SQLite (the
		// default backend here) never validates REFERENCES targets at DDL
		// time, so this was always latent; MySQL/InnoDB validates
		// immediately, same as Postgres (see
		// state_manager_postgres.go). Disabling FK constraint creation
		// during migration costs nothing for this harness specifically:
		// DingoStateProvider (state_provider.go) never queries these rows
		// back through SQL, only through DingoStateManager's in-memory
		// maps.
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql database: %w", err)
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

// dsnWithDatabase rewrites a go-sql-driver/mysql DSN to point at a
// different database name, keeping every other connection parameter
// (user, password, host, port, params) unchanged.
func dsnWithDatabase(dsn, database string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	cfg.DBName = database
	return cfg.FormatDSN(), nil
}
