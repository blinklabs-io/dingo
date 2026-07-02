// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/glebarez/sqlite"
	gormmysql "gorm.io/driver/mysql"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// DingoDBConfig selects which Dingo metadata backend to open.
// Plugin must match the value Dingo itself was configured with.
type DingoDBConfig struct {
	// Plugin is the metadata storage backend: "sqlite" (default), "postgres", or "mysql".
	Plugin string
	// DataDir is the Dingo node data directory.
	// For SQLite this must contain metadata.sqlite; for other plugins it is only
	// used to resolve the default cache path and is not required for DB access.
	DataDir string
	// DSN is the connection string for postgres and mysql plugins.
	// Examples:
	//   postgres: "host=localhost user=dingo password=secret dbname=dingo port=5432 sslmode=disable"
	//   mysql:    "dingo:secret@tcp(localhost:3306)/dingo?parseTime=true"
	// Unused for the sqlite plugin.
	DSN string
}

// DingoEpochData holds epoch-level aggregates read directly from Dingo's database.
type DingoEpochData struct {
	TotalActiveStake string // lovelace decimal string (matches Koios format)
	Fees             string // lovelace decimal string; empty when reward_ada_pots row absent
}

// DingoPoolEpochData holds per-pool reward-input data for one epoch,
// sourced from the reward_pool_input table written by Dingo's reward calculator.
type DingoPoolEpochData struct {
	DelegatedStake string // lovelace decimal string
	BlocksProduced uint64
	DelegatorCount uint64
}

// DingoDB reads reward state directly from Dingo's metadata database.
// It supports all three backends Dingo supports: SQLite, PostgreSQL, MySQL.
type DingoDB struct {
	db *gorm.DB
}

// OpenDingoDB connects to Dingo's metadata database using the configured backend.
//
//   - sqlite (default): opens {DataDir}/metadata.sqlite in read-only WAL mode.
//     SQLite WAL allows concurrent readers alongside a live node.
//   - postgres: connects with the libpq-style DSN in cfg.DSN.
//   - mysql: connects with the go-sql-driver DSN in cfg.DSN.
func OpenDingoDB(cfg DingoDBConfig) (*DingoDB, error) {
	var db *gorm.DB
	var err error

	gormCfg := &gorm.Config{Logger: gormlogger.Discard}

	switch cfg.Plugin {
	case "sqlite", "":
		db, err = openDingoSQLite(cfg.DataDir, gormCfg)
	case "postgres":
		if cfg.DSN == "" {
			return nil, errors.New("--metadata-dsn is required for postgres plugin")
		}
		db, err = gorm.Open(gormpostgres.Open(cfg.DSN), gormCfg)
		if err != nil {
			err = fmt.Errorf("open postgres metadata: %w", err)
		}
	case "mysql":
		if cfg.DSN == "" {
			return nil, errors.New("--metadata-dsn is required for mysql plugin")
		}
		db, err = gorm.Open(gormmysql.Open(cfg.DSN), gormCfg)
		if err != nil {
			err = fmt.Errorf("open mysql metadata: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported metadata plugin %q (sqlite, postgres, mysql)", cfg.Plugin)
	}
	if err != nil {
		return nil, err
	}
	return &DingoDB{db: db}, nil
}

// openDingoSQLite opens the SQLite metadata file at {dataDir}/metadata.sqlite
// in read-only WAL mode. Multiple processes may open the same file concurrently.
func openDingoSQLite(dataDir string, cfg *gorm.Config) (*gorm.DB, error) {
	path := filepath.Join(dataDir, "metadata.sqlite")
	// mode=ro prevents any write; WAL + busy_timeout let readers proceed
	// even during an active checkpoint.
	connStr := fmt.Sprintf(
		"file:%s?mode=ro&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=cache_size(-16000)",
		path,
	)
	db, err := gorm.Open(sqlite.Open(connStr), cfg)
	if err != nil {
		return nil, fmt.Errorf("open dingo metadata %s: %w", path, err)
	}
	return db, nil
}

// Close releases the database connection.
func (d *DingoDB) Close() error {
	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// GetLatestEpoch returns the highest epoch number recorded in epoch_summary.
// ctx is forwarded to the DB driver so that a cancelled context aborts the query.
func (d *DingoDB) GetLatestEpoch(ctx context.Context) (uint64, error) {
	var epoch *uint64
	if err := d.db.WithContext(ctx).Model(&models.EpochSummary{}).
		Select("MAX(epoch)").
		Scan(&epoch).Error; err != nil {
		return 0, fmt.Errorf("get latest epoch: %w", err)
	}
	if epoch == nil {
		return 0, errors.New("dingo db: no epoch_summary rows found")
	}
	return *epoch, nil
}

// GetEpochData returns epoch-level aggregates for the given epoch.
// Returns nil, nil when Dingo has not yet recorded an epoch_summary row.
func (d *DingoDB) GetEpochData(epoch uint64) (*DingoEpochData, error) {
	var summary models.EpochSummary
	if err := d.db.Where("epoch = ?", epoch).First(&summary).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("epoch_summary epoch %d: %w", epoch, err)
	}

	data := &DingoEpochData{
		TotalActiveStake: strconv.FormatUint(uint64(summary.TotalActiveStake), 10),
	}

	var pots models.RewardAdaPots
	if err := d.db.Where("epoch = ?", epoch).First(&pots).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("reward_ada_pots epoch %d: %w", epoch, err)
		}
		// Pots absent for some epochs; leave Fees empty.
	} else {
		data.Fees = strconv.FormatUint(uint64(pots.Fees), 10)
	}

	return data, nil
}

// GetPoolEpochDataMap returns all pools with a reward_pool_input row for the
// given epoch, keyed by pool-key-hash hex. One bulk query per epoch.
func (d *DingoDB) GetPoolEpochDataMap(epoch uint64) (map[string]*DingoPoolEpochData, error) {
	var inputs []models.RewardPoolInput
	if err := d.db.Where("epoch = ?", epoch).Find(&inputs).Error; err != nil {
		return nil, fmt.Errorf("reward_pool_input epoch %d: %w", epoch, err)
	}

	m := make(map[string]*DingoPoolEpochData, len(inputs))
	for i := range inputs {
		inp := &inputs[i]
		var blocks uint64
		if inp.BlocksProduced != nil {
			blocks = *inp.BlocksProduced
		}
		m[hex.EncodeToString(inp.PoolKeyHash)] = &DingoPoolEpochData{
			DelegatedStake: strconv.FormatUint(uint64(inp.DelegatedStake), 10),
			BlocksProduced: blocks,
			DelegatorCount: inp.DelegatorCount,
		}
	}
	return m, nil
}

// PoolKeyHashHex converts a pool bech32 ID ("pool1…") to its lower-hex
// 28-byte key hash. The hex string matches the keys in GetPoolEpochDataMap.
func PoolKeyHashHex(bech32 string) (string, error) {
	poolID, err := lcommon.NewPoolIdFromBech32(bech32)
	if err != nil {
		return "", fmt.Errorf("decode pool bech32 %q: %w", bech32, err)
	}
	return hex.EncodeToString(poolID[:]), nil
}

// PoolKeyHashHexToBech32 is the inverse of PoolKeyHashHex: it converts a
// lower-hex 28-byte pool key hash (as returned by GetPoolEpochDataMap) back
// to a bech32 "pool1…" string. Used so pool_only_dingo mismatches store a
// bech32 value in PoolBech32 rather than a raw hex string.
func PoolKeyHashHexToBech32(keyHex string) (string, error) {
	b, err := hex.DecodeString(keyHex)
	if err != nil {
		return "", fmt.Errorf("decode hex pool key hash %q: %w", keyHex, err)
	}
	if len(b) != 28 {
		return "", fmt.Errorf("pool key hash: expected 28 bytes, got %d", len(b))
	}
	var pid lcommon.PoolId
	copy(pid[:], b)
	return pid.String(), nil
}
