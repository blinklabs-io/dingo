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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/clause"
)

// KoiosEpochInfo holds Koios reference data for a closed epoch.
type KoiosEpochInfo struct {
	ID           uint      `gorm:"primarykey;autoIncrement"`
	Network      string    `gorm:"uniqueIndex:idx_kei_net_epoch;not null"`
	Epoch        uint64    `gorm:"uniqueIndex:idx_kei_net_epoch;not null"`
	ActiveStake  string    `gorm:"not null"`
	PoolCnt      int       `gorm:"not null"`
	DelegatorCnt int       `gorm:"not null"`
	Fees         string    `gorm:"not null"`
	TotalRewards string    `gorm:"not null"`
	FetchedAt    time.Time `gorm:"not null"`
}

func (KoiosEpochInfo) TableName() string { return "koios_epoch_info" }

// KoiosPoolEpoch holds per-pool Koios data for a closed epoch.
type KoiosPoolEpoch struct {
	ID          uint      `gorm:"primarykey;autoIncrement"`
	Network     string    `gorm:"uniqueIndex:idx_kpe_net_epoch_pool;not null"`
	Epoch       uint64    `gorm:"uniqueIndex:idx_kpe_net_epoch_pool;not null"`
	PoolBech32  string    `gorm:"uniqueIndex:idx_kpe_net_epoch_pool;not null"`
	ActiveStake string    `gorm:"not null"`
	BlockCnt    int       `gorm:"not null"`
	Delegators  int       `gorm:"not null"`
	FetchedAt   time.Time `gorm:"not null"`
}

func (KoiosPoolEpoch) TableName() string { return "koios_pool_epoch" }

// KoiosAccountRewards is schema-only; populated when #1875 is resolved.
type KoiosAccountRewards struct {
	ID           uint      `gorm:"primarykey;autoIncrement"`
	Network      string    `gorm:"uniqueIndex:idx_kar_net_epoch_addr;not null"`
	Epoch        uint64    `gorm:"uniqueIndex:idx_kar_net_epoch_addr;not null"`
	StakeAddress string    `gorm:"uniqueIndex:idx_kar_net_epoch_addr;not null"`
	Earned       string    `gorm:"not null"`
	FetchedAt    time.Time `gorm:"not null"`
}

func (KoiosAccountRewards) TableName() string { return "koios_account_rewards" }

// CheckEpochStatus stores the last check result for an epoch.
type CheckEpochStatus struct {
	ID             uint      `gorm:"primarykey;autoIncrement"`
	Network        string    `gorm:"uniqueIndex:idx_ces_net_epoch;not null"`
	Epoch          uint64    `gorm:"uniqueIndex:idx_ces_net_epoch;not null"`
	LastCheckedAt  time.Time `gorm:"not null"`
	Status         string    `gorm:"not null"` // PASS, FAIL, ERROR
	MismatchCount  int       `gorm:"not null"`
	DingoPoolCount int       `gorm:"not null"`
	KoiosPoolCount int       `gorm:"not null"`
	OnlyDingoPools string    `gorm:"not null"` // JSON array of pool IDs
	OnlyKoiosPools string    `gorm:"not null"` // JSON array of pool IDs
}

func (CheckEpochStatus) TableName() string { return "check_epoch_status" }

// CheckRun records a completed check-run invocation.
type CheckRun struct {
	ID            uint      `gorm:"primarykey;autoIncrement"`
	Network       string    `gorm:"not null"`
	RunAt         time.Time `gorm:"not null"`
	EpochsChecked int       `gorm:"not null"`
	PoolsChecked  int       `gorm:"not null"`
	MismatchCount int       `gorm:"not null"`
	ReportPath    string    `gorm:"not null"`
}

func (CheckRun) TableName() string { return "check_runs" }

// CheckMismatch records a single field-level or set-level mismatch.
type CheckMismatch struct {
	ID           uint      `gorm:"primarykey;autoIncrement"`
	Network      string    `gorm:"not null"`
	Epoch        uint64    `gorm:"not null"`
	PoolBech32   string    `gorm:"not null"`
	StakeAddress string    `gorm:"not null"`
	Field        string    `gorm:"not null"`
	DingoValue   string    `gorm:"not null"`
	KoiosValue   string    `gorm:"not null"`
	Category     string    `gorm:"not null"`
	CheckedAt    time.Time `gorm:"not null"`
}

func (CheckMismatch) TableName() string { return "check_mismatches" }

var migrateModels = []any{
	&KoiosEpochInfo{},
	&KoiosPoolEpoch{},
	&KoiosAccountRewards{},
	&CheckEpochStatus{},
	&CheckRun{},
	&CheckMismatch{},
}

// Cache wraps the SQLite cache.db.
type Cache struct {
	db     *gorm.DB
	logger *slog.Logger
}

// OpenCache opens (or creates) the SQLite cache at path, running migrations.
func OpenCache(path string, logger *slog.Logger) (*Cache, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("open cache db: %w", err)
	}
	// WAL mode for better concurrent read performance.
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	if err := db.AutoMigrate(migrateModels...); err != nil {
		return nil, fmt.Errorf("migrate cache db: %w", err)
	}
	return &Cache{db: db, logger: logger}, nil
}

// UpsertEpochInfo idempotently inserts or updates a Koios epoch info row.
func (c *Cache) UpsertEpochInfo(info KoiosEpochInfo) error {
	return c.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "network"},
			{Name: "epoch"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"active_stake", "pool_cnt", "delegator_cnt",
			"fees", "total_rewards", "fetched_at",
		}),
	}).Create(&info).Error
}

// UpsertPoolEpoch idempotently inserts or updates a Koios pool epoch row.
func (c *Cache) UpsertPoolEpoch(pe KoiosPoolEpoch) error {
	return c.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "network"},
			{Name: "epoch"},
			{Name: "pool_bech32"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"active_stake", "block_cnt", "delegators", "fetched_at",
		}),
	}).Create(&pe).Error
}

// GetEpochInfo retrieves a cached Koios epoch info record.
func (c *Cache) GetEpochInfo(network string, epoch uint64) (*KoiosEpochInfo, error) {
	var info KoiosEpochInfo
	err := c.db.
		Where("network = ? AND epoch = ?", network, epoch).
		First(&info).Error
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// GetAllPoolsForEpoch retrieves all cached pool rows for (network, epoch).
func (c *Cache) GetAllPoolsForEpoch(network string, epoch uint64) ([]KoiosPoolEpoch, error) {
	var pools []KoiosPoolEpoch
	err := c.db.
		Where("network = ? AND epoch = ?", network, epoch).
		Find(&pools).Error
	return pools, err
}

// GetFetchedEpochRange returns the min and max fetched epoch numbers.
func (c *Cache) GetFetchedEpochRange(network string) (min, max uint64, err error) {
	type rangeResult struct {
		Min *uint64
		Max *uint64
	}
	var r rangeResult
	err = c.db.Model(&KoiosEpochInfo{}).
		Select("MIN(epoch) AS min, MAX(epoch) AS max").
		Where("network = ?", network).
		Scan(&r).Error
	if r.Min != nil {
		min = *r.Min
	}
	if r.Max != nil {
		max = *r.Max
	}
	return min, max, err
}

// GetAllFetchedEpochs returns all fetched epoch numbers for a network in order.
func (c *Cache) GetAllFetchedEpochs(network string) ([]uint64, error) {
	var epochs []uint64
	err := c.db.Model(&KoiosEpochInfo{}).
		Select("epoch").
		Where("network = ?", network).
		Order("epoch ASC").
		Pluck("epoch", &epochs).Error
	return epochs, err
}

// GetEpochsNeedingCheck returns epochs with Koios data but no check result yet.
func (c *Cache) GetEpochsNeedingCheck(network string) ([]uint64, error) {
	var all []uint64
	if err := c.db.Model(&KoiosEpochInfo{}).
		Select("epoch").
		Where("network = ?", network).
		Order("epoch ASC").
		Pluck("epoch", &all).Error; err != nil {
		return nil, err
	}
	var done []uint64
	if err := c.db.Model(&CheckEpochStatus{}).
		Select("epoch").
		Where("network = ?", network).
		Pluck("epoch", &done).Error; err != nil {
		return nil, err
	}
	checked := make(map[uint64]bool, len(done))
	for _, e := range done {
		checked[e] = true
	}
	result := make([]uint64, 0)
	for _, e := range all {
		if !checked[e] {
			result = append(result, e)
		}
	}
	return result, nil
}

// UpsertCheckEpochStatus idempotently stores a check result for an epoch.
func (c *Cache) UpsertCheckEpochStatus(status CheckEpochStatus) error {
	return c.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "network"},
			{Name: "epoch"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"last_checked_at", "status", "mismatch_count",
			"dingo_pool_count", "koios_pool_count",
			"only_dingo_pools", "only_koios_pools",
		}),
	}).Create(&status).Error
}

// InsertCheckRun appends a check run record.
func (c *Cache) InsertCheckRun(run CheckRun) error {
	return c.db.Create(&run).Error
}

// InsertMismatches bulk-inserts mismatch records.
func (c *Cache) InsertMismatches(mismatches []CheckMismatch) error {
	if len(mismatches) == 0 {
		return nil
	}
	return c.db.Create(&mismatches).Error
}

// DeleteEpochMismatches removes all mismatch rows for an epoch (before re-check).
func (c *Cache) DeleteEpochMismatches(network string, epoch uint64) error {
	return c.db.
		Where("network = ? AND epoch = ?", network, epoch).
		Delete(&CheckMismatch{}).Error
}

// GetMismatches retrieves mismatch records. An empty poolBech32 returns all pools.
func (c *Cache) GetMismatches(network string, epoch uint64, poolBech32 string) ([]CheckMismatch, error) {
	q := c.db.Where("network = ? AND epoch = ?", network, epoch)
	if poolBech32 != "" {
		q = q.Where("pool_bech32 = ?", poolBech32)
	}
	var mismatches []CheckMismatch
	err := q.Find(&mismatches).Error
	return mismatches, err
}

// GetStatusSummary returns all check epoch statuses for a network in epoch order.
func (c *Cache) GetStatusSummary(network string) ([]CheckEpochStatus, error) {
	var statuses []CheckEpochStatus
	err := c.db.
		Where("network = ?", network).
		Order("epoch ASC").
		Find(&statuses).Error
	return statuses, err
}

// MarshalPoolList encodes a pool ID slice as a JSON string for DB storage.
func MarshalPoolList(pools []string) string {
	b, _ := json.Marshal(pools)
	return string(b)
}

// UnmarshalPoolList decodes a JSON string from DB storage to a pool ID slice.
func UnmarshalPoolList(s string) []string {
	var pools []string
	_ = json.Unmarshal([]byte(s), &pools)
	return pools
}
