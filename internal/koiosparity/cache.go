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
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
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
	EpochEndTime time.Time // when the epoch actually closed (from Koios end_time); zero for old cache rows
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
	ID           uint      `gorm:"primarykey;autoIncrement"                    json:"id"`
	Network      string    `gorm:"index:idx_cm_net_epoch;not null"             json:"network"`
	Epoch        uint64    `gorm:"index:idx_cm_net_epoch;not null"             json:"epoch"`
	PoolBech32   string    `gorm:"not null"                                    json:"pool_bech32"`
	StakeAddress string    `gorm:"not null"                                    json:"stake_address"`
	Field        string    `gorm:"not null"                                    json:"field"`
	DingoValue   string    `gorm:"not null"                                    json:"dingo_value"`
	KoiosValue   string    `gorm:"not null"                                    json:"koios_value"`
	Category     string    `gorm:"not null"                                    json:"category"`
	CheckedAt    time.Time `gorm:"not null"                                    json:"checked_at"`
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
	// Busy timeout prevents concurrent writers from failing immediately with
	// "database is locked"; 5 s is sufficient for the parallel check workers.
	if err := db.Exec("PRAGMA busy_timeout=5000").Error; err != nil {
		return nil, fmt.Errorf("set busy timeout: %w", err)
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
			"fees", "total_rewards", "epoch_end_time", "fetched_at",
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

// GetEpochsNeedingCheck returns epochs that have Koios reference data but
// either have no check result yet OR whose Koios data was refreshed (fetched_at
// updated) after the last check. This ensures a forced re-fetch is followed by
// an automatic re-check rather than leaving stale PASS/FAIL rows in the cache.
func (c *Cache) GetEpochsNeedingCheck(network string) ([]uint64, error) {
	// LEFT JOIN so we pick up epochs with no status row (NULL last_checked_at)
	// AND epochs where fetched_at > last_checked_at (stale check).
	type row struct {
		Epoch uint64
	}
	var rows []row
	err := c.db.Raw(`
		SELECT k.epoch
		FROM koios_epoch_info k
		LEFT JOIN check_epoch_status s
		       ON k.network = s.network AND k.epoch = s.epoch
		WHERE k.network = ?
		  AND (s.epoch IS NULL OR k.fetched_at > s.last_checked_at)
		ORDER BY k.epoch ASC
	`, network).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	result := make([]uint64, len(rows))
	for i, r := range rows {
		result[i] = r.Epoch
	}
	return result, nil
}

// GetUncachedEpochs returns epoch numbers in [from, through] (inclusive) that
// are NOT yet in koios_epoch_info for the given network. This is used by Fetch
// to fill holes left by prior failed or interrupted runs rather than naively
// resuming from max(fetched) + 1.
func (c *Cache) GetUncachedEpochs(network string, from, through uint64) ([]uint64, error) {
	// Build the full desired range in memory (typically ≤ a few thousand epochs).
	want := make(map[uint64]bool, through-from+1)
	for e := from; e <= through; e++ {
		want[e] = true
	}

	var have []uint64
	if err := c.db.Model(&KoiosEpochInfo{}).
		Select("epoch").
		Where("network = ? AND epoch >= ? AND epoch <= ?", network, from, through).
		Pluck("epoch", &have).Error; err != nil {
		return nil, err
	}
	for _, e := range have {
		delete(want, e)
	}

	missing := make([]uint64, 0, len(want))
	for e := from; e <= through; e++ {
		if want[e] {
			missing = append(missing, e)
		}
	}
	return missing, nil
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
	b, err := json.Marshal(pools)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// UnmarshalPoolList decodes a JSON string from DB storage to a pool ID slice.
func UnmarshalPoolList(s string) []string {
	var pools []string
	_ = json.Unmarshal([]byte(s), &pools)
	return pools
}
