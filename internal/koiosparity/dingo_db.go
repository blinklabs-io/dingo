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
	// TotalRewards is reward_ada_pots.rewards for this epoch alone: a fresh
	// per-epoch FLOW value (rewards.Result.TotalRewardPot, overwritten every
	// epoch — see ledger/reward_calculation.go:389,1955 and
	// ledger/rewards/rewards.go's Pots type, which carries no "rewards" field
	// forward between epochs). Koios's /totals.reward, by contrast, is a
	// monotonically increasing cumulative accumulator (verified against live
	// preview data — see CompareEpochTotals), not a per-epoch snapshot. Since
	// Dingo has no stored aggregate matching that cumulative quantity, and this
	// checker does not compute cross-epoch aggregates on Dingo's behalf (that
	// belongs in Dingo's own schema, not the checker), TotalRewards is kept for
	// reference only and is not compared against /totals.reward.
	TotalRewards string // lovelace decimal string from reward_ada_pots.rewards; empty when absent
	Treasury     string // lovelace decimal string from reward_ada_pots.treasury; empty when absent
	Reserves     string // lovelace decimal string from reward_ada_pots.reserves; empty when absent

	// RewardAdaPotsPresent distinguishes "no reward_ada_pots row for this
	// epoch" from "row exists with legitimately empty/zero values" — an empty
	// Fees/TotalRewards/Treasury/Reserves string is not itself a reliable
	// missing-data signal. On live chainsync, epoch_summary.SnapshotReady is
	// only ever set true after reward_ada_pots is written in the same
	// transaction (see ledger/chainsync.go's processEpochRollover), so the two
	// should always agree; a bootstrap-from-snapshot import, however, can set
	// SnapshotReady=true without ever writing reward_ada_pots for that epoch,
	// which is exactly the case this flag lets the comparer detect instead of
	// silently skipping the treasury/reserves/fees comparison.
	RewardAdaPotsPresent bool
}

// DingoPoolEpochData holds per-pool reward-input data assembled for one Koios
// reporting epoch. It is built from up to three separate Dingo rows spread
// across two different reward_pool_input epochs plus one reward_pool_output
// epoch — see GetPoolEpochDataMap's doc comment and ARCHITECTURE.md's Koios
// Parity Tracker "Epoch alignment" section for the full derivation of why a
// single same-numbered row cannot supply every field.
type DingoPoolEpochData struct {
	// DelegatedStake/DelegatorCount come from reward_pool_input at the "stake
	// epoch" (Koios epoch K's K-1): the mark stake distribution Praos actually
	// used as K's active-stake/reward-calculation basis.
	DelegatedStake string // lovelace decimal string
	DelegatorCount uint64

	// ParamsPresent distinguishes "no reward_pool_input row yet at the
	// 'param epoch' (K+1)" from "row exists with legitimately zero/empty
	// BlocksProduced/FixedCost/Margin" — see ComparePoolEpoch, which must
	// never silently treat the former as a comparison pass.
	ParamsPresent bool
	// BlocksProduced/FixedCost/Margin come from reward_pool_input at the
	// "param epoch" (K+1): that row's BlocksProduced/pool-params fields
	// describe the epoch immediately before it (K), because
	// buildRewardStateInputs (ledger/snapshot/rotation.go) stamps them from
	// evt.PreviousEpoch at capture time, not from the row's own Epoch.
	BlocksProduced uint64
	FixedCost      string // lovelace decimal string (reward_pool_input.cost)
	Margin         string // rational string (e.g. "1/10"); empty when null

	// MemberRewardPresent distinguishes "no reward_pool_output row yet at the
	// stake epoch (K-1)" — reward calculation not finished for this
	// pool/epoch — from "row exists with a genuinely empty/zero
	// MemberRewardTotal". See ComparePoolEpoch: neither absence may be
	// silently treated as a comparison pass.
	MemberRewardPresent bool
	// MemberRewardTotal is reward_pool_output.member_reward_total (lovelace
	// decimal string) at the stake epoch (K-1) — reward_pool_output and the
	// reward_pool_input row it was computed alongside always share the same
	// Epoch value in Dingo's schema (see reward_calculation.go's
	// stakeRewardEpochsForNewEpoch: both are read/written at
	// epochs.snapshot).
	MemberRewardTotal string
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
// ctx is forwarded to the DB driver so that a cancelled context aborts the query.
func (d *DingoDB) GetEpochData(ctx context.Context, epoch uint64) (*DingoEpochData, error) {
	var summary models.EpochSummary
	if err := d.db.WithContext(ctx).Where("epoch = ?", epoch).First(&summary).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("epoch_summary epoch %d: %w", epoch, err)
	}

	// SnapshotReady = false means Dingo has written a partial or placeholder row
	// that will be repaired later. Treat it as not-yet-ready so the checker
	// classifies this as a transient state rather than comparing in-progress
	// values against Koios's final reward data.
	if !summary.SnapshotReady {
		return nil, nil
	}

	data := &DingoEpochData{
		TotalActiveStake: strconv.FormatUint(uint64(summary.TotalActiveStake), 10),
	}

	var pots models.RewardAdaPots
	if err := d.db.WithContext(ctx).Where("epoch = ?", epoch).First(&pots).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("reward_ada_pots epoch %d: %w", epoch, err)
		}
		// Pots absent for some epochs (e.g. a bootstrap-imported epoch); leave
		// Fees/TotalRewards/Treasury/Reserves empty and RewardAdaPotsPresent
		// false so the comparer reports this rather than silently skipping it.
	} else {
		data.Fees = strconv.FormatUint(uint64(pots.Fees), 10)
		data.TotalRewards = strconv.FormatUint(uint64(pots.Rewards), 10)
		data.Treasury = strconv.FormatUint(uint64(pots.Treasury), 10)
		data.Reserves = strconv.FormatUint(uint64(pots.Reserves), 10)
		data.RewardAdaPotsPresent = true
	}

	return data, nil
}

// GetPoolEpochDataMap returns per-pool reward data assembled for Koios
// reporting epoch K, keyed by pool-key-hash hex. Dingo's reward_pool_input/
// reward_pool_output rows do not use Koios's epoch numbering uniformly across
// fields, so the caller must resolve and pass two distinct Dingo epoch
// numbers rather than K itself:
//
//   - stakeEpoch (K-1): the mark stake distribution actually used as Praos's
//     active-stake/reward-calculation basis for K. reward_pool_input's
//     DelegatedStake/DelegatorCount and reward_pool_output's
//     MemberRewardTotal are both read at this epoch — reward_calculation.go's
//     stakeRewardEpochsForNewEpoch computes both from the same
//     epochs.snapshot value, so input and output always share one Epoch.
//   - paramEpoch (K+1): reward_pool_input's BlocksProduced and pool
//     Margin/FixedCost are captured onto the row for the epoch *after* the
//     one they describe — see ledger/snapshot/rotation.go's
//     buildRewardStateInputs, which stamps these from evt.PreviousEpoch, not
//     from the row's own Epoch.
//
// See koiosStakeEpoch/koiosParamEpoch in check.go and ARCHITECTURE.md's Koios
// Parity Tracker "Epoch alignment" section for the full derivation.
//
// A pool present in only one of the two reward_pool_input reads (e.g. a pool
// with a stake-epoch row but whose param-epoch row hasn't been captured yet)
// still gets an entry — ParamsPresent/MemberRewardPresent record which pieces
// are actually available so ComparePoolEpoch never mistakes "not yet
// computed" for "compared and equal". One bulk query per table per epoch
// (three total), independent of pool count. ctx is forwarded to the DB driver
// so that a cancelled context aborts the query.
func (d *DingoDB) GetPoolEpochDataMap(
	ctx context.Context,
	stakeEpoch, paramEpoch uint64,
) (map[string]*DingoPoolEpochData, error) {
	var stakeInputs []models.RewardPoolInput
	if err := d.db.WithContext(ctx).Where("epoch = ?", stakeEpoch).Find(&stakeInputs).Error; err != nil {
		return nil, fmt.Errorf("reward_pool_input stake epoch %d: %w", stakeEpoch, err)
	}

	m := make(map[string]*DingoPoolEpochData, len(stakeInputs))
	for i := range stakeInputs {
		inp := &stakeInputs[i]
		m[hex.EncodeToString(inp.PoolKeyHash)] = &DingoPoolEpochData{
			DelegatedStake: strconv.FormatUint(uint64(inp.DelegatedStake), 10),
			DelegatorCount: inp.DelegatorCount,
		}
	}

	var paramInputs []models.RewardPoolInput
	if err := d.db.WithContext(ctx).Where("epoch = ?", paramEpoch).Find(&paramInputs).Error; err != nil {
		return nil, fmt.Errorf("reward_pool_input param epoch %d: %w", paramEpoch, err)
	}
	for i := range paramInputs {
		inp := &paramInputs[i]
		key := hex.EncodeToString(inp.PoolKeyHash)
		data, ok := m[key]
		if !ok {
			// Present at the param epoch but not the stake epoch (e.g. a
			// freshly registered pool) — still record what's available
			// rather than dropping it; DelegatedStake/DelegatorCount stay at
			// their zero value.
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.ParamsPresent = true
		if inp.BlocksProduced != nil {
			data.BlocksProduced = *inp.BlocksProduced
		}
		data.FixedCost = strconv.FormatUint(uint64(inp.Cost), 10)
		if inp.Margin != nil && inp.Margin.Rat != nil {
			data.Margin = inp.Margin.String()
		}
	}

	var outputs []models.RewardPoolOutput
	if err := d.db.WithContext(ctx).Where("epoch = ?", stakeEpoch).Find(&outputs).Error; err != nil {
		return nil, fmt.Errorf("reward_pool_output epoch %d: %w", stakeEpoch, err)
	}
	for i := range outputs {
		out := &outputs[i]
		key := hex.EncodeToString(out.PoolKeyHash)
		data, ok := m[key]
		if !ok {
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.MemberRewardPresent = true
		data.MemberRewardTotal = strconv.FormatUint(uint64(out.MemberRewardTotal), 10)
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
