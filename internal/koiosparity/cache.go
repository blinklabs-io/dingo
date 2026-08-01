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
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

// KoiosEpochInfo holds Koios reference data for a closed epoch.
// Note: pool_cnt and delegator_cnt are not returned by preview/preprod Koios and are omitted.
type KoiosEpochInfo struct {
	ID          uint
	Network     string
	Epoch       uint64
	ActiveStake string
	// Fees and TotalRewards (/epoch_info.fees, /epoch_info.total_rewards) are
	// raw block/tx accounting quantities — stored for reference only. Dingo has
	// no matching aggregate, so CompareEpochAggregates does not compare them;
	// see that function's doc comment and KoiosTotalsResp for why /totals.fees
	// and /totals.reward (compared in CompareEpochTotals) are the correct
	// counterpart to reward_ada_pots.Fees/Rewards instead.
	Fees         string
	TotalRewards string
	EpochEndTime time.Time // when the epoch actually closed (from Koios end_time); zero for old cache rows
	// PreStaking marks an epoch where Koios returned active_stake=null (e.g.
	// epochs 0-1 on preview, before the first stake snapshot exists). There is
	// no reference value to ever compare against, so fetch commits this marker
	// instead of erroring/retrying forever, and check skips comparison entirely.
	PreStaking bool
	FetchedAt  time.Time

	// Remaining fields are stored for reference from the full Koios
	// epoch_info schema but are not currently compared against any Dingo
	// value (Dingo doesn't track tx/block counts or wall-clock block times
	// per epoch).
	Era            string
	OutSum         string // "" when Koios returns null (early epochs)
	TxCount        int64
	BlkCount       int64
	EpochStartTime time.Time // from Koios start_time; zero for old cache rows
	FirstBlockTime time.Time // from Koios first_block_time; zero for old cache rows
	LastBlockTime  time.Time // from Koios last_block_time; zero for old cache rows
	AvgBlkReward   string    // "" when Koios returns null (early epochs)
}

// KoiosPoolEpoch holds per-pool Koios data for a closed epoch.
//
// Reward inputs (margin, fixed_cost) and reward outputs (pool_fees,
// deleg_rewards, member_rewards) come from /pool_history. Outputs are stored
// for reference even when Dingo has no matching aggregate to compare yet.
type KoiosPoolEpoch struct {
	ID            uint
	Network       string
	Epoch         uint64
	PoolBech32    string
	ActiveStake   string
	BlockCnt      int
	Delegators    int
	Margin        string // decimal string from Koios (e.g. "0.1"); "" if absent
	FixedCost     string // lovelace decimal string
	PoolFees      string // owner fees earned that epoch
	DelegRewards  string // total delegator rewards that epoch
	MemberRewards string // member (non-owner) rewards; "" when Koios returns null
	FetchedAt     time.Time

	// Remaining fields are stored for reference from the full Koios
	// pool_history schema but are not currently compared against any Dingo
	// value — Dingo has no equivalent network-share/saturation aggregate.
	ActiveStakePct string // "" when Koios returns null
	SaturationPct  string
	EpochRos       string // annualised return-on-stake
}

// KoiosTotals holds Koios /totals reference data for a closed epoch.
//
// Fees and Reward are deliberately named to match the Koios /totals field
// names verbatim, NOT epoch_info's Fees/TotalRewards column names on
// KoiosEpochInfo — they are different quantities (see KoiosTotalsResp) and
// CompareEpochTotals checks them against Dingo independently of
// CompareEpochAggregates' epoch_info-based checks.
type KoiosTotals struct {
	ID        uint
	Network   string
	Epoch     uint64
	Treasury  string
	Reserves  string
	Fees      string
	Reward    string
	FetchedAt time.Time

	// Remaining fields are stored for reference from the full Koios totals
	// schema but are not currently compared against any Dingo value — Dingo's
	// AdaPots model has no circulating-supply or deposit-pot aggregate (see
	// KoiosTotalsResp for why).
	Circulation   string
	Supply        string
	DepositsStake string
	// Keep the persisted column name explicit for the database schema. The
	// to "deposits_d_rep" (splitting the lone "D" from "Rep"), not
	// "deposits_drep" — pin it to match Koios's own field name exactly.
	DepositsDRep       string
	DepositsProposal   string
	TreasuryDonation   string
	TreasuryWithdrawal string
	ReservesWithdrawal string
}

// KoiosAccountRewards is schema-only; populated when #1875 is resolved.
type KoiosAccountRewards struct {
	ID           uint
	Network      string
	Epoch        uint64
	StakeAddress string
	Earned       string
	FetchedAt    time.Time
}

// CheckEpochStatus stores the last check result for an epoch.
type CheckEpochStatus struct {
	ID             uint
	Network        string
	Epoch          uint64
	LastCheckedAt  time.Time
	Status         string // PASS, FAIL, ERROR
	MismatchCount  int
	DingoPoolCount int
	KoiosPoolCount int
	OnlyDingoPools string // JSON array of pool IDs
	OnlyKoiosPools string // JSON array of pool IDs
}

// CheckRun records a completed check-run invocation.
type CheckRun struct {
	ID            uint
	Network       string
	RunAt         time.Time
	EpochsChecked int
	PoolsChecked  int
	MismatchCount int
	ReportPath    string
}

// CheckMismatch records a single field-level or set-level mismatch.
type CheckMismatch struct {
	ID           uint      `json:"id"`
	Network      string    `json:"network"`
	Epoch        uint64    `json:"epoch"`
	PoolBech32   string    `json:"pool_bech32"`
	StakeAddress string    `json:"stake_address"`
	Field        string    `json:"field"`
	DingoValue   string    `json:"dingo_value"`
	KoiosValue   string    `json:"koios_value"`
	Category     string    `json:"category"`
	CheckedAt    time.Time `json:"checked_at"`
}

// Cache wraps the SQLite cache.db.
type Cache struct {
	db     *sql.DB
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
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open cache db: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping cache db: %w", err)
	}
	// WAL mode for better concurrent read performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	// Busy timeout prevents concurrent writers from failing immediately with
	// "database is locked"; 5 s is sufficient for the parallel check workers.
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if err := createCacheSchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate cache db: %w", err)
	}
	return &Cache{db: db, logger: logger}, nil
}

// Close releases the underlying database connection.
func (c *Cache) Close() error {
	return c.db.Close()
}

// UpsertEpochInfo idempotently inserts or updates a Koios epoch info row.
func (c *Cache) UpsertEpochInfo(info KoiosEpochInfo) error {
	_, err := c.db.Exec(`INSERT INTO koios_epoch_info
		(network, epoch, active_stake, fees, total_rewards, epoch_end_time, pre_staking, fetched_at,
		 era, out_sum, tx_count, blk_count, epoch_start_time, first_block_time, last_block_time, avg_blk_reward)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch) DO UPDATE SET
		 active_stake=excluded.active_stake, fees=excluded.fees, total_rewards=excluded.total_rewards,
		 epoch_end_time=excluded.epoch_end_time, pre_staking=excluded.pre_staking, fetched_at=excluded.fetched_at,
		 era=excluded.era, out_sum=excluded.out_sum, tx_count=excluded.tx_count, blk_count=excluded.blk_count,
		 epoch_start_time=excluded.epoch_start_time, first_block_time=excluded.first_block_time,
		 last_block_time=excluded.last_block_time, avg_blk_reward=excluded.avg_blk_reward`,
		info.Network, info.Epoch, info.ActiveStake, info.Fees, info.TotalRewards, info.EpochEndTime,
		info.PreStaking, info.FetchedAt, info.Era, info.OutSum, info.TxCount, info.BlkCount,
		info.EpochStartTime, info.FirstBlockTime, info.LastBlockTime, info.AvgBlkReward)
	return err
}

// CommitEpochData atomically replaces all pool rows for the epoch and upserts
// the epoch-info and totals records in a single transaction. Committing all
// three together means:
//
//   - The pool set and koios_epoch_info.fetched_at are always in sync.
//     GetEpochsNeedingCheck uses fetched_at > last_checked_at to detect stale
//     check results; a separate commit would leave fetched_at stale if the
//     process died between the two writes, silently suppressing the recheck.
//
//   - Inserts are batched at sqlitePoolBatchSize rows per statement to stay
//     within SQLite's host-parameter limit.
//
//   - Each pool row's Network and Epoch fields are normalised from info before
//     insertion so a mismatched caller cannot corrupt a different epoch's data.
//
// totals is nil for pre-staking marker commits, which have no /totals data to
// store (see fetchEpoch).
func (c *Cache) CommitEpochData(info KoiosEpochInfo, rows []KoiosPoolEpoch, totals *KoiosTotals) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec("DELETE FROM koios_pool_epoch WHERE network = ? AND epoch = ?", info.Network, info.Epoch); err != nil {
		return err
	}
	for i := range rows {
		rows[i].Network, rows[i].Epoch = info.Network, info.Epoch
		if _, err = tx.Exec(`INSERT INTO koios_pool_epoch
			(network, epoch, pool_bech32, active_stake, block_cnt, delegators, margin, fixed_cost,
			 pool_fees, deleg_rewards, member_rewards, fetched_at, active_stake_pct, saturation_pct, epoch_ros)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rows[i].Network, rows[i].Epoch, rows[i].PoolBech32, rows[i].ActiveStake, rows[i].BlockCnt,
			rows[i].Delegators, rows[i].Margin, rows[i].FixedCost, rows[i].PoolFees, rows[i].DelegRewards,
			rows[i].MemberRewards, rows[i].FetchedAt, rows[i].ActiveStakePct, rows[i].SaturationPct, rows[i].EpochRos); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`INSERT INTO koios_epoch_info
		(network, epoch, active_stake, fees, total_rewards, epoch_end_time, pre_staking, fetched_at,
		era, out_sum, tx_count, blk_count, epoch_start_time, first_block_time, last_block_time, avg_blk_reward)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch) DO UPDATE SET active_stake=excluded.active_stake, fees=excluded.fees,
		total_rewards=excluded.total_rewards, epoch_end_time=excluded.epoch_end_time,
		pre_staking=excluded.pre_staking, fetched_at=excluded.fetched_at`,
		info.Network, info.Epoch, info.ActiveStake, info.Fees, info.TotalRewards, info.EpochEndTime,
		info.PreStaking, info.FetchedAt, info.Era, info.OutSum, info.TxCount, info.BlkCount,
		info.EpochStartTime, info.FirstBlockTime, info.LastBlockTime, info.AvgBlkReward); err != nil {
		return err
	}
	if totals != nil {
		totals.Network, totals.Epoch = info.Network, info.Epoch
		if _, err = tx.Exec(`INSERT INTO koios_totals
			(network, epoch, treasury, reserves, fees, reward, fetched_at, circulation, supply, deposits_stake,
			 deposits_drep, deposits_proposal, treasury_donation, treasury_withdrawal, reserves_withdrawal)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(network, epoch) DO UPDATE SET treasury=excluded.treasury, reserves=excluded.reserves,
			fees=excluded.fees, reward=excluded.reward, fetched_at=excluded.fetched_at, circulation=excluded.circulation,
			supply=excluded.supply, deposits_stake=excluded.deposits_stake, deposits_drep=excluded.deposits_drep,
			deposits_proposal=excluded.deposits_proposal, treasury_donation=excluded.treasury_donation,
			treasury_withdrawal=excluded.treasury_withdrawal, reserves_withdrawal=excluded.reserves_withdrawal`,
			totals.Network, totals.Epoch, totals.Treasury, totals.Reserves, totals.Fees, totals.Reward, totals.FetchedAt,
			totals.Circulation, totals.Supply, totals.DepositsStake, totals.DepositsDRep, totals.DepositsProposal,
			totals.TreasuryDonation, totals.TreasuryWithdrawal, totals.ReservesWithdrawal); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

// UpsertPoolEpoch idempotently inserts or updates a Koios pool epoch row.
func (c *Cache) UpsertPoolEpoch(pe KoiosPoolEpoch) error {
	_, err := c.db.Exec(`INSERT INTO koios_pool_epoch
		(network, epoch, pool_bech32, active_stake, block_cnt, delegators, margin, fixed_cost,
		 pool_fees, deleg_rewards, member_rewards, fetched_at, active_stake_pct, saturation_pct, epoch_ros)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch, pool_bech32) DO UPDATE SET active_stake=excluded.active_stake,
		block_cnt=excluded.block_cnt, delegators=excluded.delegators, margin=excluded.margin,
		fixed_cost=excluded.fixed_cost, pool_fees=excluded.pool_fees, deleg_rewards=excluded.deleg_rewards,
		member_rewards=excluded.member_rewards, fetched_at=excluded.fetched_at,
		active_stake_pct=excluded.active_stake_pct, saturation_pct=excluded.saturation_pct, epoch_ros=excluded.epoch_ros`,
		pe.Network, pe.Epoch, pe.PoolBech32, pe.ActiveStake, pe.BlockCnt, pe.Delegators, pe.Margin, pe.FixedCost,
		pe.PoolFees, pe.DelegRewards, pe.MemberRewards, pe.FetchedAt, pe.ActiveStakePct, pe.SaturationPct, pe.EpochRos)
	return err
}

// GetEpochInfo retrieves a cached Koios epoch info record.
func (c *Cache) GetEpochInfo(network string, epoch uint64) (*KoiosEpochInfo, error) {
	var info KoiosEpochInfo
	err := c.db.QueryRow(`SELECT network, epoch, active_stake, fees, total_rewards, epoch_end_time, pre_staking,
		fetched_at, era, out_sum, tx_count, blk_count, epoch_start_time, first_block_time, last_block_time, avg_blk_reward
		FROM koios_epoch_info WHERE network = ? AND epoch = ?`, network, epoch).Scan(
		&info.Network, &info.Epoch, &info.ActiveStake, &info.Fees, &info.TotalRewards, &info.EpochEndTime,
		&info.PreStaking, &info.FetchedAt, &info.Era, &info.OutSum, &info.TxCount, &info.BlkCount,
		&info.EpochStartTime, &info.FirstBlockTime, &info.LastBlockTime, &info.AvgBlkReward)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// GetTotals retrieves a cached Koios /totals record.
// when absent — e.g. an epoch cached before totals fetching was added, and not
// yet re-fetched. Callers must treat this as an incomplete reference row (see
// CompareEpochTotals's CategoryDBMissing "koios_totals" mismatch), not skip
// totals comparison silently.
func (c *Cache) GetTotals(network string, epoch uint64) (*KoiosTotals, error) {
	var totals KoiosTotals
	err := c.db.QueryRow(`SELECT network, epoch, treasury, reserves, fees, reward, fetched_at, circulation, supply,
		deposits_stake, deposits_drep, deposits_proposal, treasury_donation, treasury_withdrawal, reserves_withdrawal
		FROM koios_totals WHERE network = ? AND epoch = ?`, network, epoch).Scan(
		&totals.Network, &totals.Epoch, &totals.Treasury, &totals.Reserves, &totals.Fees, &totals.Reward,
		&totals.FetchedAt, &totals.Circulation, &totals.Supply, &totals.DepositsStake, &totals.DepositsDRep,
		&totals.DepositsProposal, &totals.TreasuryDonation, &totals.TreasuryWithdrawal, &totals.ReservesWithdrawal)
	if err != nil {
		return nil, err
	}
	return &totals, nil
}

// GetAllPoolsForEpoch retrieves all cached pool rows for (network, epoch).
func (c *Cache) GetAllPoolsForEpoch(network string, epoch uint64) ([]KoiosPoolEpoch, error) {
	rows, err := c.db.Query(`SELECT network, epoch, pool_bech32, active_stake, block_cnt, delegators, margin,
		fixed_cost, pool_fees, deleg_rewards, member_rewards, fetched_at, active_stake_pct, saturation_pct, epoch_ros
		FROM koios_pool_epoch WHERE network = ? AND epoch = ? ORDER BY id`, network, epoch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pools []KoiosPoolEpoch
	for rows.Next() {
		var p KoiosPoolEpoch
		if err := scanPool(rows, &p); err != nil {
			return nil, err
		}
		pools = append(pools, p)
	}
	return pools, rows.Err()
}

// GetFetchedEpochRange returns the min and max fetched epoch numbers.
func (c *Cache) GetFetchedEpochRange(network string) (min, max uint64, err error) {
	var lo, hi sql.NullInt64
	err = c.db.QueryRow("SELECT MIN(epoch), MAX(epoch) FROM koios_epoch_info WHERE network = ?", network).Scan(&lo, &hi)
	if lo.Valid {
		min = uint64(lo.Int64) //nolint:gosec // epoch values are non-negative
	}
	if hi.Valid {
		max = uint64(hi.Int64) //nolint:gosec // epoch values are non-negative
	}
	return min, max, err
}

// GetAllFetchedEpochs returns all fetched epoch numbers for a network in order.
func (c *Cache) GetAllFetchedEpochs(network string) ([]uint64, error) {
	rows, err := c.db.Query("SELECT epoch FROM koios_epoch_info WHERE network = ? ORDER BY epoch ASC", network)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var epochs []uint64
	for rows.Next() {
		var e uint64
		if err := rows.Scan(&e); err != nil {
			return nil, err
		}
		epochs = append(epochs, e)
	}
	return epochs, rows.Err()
}

// GetEpochsNeedingCheck returns epochs that have Koios reference data but
// either have no check result yet OR whose Koios data was refreshed (fetched_at
// updated) after the last check. This ensures a forced re-fetch is followed by
// an automatic re-check rather than leaving stale PASS/FAIL rows in the cache.
func (c *Cache) GetEpochsNeedingCheck(network string) ([]uint64, error) {
	// LEFT JOIN so we pick up epochs with no status row (NULL last_checked_at)
	// AND epochs where fetched_at > last_checked_at (stale check).
	rows, err := c.db.Query(`
		SELECT k.epoch
		FROM koios_epoch_info k
		LEFT JOIN check_epoch_status s
		       ON k.network = s.network AND k.epoch = s.epoch
		WHERE k.network = ?
		  AND (s.epoch IS NULL OR k.fetched_at > s.last_checked_at)
		ORDER BY k.epoch ASC
	`, network)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []uint64
	for rows.Next() {
		var e uint64
		if err := rows.Scan(&e); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
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

	rows, err := c.db.Query("SELECT epoch FROM koios_epoch_info WHERE network = ? AND epoch >= ? AND epoch <= ?", network, from, through)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var have []uint64
	for rows.Next() {
		var e uint64
		if err := rows.Scan(&e); err != nil {
			return nil, err
		}
		have = append(have, e)
	}
	if err := rows.Err(); err != nil {
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
	_, err := c.db.Exec(`INSERT INTO check_epoch_status
		(network, epoch, last_checked_at, status, mismatch_count, dingo_pool_count, koios_pool_count, only_dingo_pools, only_koios_pools)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch) DO UPDATE SET last_checked_at=excluded.last_checked_at, status=excluded.status,
		mismatch_count=excluded.mismatch_count, dingo_pool_count=excluded.dingo_pool_count,
		koios_pool_count=excluded.koios_pool_count, only_dingo_pools=excluded.only_dingo_pools,
		only_koios_pools=excluded.only_koios_pools`, status.Network, status.Epoch, status.LastCheckedAt, status.Status,
		status.MismatchCount, status.DingoPoolCount, status.KoiosPoolCount, status.OnlyDingoPools, status.OnlyKoiosPools)
	return err
}

// InsertCheckRun appends a check run record.
func (c *Cache) InsertCheckRun(run CheckRun) error {
	_, err := c.db.Exec(`INSERT INTO check_runs (network, run_at, epochs_checked, pools_checked, mismatch_count, report_path) VALUES (?, ?, ?, ?, ?, ?)`, run.Network, run.RunAt, run.EpochsChecked, run.PoolsChecked, run.MismatchCount, run.ReportPath)
	return err
}

// InsertMismatches bulk-inserts mismatch records.
func (c *Cache) InsertMismatches(mismatches []CheckMismatch) error {
	if len(mismatches) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(`INSERT INTO check_mismatches (network, epoch, pool_bech32, stake_address, field, dingo_value, koios_value, category, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, m := range mismatches {
		if _, err = stmt.Exec(m.Network, m.Epoch, m.PoolBech32, m.StakeAddress, m.Field, m.DingoValue, m.KoiosValue, m.Category, m.CheckedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteEpochMismatches removes all mismatch rows for an epoch (before re-check).
func (c *Cache) DeleteEpochMismatches(network string, epoch uint64) error {
	_, err := c.db.Exec("DELETE FROM check_mismatches WHERE network = ? AND epoch = ?", network, epoch)
	return err
}

// GetMismatches retrieves mismatch records. An empty poolBech32 returns all pools.
func (c *Cache) GetMismatches(network string, epoch uint64, poolBech32 string) ([]CheckMismatch, error) {
	query := `SELECT network, epoch, pool_bech32, stake_address, field, dingo_value, koios_value, category, checked_at FROM check_mismatches WHERE network = ? AND epoch = ?`
	args := []any{network, epoch}
	if poolBech32 != "" {
		query += " AND pool_bech32 = ?"
		args = append(args, poolBech32)
	}
	query += " ORDER BY id"
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []CheckMismatch
	for rows.Next() {
		var m CheckMismatch
		if err := rows.Scan(&m.Network, &m.Epoch, &m.PoolBech32, &m.StakeAddress, &m.Field, &m.DingoValue, &m.KoiosValue, &m.Category, &m.CheckedAt); err != nil {
			return nil, err
		}
		ret = append(ret, m)
	}
	return ret, rows.Err()
}

// GetStatusSummary returns all check epoch statuses for a network in epoch order.
func (c *Cache) GetStatusSummary(network string) ([]CheckEpochStatus, error) {
	rows, err := c.db.Query(`SELECT network, epoch, last_checked_at, status, mismatch_count, dingo_pool_count, koios_pool_count, only_dingo_pools, only_koios_pools FROM check_epoch_status WHERE network = ? ORDER BY epoch ASC`, network)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []CheckEpochStatus
	for rows.Next() {
		var s CheckEpochStatus
		if err := rows.Scan(&s.Network, &s.Epoch, &s.LastCheckedAt, &s.Status, &s.MismatchCount, &s.DingoPoolCount, &s.KoiosPoolCount, &s.OnlyDingoPools, &s.OnlyKoiosPools); err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}
	return ret, rows.Err()
}

func createCacheSchema(db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS koios_epoch_info (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			active_stake TEXT NOT NULL, fees TEXT NOT NULL, total_rewards TEXT NOT NULL,
			epoch_end_time DATETIME NOT NULL, pre_staking INTEGER NOT NULL DEFAULT 0, fetched_at DATETIME NOT NULL,
			era TEXT NOT NULL DEFAULT '', out_sum TEXT NOT NULL DEFAULT '', tx_count INTEGER NOT NULL DEFAULT 0,
			blk_count INTEGER NOT NULL DEFAULT 0, epoch_start_time DATETIME NOT NULL, first_block_time DATETIME NOT NULL,
			last_block_time DATETIME NOT NULL, avg_blk_reward TEXT NOT NULL DEFAULT '')`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_kei_net_epoch ON koios_epoch_info(network, epoch)`,
		`CREATE TABLE IF NOT EXISTS koios_pool_epoch (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			pool_bech32 TEXT NOT NULL, active_stake TEXT NOT NULL, block_cnt INTEGER NOT NULL,
			delegators INTEGER NOT NULL, margin TEXT NOT NULL DEFAULT '', fixed_cost TEXT NOT NULL DEFAULT '',
			pool_fees TEXT NOT NULL DEFAULT '', deleg_rewards TEXT NOT NULL DEFAULT '', member_rewards TEXT NOT NULL DEFAULT '',
			fetched_at DATETIME NOT NULL, active_stake_pct TEXT NOT NULL DEFAULT '', saturation_pct TEXT NOT NULL DEFAULT '',
			epoch_ros TEXT NOT NULL DEFAULT '')`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_kpe_net_epoch_pool ON koios_pool_epoch(network, epoch, pool_bech32)`,
		`CREATE TABLE IF NOT EXISTS koios_totals (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			treasury TEXT NOT NULL, reserves TEXT NOT NULL, fees TEXT NOT NULL, reward TEXT NOT NULL, fetched_at DATETIME NOT NULL,
			circulation TEXT NOT NULL DEFAULT '', supply TEXT NOT NULL DEFAULT '', deposits_stake TEXT NOT NULL DEFAULT '',
			deposits_drep TEXT NOT NULL DEFAULT '', deposits_proposal TEXT NOT NULL DEFAULT '', treasury_donation TEXT NOT NULL DEFAULT '',
			treasury_withdrawal TEXT NOT NULL DEFAULT '', reserves_withdrawal TEXT NOT NULL DEFAULT '')`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_kt_net_epoch ON koios_totals(network, epoch)`,
		`CREATE TABLE IF NOT EXISTS koios_account_rewards (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			stake_address TEXT NOT NULL, earned TEXT NOT NULL, fetched_at DATETIME NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_kar_net_epoch_addr ON koios_account_rewards(network, epoch, stake_address)`,
		`CREATE TABLE IF NOT EXISTS check_epoch_status (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			last_checked_at DATETIME NOT NULL, status TEXT NOT NULL, mismatch_count INTEGER NOT NULL,
			dingo_pool_count INTEGER NOT NULL, koios_pool_count INTEGER NOT NULL, only_dingo_pools TEXT NOT NULL, only_koios_pools TEXT NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_ces_net_epoch ON check_epoch_status(network, epoch)`,
		`CREATE TABLE IF NOT EXISTS check_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, run_at DATETIME NOT NULL,
			epochs_checked INTEGER NOT NULL, pools_checked INTEGER NOT NULL, mismatch_count INTEGER NOT NULL, report_path TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS check_mismatches (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			pool_bech32 TEXT NOT NULL, stake_address TEXT NOT NULL, field TEXT NOT NULL, dingo_value TEXT NOT NULL,
			koios_value TEXT NOT NULL, category TEXT NOT NULL, checked_at DATETIME NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_cm_net_epoch ON check_mismatches(network, epoch)`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	// Older cache files may contain columns that the current structs no longer write.
	for _, item := range [][2]string{{"koios_epoch_info", "pool_cnt"}, {"koios_epoch_info", "delegator_cnt"}, {"koios_totals", "deposits_d_rep"}} {
		rows, err := db.Query("SELECT 1 FROM pragma_table_info(?) WHERE name = ?", item[0], item[1])
		if err != nil {
			continue
		}
		defer rows.Close()
		present := rows.Next()
		if err := rows.Err(); err != nil {
			continue
		}
		if present {
			_, _ = db.Exec("ALTER TABLE " + item[0] + " DROP COLUMN " + item[1])
		}
	}
	return nil
}

func scanPool(rows *sql.Rows, p *KoiosPoolEpoch) error {
	return rows.Scan(&p.Network, &p.Epoch, &p.PoolBech32, &p.ActiveStake, &p.BlockCnt, &p.Delegators,
		&p.Margin, &p.FixedCost, &p.PoolFees, &p.DelegRewards, &p.MemberRewards, &p.FetchedAt,
		&p.ActiveStakePct, &p.SaturationPct, &p.EpochRos)
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
