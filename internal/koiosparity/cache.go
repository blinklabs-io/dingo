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
	// is the correct counterpart to reward_ada_pots.Fees. /totals.reward has no
	// matching Dingo aggregate and is intentionally not compared.
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

// KoiosAccountRewards holds one Koios /account_reward_history reference row
// for (network, epoch, stake_address, reward_type) — issue #3097's
// per-account exact-parity comparison consumes this. RewardType is part of
// the key (not just a stored field) because a single account can
// legitimately have both a "member" and a "leader" row in the same epoch
// (e.g. a pool owner delegating to their own pool) — see
// createCacheSchema's idx_kar_net_epoch_addr_type.
type KoiosAccountRewards struct {
	ID           uint
	Network      string
	Epoch        uint64
	StakeAddress string
	// RewardType is Koios's /account_reward_history "type" enum value
	// verbatim: "member", "leader", "treasury", "reserves", or "refund".
	// CompareAccountEpoch currently treats treasury/reserves/refund as out
	// of scope (see its doc comment) — stored here regardless, per this
	// cache's "store the full documented schema" convention.
	RewardType string
	Earned     string // lovelace decimal string (Koios "amount")
	// SpendableEpoch is Koios's spendable_epoch — stored for reference only;
	// not yet compared against anything in Dingo's schema.
	SpendableEpoch uint64
	// PoolIDBech32 is Koios's pool_id_bech32 — null/empty for reward types
	// with no associated pool. Stored for reference only.
	PoolIDBech32 string
	FetchedAt    time.Time
}

// KoiosAccountCoverage records whether a per-epoch Koios account-reward fetch
// (FetchAccountRewardsForEpoch) completed successfully across every chunk of
// the requested address universe. Complete must only ever be set true when
// every chunk succeeded — see FetchAccountRewardsForEpoch and
// CommitAccountRewardsForEpoch. checkEpoch's per-account comparison phase
// consults this before ever treating koios_account_rewards as a complete
// reference set for the epoch (mirroring how a missing koios_totals row
// already gates CompareEpochTotals); an absent or incomplete row must
// produce an explicit ERROR-category mismatch
// (CategoryAcctCoverageIncomplete), never a silent skip that could let a
// partially-fetched epoch read as PASS.
type KoiosAccountCoverage struct {
	ID      uint
	Network string
	Epoch   uint64
	// RequestedCount is the size of the address universe requested for this
	// fetch (Dingo's own known addresses unioned with Koios's full
	// historical account list — see FetchAccountRewardsForEpoch).
	RequestedCount int
	// FetchedCount is the number of Koios account-reward rows actually
	// stored (can differ from RequestedCount: most requested addresses have
	// no reward at all in a given epoch, so they contribute zero rows).
	FetchedCount int
	Complete     bool
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
	db, err := sql.Open(
		"sqlite",
		path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	)
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
	_, err := c.db.Exec(
		`INSERT INTO koios_epoch_info
		(network, epoch, active_stake, fees, total_rewards, epoch_end_time, pre_staking, fetched_at,
		 era, out_sum, tx_count, blk_count, epoch_start_time, first_block_time, last_block_time, avg_blk_reward)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch) DO UPDATE SET
		 active_stake=excluded.active_stake, fees=excluded.fees, total_rewards=excluded.total_rewards,
		 epoch_end_time=excluded.epoch_end_time, pre_staking=excluded.pre_staking, fetched_at=excluded.fetched_at,
		 era=excluded.era, out_sum=excluded.out_sum, tx_count=excluded.tx_count, blk_count=excluded.blk_count,
		 epoch_start_time=excluded.epoch_start_time, first_block_time=excluded.first_block_time,
		 last_block_time=excluded.last_block_time, avg_blk_reward=excluded.avg_blk_reward`,
		info.Network,
		info.Epoch,
		info.ActiveStake,
		info.Fees,
		info.TotalRewards,
		info.EpochEndTime,
		info.PreStaking,
		info.FetchedAt,
		info.Era,
		info.OutSum,
		info.TxCount,
		info.BlkCount,
		info.EpochStartTime,
		info.FirstBlockTime,
		info.LastBlockTime,
		info.AvgBlkReward,
	)
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
func (c *Cache) CommitEpochData(
	info KoiosEpochInfo,
	rows []KoiosPoolEpoch,
	totals *KoiosTotals,
) error {
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
	_, err := c.db.Exec(
		`INSERT INTO koios_pool_epoch
		(network, epoch, pool_bech32, active_stake, block_cnt, delegators, margin, fixed_cost,
		 pool_fees, deleg_rewards, member_rewards, fetched_at, active_stake_pct, saturation_pct, epoch_ros)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch, pool_bech32) DO UPDATE SET active_stake=excluded.active_stake,
		block_cnt=excluded.block_cnt, delegators=excluded.delegators, margin=excluded.margin,
		fixed_cost=excluded.fixed_cost, pool_fees=excluded.pool_fees, deleg_rewards=excluded.deleg_rewards,
		member_rewards=excluded.member_rewards, fetched_at=excluded.fetched_at,
		active_stake_pct=excluded.active_stake_pct, saturation_pct=excluded.saturation_pct, epoch_ros=excluded.epoch_ros`,
		pe.Network,
		pe.Epoch,
		pe.PoolBech32,
		pe.ActiveStake,
		pe.BlockCnt,
		pe.Delegators,
		pe.Margin,
		pe.FixedCost,
		pe.PoolFees,
		pe.DelegRewards,
		pe.MemberRewards,
		pe.FetchedAt,
		pe.ActiveStakePct,
		pe.SaturationPct,
		pe.EpochRos,
	)
	return err
}

// GetEpochInfo retrieves a cached Koios epoch info record.
func (c *Cache) GetEpochInfo(
	network string,
	epoch uint64,
) (*KoiosEpochInfo, error) {
	var info KoiosEpochInfo
	err := c.db.QueryRow(`SELECT network, epoch, active_stake, fees, total_rewards, epoch_end_time, pre_staking,
		fetched_at, era, out_sum, tx_count, blk_count, epoch_start_time, first_block_time, last_block_time, avg_blk_reward
		FROM koios_epoch_info WHERE network = ? AND epoch = ?`, network, epoch).
		Scan(
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
		FROM koios_totals WHERE network = ? AND epoch = ?`, network, epoch).
		Scan(
			&totals.Network, &totals.Epoch, &totals.Treasury, &totals.Reserves, &totals.Fees, &totals.Reward,
			&totals.FetchedAt, &totals.Circulation, &totals.Supply, &totals.DepositsStake, &totals.DepositsDRep,
			&totals.DepositsProposal, &totals.TreasuryDonation, &totals.TreasuryWithdrawal, &totals.ReservesWithdrawal)
	if err != nil {
		return nil, err
	}
	return &totals, nil
}

// GetAllPoolsForEpoch retrieves all cached pool rows for (network, epoch).
func (c *Cache) GetAllPoolsForEpoch(
	network string,
	epoch uint64,
) ([]KoiosPoolEpoch, error) {
	rows, err := c.db.Query(
		`SELECT network, epoch, pool_bech32, active_stake, block_cnt, delegators, margin,
		fixed_cost, pool_fees, deleg_rewards, member_rewards, fetched_at, active_stake_pct, saturation_pct, epoch_ros
		FROM koios_pool_epoch WHERE network = ? AND epoch = ? ORDER BY id`,
		network,
		epoch,
	)
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

// CommitAccountRewardsForEpoch atomically replaces every koios_account_rewards
// row for (network, epoch) and records coverage in a single transaction — the
// same "delete then bulk insert, commit together" pattern CommitEpochData
// uses for pool rows, so a partial account fetch (a crash or cancellation
// mid-commit) can never leave a half-written epoch that GetAccountCoverage
// would report as complete. requestedCount is the size of the address
// universe FetchAccountRewardsForEpoch actually requested Koios reference
// data for; complete must be true only when every chunk of that fetch
// succeeded — passing complete=false records the attempt (for
// observability) without ever letting the epoch read as a valid reference
// set.
func (c *Cache) CommitAccountRewardsForEpoch(
	network string,
	epoch uint64,
	rows []KoiosAccountRewards,
	requestedCount int,
	complete bool,
	fetchedAt time.Time,
) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec("DELETE FROM koios_account_rewards WHERE network = ? AND epoch = ?", network, epoch); err != nil {
		return err
	}
	if len(rows) > 0 {
		var stmt *sql.Stmt
		stmt, err = tx.Prepare(`INSERT INTO koios_account_rewards
			(network, epoch, stake_address, reward_type, earned, spendable_epoch, pool_id_bech32, fetched_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close() //nolint:errcheck
		for i := range rows {
			rows[i].Network, rows[i].Epoch = network, epoch
			if _, err = stmt.Exec(
				rows[i].Network, rows[i].Epoch, rows[i].StakeAddress, rows[i].RewardType, rows[i].Earned,
				rows[i].SpendableEpoch, rows[i].PoolIDBech32, rows[i].FetchedAt); err != nil {
				return err
			}
		}
	}
	if _, err = tx.Exec("DELETE FROM koios_account_coverage WHERE network = ? AND epoch = ?", network, epoch); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO koios_account_coverage
		(network, epoch, requested_count, fetched_count, complete, fetched_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		network, epoch, requestedCount, len(rows), complete, fetchedAt); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

// GetAccountRewardsForEpoch retrieves all cached Koios account-reward rows
// for (network, epoch).
func (c *Cache) GetAccountRewardsForEpoch(
	network string,
	epoch uint64,
) ([]KoiosAccountRewards, error) {
	rows, err := c.db.Query(
		`SELECT network, epoch, stake_address, reward_type, earned, spendable_epoch, pool_id_bech32, fetched_at
		FROM koios_account_rewards WHERE network = ? AND epoch = ? ORDER BY id`,
		network,
		epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KoiosAccountRewards
	for rows.Next() {
		var r KoiosAccountRewards
		if err := rows.Scan(&r.Network, &r.Epoch, &r.StakeAddress, &r.RewardType, &r.Earned,
			&r.SpendableEpoch, &r.PoolIDBech32, &r.FetchedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetAccountCoverage retrieves the account-fetch coverage record for
// (network, epoch). Returns sql.ErrNoRows (via the driver, propagated
// unwrapped like GetEpochInfo/GetTotals) when no fetch has ever been
// attempted for this epoch — callers must treat that identically to
// Complete == false, never as "nothing to compare".
func (c *Cache) GetAccountCoverage(
	network string,
	epoch uint64,
) (*KoiosAccountCoverage, error) {
	var cov KoiosAccountCoverage
	err := c.db.QueryRow(`SELECT network, epoch, requested_count, fetched_count, complete, fetched_at
		FROM koios_account_coverage WHERE network = ? AND epoch = ?`, network, epoch).
		Scan(&cov.Network, &cov.Epoch, &cov.RequestedCount, &cov.FetchedCount, &cov.Complete, &cov.FetchedAt)
	if err != nil {
		return nil, err
	}
	return &cov, nil
}

// MarkAccountCoverageIncomplete downgrades an existing koios_account_coverage
// row for (network, epoch) to complete = false, touching nothing else —
// not koios_account_rewards, not koios_account_checked/
// koios_account_fetch_staged_rows. Called when a --force-refresh attempt
// (fetchAccountRewardsForEpoch's forceRefresh path) fails partway through:
// because forceRefresh re-dispatches every chunk without pre-invalidating
// existing checkpoint data (deleting it up front would risk losing valid
// data before its replacement is confirmed fetched — see that function's
// doc comment), a partial failure can leave koios_account_checked holding a
// mix of freshly-refreshed chunks and untouched pre-refresh chunks: two
// different Koios snapshots that were never actually valid together. The
// stale complete = true row from the last successful (pre-refresh) commit
// would otherwise still make compareEpochAccounts trust that mixed state —
// its coverage.Complete gate is exactly what stops CompareAccountEpoch/
// accountLifecycleMismatches from reading it once this flips to false, and
// GetEpochsMissingAccountCoverage's `a.complete = 0` filter picks the epoch
// back up for a normal (non-force-refresh) fetch attempt to resume and
// eventually re-commit a fresh, fully consistent complete = true row —
// no further explicit --force-refresh required. A no-op, not an error, if
// no coverage row exists yet for this (network, epoch).
func (c *Cache) MarkAccountCoverageIncomplete(network string, epoch uint64) error {
	_, err := c.db.Exec(
		`UPDATE koios_account_coverage SET complete = 0 WHERE network = ? AND epoch = ?`,
		network,
		epoch,
	)
	return err
}

// SaveAccountFetchChunkProgress durably records one successfully-fetched
// chunk's rows and per-address "checked" markers for (network, epoch),
// atomically — dingo #3099's resumable checkpoint: FetchAccountRewardsForEpoch
// calls this once per chunk as it completes, instead of only accumulating
// rows in memory, so a killed/restarted process resumes from whichever
// chunks already committed here rather than redoing the whole epoch.
//
// Safe to call again for the same chunkHash (e.g. a resumed attempt that
// re-fetches a chunk that was in flight but never confirmed committed): it
// first deletes any prior staged rows/checked markers for this exact
// (network, epoch, chunk_hash) before inserting the fresh ones, so a retry
// can never duplicate rows.
//
// addressesInChunk is the full requested address list for the chunk — every
// address gets a koios_account_checked row (reward_row_count counts how many
// of rows belong to it) even if Koios returned zero reward rows for it,
// which is exactly what distinguishes a confirmed zero-reward address from
// one no chunk has ever covered.
func (c *Cache) SaveAccountFetchChunkProgress(
	network string,
	epoch uint64,
	chunkHash string,
	rows []KoiosAccountRewards,
	addressesInChunk []string,
	now time.Time,
) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(
		`DELETE FROM koios_account_fetch_staged_rows WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
		network, epoch, chunkHash,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM koios_account_checked WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
		network, epoch, chunkHash,
	); err != nil {
		return err
	}

	if len(rows) > 0 {
		var stmt *sql.Stmt
		stmt, err = tx.Prepare(`INSERT INTO koios_account_fetch_staged_rows
			(network, epoch, chunk_hash, stake_address, reward_type, earned, spendable_epoch, pool_id_bech32, fetched_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close() //nolint:errcheck
		for i := range rows {
			if _, err = stmt.Exec(
				network, epoch, chunkHash, rows[i].StakeAddress, rows[i].RewardType,
				rows[i].Earned, rows[i].SpendableEpoch, rows[i].PoolIDBech32, rows[i].FetchedAt,
			); err != nil {
				return err
			}
		}
	}

	rewardRowCount := make(map[string]int, len(addressesInChunk))
	for _, r := range rows {
		rewardRowCount[r.StakeAddress]++
	}
	var checkedStmt *sql.Stmt
	checkedStmt, err = tx.Prepare(`INSERT INTO koios_account_checked
		(network, epoch, stake_address, chunk_hash, reward_row_count, checked_at)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer checkedStmt.Close() //nolint:errcheck
	for _, addr := range addressesInChunk {
		if _, err = checkedStmt.Exec(
			network, epoch, addr, chunkHash, rewardRowCount[addr], now,
		); err != nil {
			return err
		}
	}

	err = tx.Commit()
	return err
}

// GetDoneAccountChunkHashes returns the set of chunk hashes already
// checkpointed for (network, epoch) — FetchAccountRewardsForEpoch skips
// dispatching a chunk whose hash is present here on resume.
func (c *Cache) GetDoneAccountChunkHashes(
	network string,
	epoch uint64,
) (map[string]bool, error) {
	rows, err := c.db.Query(
		`SELECT DISTINCT chunk_hash FROM koios_account_checked WHERE network = ? AND epoch = ?`,
		network,
		epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	done := make(map[string]bool)
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		done[hash] = true
	}
	return done, rows.Err()
}

// GetChunkHashesWithStagedRows returns the set of chunk hashes for
// (network, epoch) that have at least one row in
// koios_account_fetch_staged_rows — i.e. chunks whose checkpointed result
// was genuinely non-empty, as opposed to a chunk that checkpointed with zero
// rows (see fetchAccountRewardsForEpoch's grace-window trust logic: an
// empty-but-done chunk checkpointed while Koios's account_reward_history
// publishing lag was still possible must not be blindly trusted as final
// until that grace window has actually closed).
func (c *Cache) GetChunkHashesWithStagedRows(
	network string,
	epoch uint64,
) (map[string]bool, error) {
	rows, err := c.db.Query(
		`SELECT DISTINCT chunk_hash FROM koios_account_fetch_staged_rows WHERE network = ? AND epoch = ?`,
		network,
		epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	withRows := make(map[string]bool)
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		withRows[hash] = true
	}
	return withRows, rows.Err()
}

// GetStagedAccountRows returns every checkpointed row for (network, epoch)
// across all committed chunks — read back once every chunk in the current
// plan is done, then passed as-is to the existing, unmodified
// Cache.CommitAccountRewardsForEpoch to finalize the epoch exactly the way
// #3097 already does.
func (c *Cache) GetStagedAccountRows(
	network string,
	epoch uint64,
) ([]KoiosAccountRewards, error) {
	rows, err := c.db.Query(
		`SELECT stake_address, reward_type, earned, spendable_epoch, pool_id_bech32, fetched_at
		FROM koios_account_fetch_staged_rows WHERE network = ? AND epoch = ? ORDER BY id`,
		network,
		epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KoiosAccountRewards
	for rows.Next() {
		var r KoiosAccountRewards
		if err := rows.Scan(
			&r.StakeAddress, &r.RewardType, &r.Earned, &r.SpendableEpoch, &r.PoolIDBech32, &r.FetchedAt,
		); err != nil {
			return nil, err
		}
		r.Network, r.Epoch = network, epoch
		out = append(out, r)
	}
	return out, rows.Err()
}

// InvalidateStaleAccountChunks deletes staged rows/checked markers for any
// chunk hash not present in currentChunkHashes — dingo #3099's "invalidate/
// re-fetch affected chunks when request parameters or reference data change"
// requirement. Because chunk hashes are content-addressed (sha256 of a
// chunk's own sorted address list), only chunks whose address grouping is no
// longer part of the current plan are pruned; an unaffected chunk (same
// address set under the new plan) keeps its checkpointed progress and still
// counts as done.
//
// Every stale chunk's pair of deletes — and every stale chunk together —
// runs in one transaction. Without this, a crash partway through (either
// between a chunk's two deletes, or between two chunks) could delete a
// chunk's staged rows but leave its koios_account_checked markers in place;
// GetDoneAccountChunkHashes would then still report that chunk as done, so
// fetchAccountRewardsForEpoch would skip it and GetStagedAccountRows would
// return nothing for it — letting the epoch commit complete=true with a
// silently incomplete reward set, exactly what #3099 must prevent.
func (c *Cache) InvalidateStaleAccountChunks(
	network string,
	epoch uint64,
	currentChunkHashes []string,
) error {
	done, err := c.GetDoneAccountChunkHashes(network, epoch)
	if err != nil {
		return err
	}
	if len(done) == 0 {
		return nil
	}
	current := make(map[string]bool, len(currentChunkHashes))
	for _, h := range currentChunkHashes {
		current[h] = true
	}
	var stale []string
	for hash := range done {
		if !current[hash] {
			stale = append(stale, hash)
		}
	}
	if len(stale) == 0 {
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
	for _, hash := range stale {
		if _, err = tx.Exec(
			`DELETE FROM koios_account_fetch_staged_rows WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
			network, epoch, hash,
		); err != nil {
			return err
		}
		if _, err = tx.Exec(
			`DELETE FROM koios_account_checked WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
			network, epoch, hash,
		); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

// GetZeroRewardAccountsForEpoch returns addresses Koios confirmed it
// answered for (network, epoch) with zero reward rows — a definitive "no
// reward this epoch," proven checked rather than merely absent from
// koios_account_rewards.
func (c *Cache) GetZeroRewardAccountsForEpoch(
	network string,
	epoch uint64,
) ([]string, error) {
	rows, err := c.db.Query(
		`SELECT stake_address FROM koios_account_checked
		WHERE network = ? AND epoch = ? AND reward_row_count = 0
		ORDER BY stake_address`,
		network, epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var addrs []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, rows.Err()
}

// GetAccountUniverseForEpoch returns the persisted set of addresses actually
// checked for (network, epoch) — used to diff adjacent epochs' universes for
// newly-registered/deregistered reporting without re-deriving from Dingo's
// own source or Koios's live /account_list.
func (c *Cache) GetAccountUniverseForEpoch(
	network string,
	epoch uint64,
) ([]string, error) {
	rows, err := c.db.Query(
		`SELECT stake_address FROM koios_account_checked
		WHERE network = ? AND epoch = ? ORDER BY stake_address`,
		network, epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var addrs []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, rows.Err()
}

// GetFetchedEpochRange returns the min and max fetched epoch numbers.
func (c *Cache) GetFetchedEpochRange(
	network string,
) (min, max uint64, err error) {
	var lo, hi sql.NullInt64
	err = c.db.QueryRow("SELECT MIN(epoch), MAX(epoch) FROM koios_epoch_info WHERE network = ?", network).
		Scan(&lo, &hi)
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
	rows, err := c.db.Query(
		"SELECT epoch FROM koios_epoch_info WHERE network = ? ORDER BY epoch ASC",
		network,
	)
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
// either have no check result yet, OR whose Koios data was refreshed
// (fetched_at updated) after the last check, OR — when accountsEnabled is
// true — whose #3097 per-account reference data (koios_account_coverage) is
// absent, incomplete, or was refreshed after the last check. This ensures a
// forced re-fetch (pool-level or account-level) is always followed by an
// automatic re-check rather than leaving stale PASS/FAIL/ERROR rows in the
// cache.
//
// The accountsEnabled parameter exists because koios_account_coverage
// freshness is only ever a meaningful recheck trigger when the caller
// actually runs the per-account comparison phase (CheckConfig.AccountsEnabled/
// ObserverConfig.AccountsEnabled) — passing false reproduces the exact
// pre-#3097 query (pool/aggregate staleness only), so a caller that never
// enables accounts sees no behavior change and never has an epoch queued for
// recheck purely because its account coverage happens to be absent (which is
// simply expected in that mode, not a discrepancy worth flagging). See
// ARCHITECTURE.md's Koios Parity Tracker "Per-account exact parity"
// subsection for the full epoch-selection design this is part of.
func (c *Cache) GetEpochsNeedingCheck(
	network string,
	accountsEnabled bool,
) ([]uint64, error) {
	// LEFT JOINs so we pick up epochs with no status row (NULL
	// last_checked_at), epochs where fetched_at > last_checked_at (stale pool
	// check), and — when accountsEnabled — epochs with no/incomplete/stale
	// account coverage relative to the last check.
	query := `
		SELECT k.epoch
		FROM koios_epoch_info k
		LEFT JOIN check_epoch_status s
		       ON k.network = s.network AND k.epoch = s.epoch`
	if accountsEnabled {
		query += `
		LEFT JOIN koios_account_coverage a
		       ON k.network = a.network AND k.epoch = a.epoch`
	}
	query += `
		WHERE k.network = ?
		  AND (s.epoch IS NULL OR k.fetched_at > s.last_checked_at`
	if accountsEnabled {
		query += `
		       OR a.epoch IS NULL OR a.complete = 0 OR a.fetched_at > s.last_checked_at`
	}
	query += `)
		ORDER BY k.epoch ASC`
	rows, err := c.db.Query(query, network)
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

// GetEpochsMissingAccountCoverage returns epoch numbers in [from, through]
// that already have a fetched koios_epoch_info row but whose #3097
// per-account Koios reference data (koios_account_coverage) is either absent
// or present with complete = 0.
//
// This is the fetch-side counterpart to GetEpochsNeedingCheck's
// accountsEnabled branch: GetUncachedEpochs alone (keyed purely off
// koios_epoch_info presence) can never re-select an epoch whose pool-level
// data was fetched before AccountsEnabled existed or was turned on — it would
// look "already fetched" forever and never get a per-account backfill. Fetch
// unions this into its epoch list only when cfg.AccountsEnabled, and skips
// the redundant pool-history re-fetch for any epoch this returns that
// GetUncachedEpochs did not also return (see fetchEpoch's caller in fetch.go).
// See ARCHITECTURE.md's Koios Parity Tracker "Per-account exact parity"
// subsection.
func (c *Cache) GetEpochsMissingAccountCoverage(
	network string,
	from, through uint64,
) ([]uint64, error) {
	// pre_staking = 0 excludes epochs <= preStakingThroughEpoch: those get a
	// koios_epoch_info row (the PreStaking marker) but never a
	// koios_account_coverage row — FetchEpochAccountsWithAddrs skips them
	// entirely, matching fetchEpoch/checkEpoch's own exclusion of the same
	// epochs — so without this filter they would be selected for account
	// backfill on every fetch run forever.
	rows, err := c.db.Query(`
		SELECT k.epoch
		FROM koios_epoch_info k
		LEFT JOIN koios_account_coverage a
		       ON k.network = a.network AND k.epoch = a.epoch
		WHERE k.network = ?
		  AND k.epoch >= ? AND k.epoch <= ?
		  AND k.pre_staking = 0
		  AND (a.epoch IS NULL OR a.complete = 0)
		ORDER BY k.epoch ASC
	`, network, from, through)
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
func (c *Cache) GetUncachedEpochs(
	network string,
	from, through uint64,
) ([]uint64, error) {
	// Build the full desired range in memory (typically ≤ a few thousand epochs).
	want := make(map[uint64]bool, through-from+1)
	for e := from; e <= through; e++ {
		want[e] = true
	}

	rows, err := c.db.Query(
		"SELECT epoch FROM koios_epoch_info WHERE network = ? AND epoch >= ? AND epoch <= ?",
		network,
		from,
		through,
	)
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
	_, err := c.db.Exec(
		`INSERT INTO check_epoch_status
		(network, epoch, last_checked_at, status, mismatch_count, dingo_pool_count, koios_pool_count, only_dingo_pools, only_koios_pools)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(network, epoch) DO UPDATE SET last_checked_at=excluded.last_checked_at, status=excluded.status,
		mismatch_count=excluded.mismatch_count, dingo_pool_count=excluded.dingo_pool_count,
		koios_pool_count=excluded.koios_pool_count, only_dingo_pools=excluded.only_dingo_pools,
		only_koios_pools=excluded.only_koios_pools`,
		status.Network,
		status.Epoch,
		status.LastCheckedAt,
		status.Status,
		status.MismatchCount,
		status.DingoPoolCount,
		status.KoiosPoolCount,
		status.OnlyDingoPools,
		status.OnlyKoiosPools,
	)
	return err
}

// InsertCheckRun appends a check run record.
func (c *Cache) InsertCheckRun(run CheckRun) error {
	_, err := c.db.Exec(
		`INSERT INTO check_runs (network, run_at, epochs_checked, pools_checked, mismatch_count, report_path) VALUES (?, ?, ?, ?, ?, ?)`,
		run.Network,
		run.RunAt,
		run.EpochsChecked,
		run.PoolsChecked,
		run.MismatchCount,
		run.ReportPath,
	)
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
	stmt, err := tx.Prepare(
		`INSERT INTO check_mismatches (network, epoch, pool_bech32, stake_address, field, dingo_value, koios_value, category, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	)
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
	_, err := c.db.Exec(
		"DELETE FROM check_mismatches WHERE network = ? AND epoch = ?",
		network,
		epoch,
	)
	return err
}

// GetMismatches retrieves mismatch records. An empty poolBech32 returns all pools.
func (c *Cache) GetMismatches(
	network string,
	epoch uint64,
	poolBech32 string,
) ([]CheckMismatch, error) {
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
	rows, err := c.db.Query(
		`SELECT network, epoch, last_checked_at, status, mismatch_count, dingo_pool_count, koios_pool_count, only_dingo_pools, only_koios_pools FROM check_epoch_status WHERE network = ? ORDER BY epoch ASC`,
		network,
	)
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
			stake_address TEXT NOT NULL, reward_type TEXT NOT NULL DEFAULT '', earned TEXT NOT NULL,
			spendable_epoch INTEGER NOT NULL DEFAULT 0, pool_id_bech32 TEXT NOT NULL DEFAULT '',
			fetched_at DATETIME NOT NULL)`,
		// idx_kar_net_epoch_addr_type is deliberately NOT created here: on an
		// older cache.db (schema-only #1875 era) the table exists without a
		// reward_type column yet, so creating an index that references it
		// would fail. It's created below, after the additive-column
		// migration guarantees reward_type exists on every koios_account_rewards
		// table, old or new.
		`CREATE TABLE IF NOT EXISTS koios_account_coverage (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			requested_count INTEGER NOT NULL DEFAULT 0, fetched_count INTEGER NOT NULL DEFAULT 0,
			complete INTEGER NOT NULL DEFAULT 0, fetched_at DATETIME NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_kac_net_epoch ON koios_account_coverage(network, epoch)`,

		// dingo #3099: durable per-chunk checkpoint staging so a killed/restarted
		// FetchAccountRewardsForEpoch resumes from already-committed chunks
		// instead of redoing the whole epoch (see fetch_accounts.go). Purely
		// additive alongside #3097's koios_account_rewards/koios_account_coverage
		// — CommitAccountRewardsForEpoch's contract and existing tests are
		// untouched; these staged rows are only ever read back and passed to it
		// once every chunk in the current plan has committed.
		`CREATE TABLE IF NOT EXISTS koios_account_fetch_staged_rows (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			chunk_hash TEXT NOT NULL, stake_address TEXT NOT NULL, reward_type TEXT NOT NULL,
			earned TEXT NOT NULL, spendable_epoch INTEGER NOT NULL DEFAULT 0,
			pool_id_bech32 TEXT NOT NULL DEFAULT '', fetched_at DATETIME NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_kafsr_net_epoch ON koios_account_fetch_staged_rows(network, epoch)`,
		`CREATE INDEX IF NOT EXISTS idx_kafsr_net_epoch_chunk ON koios_account_fetch_staged_rows(network, epoch, chunk_hash)`,

		// koios_account_checked doubles as: (a) the done-chunk marker enabling
		// resume (DISTINCT chunk_hash for (network, epoch)), (b) zero-reward-
		// confirmed detection (reward_row_count = 0 — Koios answered for this
		// address and it earned nothing, distinct from never having been asked
		// about at all), and (c) the persisted per-epoch address universe used
		// to diff newly-registered/deregistered accounts between adjacent
		// epochs.
		`CREATE TABLE IF NOT EXISTS koios_account_checked (
			id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
			stake_address TEXT NOT NULL, chunk_hash TEXT NOT NULL, reward_row_count INTEGER NOT NULL DEFAULT 0,
			checked_at DATETIME NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_kaced_net_epoch_addr ON koios_account_checked(network, epoch, stake_address)`,
		// Every per-chunk delete (SaveAccountFetchChunkProgress,
		// InvalidateStaleAccountChunks) filters on (network, epoch, chunk_hash);
		// without this index that lookup falls back to scanning every checked
		// row for the whole epoch instead of just the rows for one chunk,
		// making stale-chunk cleanup cost scale with the epoch's total address
		// count rather than the chunk being removed.
		`CREATE INDEX IF NOT EXISTS idx_kaced_net_epoch_chunk ON koios_account_checked(network, epoch, chunk_hash)`,

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
		rows, err := db.Query(
			"SELECT 1 FROM pragma_table_info(?) WHERE name = ?",
			item[0],
			item[1],
		)
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

	// Older cache files created before #3097 have a koios_account_rewards
	// table missing reward_type/spendable_epoch/pool_id_bech32 (schema-only
	// era, #1875) — add each column additively rather than dropping and
	// recreating the table, so any rows a prior partial run may have written
	// are preserved rather than lost. Guarded by pragma_table_info the same
	// way the drop-column migration above is, just adding instead of
	// dropping.
	for _, col := range [][2]string{
		{"reward_type", "TEXT NOT NULL DEFAULT ''"},
		{"spendable_epoch", "INTEGER NOT NULL DEFAULT 0"},
		{"pool_id_bech32", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := addColumnIfMissing(db, "koios_account_rewards", col[0], col[1]); err != nil {
			return fmt.Errorf(
				"migrate koios_account_rewards: add column %s: %w",
				col[0],
				err,
			)
		}
	}
	// The pre-#3097 unique index only covered (network, epoch,
	// stake_address); drop it now that reward_type is guaranteed to exist
	// (old rows default to "" via the ADD COLUMN above, which is fine:
	// reward_type was never populated pre-#3097 in practice since the table
	// was schema-only) and create the widened replacement.
	if _, err := db.Exec("DROP INDEX IF EXISTS idx_kar_net_epoch_addr"); err != nil {
		return fmt.Errorf(
			"migrate koios_account_rewards: drop old index: %w",
			err,
		)
	}
	// idx_kar_net_epoch_addr_type must be non-unique: Koios can legitimately
	// return duplicate (network, epoch, stake_address, reward_type) rows
	// (see CategoryAcctDuplicate's doc comment), and a unique constraint
	// would abort CommitAccountRewardsForEpoch's insert with a constraint
	// error before CompareAccountEpoch ever gets a chance to detect and
	// report the duplicate as an acct_duplicate FAIL. Explicitly drop any
	// unique version of this index a previous run of this migration may
	// already have created before this fix, since "CREATE INDEX IF NOT
	// EXISTS" would otherwise leave an existing unique index in place.
	if _, err := db.Exec("DROP INDEX IF EXISTS idx_kar_net_epoch_addr_type"); err != nil {
		return fmt.Errorf(
			"migrate koios_account_rewards: drop unique widened index: %w",
			err,
		)
	}
	if _, err := db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_kar_net_epoch_addr_type ON koios_account_rewards(network, epoch, stake_address, reward_type)",
	); err != nil {
		return fmt.Errorf(
			"migrate koios_account_rewards: create widened index: %w",
			err,
		)
	}
	return nil
}

// addColumnIfMissing adds column columnDDL (e.g. TEXT NOT NULL DEFAULT with an
// empty-string default) to table if it is not already present, so
// re-running createCacheSchema
// against an older cache.db is idempotent and never errors on a column that
// already exists.
func addColumnIfMissing(db *sql.DB, table, column, columnDDL string) error {
	rows, err := db.Query(
		"SELECT 1 FROM pragma_table_info(?) WHERE name = ?",
		table,
		column,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	present := rows.Next()
	if err := rows.Err(); err != nil {
		return err
	}
	if present {
		return nil
	}
	_, err = db.Exec(
		"ALTER TABLE " + table + " ADD COLUMN " + column + " " + columnDDL,
	)
	return err
}

func scanPool(rows *sql.Rows, p *KoiosPoolEpoch) error {
	return rows.Scan(
		&p.Network,
		&p.Epoch,
		&p.PoolBech32,
		&p.ActiveStake,
		&p.BlockCnt,
		&p.Delegators,
		&p.Margin,
		&p.FixedCost,
		&p.PoolFees,
		&p.DelegRewards,
		&p.MemberRewards,
		&p.FetchedAt,
		&p.ActiveStakePct,
		&p.SaturationPct,
		&p.EpochRos,
	)
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
