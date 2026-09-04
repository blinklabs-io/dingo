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
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	_ "github.com/glebarez/go-sqlite"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	// TotalPoolCount is epoch_summary.total_pool_count: the number of pools
	// in the distribution the mark pool_stake_snapshot rows for this epoch
	// were written from (rotation.go sets both from the same
	// StakeDistribution), so it is how many of those rows must be readable
	// for the set to be complete. Deliberately not RewardSnapshot's
	// TotalPoolCount, which counts the reduced reward distribution with
	// degraded pools already excluded.
	TotalPoolCount uint64
	Fees           string // lovelace decimal string; empty when reward_ada_pots row absent
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
	// StakePresent distinguishes "no reward_pool_input row yet at the 'stake
	// epoch' (K-1)" from "row exists with legitimately zero/empty
	// DelegatedStake/DelegatorCount" — mirrors ParamsPresent/
	// MemberRewardPresent below. Without this flag a pool present only in
	// the param-epoch or output query (e.g. a freshly registered pool whose
	// stake-epoch row hasn't landed yet) would get a bare zero-value stub
	// from GetPoolEpochDataMap, and ComparePoolEpoch would compare that zero
	// against Koios's real figures as a false value_mismatch instead of
	// reporting the row as genuinely missing. See ComparePoolEpoch, which
	// must never silently treat StakePresent == false as a comparison pass.
	StakePresent bool
	// DelegatedStake/DelegatorCount/FixedCost/Margin come from
	// reward_pool_input at the "stake epoch" (Koios epoch K's K-1): the mark
	// stake distribution Praos actually used as K's active-stake/
	// reward-calculation basis. A mark snapshot records the pool parameters
	// as of its own boundary, and those are the ones in force for the epoch
	// that snapshot is the basis for, so cost and margin align here rather
	// than with BlocksProduced at the param epoch (dingo #3484).
	DelegatedStake string // lovelace decimal string
	DelegatorCount uint64
	FixedCost      string // lovelace decimal string (reward_pool_input.cost)
	Margin         string // rational string (e.g. "1/10"); empty when null

	// ParamsPresent distinguishes "no reward_pool_input row yet at the
	// 'param epoch' (K+1)" from "row exists with a legitimately zero
	// BlocksProduced" — see ComparePoolEpoch, which must never silently
	// treat the former as a comparison pass. It covers BlocksProduced only;
	// FixedCost/Margin presence follows StakePresent.
	ParamsPresent bool
	// BlocksProduced comes from reward_pool_input at the "param epoch"
	// (K+1): that row's BlocksProduced describes the epoch immediately
	// before it (K), because buildRewardStateInputs
	// (ledger/snapshot/rotation.go) stamps it from evt.PreviousEpoch at
	// capture time, not from the row's own Epoch.
	BlocksProduced uint64

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
	// PoolUnspendable is reward_pool_output.unspendable at the stake epoch:
	// every reward the calculation attributed to this pool and the ledger
	// then withheld, member and leader alike. Zero is the case that makes
	// MemberRewardTotal usable as the comparable quantity on its own — with
	// nothing withheld, the pool's member total is its spendable member total
	// by construction.
	PoolUnspendable uint64

	// RewardsPending reports that the node has NOT yet reached the boundary at
	// which this stake epoch's rewards are applied
	// (reward_pool_output.boundary_slot, three epochs after the stake epoch).
	//
	// Before that boundary the per-account spendable flags are provisional: a
	// reward computed for a credential that deregisters in the meantime is
	// still marked spendable, and only the application flips it. Koios reports
	// rewards that were actually distributed, so comparing earlier makes Dingo
	// read high by exactly the forfeitures that have not happened yet
	// (dingo #3852). A difference before the boundary is a statement about
	// timing, not about correctness.
	//
	// The sense is deliberately negative so the zero value compares strictly.
	// A source that cannot establish the boundary must not silently downgrade
	// a real divergence to a lag; reporting a spurious mismatch is the safer
	// failure for a verification tool than hiding a true one.
	RewardsPending bool
	// SpendableMemberRewardPresent reports that reward_account_output rows
	// exist for the stake epoch at all, which is what makes a per-pool
	// spendable sum meaningful: a pool with no rows then genuinely earned no
	// spendable member reward, rather than the table having been pruned out
	// from under the read (cleanupOldSnapshots retains reward_account_output
	// without bound only in api storage mode).
	SpendableMemberRewardPresent bool
	// SpendableMemberRewardTotal is the sum of reward_account_output.amount
	// over the stake epoch's member rows the ledger actually credits —
	// spendable and not guarded by CIP-0163 expiry, the same pair
	// applyStakeRewards tests before crediting — which is the quantity Koios
	// pool_history.member_rewards reports.
	//
	// reward_pool_output.member_reward_total is deliberately not that
	// quantity: Result.addReward (ledger/rewards/rewards.go) accumulates it
	// from every member reward the calculation produced, spendable or not, so
	// the two differ by exactly the pool's unspendable member rewards — a
	// reward computed for a credential the ledger correctly never credits.
	// Comparing it against Koios reported a value_mismatch for any pool
	// holding one, against a ledger that was right (dingo #3797).
	//
	// Subtracting reward_pool_output.unspendable from member_reward_total
	// would not be equivalent: that column accumulates unspendable leader
	// rewards too.
	SpendableMemberRewardTotal string
}

// rewardTypeMember is the reward_account_output.reward_type value Dingo writes
// for a delegator's share of a pool reward, as opposed to the operator's
// "leader" row. Matches Koios /account_reward_history's own "member" type.
const rewardTypeMember = "member"

// DingoDB reads reward state directly from Dingo's metadata database.
// It supports all three backends Dingo supports: SQLite, PostgreSQL, MySQL.
type DingoDB struct {
	db      *sql.DB
	dialect string
}

// OpenDingoDB connects to Dingo's metadata database using the configured backend.
//
//   - sqlite (default): opens {DataDir}/metadata.sqlite in read-only WAL mode.
//     SQLite WAL allows concurrent readers alongside a live node.
//   - postgres: connects with the libpq-style DSN in cfg.DSN.
//   - mysql: connects with the go-sql-driver DSN in cfg.DSN.
func OpenDingoDB(cfg DingoDBConfig) (*DingoDB, error) {
	var driver, dsn string

	switch cfg.Plugin {
	case "sqlite", "":
		driver = "sqlite"
		path := filepath.Join(cfg.DataDir, "metadata.sqlite")
		dsn = fmt.Sprintf(
			"file:%s?mode=ro&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=cache_size(-16000)",
			path,
		)
	case "postgres":
		if cfg.DSN == "" {
			return nil, errors.New(
				"--metadata-dsn is required for postgres plugin",
			)
		}
		driver, dsn = "pgx", cfg.DSN
	case "mysql":
		if cfg.DSN == "" {
			return nil, errors.New(
				"--metadata-dsn is required for mysql plugin",
			)
		}
		driver, dsn = "mysql", cfg.DSN
	default:
		return nil, fmt.Errorf(
			"unsupported metadata plugin %q (sqlite, postgres, mysql)",
			cfg.Plugin,
		)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open dingo metadata: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping dingo metadata: %w", err)
	}
	return &DingoDB{db: db, dialect: cfg.Plugin}, nil
}

// Close releases the database connection.
func (d *DingoDB) Close() error {
	return d.db.Close()
}

// GetLatestEpoch returns the highest epoch number recorded in epoch_summary.
// ctx is forwarded to the DB driver so that a cancelled context aborts the query.
func (d *DingoDB) GetLatestEpoch(ctx context.Context) (uint64, error) {
	var epoch sql.NullInt64
	if err := d.queryRow(ctx, "SELECT MAX(epoch) FROM epoch_summary").Scan(&epoch); err != nil {
		return 0, fmt.Errorf("get latest epoch: %w", err)
	}
	if !epoch.Valid {
		return 0, errors.New("dingo db: no epoch_summary rows found")
	}
	return uint64( //nolint:gosec // epoch values are non-negative
		epoch.Int64,
	), nil
}

// GetEpochData returns epoch-level aggregates for the given epoch.
// Returns nil, nil when Dingo has not yet recorded an epoch_summary row.
// ctx is forwarded to the DB driver so that a cancelled context aborts the query.
func (d *DingoDB) GetEpochData(
	ctx context.Context,
	epoch uint64,
) (*DingoEpochData, error) {
	var summary models.EpochSummary
	if err := d.queryRow(ctx, `SELECT epoch, total_active_stake, total_pool_count, total_delegators, epoch_nonce, boundary_slot, snapshot_ready FROM epoch_summary WHERE epoch = ?`, epoch).Scan(
		&summary.Epoch, &summary.TotalActiveStake, &summary.TotalPoolCount, &summary.TotalDelegators, &summary.EpochNonce, &summary.BoundarySlot, &summary.SnapshotReady); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
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
		TotalActiveStake: strconv.FormatUint(
			uint64(summary.TotalActiveStake),
			10,
		),
		TotalPoolCount: summary.TotalPoolCount,
	}

	var pots models.RewardAdaPots
	if err := d.queryRow(ctx, `SELECT epoch, treasury, reserves, fees, rewards, captured_slot FROM reward_ada_pots WHERE epoch = ?`, epoch).Scan(
		&pots.Epoch, &pots.Treasury, &pots.Reserves, &pots.Fees, &pots.Rewards, &pots.CapturedSlot); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
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
//     DelegatedStake/DelegatorCount/Margin/FixedCost and
//     reward_pool_output's MemberRewardTotal are all read at this epoch — reward_calculation.go's
//     stakeRewardEpochsForNewEpoch computes both from the same
//     epochs.snapshot value, so input and output always share one Epoch.
//   - paramEpoch (K+1): reward_pool_input's BlocksProduced is captured onto
//     the row for the epoch *after* the one it describes — see
//     ledger/snapshot/rotation.go's buildRewardStateInputs, which stamps it
//     from evt.PreviousEpoch, not from the row's own Epoch. Only
//     BlocksProduced is read there; Margin/FixedCost are stake-epoch fields
//     (dingo #3484).
//
// See koiosStakeEpoch/koiosParamEpoch in check.go and ARCHITECTURE.md's Koios
// Parity Tracker "Epoch alignment" section for the full derivation.
//
// A pool present in only one of the two reward_pool_input reads (e.g. a pool
// with a stake-epoch row but whose param-epoch row hasn't been captured yet,
// or vice versa — a freshly registered pool whose param/output rows exist
// before its stake-epoch row does) still gets an entry — StakePresent/
// ParamsPresent/MemberRewardPresent record which pieces are actually
// available so ComparePoolEpoch never mistakes "not yet computed" for
// "compared and equal". One bulk query per table per epoch (three total),
// independent of pool count. ctx is forwarded to the DB driver so that a
// cancelled context aborts the query.
// GetPoolStakeSnapshotMembers implements RewardParitySource by reading the
// mark pool_stake_snapshot rows for epoch. See the interface doc comment for
// why this, and not epoch_summary.SnapshotReady, is the per-pool evidence of
// pool-set membership.
func (d *DingoDB) GetPoolStakeSnapshotMembers(
	ctx context.Context,
	epoch uint64,
) (map[string]struct{}, error) {
	rows, err := d.query(
		ctx,
		`SELECT pool_key_hash FROM pool_stake_snapshot WHERE epoch = ? AND snapshot_type = ?`,
		epoch,
		snapshotTypeMark,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"pool_stake_snapshot epoch %d: %w",
			epoch,
			err,
		)
	}
	defer rows.Close()
	members := make(map[string]struct{})
	for rows.Next() {
		var poolHash []byte
		if err := rows.Scan(&poolHash); err != nil {
			return nil, err
		}
		members[hex.EncodeToString(poolHash)] = struct{}{}
	}
	return members, rows.Err()
}

func (d *DingoDB) GetPoolEpochDataMap(
	ctx context.Context,
	stakeEpoch, paramEpoch uint64,
) (map[string]*DingoPoolEpochData, error) {
	rows, err := d.query(
		ctx,
		`SELECT pool_key_hash, delegated_stake, delegator_count, cost, margin FROM reward_pool_input WHERE epoch = ?`,
		stakeEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"reward_pool_input stake epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	defer rows.Close()
	m := make(map[string]*DingoPoolEpochData)
	for rows.Next() {
		var poolHash []byte
		var stake types.Uint64
		var delegators uint64
		var cost sql.NullInt64
		var margin types.Rat
		if err := rows.Scan(
			&poolHash, &stake, &delegators, &cost, &margin,
		); err != nil {
			return nil, err
		}
		data := &DingoPoolEpochData{
			StakePresent:   true,
			DelegatedStake: strconv.FormatUint(uint64(stake), 10),
			DelegatorCount: delegators,
		}
		if cost.Valid {
			data.FixedCost = strconv.FormatUint(
				//nolint:gosec // metadata values are non-negative
				uint64(cost.Int64),
				10,
			)
		}
		if margin.Rat != nil {
			data.Margin = margin.String()
		}
		m[hex.EncodeToString(poolHash)] = data
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = d.query(
		ctx,
		`SELECT pool_key_hash, blocks_produced FROM reward_pool_input WHERE epoch = ?`,
		paramEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"reward_pool_input param epoch %d: %w",
			paramEpoch,
			err,
		)
	}
	for rows.Next() {
		var poolHash []byte
		var blocks sql.NullInt64
		if err := rows.Scan(&poolHash, &blocks); err != nil {
			_ = rows.Close() //nolint:sqlclosecheck
			return nil, err
		}
		key := hex.EncodeToString(poolHash)
		data, ok := m[key]
		if !ok {
			// Present at the param epoch but not the stake epoch (e.g. a
			// freshly registered pool) — still record what's available
			// rather than dropping it. StakePresent stays false so
			// ComparePoolEpoch never compares the zero-value
			// DelegatedStake/DelegatorCount below as if they were real.
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.ParamsPresent = true
		if blocks.Valid {
			data.BlocksProduced = uint64( //nolint:gosec // metadata values are non-negative
				blocks.Int64,
			)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close() //nolint:sqlclosecheck
		return nil, err
	}
	_ = rows.Close() //nolint:sqlclosecheck

	// The tip decides whether this stake epoch's rewards have been applied;
	// see DingoPoolEpochData.RewardsPending. An unreadable or empty tip table
	// leaves tipKnown false, which keeps the comparison strict rather than
	// downgrading a real divergence on incomplete information.
	var tipSlot uint64
	tipKnown := false
	if tipRow := d.queryRow(ctx, `SELECT slot FROM tip ORDER BY id DESC LIMIT 1`); tipRow != nil {
		var slot sql.NullInt64
		if err := tipRow.Scan(&slot); err == nil && slot.Valid &&
			slot.Int64 > 0 {
			tipSlot = uint64(slot.Int64)
			tipKnown = true
		}
	}

	rows, err = d.query(
		ctx,
		`SELECT pool_key_hash, member_reward_total, unspendable, boundary_slot FROM reward_pool_output WHERE epoch = ?`,
		stakeEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"reward_pool_output epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var poolHash []byte
		var reward types.Uint64
		var unspendable types.Uint64
		var boundarySlot uint64
		if err := rows.Scan(
			&poolHash, &reward, &unspendable, &boundarySlot,
		); err != nil {
			return nil, err
		}
		key := hex.EncodeToString(poolHash)
		data, ok := m[key]
		if !ok {
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.MemberRewardPresent = true
		data.MemberRewardTotal = strconv.FormatUint(uint64(reward), 10)
		data.PoolUnspendable = uint64(unspendable)
		data.RewardsPending = tipKnown && tipSlot < boundarySlot
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := d.addSpendableMemberRewards(ctx, m, stakeEpoch); err != nil {
		return nil, err
	}
	return m, nil
}

// addSpendableMemberRewards fills SpendableMemberRewardTotal/Present from the
// stake epoch's reward_account_output rows — the sum Koios
// pool_history.member_rewards reports, which reward_pool_output's own
// member_reward_total is not (see DingoPoolEpochData).
//
// The predicate matches what the ledger actually credits: applyStakeRewards
// (ledger/reward_calculation.go) skips a reward that is not spendable and one
// whose reward account is guarded by CIP-0163 expiry, so both are excluded
// from the sum — but not from the presence test, which asks only whether the
// epoch's rows survive at all.
//
// The rows are summed in Go rather than by the database: amount is a decimal
// TEXT column, and the cast a portable SUM would need differs across the three
// backends DingoDB supports. Preview and preprod produce on the order of a
// hundred rows per epoch, and the observer only runs on those two networks.
//
// Presence is epoch-level. A pool with no spendable member row legitimately
// earned nothing, but only if the table holds the epoch at all —
// cleanupOldSnapshots retains reward_account_output without bound in api
// storage mode and prunes it in core, so an empty read must not be reported as
// a pool-wide zero.
func (d *DingoDB) addSpendableMemberRewards(
	ctx context.Context,
	m map[string]*DingoPoolEpochData,
	stakeEpoch uint64,
) error {
	// Every row for the epoch is read and filtered here rather than in the
	// WHERE clause, so presence means "the epoch's per-account rows exist"
	// exactly as it does in DatabaseSource.GetPoolEpochDataMap. Filtering in
	// SQL would make an epoch holding only leader or only withheld rows look
	// like a pruned epoch in one implementation and a populated one in the
	// other, and the two must not be able to disagree.
	rows, err := d.query(
		ctx,
		`SELECT pool_key_hash, reward_type, amount, spendable, guarded
FROM reward_account_output WHERE epoch = ?`,
		stakeEpoch,
	)
	if err != nil {
		return fmt.Errorf(
			"reward_account_output epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	defer rows.Close()
	totals := make(map[string]uint64)
	any := false
	for rows.Next() {
		var poolHash []byte
		var rewardType string
		var amount types.Uint64
		var spendable, guarded bool
		if err := rows.Scan(
			&poolHash, &rewardType, &amount, &spendable, &guarded,
		); err != nil {
			return err
		}
		any = true
		if rewardType != rewardTypeMember || !spendable || guarded {
			continue
		}
		totals[hex.EncodeToString(poolHash)] += uint64(amount)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !any {
		return nil
	}
	for key, total := range totals {
		data, ok := m[key]
		if !ok {
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.SpendableMemberRewardTotal = strconv.FormatUint(total, 10)
	}
	for _, data := range m {
		data.SpendableMemberRewardPresent = true
		if data.SpendableMemberRewardTotal == "" {
			data.SpendableMemberRewardTotal = "0"
		}
	}
	return nil
}

func (d *DingoDB) query(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, rebind(query, d.dialect), args...)
}

func (d *DingoDB) queryRow(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	return d.db.QueryRowContext(ctx, rebind(query, d.dialect), args...)
}

func rebind(query, dialect string) string {
	if dialect != "postgres" {
		return query
	}
	idx := 1
	out := make([]byte, 0, len(query)+8)
	for _, ch := range []byte(query) {
		if ch == '?' {
			out = append(out, '$')
			out = strconv.AppendInt(out, int64(idx), 10)
			idx++
			continue
		}
		out = append(out, ch)
	}
	return string(out)
}

// GetRewardAccountOutputs returns every per-account reward calculation
// output row Dingo committed for epoch, straight from reward_account_output.
// Not yet consumed by any comparison — #3097 (per-account exact parity) is
// what wires this up; it exists on DingoDB now so the standalone-CLI and
// in-process (DatabaseSource) implementations of RewardParitySource stay
// symmetric. ctx is forwarded to the DB driver so a cancelled context aborts
// the query.
func (d *DingoDB) GetRewardAccountOutputs(
	ctx context.Context,
	epoch uint64,
) ([]*models.RewardAccountOutput, error) {
	rows, err := d.query(
		ctx,
		`SELECT staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount, spendable, guarded, captured_slot, boundary_slot FROM reward_account_output WHERE epoch = ?`,
		epoch,
	)
	if err != nil {
		return nil, fmt.Errorf("reward_account_output epoch %d: %w", epoch, err)
	}
	defer rows.Close()

	var out []*models.RewardAccountOutput
	for rows.Next() {
		row := &models.RewardAccountOutput{}
		if err := rows.Scan(
			&row.StakingKey,
			&row.PoolKeyHash,
			&row.RewardType,
			&row.Epoch,
			&row.CredentialTag,
			&row.Amount,
			&row.Spendable,
			&row.Guarded,
			&row.CapturedSlot,
			&row.BoundarySlot,
		); err != nil {
			return nil, fmt.Errorf(
				"reward_account_output epoch %d: %w",
				epoch,
				err,
			)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reward_account_output epoch %d: %w", epoch, err)
	}
	return out, nil
}

// StakeAddressFromCredential converts a reward-account credential — as
// stored on models.RewardAccountOutput/RewardStakeInput (StakingKey +
// CredentialTag) — to its bech32 stake address ("stake1…"/"stake_test1…").
// credentialTag must be one of models.CredentialTagFromUint64's two values:
// 0 (key hash) or 1 (script hash); see that function's doc comment for the
// 0/1 meaning.
//
// koios-parity only ever targets preview/preprod, both testnet networks, so
// the address network ID is always lcommon.AddressNetworkTestnet — this
// never needs a network parameter. This mirrors
// api/blockfrost/adapter.go's stakeAddressFromCredential exactly (same
// lcommon.NewAddressFromParts call with paymentAddr=nil), reimplemented here
// rather than imported: the api package is a much larger dependency edge
// (importing it would pull the whole Blockfrost adapter surface into
// koiosparity) for one 12-line helper, and koios-parity must not gain an HTTP
// client for the Dingo side of any comparison regardless of the underlying
// address logic being identical — see this file's/ARCHITECTURE.md's "never
// add an HTTP client for the Dingo side" invariant.
func StakeAddressFromCredential(
	stakingKey []byte,
	credentialTag uint8,
) (string, error) {
	addrType := uint8(lcommon.AddressTypeNoneKey)
	switch credentialTag {
	case 0:
		// key hash; addrType already set above.
	case 1:
		addrType = lcommon.AddressTypeNoneScript
	default:
		return "", fmt.Errorf(
			"unsupported stake credential tag: %d",
			credentialTag,
		)
	}
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		nil,
		stakingKey,
	)
	if err != nil {
		return "", fmt.Errorf("build stake address: %w", err)
	}
	return addr.String(), nil
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
		return "", fmt.Errorf(
			"pool key hash: expected 28 bytes, got %d",
			len(b),
		)
	}
	var pid lcommon.PoolId
	copy(pid[:], b)
	return pid.String(), nil
}
