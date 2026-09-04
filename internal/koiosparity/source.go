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

package koiosparity

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
)

// RewardParitySource is the read-only view of Dingo's committed reward state
// that the parity checker needs, independent of how that view is obtained.
//
// DingoDB (dingo_db.go) implements this by opening its own read-only SQL
// connection to a separate metadata.sqlite/postgres/mysql instance — the
// shipped, standalone-CLI design (dingo #2684). DatabaseSource (this file)
// implements it by reading directly from a live, in-process
// *database.Database via its existing typed MetadataStore accessors — the
// dingo #3098 in-process observer's narrow "reward-parity source" adapter,
// built entirely from already-committed reward-calculation state
// (reward_pool_input, reward_pool_output, reward_stake_input,
// reward_account_output, epoch_summary, reward_ada_pots) with no export, no
// second Dingo sync, and no new permanent parity-only table.
//
// checkEpoch/CheckEpoch operate purely against this interface, so the
// comparison logic in check.go/compare.go is identical regardless of which
// implementation backs a given run.
//
// GetRewardAccountOutputs is included even though today's comparisons
// (compare.go) are pool-level only — issue #3097 (per-account exact parity)
// needs the full committed per-account view, and this interface is the
// place that decision has to be made once, for both implementations, so a
// later per-account comparison does not need a second, incompatible source
// abstraction.
type RewardParitySource interface {
	// GetLatestEpoch returns the highest epoch number Dingo has committed an
	// epoch_summary row for (the node's own current/most-recent epoch, not
	// necessarily "safely closed" — callers needing a closed epoch should
	// use GetLatestEpoch()-1 or the epoch named by an
	// event.EpochTransitionEvent's PreviousEpoch field instead).
	GetLatestEpoch(ctx context.Context) (uint64, error)
	// GetEpochData returns epoch-level aggregates for the given epoch, or
	// nil, nil when Dingo has not yet recorded a ready epoch_summary row for
	// it (including when the row was pruned — see DatabaseSource's doc
	// comment on core-mode retention).
	GetEpochData(ctx context.Context, epoch uint64) (*DingoEpochData, error)
	// GetPoolEpochDataMap returns per-pool reward data assembled for Koios
	// reporting epoch koiosEpoch — see DingoDB.GetPoolEpochDataMap's doc
	// comment for the stakeEpoch/paramEpoch derivation every implementation
	// must honor identically.
	GetPoolEpochDataMap(
		ctx context.Context,
		stakeEpoch, paramEpoch uint64,
	) (map[string]*DingoPoolEpochData, error)
	// GetPoolStakeSnapshotMembers returns the set of pool key hashes (hex)
	// present in the mark pool_stake_snapshot for epoch, which is written on
	// every epoch transition regardless of reward-input availability (see
	// ledger/snapshot/rotation.go's saveSnapshotInTxn). It is therefore the
	// per-pool evidence of whether a pool was still in the pool set at that
	// epoch, which epoch_summary.SnapshotReady cannot provide: that flag is
	// epoch-level and is set even when the whole reward-input bundle was
	// skipped, and buildRewardStateInputs deliberately omits a degraded
	// active pool from reward_pool_input while keeping it here.
	GetPoolStakeSnapshotMembers(
		ctx context.Context,
		epoch uint64,
	) (map[string]struct{}, error)
	// GetRewardAccountOutputs returns every per-account reward calculation
	// output row Dingo committed for epoch. Not yet consumed by any
	// comparison (that is #3097's scope); exposed now so the source
	// abstraction does not have to be revisited to add it later.
	GetRewardAccountOutputs(
		ctx context.Context,
		epoch uint64,
	) ([]*models.RewardAccountOutput, error)
}

var (
	_ RewardParitySource = (*DingoDB)(nil)
	_ RewardParitySource = (*DatabaseSource)(nil)
)

// DatabaseSource is the dingo #3098 in-process reward-parity source: it reads
// Dingo's committed reward-calculation state directly from a live, running
// *database.Database via read-only transactions against the existing
// MetadataStore accessors (GetEpochSummary, GetRewardAdaPots,
// GetRewardPoolInputs, GetRewardPoolOutputs, GetRewardAccountOutputs) — the
// same tables reward_calculation.go/ledger/snapshot's rotation.go already
// populate at every epoch boundary. It opens no second database connection,
// requires no export step, and adds no new table.
//
// Core-mode pruning: ledger/snapshot/rotation.go's cleanupOldSnapshots keeps
// reward_pool_input/reward_pool_output/reward_account_output for the current
// epoch and the three that precede it (a rolling 4-epoch window; API storage
// mode retains reward_account_output without bound instead). DatabaseSource
// does not race that pruning in any special way — it just reads whatever is
// currently committed, the same as DingoDB would against a separately synced
// copy. What actually satisfies "available before cleanup runs" is
// process-level timing: the in-process observer (observer.go) processes a
// newly closed epoch promptly after its own event.EpochTransitionEvent
// fires, which is many epochs (hours to days on preview/preprod) before that
// epoch's rows would fall out of the retention window. A GetEpochData or
// GetPoolEpochDataMap call made long after an epoch's data has aged out of
// that window reads back as absent (nil / *Present == false) — the same
// signal DingoDB already reports for "not yet computed" — not as an error;
// it is the caller's responsibility (the observer, or an operator invoking
// this source directly) to read promptly.
type DatabaseSource struct {
	db *database.Database
}

// NewDatabaseSource wraps an already-open, in-process *database.Database as
// a RewardParitySource. db must not be nil.
func NewDatabaseSource(db *database.Database) (*DatabaseSource, error) {
	if db == nil {
		return nil, errors.New(
			"koiosparity: DatabaseSource requires a non-nil database",
		)
	}
	return &DatabaseSource{db: db}, nil
}

// GetLatestEpoch returns the highest epoch number Dingo has committed an
// epoch_summary row for.
func (s *DatabaseSource) GetLatestEpoch(ctx context.Context) (uint64, error) {
	// database.Txn/the metadata store's MetadataStore accessors take no
	// context.Context of their own (types.Txn is Commit/Rollback only, and
	// every GetX(..., types.Txn) signature in
	// database/plugin/metadata/store.go is context-free) — there is no
	// context-aware transaction/accessor option to thread ctx into here, so
	// a query already in flight cannot be interrupted mid-call. What this
	// check does provide: bailing out before opening a new transaction if
	// ctx is already done, so an Observer.Stop-driven shutdown racing this
	// call doesn't start fresh DB work only to discard the result.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	txn := s.db.Transaction(false)
	defer txn.Release()
	summary, err := s.db.Metadata().GetLatestEpochSummary(txn.Metadata())
	if err != nil {
		return 0, fmt.Errorf("get latest epoch summary: %w", err)
	}
	if summary == nil {
		return 0, errors.New("koiosparity: no epoch_summary rows found")
	}
	return summary.Epoch, nil
}

// GetEpochData returns epoch-level aggregates for the given epoch, or nil,
// nil if Dingo has no ready epoch_summary row for it — see
// RewardParitySource's doc comment.
func (s *DatabaseSource) GetEpochData(
	ctx context.Context,
	epoch uint64,
) (*DingoEpochData, error) {
	// See GetLatestEpoch's comment: no context-aware transaction/accessor
	// exists to thread ctx into further, so this only guards against
	// starting new work after ctx is already done.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.db.Transaction(false)
	defer txn.Release()
	meta := s.db.Metadata()

	summary, err := meta.GetEpochSummary(epoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("epoch_summary epoch %d: %w", epoch, err)
	}
	if summary == nil || !summary.SnapshotReady {
		// SnapshotReady == false: a partial/placeholder row Dingo will
		// repair later — treat identically to "not yet ready", matching
		// DingoDB.GetEpochData.
		return nil, nil
	}

	data := &DingoEpochData{
		TotalActiveStake: strconv.FormatUint(
			uint64(summary.TotalActiveStake),
			10,
		),
		TotalPoolCount: summary.TotalPoolCount,
	}

	pots, err := meta.GetRewardAdaPots(epoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("reward_ada_pots epoch %d: %w", epoch, err)
	}
	if pots != nil {
		data.Fees = strconv.FormatUint(uint64(pots.Fees), 10)
		data.TotalRewards = strconv.FormatUint(uint64(pots.Rewards), 10)
		data.Treasury = strconv.FormatUint(uint64(pots.Treasury), 10)
		data.Reserves = strconv.FormatUint(uint64(pots.Reserves), 10)
		data.RewardAdaPotsPresent = true
	}
	return data, nil
}

// GetPoolEpochDataMap returns per-pool reward data assembled for Koios
// reporting epoch koiosEpoch — see DingoDB.GetPoolEpochDataMap's doc comment
// for the stakeEpoch/paramEpoch derivation this mirrors exactly.
// GetPoolStakeSnapshotMembers implements RewardParitySource by reading the
// mark pool_stake_snapshot rows for epoch.
// snapshotTypeMark is the pool_stake_snapshot/reward_snapshot type the parity
// comparison reads; Dingo writes the boundary capture under this name.
const snapshotTypeMark = "mark"

func (s *DatabaseSource) GetPoolStakeSnapshotMembers(
	ctx context.Context,
	epoch uint64,
) (map[string]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.db.Transaction(false)
	defer txn.Release()
	rows, err := s.db.Metadata().GetPoolStakeSnapshotsByEpoch(
		epoch,
		snapshotTypeMark,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"pool_stake_snapshot epoch %d: %w",
			epoch,
			err,
		)
	}
	members := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		members[hex.EncodeToString(row.PoolKeyHash)] = struct{}{}
	}
	return members, nil
}

func (s *DatabaseSource) GetPoolEpochDataMap(
	ctx context.Context,
	stakeEpoch, paramEpoch uint64,
) (map[string]*DingoPoolEpochData, error) {
	// See GetLatestEpoch's comment: no context-aware transaction/accessor
	// exists to thread ctx into further, so this only guards against
	// starting new work after ctx is already done.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.db.Transaction(false)
	defer txn.Release()
	meta := s.db.Metadata()

	var tipSlot uint64
	tipKnown := false
	tip, tipErr := s.db.GetTip(txn)
	if tipErr != nil {
		return nil, fmt.Errorf("tip lookup: %w", tipErr)
	}
	if len(tip.Point.Hash) > 0 {
		tipSlot = tip.Point.Slot
		tipKnown = true
	}

	epochRewardsPending := false
	if tipKnown {
		applyEpoch, err := meta.GetEpoch(stakeEpoch+3, txn.Metadata())
		if err != nil {
			return nil, fmt.Errorf(
				"epoch lookup %d: %w", stakeEpoch+3, err,
			)
		}
		if applyEpoch == nil {
			epochRewardsPending = true
		} else {
			epochRewardsPending = tipSlot < applyEpoch.StartSlot
		}
	}

	stakeInputs, err := meta.GetRewardPoolInputs(stakeEpoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"reward_pool_input stake epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	m := make(map[string]*DingoPoolEpochData, len(stakeInputs))
	for _, inp := range stakeInputs {
		data := &DingoPoolEpochData{
			StakePresent:   true,
			DelegatedStake: strconv.FormatUint(uint64(inp.DelegatedStake), 10),
			DelegatorCount: inp.DelegatorCount,
			FixedCost:      strconv.FormatUint(uint64(inp.Cost), 10),
		}
		if inp.Margin != nil && inp.Margin.Rat != nil {
			data.Margin = inp.Margin.String()
		}
		m[hex.EncodeToString(inp.PoolKeyHash)] = data
	}

	paramInputs, err := meta.GetRewardPoolInputs(paramEpoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"reward_pool_input param epoch %d: %w",
			paramEpoch,
			err,
		)
	}
	for _, inp := range paramInputs {
		key := hex.EncodeToString(inp.PoolKeyHash)
		data, ok := m[key]
		if !ok {
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.ParamsPresent = true
		if inp.BlocksProduced != nil {
			data.BlocksProduced = *inp.BlocksProduced
		}
	}

	outputs, err := meta.GetRewardPoolOutputs(stakeEpoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"reward_pool_output epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	for _, out := range outputs {
		key := hex.EncodeToString(out.PoolKeyHash)
		data, ok := m[key]
		if !ok {
			data = &DingoPoolEpochData{}
			m[key] = data
		}
		data.MemberRewardPresent = true
		data.MemberRewardTotal = strconv.FormatUint(
			uint64(out.MemberRewardTotal),
			10,
		)
		data.PoolUnspendable = uint64(out.Unspendable)
		data.RewardsPending = tipKnown && tipSlot < out.BoundarySlot
	}

	// The comparable member-reward quantity, formed the same way DingoDB
	// forms it — see DingoDB.addSpendableMemberRewards for why
	// reward_pool_output.member_reward_total is not it, and why presence is
	// established epoch-wide rather than per pool.
	accountOutputs, err := meta.GetRewardAccountOutputs(
		stakeEpoch,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"reward_account_output epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	if len(accountOutputs) > 0 {
		totals := make(map[string]uint64, len(m))
		for _, out := range accountOutputs {
			if out == nil || out.RewardType != rewardTypeMember {
				continue
			}
			// applyStakeRewards credits a reward only when it is spendable
			// and not guarded by CIP-0163 expiry; anything else was computed
			// and withheld, and Koios never reports it.
			if !out.Spendable || out.Guarded {
				continue
			}
			totals[hex.EncodeToString(out.PoolKeyHash)] += uint64(out.Amount)
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
	}
	if epochRewardsPending {
		for _, data := range m {
			if !data.MemberRewardPresent {
				data.RewardsPending = true
			}
		}
	}
	return m, nil
}

// GetRewardAccountOutputs returns every per-account reward calculation
// output row Dingo committed for epoch, straight from reward_account_output
// — the same committed state #3097's per-account comparison will consume.
func (s *DatabaseSource) GetRewardAccountOutputs(
	ctx context.Context,
	epoch uint64,
) ([]*models.RewardAccountOutput, error) {
	// See GetLatestEpoch's comment: no context-aware transaction/accessor
	// exists to thread ctx into further, so this only guards against
	// starting new work after ctx is already done.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.db.Transaction(false)
	defer txn.Release()
	rows, err := s.db.Metadata().GetRewardAccountOutputs(epoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("reward_account_output epoch %d: %w", epoch, err)
	}
	return rows, nil
}
