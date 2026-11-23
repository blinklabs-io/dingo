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
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
)

// RewardParitySource is the read-only view of Dingo's committed reward state
// that the parity checker needs, independent of how that view is obtained.
//
// DingoDB (dingo_db.go) implements this by opening its own read-only SQL
// connection to a separate metadata.sqlite/postgres/mysql instance — the
// shipped, standalone-CLI design. DatabaseSource (this file)
// implements it by reading directly from a live, in-process
// *database.Database via its existing typed MetadataStore accessors — the
// in-process observer's narrow "reward-parity source" adapter,
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
// (compare.go) are pool-level only — the per-account exact-parity comparison
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
	// it.
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
	// GetPoolsRetiredByEpoch returns the set of pool key hashes (hex) whose
	// effective retirement, resolved as of boundarySlot, takes effect at or
	// before epoch. It is the second, independent route to the departure
	// proof GetPoolStakeSnapshotMembers provides: pool_stake_snapshot is
	// pruned to currentEpoch-3, while pool_registration/pool_retirement are
	// retained for the life of the database, so an observer trailing the
	// node has only this one left.
	//
	// Membership in this set is positive per-pool evidence rather than
	// absence from a set, so it needs no completeness argument. Both
	// implementations must apply the same cancellation rule
	// MetadataStore.GetPoolKeyHashesRetiredByEpoch documents: a registration
	// filed after the retirement puts the pool back, and treating a bare
	// "retirement certificate exists" as departure would downgrade a real
	// missing-input ERROR to informational.
	GetPoolsRetiredByEpoch(
		ctx context.Context,
		epoch uint64,
		boundarySlot uint64,
	) (map[string]struct{}, error)
	// GetRewardSnapshot returns the mark reward_snapshot completeness fields
	// for epoch, or nil, nil when no such row exists. checkEpoch uses it to
	// prove the K+1 reward-input set is the network's complete
	// positive-stake pool set: reward_snapshot.TotalPoolCount
	// is written by ledger/snapshot/rotation.go's buildRewardStateInputs
	// from exactly the set reward_pool_input holds rows for, unlike
	// epoch_summary.TotalPoolCount (GetEpochData/DingoEpochData), which
	// counts every delegated pool regardless of stake. reward_snapshot rows
	// are retained for the life of the database (never pruned by
	// Manager.cleanupOldSnapshots), so this proof survives the same
	// snapshot-retention window that closes GetPoolStakeSnapshotMembers for
	// a trailing observer.
	GetRewardSnapshot(
		ctx context.Context,
		epoch uint64,
	) (*DingoRewardSnapshotSummary, error)
	// GetProtocolParams returns the protocol parameters in force for epoch,
	// resolved from the `pparams` row that actually applies to it and
	// decoded as the era the `epoch` table records for that epoch — see
	// DingoDB.GetProtocolParams for why neither the epoch nor the era can be
	// matched naively. Returns nil, nil when no row resolves, which the
	// comparison reports rather than treating as nothing to compare.
	GetProtocolParams(
		ctx context.Context,
		epoch uint64,
	) (*DingoProtocolParams, error)
	// GetRewardAccountOutputs returns every per-account reward calculation
	// output row Dingo committed for epoch. Not yet consumed by any
	// comparison (that belongs to the per-account comparison); exposed now so
	// the source abstraction does not have to be revisited to add it later.
	GetRewardAccountOutputs(
		ctx context.Context,
		epoch uint64,
	) ([]*models.RewardAccountOutput, error)
	// GetProposalDepositRefunds returns the governance proposal deposits that
	// were credited to a reward account when epoch refundEpoch began: those of
	// proposals enacted or dropped at that boundary whose return account was
	// registered. A deposit returned to the treasury because the account was
	// not registered is absent, as it is from Koios's proposal_refund rows.
	// Koios reporting epoch K pairs with refundEpoch K+1: Koios files a
	// credit made at the K to K+1 boundary under earned_epoch K. See
	// ProposalDepositRefunds.Pending for when the result is still incomplete.
	GetProposalDepositRefunds(
		ctx context.Context,
		refundEpoch uint64,
	) (ProposalDepositRefunds, error)
	// GetEarliestAvailableEpoch returns the earliest Koios reporting epoch
	// this node could plausibly have genuine, locally computed
	// reward-calculation state for, derived from its own Mithril bootstrap
	// boundary. A Mithril-bootstrapped node has no ledger
	// history before that boundary by construction — epochs 0-1 are not the
	// only ones that can never have local data; every epoch through the
	// bootstrap boundary itself is in the same position, regardless of
	// whether Koios (which has full protocol history) has real reference
	// data for them.
	//
	// ok is false when no Mithril boundary is recorded at all (a
	// non-Mithril, genesis-synced node), and when a recorded boundary slot
	// falls inside no epoch the node's own epoch table describes, which
	// names no epoch to bound against — callers must then apply no lower
	// bound beyond the existing preStakingThroughEpoch floor, leaving
	// behavior exactly as it was before this method existed. A boundary
	// that is recorded but unreadable, empty, or otherwise malformed is an
	// error, not ok = false. When ok is true, epoch is the
	// first Koios reporting epoch a caller should ever attempt to compare;
	// every epoch below it should be treated the same way a pre-staking
	// epoch is treated today (a recorded PASS with nothing compared, not a
	// hard mismatch).
	GetEarliestAvailableEpoch(
		ctx context.Context,
	) (epoch uint64, ok bool, err error)
}

var (
	_ RewardParitySource = (*DingoDB)(nil)
	_ RewardParitySource = (*DatabaseSource)(nil)
)

// DatabaseSource is the in-process reward-parity source: it reads
// Dingo's committed reward-calculation state directly from a live, running
// *database.Database via read-only transactions against the existing
// MetadataStore accessors (GetEpochSummary, GetRewardAdaPots,
// GetRewardPoolInputs, GetRewardPoolOutputs, GetRewardAccountOutputs) — the
// same tables reward_calculation.go/ledger/snapshot's rotation.go already
// populate at every epoch boundary. It opens no second database connection,
// requires no export step, and adds no new table.
//
// Retention: the LedgerDB keeps reward_pool_output, reward_account_output,
// reward_stake_input and pool_stake_snapshot only for the window
// snapshot.RewardStateRetainedFrom names. Reward history older than that is
// API data, held by the IndexerDB. A source built with WithIndexerDB reads
// the reward state from the IndexerDB and can compare any epoch. A source
// reading the ledger alone reports the window through RewardStateRetainedFrom,
// and the check records an epoch whose stake epoch lies below it as
// NOT_CHECKED rather than reading the absent rows as a mismatch.
type DatabaseSource struct {
	db *database.Database
	// ldb is the LedgerDB the ledger state reads go through, so they see
	// what the ledger sees: the live UTxO set's answer in set mode.
	ldb ledgerdb.LedgerDB
	// rewards is what the reward state reads go through: ldb, or the
	// IndexerDB set by WithIndexerDB.
	rewards ledgerdb.LedgerDB
	// history is the database the transaction sample reads: db itself, or
	// the IndexerDB set by WithIndexerDB.
	history *database.Database
	// historyTrails records that history and rewards are filled behind db,
	// so a closed epoch is read only once history's tip covers it.
	historyTrails bool
	// retainedFrom is the highest LedgerDB retention floor read so far. A
	// rollback can move the tip back across a boundary, but it restores no
	// pruned rows, so the floor never moves down.
	retainedFrom atomic.Uint64
}

// NewDatabaseSource wraps an already-open, in-process *database.Database as
// a RewardParitySource. db must not be nil.
func NewDatabaseSource(db *database.Database) (*DatabaseSource, error) {
	if db == nil {
		return nil, errors.New(
			"koiosparity: DatabaseSource requires a non-nil database",
		)
	}
	return NewDatabaseSourceWithLedgerDB(db, dbadapter.New(db))
}

// NewDatabaseSourceWithLedgerDB is NewDatabaseSource reading ledger state
// through ldb, the LedgerDB the node's ledger reads through, rather than the
// metadata store directly. Neither may be nil.
func NewDatabaseSourceWithLedgerDB(
	db *database.Database,
	ldb ledgerdb.LedgerDB,
) (*DatabaseSource, error) {
	if db == nil || ldb == nil {
		return nil, errors.New(
			"koiosparity: DatabaseSource requires a non-nil database and LedgerDB",
		)
	}
	return &DatabaseSource{db: db, ldb: ldb, rewards: ldb, history: db}, nil
}

// WithIndexerDB makes the reward state and transaction sample reads go to
// indexer, the IndexerDB of an API node, which holds them for every epoch;
// the LedgerDB keeps the reward state only for its own window and holds no
// transaction history. Reads of current ledger state (tip, epochs, protocol
// parameters, pool certificates, governance) stay on the ledger. The
// IndexerDB is filled asynchronously, so a read of an epoch its tip has not
// passed is ErrRewardHistoryPending or ErrTransactionHistoryPending. A nil
// indexer leaves the source unchanged.
func (s *DatabaseSource) WithIndexerDB(
	indexer *database.Database,
) *DatabaseSource {
	if indexer == nil {
		return s
	}
	s.history = indexer
	s.rewards = dbadapter.New(indexer)
	s.historyTrails = true
	return s
}

// ErrRewardHistoryPending reports that the IndexerDB the reward state reads
// has not applied the boundary that closes the epoch yet. The observer defers
// the epoch (Observer.deferForHistory) rather than failing it.
var ErrRewardHistoryPending = errors.New(
	"koiosparity: reward history has not reached the end of the epoch",
)

// RewardStateRetainedFrom returns the first stake epoch whose reward outputs
// the source still holds. bounded is false for a source reading the
// IndexerDB, which holds every epoch. Otherwise the floor follows the
// ledger's tip, which is never behind the boundary the LedgerDB last pruned
// at, so every epoch at or above the floor is still held.
func (s *DatabaseSource) RewardStateRetainedFrom(
	ctx context.Context,
) (epoch uint64, bounded bool, err error) {
	if s.historyTrails {
		return 0, false, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	txn := s.ldb.Begin(ctx, false)
	defer txn.Release()
	tip, err := s.ldb.GetTip(txn)
	if err != nil {
		return 0, false, fmt.Errorf("tip lookup: %w", err)
	}
	if len(tip.Point.Hash) > 0 {
		tipEpoch, err := s.ldb.GetEpochBySlot(tip.Point.Slot, txn)
		if err != nil {
			return 0, false, fmt.Errorf(
				"epoch for tip slot %d: %w", tip.Point.Slot, err,
			)
		}
		if tipEpoch != nil {
			floor := snapshot.RewardStateRetainedFrom(tipEpoch.EpochId)
			for {
				prev := s.retainedFrom.Load()
				if floor <= prev ||
					s.retainedFrom.CompareAndSwap(prev, floor) {
					break
				}
			}
		}
	}
	return s.retainedFrom.Load(), true, nil
}

// rewardHistoryReady returns ErrRewardHistoryPending until the IndexerDB the
// reward state reads has applied the boundary into the epoch after epoch:
// the reward inputs a check of epoch reads are captured there.
func (s *DatabaseSource) rewardHistoryReady(
	ctx context.Context,
	epoch uint64,
) error {
	if !s.historyTrails {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ltxn := s.ldb.Begin(ctx, false)
	defer ltxn.Release()
	next, err := s.ldb.GetEpoch(epoch+1, ltxn)
	if err != nil {
		return fmt.Errorf("epoch lookup %d: %w", epoch+1, err)
	}
	if next == nil {
		return fmt.Errorf(
			"%w: epoch %d has not closed", ErrRewardHistoryPending, epoch,
		)
	}
	txn := s.history.Transaction(ctx, false)
	defer txn.Release()
	tip, err := s.history.GetTip(txn)
	if err != nil {
		return fmt.Errorf("epoch %d reward history tip: %w", epoch, err)
	}
	if len(tip.Point.Hash) == 0 || tip.Point.Slot < next.StartSlot {
		return fmt.Errorf(
			"%w: epoch %d closes at slot %d, reward history tip is at slot %d",
			ErrRewardHistoryPending,
			epoch, next.StartSlot, tip.Point.Slot,
		)
	}
	return nil
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
	txn := s.ldb.Begin(ctx, false)
	defer txn.Release()
	summary, err := s.ldb.GetLatestEpochSummary(txn)
	if err != nil {
		return 0, fmt.Errorf("get latest epoch summary: %w", err)
	}
	if summary == nil {
		return 0, errors.New("koiosparity: no epoch_summary rows found")
	}
	return summary.Epoch, nil
}

// GetEarliestAvailableEpoch implements RewardParitySource by resolving this
// node's own Mithril bootstrap boundary (the mithril_ledger_slot sync-state
// key mithril/sync_import.go writes at import time, surfaced at the
// database-package level as MithrilTrustBoundarySlotStrict/GetEpochBySlot)
// into the first Koios reporting epoch this node could
// plausibly have genuinely computed local reward state for: one past the
// epoch that slot falls in, since the epoch containing (and every epoch
// before) the boundary slot was inherited from the Mithril snapshot rather
// than computed by this node's own epoch-transition reward calculation.
//
// MithrilTrustBoundarySlotStrict, not MithrilTrustBoundarySlot: a boundary
// that exists but cannot be read or parsed — including one recorded with an
// empty value — must surface as an error here rather than as ok = false,
// which callers apply no bound for. Reading a malformed boundary as an
// absent one would restore the earlier unbounded behavior on exactly the
// node whose boundary could not be confirmed.
func (s *DatabaseSource) GetEarliestAvailableEpoch(
	ctx context.Context,
) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	txn := s.db.Transaction(ctx, false)
	defer txn.Release()
	slot, err := s.db.ImportedLedgerStateBoundarySlotStrict(txn)
	if err != nil {
		return 0, false, fmt.Errorf("mithril trust boundary: %w", err)
	}
	if slot == 0 {
		return 0, false, nil
	}
	ltxn := s.ldb.Begin(ctx, false)
	defer ltxn.Release()
	boundaryEpoch, err := s.ldb.GetEpochBySlot(slot, ltxn)
	if err != nil {
		return 0, false, fmt.Errorf(
			"epoch for mithril boundary slot %d: %w",
			slot,
			err,
		)
	}
	if boundaryEpoch == nil {
		return 0, false, nil
	}
	return boundaryEpoch.EpochId + 1, true, nil
}

// ProposalDepositRefunds is what RewardParitySource.GetProposalDepositRefunds
// read for one refund epoch.
type ProposalDepositRefunds struct {
	Refunds []governance.ProposalDepositRefund
	// Pending reports that the tip has not reached the start of the refund
	// epoch, so the boundary that journals these refunds has not been applied
	// and Refunds is not yet final. It is independent of
	// DingoPoolEpochData.RewardsPending: refunds for Koios earned epoch K are
	// journaled when K+1 begins, a full epoch before the member and leader
	// rewards of the same earned epoch are applied. A missing refund-epoch row
	// reads as pending; an unknown tip leaves it false, so the comparison
	// stays strict rather than downgrading a divergence on incomplete
	// metadata.
	Pending bool
}

// GetProposalDepositRefunds implements RewardParitySource from the proposal
// and reward-credit journal tables the epoch boundary wrote.
func (s *DatabaseSource) GetProposalDepositRefunds(
	ctx context.Context,
	refundEpoch uint64,
) (ProposalDepositRefunds, error) {
	var out ProposalDepositRefunds
	// See GetLatestEpoch's comment on why ctx is only checked up front.
	if err := ctx.Err(); err != nil {
		return out, err
	}
	txn := s.ldb.Begin(ctx, false)
	defer txn.Release()
	tip, err := s.ldb.GetTip(txn)
	if err != nil {
		return out, fmt.Errorf("tip lookup: %w", err)
	}
	// As in GetPoolEpochDataMap, a genuine tip always carries a block hash.
	if len(tip.Point.Hash) > 0 && tip.Point.Slot > 0 {
		epoch, err := s.ldb.GetEpoch(refundEpoch, txn)
		if err != nil {
			return out, fmt.Errorf("epoch lookup %d: %w", refundEpoch, err)
		}
		out.Pending = epoch == nil || tip.Point.Slot < epoch.StartSlot
	}
	proposals, err := s.ldb.GetGovernanceProposalsRefundedAt(
		refundEpoch,
		txn,
	)
	if err != nil {
		return out, fmt.Errorf(
			"governance proposals refunded at epoch %d: %w",
			refundEpoch,
			err,
		)
	}
	var credits []*models.AccountRewardDelta
	for _, slot := range governance.ProposalDepositRefundSlots(
		proposals,
		refundEpoch,
	) {
		slotCredits, err := s.ldb.GetPostSnapshotRewardCreditEvents(
			slot,
			txn,
		)
		if err != nil {
			return out, fmt.Errorf(
				"reward credits at slot %d: %w",
				slot,
				err,
			)
		}
		credits = append(credits, slotCredits...)
	}
	out.Refunds, err = governance.MatchProposalDepositRefunds(
		proposals,
		refundEpoch,
		credits,
	)
	return out, err
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
	txn := s.rewards.Begin(ctx, false)
	defer txn.Release()

	summary, err := s.rewards.GetEpochSummary(epoch, txn)
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
		BoundarySlot:   summary.BoundarySlot,
	}

	pots, err := s.rewards.GetRewardAdaPots(epoch, txn)
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
	txn := s.rewards.Begin(ctx, false)
	defer txn.Release()
	rows, err := s.rewards.GetPoolStakeSnapshotsByEpoch(
		epoch,
		snapshotTypeMark,
		txn,
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

// GetPoolsRetiredByEpoch implements RewardParitySource through the metadata
// store's own GetPoolKeyHashesRetiredByEpoch, so the in-process observer and
// the standalone CLI resolve departure from one shared query rather than two
// copies of the certificate-ordering rules.
func (s *DatabaseSource) GetPoolsRetiredByEpoch(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
) (map[string]struct{}, error) {
	// See GetLatestEpoch's comment: no context-aware transaction/accessor
	// exists to thread ctx into further, so this only guards against
	// starting new work after ctx is already done.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.ldb.Begin(ctx, false)
	defer txn.Release()
	keyHashes, err := s.ldb.GetPoolKeyHashesRetiredByEpoch(
		epoch,
		boundarySlot,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"pool_retirement through epoch %d: %w",
			epoch,
			err,
		)
	}
	retired := make(map[string]struct{}, len(keyHashes))
	for _, keyHash := range keyHashes {
		retired[hex.EncodeToString(keyHash)] = struct{}{}
	}
	return retired, nil
}

// GetRewardSnapshot implements RewardParitySource by reading the mark
// reward_snapshot row for epoch directly through the metadata store's own
// GetRewardSnapshot -- see RewardParitySource's doc comment for why this
// table, not epoch_summary, is the source of the K+1 completeness proof.
func (s *DatabaseSource) GetRewardSnapshot(
	ctx context.Context,
	epoch uint64,
) (*DingoRewardSnapshotSummary, error) {
	// See GetLatestEpoch's comment: no context-aware transaction/accessor
	// exists to thread ctx into further, so this only guards against
	// starting new work after ctx is already done.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.rewards.Begin(ctx, false)
	defer txn.Release()
	row, err := s.rewards.GetRewardSnapshot(
		epoch,
		snapshotTypeMark,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("reward_snapshot epoch %d: %w", epoch, err)
	}
	if row == nil {
		return nil, nil
	}
	out := &DingoRewardSnapshotSummary{
		TotalPoolCount: row.TotalPoolCount,
	}
	if row.ExcludedActiveStake != nil {
		out.ExcludedActiveStake = uint64(*row.ExcludedActiveStake)
		out.ExcludedActiveStakeKnown = true
	}
	return out, nil
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
	txn := s.rewards.Begin(ctx, false)
	defer txn.Release()

	// The tip decides whether this stake epoch's rewards have been applied
	// yet; see DingoPoolEpochData.RewardsPending. A read failure is reported
	// rather than guessed at, and an empty tip leaves tipKnown false, so the
	// comparison stays strict rather than downgrading a possible divergence on
	// incomplete metadata.
	var tipSlot uint64
	tipKnown := false
	tip, tipErr := s.rewards.GetTip(txn)
	if tipErr != nil {
		return nil, fmt.Errorf("tip lookup: %w", tipErr)
	}
	// GetTip reports sql.ErrNoRows as a zero Tip with a nil error, so a nil
	// error alone would accept slot 0 as a real tip and mark every epoch
	// pending. A genuine tip always carries a block hash.
	if len(tip.Point.Hash) > 0 && tip.Point.Slot > 0 {
		tipSlot = tip.Point.Slot
		tipKnown = true
	}

	epochRewardsPending := false
	if tipKnown {
		applyEpoch, err := s.rewards.GetEpoch(stakeEpoch+3, txn)
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

	stakeInputs, err := s.rewards.GetRewardPoolInputs(stakeEpoch, txn)
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

	paramInputs, err := s.rewards.GetRewardPoolInputs(paramEpoch, txn)
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

	outputs, err := s.rewards.GetRewardPoolOutputs(stakeEpoch, txn)
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
	accountOutputs, err := s.rewards.GetRewardAccountOutputs(
		stakeEpoch,
		txn,
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

// GetProtocolParams implements RewardParitySource against the live,
// in-process metadata store. It mirrors DingoDB.GetProtocolParams exactly:
// resolve the epoch's era from the `epoch` row first, then let
// MetadataStore.GetPParams pick the latest parameter row at or before the
// epoch within that era (its query is already `epoch <= ? AND era_id = ?`
// ordered newest-first), then decode with that era's decoder. Both reads run
// under one read transaction so a rollover committing between them cannot
// hand back a row whose era disagrees with the era that selected it.
func (s *DatabaseSource) GetProtocolParams(
	ctx context.Context,
	epoch uint64,
) (*DingoProtocolParams, error) {
	// See GetLatestEpoch's comment: no context-aware transaction/accessor
	// exists to thread ctx into further, so this only guards against
	// starting new work after ctx is already done.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.ldb.Begin(ctx, false)
	defer txn.Release()

	epochRow, err := s.ldb.GetEpoch(epoch, txn)
	if err != nil {
		return nil, fmt.Errorf("epoch %d: %w", epoch, err)
	}
	if epochRow == nil {
		return nil, nil
	}
	rows, err := s.ldb.GetPParamsRows(epoch, epochRow.EraId, txn)
	if err != nil {
		return nil, fmt.Errorf("pparams epoch %d: %w", epoch, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	out, err := decodeProtocolParams(
		rows[0].Cbor,
		epochRow.EraId,
		rows[0].Epoch,
	)
	if err != nil {
		return nil, err
	}
	// See isSyntheticV2CostModel's doc comment:
	// the durable cleared-epoch marker is the same one
	// ledger.queryShelleyCurrentProtocolParams reads for its own historical
	// path, read here directly via the database package rather than
	// DingoDB's duplicated raw-SQL copy since this source already holds a
	// live *database.Database.
	syncTxn := s.db.Transaction(ctx, false)
	defer syncTxn.Release()
	clearedEpoch, cleared, err := database.SyntheticV2CostModelClearedEpoch(
		s.db, syncTxn,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"synthetic v2 cost model cleared epoch: %w", err,
		)
	}
	v2, hasV2 := out.CostModels["PlutusV2"]
	out.SyntheticV2CostModel = isSyntheticV2CostModel(
		v2, hasV2, epoch, clearedEpoch, cleared,
	)
	return out, nil
}

// GetRewardAccountOutputs returns every per-account reward calculation
// output row Dingo committed for epoch, straight from reward_account_output
// — the same committed state the per-account comparison will consume.
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
	txn := s.rewards.Begin(ctx, false)
	defer txn.Release()
	rows, err := s.rewards.GetRewardAccountOutputs(epoch, txn)
	if err != nil {
		return nil, fmt.Errorf("reward_account_output epoch %d: %w", epoch, err)
	}
	return rows, nil
}
