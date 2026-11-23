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

package ledger

import (
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

// rewardSnapshotTypes are the snapshot types a reward snapshot header or seed
// failure can carry.
var rewardSnapshotTypes = []string{
	models.PoolStakeSnapshotTypeMark,
	models.PoolStakeSnapshotTypeSet,
	models.PoolStakeSnapshotTypeGo,
	models.PoolStakeSnapshotTypeActive,
}

// rewardInputsExport is the reward inputs view an export reads, with the
// epochs the set holds rows of.
type rewardInputsExport struct {
	view   rewardInputsView
	epochs []uint64
	pots   []uint64
}

func (ls *LedgerState) rewardInputsExport() (rewardInputsExport, error) {
	r := ls.rewardInputsReader(nil)
	if r == nil {
		return rewardInputsExport{}, ledgerStateUnserved(
			nodesettings.FamilyRewardInputs,
		)
	}
	snaps, pots, err := r.RewardInputEpochs()
	if err != nil {
		return rewardInputsExport{}, err
	}
	epochs := slices.Clone(snaps)
	slices.Sort(epochs)
	return rewardInputsExport{
		view:   ls.newRewardInputsView(r, nil),
		epochs: slices.Compact(epochs),
		pots:   pots,
	}, nil
}

// optUint64Text is a nullable Uint64 column.
func optUint64Text(v *types.Uint64) any {
	if v == nil {
		return nil
	}
	return uint64Text(*v)
}

func optSQLInt(v *uint64) any {
	if v == nil {
		return nil
	}
	return sqlInt(*v)
}

func optRatText(r *types.Rat) any {
	if r == nil {
		return nil
	}
	v, _ := r.Value()
	return v
}

func (ls *LedgerState) rewardAdaPotsFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	ex, err := ls.rewardInputsExport()
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "treasury", "reserves", "fees", "rewards", "captured_slot",
		"imported_epoch_fees",
	}}
	for _, epoch := range sortedUnique(ex.pots) {
		p, err := ex.view.pots(epoch)
		if err != nil {
			return metadata.FactRows{}, err
		}
		if p == nil || !inSlotRange(p.CapturedSlot, from, through) {
			continue
		}
		out.Rows = append(out.Rows, []any{
			sqlInt(p.Epoch), uint64Text(p.Treasury), uint64Text(p.Reserves),
			uint64Text(p.Fees), uint64Text(p.Rewards), sqlInt(p.CapturedSlot),
			optUint64Text(p.ImportedEpochFees),
		})
	}
	return out, nil
}

func sortedUnique(v []uint64) []uint64 {
	out := slices.Clone(v)
	slices.Sort(out)
	return slices.Compact(out)
}

func (ls *LedgerState) rewardSnapshotFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	ex, err := ls.rewardInputsExport()
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "snapshot_type", "total_active_stake", "total_pool_count",
		"total_delegators", "captured_slot", "boundary_slot", "epoch_nonce",
		"protocol_version", "authoritative", "calculation_version",
		"excluded_active_stake",
	}}
	for _, epoch := range ex.epochs {
		for _, typ := range rewardSnapshotTypes {
			s, err := ex.view.snapshot(epoch, typ)
			if err != nil {
				return metadata.FactRows{}, err
			}
			if s == nil || !inSlotRange(s.CapturedSlot, from, through) {
				continue
			}
			out.Rows = append(out.Rows, []any{
				sqlInt(s.Epoch), s.SnapshotType,
				uint64Text(s.TotalActiveStake), sqlInt(s.TotalPoolCount),
				sqlInt(s.TotalDelegators), sqlInt(s.CapturedSlot),
				sqlInt(s.BoundarySlot), nilBytes(s.EpochNonce),
				sqlInt(s.ProtocolVersion), boolInt(s.Authoritative),
				sqlInt(s.CalculationVersion),
				optUint64Text(s.ExcludedActiveStake),
			})
		}
	}
	return out, nil
}

func (ls *LedgerState) rewardSeedFailureFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r := ls.rewardInputsReader(nil)
	if r == nil {
		return metadata.FactRows{}, ledgerStateUnserved(
			nodesettings.FamilyRewardInputs,
		)
	}
	ex, err := ls.rewardInputsExport()
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "snapshot_type", "failure_reason", "captured_slot",
	}}
	for _, epoch := range ex.epochs {
		for _, typ := range rewardSnapshotTypes {
			reason, slot, err := r.RewardFailureRow(epoch, typ, ex.view.at)
			if err != nil {
				return metadata.FactRows{}, err
			}
			if reason == "" || !inSlotRange(slot, from, through) {
				continue
			}
			out.Rows = append(out.Rows, []any{
				sqlInt(epoch), typ, reason, sqlInt(slot),
			})
		}
	}
	return out, nil
}

func (ls *LedgerState) rewardPoolInputFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	ex, err := ls.rewardInputsExport()
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"margin", "pool_key_hash", "reward_account", "blocks_produced",
		"total_blocks_in_epoch", "epoch", "pledge", "delegated_stake",
		"owner_stake", "cost", "delegator_count",
		"reward_account_credential_tag", "captured_slot", "boundary_slot",
	}}
	for _, epoch := range ex.epochs {
		pools, err := ex.view.pools(epoch)
		if err != nil {
			return metadata.FactRows{}, err
		}
		for _, p := range pools {
			if !inSlotRange(p.CapturedSlot, from, through) {
				continue
			}
			out.Rows = append(out.Rows, []any{
				optRatText(p.Margin), nilBytes(p.PoolKeyHash),
				nilBytes(p.RewardAccount), optSQLInt(p.BlocksProduced),
				optSQLInt(p.TotalBlocksInEpoch), sqlInt(p.Epoch),
				uint64Text(p.Pledge), uint64Text(p.DelegatedStake),
				uint64Text(p.OwnerStake), uint64Text(p.Cost),
				sqlInt(p.DelegatorCount),
				sqlInt(uint64(p.RewardAccountCredentialTag)),
				sqlInt(p.CapturedSlot), sqlInt(p.BoundarySlot),
			})
		}
	}
	return out, nil
}

var rewardStakeInputColumns = []string{
	"pool_key_hash", "staking_key", "epoch", "credential_tag", "stake",
	"owner", "registered", "captured_slot", "boundary_slot",
}

func rewardStakeInputRow(s *models.RewardStakeInput) []any {
	return []any{
		nilBytes(s.PoolKeyHash), nilBytes(s.StakingKey), sqlInt(s.Epoch),
		sqlInt(uint64(s.CredentialTag)), uint64Text(s.Stake),
		boolInt(s.Owner), boolInt(s.Registered), sqlInt(s.CapturedSlot),
		sqlInt(s.BoundarySlot),
	}
}

// stakeCursorPoolBits is the share of a stake input cursor that holds the
// ordinal of the next pool within its epoch; the rest is the epoch number.
const stakeCursorPoolBits = 24

// rewardStakeInputFactPage serves stake input rows in (epoch, pool,
// credential) order. The cursor names the next epoch, by its number, and
// pool to read, so each page reads only the pools it returns rather than
// re-exporting the rows already served, and a prune of the oldest epochs
// between pages does not move it; a page holds whole pools, and so may exceed
// limit by up to one pool's rows.
func (ls *LedgerState) rewardStakeInputFactPage(
	_ metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	ex, err := ls.rewardInputsExport()
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	return stakeInputPageOver(ex, from, through, cursor, limit)
}

func stakeInputPageOver(
	ex rewardInputsExport,
	from, through uint64,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	out := metadata.FactRows{Columns: rewardStakeInputColumns}
	startEpoch := uint64(cursor >> stakeCursorPoolBits) //nolint:gosec // a cursor this code made
	poolIdx := int(cursor & (1<<stakeCursorPoolBits - 1))
	epochIdx, _ := slices.BinarySearch(ex.epochs, startEpoch)
	if epochIdx < len(ex.epochs) && ex.epochs[epochIdx] != startEpoch {
		// The epoch the cursor named has been pruned: resume at the next.
		poolIdx = 0
	}
	for ; epochIdx < len(ex.epochs); epochIdx, poolIdx = epochIdx+1, 0 {
		epoch := ex.epochs[epochIdx]
		// An epoch captured outside the range has no row to serve, so its
		// pools are not read either.
		if skip, err := ex.view.stakesCapturedOutside(
			epoch, from, through,
		); err != nil {
			return metadata.FactRows{}, 0, err
		} else if skip {
			continue
		}
		pools, err := ex.view.pools(epoch)
		if err != nil {
			return metadata.FactRows{}, 0, err
		}
		for ; poolIdx < len(pools); poolIdx++ {
			if len(out.Rows) >= limit {
				return out, int64(epoch)<<stakeCursorPoolBits | //nolint:gosec // an epoch number
					int64(poolIdx), nil
			}
			pool := pools[poolIdx].PoolKeyHash
			rows, err := ex.view.stakes(epoch, pool, pool)
			if err != nil {
				return metadata.FactRows{}, 0, err
			}
			for _, s := range rows {
				if inSlotRange(s.CapturedSlot, from, through) {
					out.Rows = append(out.Rows, rewardStakeInputRow(s))
				}
			}
		}
	}
	return out, 0, nil
}

// rewardDerivedExport reads the derived reward rows as the set holds them.
func (ls *LedgerState) rewardDerivedExport() (
	RewardDerivedReader, []uint64, error,
) {
	r := ls.rewardDerivedReader(nil)
	if r == nil {
		return nil, nil, ledgerStateUnserved(nodesettings.FamilyRewardOutputs)
	}
	epochs, err := r.RewardDerivedEpochs()
	if err != nil {
		return nil, nil, err
	}
	return r, sortedUnique(epochs), nil
}

func (ls *LedgerState) rewardPoolOutputFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r, epochs, err := ls.rewardDerivedExport()
	if err != nil {
		return metadata.FactRows{}, err
	}
	return poolOutputRowsOver(r, epochs, from, through)
}

func poolOutputRowsOver(
	r RewardDerivedReader,
	epochs []uint64,
	from, through uint64,
) (metadata.FactRows, error) {
	out := metadata.FactRows{Columns: []string{
		"apparent_performance", "pool_key_hash", "epoch", "optimal_reward",
		"total_reward", "leader_reward", "leader_reward_deficit",
		"member_reward_total", "owner_stake", "undistributed", "unspendable",
		"captured_slot", "boundary_slot",
	}}
	for _, epoch := range epochs {
		rows, err := r.RewardPoolOutputRows(epoch, nil)
		if err != nil {
			return metadata.FactRows{}, err
		}
		for _, p := range rows {
			if !inSlotRange(p.CapturedSlot, from, through) {
				continue
			}
			out.Rows = append(out.Rows, []any{
				optRatText(p.ApparentPerformance), nilBytes(p.PoolKeyHash),
				sqlInt(p.Epoch), uint64Text(p.OptimalReward),
				uint64Text(p.TotalReward), uint64Text(p.LeaderReward),
				uint64Text(p.LeaderRewardDeficit),
				uint64Text(p.MemberRewardTotal), uint64Text(p.OwnerStake),
				uint64Text(p.Undistributed), uint64Text(p.Unspendable),
				sqlInt(p.CapturedSlot), sqlInt(p.BoundarySlot),
			})
		}
	}
	return out, nil
}

// rewardAccountOutputFactPage serves account outputs epoch by epoch; the
// cursor is the number of the next epoch, so a page reads only the epochs it
// returns and a prune of the oldest epochs between pages does not move it. An
// epoch's rows are not split across pages.
func (ls *LedgerState) rewardAccountOutputFactPage(
	_ metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	r, epochs, err := ls.rewardDerivedExport()
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	return accountOutputPageOver(r, epochs, from, through, cursor, limit)
}

func accountOutputPageOver(
	r RewardDerivedReader,
	epochs []uint64,
	from, through uint64,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	out := metadata.FactRows{Columns: []string{
		"staking_key", "pool_key_hash", "reward_type", "epoch",
		"credential_tag", "amount", "spendable", "guarded", "captured_slot",
		"boundary_slot",
	}}
	start, _ := slices.BinarySearch(epochs, uint64(cursor)) //nolint:gosec // a cursor this code made
	for i := start; i < len(epochs); i++ {
		if len(out.Rows) >= limit {
			return out, int64(epochs[i]), nil //nolint:gosec // an epoch number
		}
		skip, err := rewardAccountEpochOutsideRange(r, epochs[i], from, through)
		if err != nil {
			return metadata.FactRows{}, 0, err
		}
		if skip {
			continue
		}
		rows, err := r.RewardAccountOutputRows(epochs[i], nil)
		if err != nil {
			return metadata.FactRows{}, 0, err
		}
		for _, a := range rows {
			if !inSlotRange(a.CapturedSlot, from, through) {
				continue
			}
			out.Rows = append(out.Rows, []any{
				nilBytes(a.StakingKey), nilBytes(a.PoolKeyHash), a.RewardType,
				sqlInt(a.Epoch), sqlInt(uint64(a.CredentialTag)),
				uint64Text(a.Amount), boolInt(a.Spendable), boolInt(a.Guarded),
				sqlInt(a.CapturedSlot), sqlInt(a.BoundarySlot),
			})
		}
	}
	return out, 0, nil
}

func (ls *LedgerState) rewardCreditRoundFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r := ls.rewardOutputReader(nil)
	if r == nil {
		return metadata.FactRows{}, ledgerStateUnserved(
			nodesettings.FamilyRewardOutputs,
		)
	}
	rounds, err := r.RewardRounds(nil)
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"snapshot_epoch", "boundary_slot",
	}}
	for _, round := range rounds {
		if !inSlotRange(round.BoundarySlot, from, through) {
			continue
		}
		out.Rows = append(out.Rows, []any{
			sqlInt(round.SnapshotEpoch), sqlInt(round.BoundarySlot),
		})
	}
	return out, nil
}

// RewardPrecomputeCursors returns each retained reward precompute cursor as
// the LedgerDB set stores it, by snapshot epoch.
func (ls *LedgerState) RewardPrecomputeCursors() (map[uint64]string, error) {
	_, epochs, err := ls.rewardDerivedExport()
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string, len(epochs))
	for _, epoch := range epochs {
		raw, err := ls.ledgerDB().GetSyncState(
			rewardPrecomputeCursorKey(epoch), nil,
		)
		if err != nil {
			return nil, err
		}
		if raw != "" {
			out[epoch] = raw
		}
	}
	return out, nil
}

// rewardAccountEpochOutsideRange reports whether none of epoch's account
// output rows can fall in [from, through], judged from its pool output rows,
// one per pool, so the copy need not decode the epoch's whole delegator set to
// find that none of it belongs to the range. A precompute chunk saves its
// pools' output rows and their account rows in one transaction at one captured
// slot, and the credit-time recheck keeps that slot, so every account row's
// slot lies within its epoch's lowest and highest pool row slot. An epoch can
// hold several slots, one per resumed run of a round interrupted before rounds
// adopted their cursor's slot, so every pool row is read rather than the
// first. An epoch with no pool row is never skipped.
func rewardAccountEpochOutsideRange(
	r RewardDerivedReader,
	epoch, from, through uint64,
) (bool, error) {
	pools, err := r.RewardPoolOutputRows(epoch, nil)
	if err != nil || len(pools) == 0 {
		return false, err
	}
	lo, hi := pools[0].CapturedSlot, pools[0].CapturedSlot
	for _, p := range pools[1:] {
		lo, hi = min(lo, p.CapturedSlot), max(hi, p.CapturedSlot)
	}
	return hi < from || lo > through, nil
}
