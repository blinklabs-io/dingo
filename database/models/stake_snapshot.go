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

package models

import "github.com/blinklabs-io/dingo/database/types"

// PoolRewardAccountAutoVote enumerates the CIP-1694 reward-account
// DRep-delegation outcomes that produce an implicit SPO vote when a
// pool did not cast an explicit vote on a proposal. Resolved and
// frozen at the epoch boundary that captures the stake snapshot so
// the governance tally uses snapshot-era state rather than live state.
const (
	// PoolRewardAccountAutoVoteNone means the pool's reward account is
	// unset, deregistered, or delegated to anything other than the
	// predefined Always{Abstain,NoConfidence} DRep options. Such a
	// pool contributes to SPOTotalStake only and counts as implicit
	// no under CIP-1694.
	PoolRewardAccountAutoVoteNone uint8 = 0
	// PoolRewardAccountAutoVoteAbstain means the pool's reward
	// account delegates to AlwaysAbstain at the snapshot epoch. Its
	// stake is bucketed into SPOAbstainStake (excluded from the
	// active denominator).
	PoolRewardAccountAutoVoteAbstain uint8 = 1
	// PoolRewardAccountAutoVoteNoConfidence means the pool's reward
	// account delegates to AlwaysNoConfidence at the snapshot epoch.
	// For NoConfidence actions the pool stake is bucketed Yes; for
	// any other action type it is bucketed No (mirrors the
	// AlwaysNoConfidence DRep handling).
	PoolRewardAccountAutoVoteNoConfidence uint8 = 2
)

const (
	// PoolStakeSnapshotTypeMark is the epoch-boundary mark snapshot used by
	// governance and by the normal Praos epoch-offset rotation.
	PoolStakeSnapshotTypeMark = "mark"
	// PoolStakeSnapshotTypeSet is the mark snapshot rotated forward one
	// epoch, per the Shelley set/go/mark rotation.
	PoolStakeSnapshotTypeSet = "set"
	// PoolStakeSnapshotTypeGo is the set snapshot rotated forward one more
	// epoch; this is the row consulted for live leader-election stake.
	PoolStakeSnapshotTypeGo = "go"
	// PoolStakeSnapshotTypeActive is the active consensus pool distribution
	// imported from a Mithril NewEpochState.pool-distr field. TotalStake
	// stores the fraction numerator and StakeDenominator stores the
	// denominator for the same row.
	PoolStakeSnapshotTypeActive = "actv"
)

// ValidPoolStakeSnapshotType reports whether snapshotType is one of the
// known pool_stake_snapshot.snapshot_type values. Callers that accept a
// snapshotType from outside the package should validate with this before
// querying, so a typo or stale value fails fast instead of silently
// returning zero rows.
func ValidPoolStakeSnapshotType(snapshotType string) bool {
	switch snapshotType {
	case PoolStakeSnapshotTypeMark,
		PoolStakeSnapshotTypeSet,
		PoolStakeSnapshotTypeGo,
		PoolStakeSnapshotTypeActive:
		return true
	default:
		return false
	}
}

// PoolStakeSnapshot captures pool stake for an epoch snapshot. Mark rows store
// lovelace totals at an epoch boundary. Active rows imported from Mithril store
// a consensus stake fraction as TotalStake/StakeDenominator.
type PoolStakeSnapshot struct {
	ID               uint
	Epoch            uint64
	SnapshotType     string // "mark", "set", "go", "actv"
	PoolKeyHash      []byte
	TotalStake       types.Uint64
	StakeDenominator types.Uint64
	DelegatorCount   uint64
	CapturedSlot     uint64
	// CalculationVersion identifies the stake-accounting algorithm used to
	// produce Mark/Set/Go rows. Zero denotes a pre-provenance snapshot.
	CalculationVersion uint
	// RewardAccountAutoVote captures the CIP-1694 SPO auto-vote
	// outcome implied by the pool's reward-account DRep delegation at
	// the snapshot epoch. Values come from PoolRewardAccountAutoVote*.
	// This field is only meaningful when RewardAccountAutoVoteResolved
	// is true; otherwise its value is undefined and must not be read
	// by the tally.
	RewardAccountAutoVote uint8
	// RewardAccountAutoVoteResolved disambiguates "resolved as none"
	// from "never resolved". The resolver sets this to true after it
	// has computed RewardAccountAutoVote against snapshot-era state.
	// Rows imported by Mithril for set/go rotations (which only have
	// live state available at import time and cannot be faithfully
	// resolved against historical boundaries) intentionally leave this
	// false; the tally treats them as PoolRewardAccountAutoVoteNone,
	// matching pre-CIP-1694 behaviour for those rows. Pre-CIP-1694
	// rows in upgraded databases also remain false until re-resolved.
	RewardAccountAutoVoteResolved bool
}

// EpochSummary captures network-wide aggregate statistics at epoch boundary.
type EpochSummary struct {
	ID               uint
	Epoch            uint64
	TotalActiveStake types.Uint64
	TotalPoolCount   uint64
	TotalDelegators  uint64
	EpochNonce       []byte
	BoundarySlot     uint64
	SnapshotReady    bool
}
