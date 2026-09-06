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

// RewardStakeCalculationVersion identifies the stake-accounting algorithm
// used to produce persisted live stake and consensus snapshots. Bump it when
// changing that calculation so upgrades cannot trust older values.
const RewardStakeCalculationVersion uint = 1

// RewardAdaPots captures the reward-related ADA pots at an epoch boundary.
type RewardAdaPots struct {
	ID           uint
	Epoch        uint64
	Treasury     types.Uint64
	Reserves     types.Uint64
	Fees         types.Uint64
	Rewards      types.Uint64
	CapturedSlot uint64
}

// RewardSnapshot captures reward-calculation snapshot metadata for an epoch.
type RewardSnapshot struct {
	ID               uint
	Epoch            uint64
	SnapshotType     string
	TotalActiveStake types.Uint64
	TotalPoolCount   uint64
	TotalDelegators  uint64
	CapturedSlot     uint64
	BoundarySlot     uint64
	EpochNonce       []byte
	ProtocolVersion  uint
	// Authoritative marks a snapshot captured inside the ledger epoch-rollover
	// write transaction at the SNAP point (CaptureEpochBoundarySnapshot). The
	// event-driven fallback capture (captureMarkSnapshot) never overwrites an
	// authoritative row: it either claims a fresh row or is superseded. Defaults
	// to false, so pre-existing rows and fallback captures read as provisional.
	Authoritative bool
	// CalculationVersion ties authoritative Mark metadata to the stake
	// calculation that produced its accompanying pool snapshots.
	CalculationVersion uint
}

// ImportedPoolBlockCount records how many blocks one pool minted during one
// epoch, taken from a bootstrap snapshot's NewEpochState BlocksMade rather
// than counted from blocks this node applied.
//
// A bootstrapped node holds no block history below its trust anchor, so for
// the epochs preceding the anchor it cannot count blocks at all. Pool reward
// performance is beta/sigma_a with beta the pool's share of the blocks minted
// in the performance epoch, so those epochs have no performance without these
// rows -- and an absent count is not zero blocks, it is an unknown the reward
// round must decline rather than distribute.
type ImportedPoolBlockCount struct {
	PoolKeyHash    []byte
	Epoch          uint64
	BlocksProduced uint64
	CapturedSlot   uint64
}

// RewardPoolInput captures per-pool inputs needed by reward calculation.
type RewardPoolInput struct {
	Margin                     *types.Rat
	PoolKeyHash                []byte
	RewardAccount              []byte
	BlocksProduced             *uint64
	TotalBlocksInEpoch         *uint64
	ID                         uint
	Epoch                      uint64
	Pledge                     types.Uint64
	DelegatedStake             types.Uint64
	OwnerStake                 types.Uint64
	Cost                       types.Uint64
	DelegatorCount             uint64
	RewardAccountCredentialTag uint8
	CapturedSlot               uint64
	BoundarySlot               uint64
}

// RewardStakeInput captures per-credential stake at the reward snapshot.
type RewardStakeInput struct {
	PoolKeyHash   []byte
	StakingKey    []byte
	ID            uint
	Epoch         uint64
	CredentialTag uint8
	Stake         types.Uint64
	Owner         bool
	Registered    bool
	CapturedSlot  uint64
	BoundarySlot  uint64
}

// RewardLiveStake is the live per-stake-credential aggregate maintained for a
// reward and leader-election snapshot consumers. UtxoStake and RewardStake are stored
// separately so rollback/account-reward repair can refresh only the affected
// credential while TotalStake remains directly queryable.
type RewardLiveStake struct {
	PoolKeyHash   []byte
	StakingKey    []byte
	ID            uint
	CredentialTag uint8
	UtxoStake     types.Uint64
	RewardStake   types.Uint64
	TotalStake    types.Uint64
	Registered    bool
	// PoolDelegation* records the certificate order used to derive PoolKeyHash.
	// It is rollback/rebuild bookkeeping; snapshot consumers select eligible
	// pools independently at the requested slot.
	PoolDelegationSlot       uint64
	PoolDelegationBlockIndex uint64
	PoolDelegationCertIndex  uint32
	UpdatedSlot              uint64
	// CalculationVersion is set by every rebuild and incremental update. Zero
	// denotes rows created before calculation provenance was introduced.
	CalculationVersion uint
}

// RewardPoolOutput captures per-pool reward calculation output for an epoch.
type RewardPoolOutput struct {
	ApparentPerformance *types.Rat
	PoolKeyHash         []byte
	ID                  uint
	Epoch               uint64
	OptimalReward       types.Uint64
	TotalReward         types.Uint64
	LeaderReward        types.Uint64
	MemberRewardTotal   types.Uint64
	OwnerStake          types.Uint64
	Undistributed       types.Uint64
	Unspendable         types.Uint64
	CapturedSlot        uint64
	BoundarySlot        uint64
}

// RewardAccountOutput captures per-account reward calculation output.
type RewardAccountOutput struct {
	StakingKey    []byte
	PoolKeyHash   []byte
	RewardType    string
	ID            uint
	Epoch         uint64
	CredentialTag uint8
	Amount        types.Uint64
	Spendable     bool
	// Guarded records that CIP-0163 account expiry prevented this otherwise
	// spendable reward from being credited.
	Guarded      bool
	CapturedSlot uint64
	BoundarySlot uint64
}
