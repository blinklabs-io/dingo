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

import (
	"errors"
	"math/big"
)

// EpochStakeRow is one credential's stake in the distribution a pool held at
// an epoch, as recorded by the epoch-boundary reward snapshot.
type EpochStakeRow struct {
	PoolKeyHash   []byte
	StakingKey    []byte
	CredentialTag uint8
	Stake         uint64
}

// PoolHistoryRow is one epoch of a pool's history: the stake distribution
// snapshot labelled Epoch joined with the epoch totals and the reward outputs
// calculated from it. RewardsCalculated is false while the snapshot's rewards
// have not been calculated, in which case the reward amounts are zero.
type PoolHistoryRow struct {
	Epoch             uint64
	Stake             uint64
	DelegatorCount    uint64
	TotalActiveStake  uint64
	MemberRewards     uint64
	LeaderRewards     uint64
	RewardsCalculated bool
	// TotalActiveStakeKnown is false when the epoch has no ready summary, so
	// TotalActiveStake is unknown rather than zero.
	TotalActiveStakeKnown bool
}

// PoolDelegatorRow is one registered delegator of a pool with its live stake.
type PoolDelegatorRow struct {
	StakingKey    []byte
	CredentialTag uint8
	Stake         uint64
}

// PoolCertificateEvent is one registration or retirement certificate of a
// pool with its position inside its transaction.
type PoolCertificateEvent struct {
	TxHash    []byte
	CertIndex uint32
	// Action is "registered" or "deregistered".
	Action string
}

// AccountStakeHistoryRow is the stake one credential held in the
// distribution captured by the snapshot labelled Epoch.
type AccountStakeHistoryRow struct {
	PoolKeyHash []byte
	Epoch       uint64
	Stake       uint64
}

// AccountMIRRow is one Move Instantaneous Rewards payment to a credential.
type AccountMIRRow struct {
	// Amount is the signed delta_coin paid to the credential.
	Amount     *big.Int
	TxHash     []byte
	BlockHash  []byte
	TxSlot     uint64
	BlockIndex uint32
}

// AssetQuantity is a quantity of one native asset, held unbounded so a sum
// across UTxOs cannot wrap.
type AssetQuantity struct {
	Quantity *big.Int
	PolicyID []byte
	Name     []byte
}

// AccountAggregateLimits bounds the work of one account aggregate read. A
// zero field leaves that dimension unbounded.
type AccountAggregateLimits struct {
	// MaxUtxos is the most UTxO rows, spent or live, one lovelace sum may
	// read.
	MaxUtxos int
	// MaxAssetRows is the most asset holdings the read may visit.
	MaxAssetRows int
	// MaxAssets is the most distinct assets the read may accumulate.
	MaxAssets int
}

// ErrAccountAggregateLimit reports an account aggregate read stopped by its
// AccountAggregateLimits before the whole aggregate was read.
var ErrAccountAggregateLimit = errors.New(
	"account aggregate read exceeds its limit",
)

// AccountAddressTotals are the lifetime totals across every address that
// carries a stake credential.
type AccountAddressTotals struct {
	// ReceivedLovelace and SentLovelace are unbounded: each output fits in
	// uint64, but a lifetime sum of them need not.
	ReceivedLovelace *big.Int
	SentLovelace     *big.Int
	ReceivedAssets   []AssetQuantity
	SentAssets       []AssetQuantity
	// TxCount is the number of distinct transactions touching the addresses.
	TxCount int
}

// GovernanceVoteFilter selects votes. Nil and empty fields do not filter.
type GovernanceVoteFilter struct {
	ProposalID *uint
	VoterType  *uint8
	// VoterCredentials selects votes cast under any of the tagged
	// credentials.
	VoterCredentials []CommitteeCredential
	// AuthorizedBy selects votes cast under any hot credential the cold
	// committee credential has authorized, superseded ones included.
	AuthorizedBy *CommitteeCredential
}

// GovernanceVoteRow is a stored vote with the proposal it was cast on. The
// vote's casting transaction is not stored; CastSlot locates its block.
type GovernanceVoteRow struct {
	Vote                GovernanceVote
	ProposalTxHash      []byte
	ProposalActionIndex uint32
	ProposalActionType  uint8
	// CastSlot is the slot of the block holding the transaction that cast
	// this vote. Vote carries the value and anchor of this cast.
	CastSlot uint64
	// CastTxHash and CastCertIndex locate the casting voting procedure.
	// CastTxHash is nil for a cast recorded before the history kept them.
	CastTxHash    []byte
	CastCertIndex uint32
	// CastSlotRank and CastSlotCount place this entry among the vote's
	// history entries at CastSlot, in recording order: a cast with no
	// recorded position is matched to the block's casts by them.
	CastSlotRank  int
	CastSlotCount int
	// CastSlotEarliest reports that no history entry of the vote precedes
	// CastSlot. Migration v22 seeded each vote that existed before it with
	// one such entry, carrying only its value current at the upgrade.
	CastSlotEarliest bool
	// Counted reports whether the cast counts toward the tally: it is the
	// voter's latest cast on the proposal and was not invalidated by its
	// DRep deregistering.
	Counted bool
}

// DRepUpdateRow is one registration, update or deregistration certificate of
// a DRep with its position inside its transaction.
type DRepUpdateRow struct {
	TxHash    []byte
	CertIndex uint32
	// Action is "registered", "updated" or "deregistered".
	Action string
	// Deposit is the deposit paid or refunded; nil for an update.
	Deposit *uint64
}
