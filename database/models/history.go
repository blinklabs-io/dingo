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

import "math/big"

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

// AccountAddressTotals are the lifetime totals across every address that
// carries a stake credential.
type AccountAddressTotals struct {
	ReceivedLovelace uint64
	SentLovelace     uint64
	ReceivedAssets   []AssetQuantity
	SentAssets       []AssetQuantity
	// TxCount is the number of distinct transactions touching the addresses.
	TxCount int
}
