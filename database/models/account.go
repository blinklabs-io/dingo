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

package models

import (
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

var ErrAccountNotFound = errors.New("account not found")

// AccountCreatedSlotUnset is the sentinel the account create helpers stamp on a
// freshly built (not-yet-persisted) account so the save helpers can resolve
// Account.CreatedSlot to the account's AddedSlot at insert time without
// overwriting the immutable CreatedSlot of an existing row (which is loaded from
// the database and is never equal to this sentinel). It is math.MaxInt64 rather
// than ^uint64(0) because database/sql cannot bind a uint64 with the high bit
// set, and no real slot ever reaches it.
const AccountCreatedSlotUnset = uint64(math.MaxInt64)

const (
	DrepTypeAddrKeyHash uint64 = iota
	DrepTypeScriptHash
	DrepTypeAlwaysAbstain
	DrepTypeAlwaysNoConfidence
)

func DrepTypeFromInt(drepType int) (uint64, error) {
	switch drepType {
	case 0:
		return DrepTypeAddrKeyHash, nil
	case 1:
		return DrepTypeScriptHash, nil
	case 2:
		return DrepTypeAlwaysAbstain, nil
	case 3:
		return DrepTypeAlwaysNoConfidence, nil
	default:
		return 0, fmt.Errorf("unknown drep type: %d", drepType)
	}
}

func CredentialTagFromUint(tag uint) (uint8, error) {
	return CredentialTagFromUint64(uint64(tag))
}

func CredentialTagFromUint64(tag uint64) (uint8, error) {
	switch tag {
	case 0:
		return 0, nil
	case 1:
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported stake credential tag: %d", tag)
	}
}

// ValidatePredefinedDrepTypes rejects credential-backed DRep delegation
// types. GetDRepVotingPowerByType is only for predefined, credentialless
// DRep options.
func ValidatePredefinedDrepTypes(drepTypes []uint64) error {
	for _, drepType := range drepTypes {
		switch drepType {
		case DrepTypeAddrKeyHash, DrepTypeScriptHash:
			return fmt.Errorf(
				"drep type %d is credential-backed; use credential voting power",
				drepType,
			)
		case DrepTypeAlwaysAbstain, DrepTypeAlwaysNoConfidence:
			continue
		default:
			return fmt.Errorf("unknown predefined drep type: %d", drepType)
		}
	}
	return nil
}

type Account struct {
	StakingKey    []byte
	CredentialTag uint8
	Pool          []byte
	Drep          []byte
	ID            uint
	AddedSlot     uint64
	// CreatedSlot is the slot at which this account row was first created
	// (0 for Shelley-genesis delegated accounts). Unlike AddedSlot it is
	// immutable after creation — never bumped by later delegation/registration
	// changes. See AccountCreatedSlotUnset for the sentinel used by the
	// create/save helpers.
	CreatedSlot   uint64
	CertificateID uint
	Reward        types.Uint64
	// DrepType is the DRep delegation type code, an internal enum
	// matching the Cardano ledger CBOR sum-type tag:
	//   0 = key credential, 1 = script credential,
	//   2 = AlwaysAbstain, 3 = AlwaysNoConfidence.
	// A zero value (0) means either "key credential" or "no delegation set",
	// disambiguated by whether Drep is nil.
	DrepType uint64
	Active   bool
	// ExpirationEpoch is the CIP-0163 reward-account inactivity expiry: the
	// last epoch in which the account remains active unless it
	// witnesses again. 0 means unset (treated as active). Mirrors
	// Drep.ExpiryEpoch. Set/bumped by RenewAccountExpirations and the one-time
	// activation stamp; only read when the delegator-inactivity gate is on.
	ExpirationEpoch uint64
}

// AccountInactivityActivation records the exact reward-account credentials
// stamped by the one-time CIP-0163 activation. Membership cannot be inferred
// from CreatedSlot alone because an account may have existed but been inactive
// at the activation boundary.
type AccountInactivityActivation struct {
	CredentialTag uint8
	StakingKey    []byte
}

type StakeCredentialRef struct {
	Tag uint8
	Key []byte
}

func NewStakeCredentialRef(tag uint8, key []byte) StakeCredentialRef {
	return StakeCredentialRef{
		Tag: tag,
		Key: key,
	}
}

func (r StakeCredentialRef) MapKey() string {
	return string([]byte{r.Tag}) + string(r.Key)
}

// AccountRewardDelta records reward-account balance changes that are not
// otherwise represented by a rollback-aware certificate row. Credits store the
// credited Amount. Withdrawals store the withdrawal Amount, PreviousReward, and
// TxHash so rollback can restore the cleared reward balance.
//
// The unique index idx_account_reward_delta_w_tx_s_slot includes AddedSlot.
// Withdrawal writers use TxHash and the credential as their logical replay key,
// regardless of slot. Credit deltas (deposit refunds, MIR, POOLREAP) use an
// event discriminator in TxHash when one is available and otherwise use the
// normalized empty value. Without AddedSlot, repeated per-epoch credits to a
// given account could still collapse onto a single row — colliding across
// epochs even on a clean first pass and breaking per-row rollback accounting in
// DeleteAccountRewardsAfterSlot. Including AddedSlot makes each per-epoch
// credit a distinct row while keeping a replayed epoch-rollover credit (same
// account, same event discriminator, same boundary slot) mapped to the same row
// so it can be skipped idempotently instead of erroring.
type AccountRewardDelta struct {
	StakingKey     []byte
	CredentialTag  uint8
	TxHash         []byte
	Amount         types.Uint64
	PreviousReward types.Uint64
	ID             uint
	AddedSlot      uint64
	Withdrawal     bool
	// PostSnapshot marks a credit that cardano-ledger applies AFTER the
	// epoch-boundary stake snapshot (the SNAP rule): POOLREAP deposit refunds,
	// enacted treasury withdrawals and governance proposal-deposit refunds.
	// Two boundary credits precede SNAP and leave this false: the delayed reward
	// update (applyRUpd / applyStakeRewards) and the Shelley-era MIR rule, which
	// NEWEPOCH embeds between applyRUpd and EPOCH.
	//
	// Every boundary credit lands at the same added_slot (the boundary slot), so
	// slot alone cannot separate the pre-SNAP reward update from the post-SNAP
	// credits. The epoch-boundary stake reconstruction
	// (stakequery.historicalDelegatorStakeCTE with a nonzero boundary slot)
	// needs exactly that separation to reproduce the authoritative SNAP-point
	// capture, which observes the reward update and none of the rest.
	//
	// Not indexed: it is only ever read alongside added_slot for a single
	// boundary slot, which idx_account_reward_delta_credential/added_slot
	// already narrow.
	PostSnapshot bool
}

// AccountWithdrawalWitness records every valid reward-withdrawal map entry,
// including zero-amount withdrawals. It is separate from balance deltas because
// CIP-0163 treats the credential witness as activity even when no reward moves.
type AccountWithdrawalWitness struct {
	StakingKey    []byte
	CredentialTag uint8
	TxHash        []byte
	ID            uint
	AddedSlot     uint64
}

// String returns the bech32-encoded representation of the Account's StakingKey
// with the "stake" human-readable part. Returns an error if the StakingKey is
// empty or if encoding fails.
func (a *Account) String() (string, error) {
	if len(a.StakingKey) == 0 {
		return "", errors.New("staking key is empty")
	}
	// Convert data to base32 and encode as bech32
	convData, err := bech32.ConvertBits(a.StakingKey, 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("failed to convert bits: %w", err)
	}
	encoded, err := bech32.Encode("stake", convData)
	if err != nil {
		return "", fmt.Errorf("failed to encode bech32: %w", err)
	}
	return encoded, nil
}

type Deregistration struct {
	StakingKey    []byte
	CredentialTag uint8
	ID            uint
	CertificateID uint
	AddedSlot     uint64
	Amount        types.Uint64
}

type Registration struct {
	StakingKey    []byte
	CredentialTag uint8
	ID            uint
	CertificateID uint
	AddedSlot     uint64
	DepositAmount types.Uint64
}

type StakeDelegation struct {
	StakingKey    []byte
	CredentialTag uint8
	PoolKeyHash   []byte
	CertificateID uint
	ID            uint
	AddedSlot     uint64
}

type StakeDeregistration struct {
	StakingKey    []byte
	CredentialTag uint8
	CertificateID uint
	ID            uint
	AddedSlot     uint64
}

type StakeRegistration struct {
	StakingKey    []byte
	CredentialTag uint8
	CertificateID uint
	ID            uint
	AddedSlot     uint64
	DepositAmount types.Uint64
}

type StakeRegistrationDelegation struct {
	StakingKey    []byte
	CredentialTag uint8
	PoolKeyHash   []byte
	CertificateID uint
	ID            uint
	AddedSlot     uint64
	DepositAmount types.Uint64
}

type StakeVoteDelegation struct {
	StakingKey    []byte
	CredentialTag uint8
	PoolKeyHash   []byte
	Drep          []byte
	DrepType      uint64
	CertificateID uint
	ID            uint
	AddedSlot     uint64
}

type StakeVoteRegistrationDelegation struct {
	StakingKey    []byte
	CredentialTag uint8
	PoolKeyHash   []byte
	Drep          []byte
	DrepType      uint64
	CertificateID uint
	ID            uint
	AddedSlot     uint64
	DepositAmount types.Uint64
}

type VoteDelegation struct {
	StakingKey    []byte
	CredentialTag uint8
	Drep          []byte
	DrepType      uint64
	CertificateID uint
	ID            uint
	AddedSlot     uint64
}

type VoteRegistrationDelegation struct {
	StakingKey    []byte
	CredentialTag uint8
	Drep          []byte
	DrepType      uint64
	CertificateID uint
	ID            uint
	AddedSlot     uint64
	DepositAmount types.Uint64
}
