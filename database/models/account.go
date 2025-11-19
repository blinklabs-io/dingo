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

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

var ErrAccountNotFound = errors.New("account not found")

type Account struct {
	StakingKey    []byte `gorm:"uniqueIndex"`
	Pool          []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	Active        bool   `gorm:"index;default:true"`
}

func (a *Account) TableName() string {
	return "account"
}

// String returns the bech32-encoded representation of the Account's StakingKey
// with the "stake" human-readable part for mainnet. Returns an error if the StakingKey is
// empty or if encoding fails.
func (a *Account) String() (string, error) {
	return a.StringWithNetwork(false)
}

// StringWithNetwork returns the bech32-encoded representation of the Account's StakingKey
// with the appropriate human-readable part based on the network.
// Use isTestnet=true for testnet networks (HRP: "stake_test"), false for mainnet (HRP: "stake").
// Returns an error if the StakingKey is empty or if encoding fails.
func (a *Account) StringWithNetwork(isTestnet bool) (string, error) {
	if len(a.StakingKey) == 0 {
		return "", errors.New("staking key is empty")
	}
	// Convert data to base32 and encode as bech32
	convData, err := bech32.ConvertBits(a.StakingKey, 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("failed to convert bits: %w", err)
	}
	hrp := "stake"
	if isTestnet {
		hrp = "stake_test"
	}
	encoded, err := bech32.Encode(hrp, convData)
	if err != nil {
		return "", fmt.Errorf("failed to encode bech32: %w", err)
	}
	return encoded, nil
}

// Certificate models constraint strategy:
// - Deregistration, StakeDeregistration, and StakeRegistration use composite unique constraints on (StakingKey, CertificateID)
//   to prevent duplicate certificates for the same staking key and certificate ID.
// - Other certificate models (Registration, StakeDelegation, etc.) use simple indexes
//   without uniqueness constraints, allowing multiple entries for the same staking key across different certificates.
// This design ensures that deregistration and stake registration certificates are processed only once,
// while other certificate types can be handled multiple times if needed (e.g., for historical tracking or retries).

type Deregistration struct {
	StakingKey    []byte `gorm:"index;uniqueIndex:uniq_deregistration_cert"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"uniqueIndex:uniq_deregistration_cert"`
	AddedSlot     uint64
	Amount        types.Uint64
}

func (Deregistration) TableName() string {
	return "deregistration"
}

type Registration struct {
	StakingKey    []byte `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64
	DepositAmount types.Uint64
}

func (Registration) TableName() string {
	return "registration"
}

type StakeDelegation struct {
	StakingKey    []byte `gorm:"index"`
	PoolKeyHash   []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
}

func (StakeDelegation) TableName() string {
	return "stake_delegation"
}

type StakeDeregistration struct {
	StakingKey    []byte `gorm:"index;uniqueIndex:uniq_stake_deregistration_cert"`
	CertificateID uint   `gorm:"uniqueIndex:uniq_stake_deregistration_cert"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
}

func (StakeDeregistration) TableName() string {
	return "stake_deregistration"
}

type StakeRegistration struct {
	StakingKey    []byte `gorm:"index;uniqueIndex:uniq_stake_registration_cert"`
	CertificateID uint   `gorm:"uniqueIndex:uniq_stake_registration_cert"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
	DepositAmount types.Uint64
}

func (StakeRegistration) TableName() string {
	return "stake_registration"
}

type StakeRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index"`
	PoolKeyHash   []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
	DepositAmount types.Uint64
}

func (StakeRegistrationDelegation) TableName() string {
	return "stake_registration_delegation"
}

type StakeVoteDelegation struct {
	StakingKey    []byte `gorm:"index"`
	PoolKeyHash   []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
}

func (StakeVoteDelegation) TableName() string {
	return "stake_vote_delegation"
}

type StakeVoteRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index"`
	PoolKeyHash   []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
	DepositAmount types.Uint64
}

func (StakeVoteRegistrationDelegation) TableName() string {
	return "stake_vote_registration_delegation"
}

type VoteDelegation struct {
	StakingKey    []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
}

func (VoteDelegation) TableName() string {
	return "vote_delegation"
}

type VoteRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
	DepositAmount types.Uint64
}

func (VoteRegistrationDelegation) TableName() string {
	return "vote_registration_delegation"
}
