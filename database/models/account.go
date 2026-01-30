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
	StakingKey    []byte `gorm:"uniqueIndex;size:28"`
	Pool          []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	Active        bool   `gorm:"default:true"`
}

func (a *Account) TableName() string {
	return "account"
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
	StakingKey    []byte `gorm:"index;size:28"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64 `gorm:"index"`
	Amount        types.Uint64
}

func (Deregistration) TableName() string {
	return "deregistration"
}

type Registration struct {
	StakingKey    []byte `gorm:"index;size:28"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (Registration) TableName() string {
	return "registration"
}

type StakeDelegation struct {
	StakingKey    []byte `gorm:"index;size:28"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (StakeDelegation) TableName() string {
	return "stake_delegation"
}

type StakeDeregistration struct {
	StakingKey    []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (StakeDeregistration) TableName() string {
	return "stake_deregistration"
}

type StakeRegistration struct {
	StakingKey    []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (StakeRegistration) TableName() string {
	return "stake_registration"
}

type StakeRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index;size:28"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (StakeRegistrationDelegation) TableName() string {
	return "stake_registration_delegation"
}

type StakeVoteDelegation struct {
	StakingKey    []byte `gorm:"index;size:28"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (StakeVoteDelegation) TableName() string {
	return "stake_vote_delegation"
}

type StakeVoteRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index;size:28"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (StakeVoteRegistrationDelegation) TableName() string {
	return "stake_vote_registration_delegation"
}

type VoteDelegation struct {
	StakingKey    []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (VoteDelegation) TableName() string {
	return "vote_delegation"
}

type VoteRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (VoteRegistrationDelegation) TableName() string {
	return "vote_registration_delegation"
}
