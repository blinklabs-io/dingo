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

import "errors"

var ErrAccountNotFound = errors.New("account not found")

type Account struct {
	StakingKey    []byte `gorm:"uniqueIndex"`
	Pool          []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
	CertificateID uint `gorm:"index"`
	Active        bool `gorm:"default:true"`
}

func (a *Account) TableName() string {
	return "account"
}

type Deregistration struct {
	StakingKey    []byte `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64
	Amount        int64
}

func (Deregistration) TableName() string {
	return "deregistration"
}

type Registration struct {
	StakingKey    []byte `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64
	DepositAmount uint64
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
	StakingKey    []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
}

func (StakeDeregistration) TableName() string {
	return "stake_deregistration"
}

type StakeRegistration struct {
	StakingKey    []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64
	DepositAmount uint64
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
	DepositAmount uint64
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
	DepositAmount uint64
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
	DepositAmount uint64
}

func (VoteRegistrationDelegation) TableName() string {
	return "vote_registration_delegation"
}
