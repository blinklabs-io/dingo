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

type Account struct {
	ID         uint   `gorm:"primarykey"`
	StakingKey []byte `gorm:"uniqueIndex"`
	Pool       []byte `gorm:"index"`
	Drep       []byte `gorm:"index"`
	AddedSlot  uint64
	Active     bool `gorm:"default:true"`
}

func (a *Account) TableName() string {
	return "account"
}

type Deregistration struct {
	ID         uint   `gorm:"primarykey"`
	StakingKey []byte `gorm:"index"`
	AddedSlot  uint64
}

func (Deregistration) TableName() string {
	return "deregistration"
}

type Registration struct {
	ID            uint   `gorm:"primarykey"`
	StakingKey    []byte `gorm:"index"`
	AddedSlot     uint64
	DepositAmount uint64
}

func (Registration) TableName() string {
	return "registration"
}

type StakeDelegation struct {
	ID          uint   `gorm:"primarykey"`
	StakingKey  []byte `gorm:"index"`
	PoolKeyHash []byte `gorm:"index"`
	AddedSlot   uint64
}

func (StakeDelegation) TableName() string {
	return "stake_delegation"
}

type StakeDeregistration struct {
	ID         uint   `gorm:"primarykey"`
	StakingKey []byte `gorm:"index"`
	AddedSlot  uint64
}

func (StakeDeregistration) TableName() string {
	return "stake_deregistration"
}

type StakeRegistration struct {
	ID            uint   `gorm:"primarykey"`
	StakingKey    []byte `gorm:"index"`
	AddedSlot     uint64
	DepositAmount uint64
}

func (StakeRegistration) TableName() string {
	return "stake_registration"
}

type StakeRegistrationDelegation struct {
	ID            uint   `gorm:"primarykey"`
	StakingKey    []byte `gorm:"index"`
	PoolKeyHash   []byte `gorm:"index"`
	AddedSlot     uint64
	DepositAmount uint64
}

func (StakeRegistrationDelegation) TableName() string {
	return "stake_registration_delegation"
}

type StakeVoteRegistrationDelegation struct {
	ID            uint   `gorm:"primarykey"`
	StakingKey    []byte `gorm:"index"`
	PoolKeyHash   []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	AddedSlot     uint64
	DepositAmount uint64
}

func (StakeVoteRegistrationDelegation) TableName() string {
	return "stake_vote_registration_delegation"
}

type VoteRegistrationDelegation struct {
	ID            uint   `gorm:"primarykey"`
	StakingKey    []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	AddedSlot     uint64
	DepositAmount uint64
}

func (VoteRegistrationDelegation) TableName() string {
	return "vote_registration_delegation"
}
