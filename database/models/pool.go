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
	"net"

	"github.com/blinklabs-io/dingo/database/types"
)

var ErrPoolNotFound = errors.New("pool not found")

// Error 1170 (42000): BLOB/TEXT column 'staking_key' used in key specification without a key length
type Pool struct {
	Margin               *types.Rat
	PoolKeyHash          []byte `gorm:"uniqueIndex;size:28"`
	VrfKeyHash           []byte
	RewardAccount        []byte
	LatestOpCertSequence uint64
	// RewardAccountCredentialTag is the stake credential type of the pool's
	// reward account: 0 = key hash, 1 = script hash. The on-chain pool cert
	// encodes the reward_account as a 29-byte reward address (header + 28-byte
	// hash). The gouroboros library stores only the first 28 bytes in
	// RewardAccount (AddrKeyHash), discarding the header. We decode the raw
	// cert CBOR to preserve the credential type here.
	RewardAccountCredentialTag uint8 `gorm:"not null;default:0"`
	// Owners and Relays are query-only associations (no CASCADE).
	// The actual parent-child relationship is PoolRegistration -> Owners/Relays.
	// When Pool is deleted, PoolRegistrations cascade, which then cascade to Owners/Relays.
	Owners       []PoolRegistrationOwner `gorm:"foreignKey:PoolID"`
	Relays       []PoolRegistrationRelay `gorm:"foreignKey:PoolID"`
	Registration []PoolRegistration      `gorm:"foreignKey:PoolID;constraint:OnDelete:CASCADE"`
	Retirement   []PoolRetirement        `gorm:"foreignKey:PoolID;constraint:OnDelete:CASCADE"`
	ID           uint                    `gorm:"primarykey"`
	Pledge       types.Uint64
	Cost         types.Uint64
}

func (p *Pool) TableName() string {
	return "pool"
}

type PoolOpCertSequence struct {
	PoolKeyHash []byte `gorm:"uniqueIndex:idx_pool_opcert_sequence_pool_slot;size:28"`
	ID          uint   `gorm:"primarykey"`
	Slot        uint64 `gorm:"uniqueIndex:idx_pool_opcert_sequence_pool_slot;index"`
	Sequence    uint64
}

func (PoolOpCertSequence) TableName() string {
	return "pool_opcert_sequence"
}

type PoolRegistration struct {
	Margin                     *types.Rat
	Pool                       *Pool // Belongs-to relationship; CASCADE is defined on Pool.Registration
	MetadataUrl                string
	VrfKeyHash                 []byte
	PoolKeyHash                []byte `gorm:"index;size:28"`
	RewardAccount              []byte
	RewardAccountCredentialTag uint8 `gorm:"not null;default:0"`
	MetadataHash               []byte
	Owners                     []PoolRegistrationOwner `gorm:"foreignKey:PoolRegistrationID;constraint:OnDelete:CASCADE"`
	Relays                     []PoolRegistrationRelay `gorm:"foreignKey:PoolRegistrationID;constraint:OnDelete:CASCADE"`
	Pledge                     types.Uint64
	Cost                       types.Uint64
	CertificateID              uint   `gorm:"index"`
	ID                         uint   `gorm:"primarykey"`
	PoolID                     uint   `gorm:"uniqueIndex:idx_pool_reg_pool_slot"`
	AddedSlot                  uint64 `gorm:"uniqueIndex:idx_pool_reg_pool_slot"`
	DepositAmount              types.Uint64
}

func (PoolRegistration) TableName() string {
	return "pool_registration"
}

type PoolRegistrationOwner struct {
	KeyHash            []byte
	ID                 uint `gorm:"primarykey"`
	PoolRegistrationID uint `gorm:"index"`
	PoolID             uint `gorm:"index"`
}

func (PoolRegistrationOwner) TableName() string {
	return "pool_registration_owner"
}

type PoolRegistrationRelay struct {
	Ipv4               *net.IP
	Ipv6               *net.IP
	Hostname           string
	ID                 uint `gorm:"primarykey"`
	PoolRegistrationID uint `gorm:"index"`
	PoolID             uint `gorm:"index"`
	Port               uint
}

func (PoolRegistrationRelay) TableName() string {
	return "pool_registration_relay"
}

type PoolRetirement struct {
	PoolKeyHash   []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	PoolID        uint   `gorm:"index"`
	Epoch         uint64
	AddedSlot     uint64 `gorm:"index"`
}

// PoolRetirementRefund identifies a pool retiring at an epoch boundary along
// with the reward account and deposit needed to refund its POOLREAP deposit.
// It is a query result, not a persisted table.
type PoolRetirementRefund struct {
	PoolKeyHash                []byte
	RewardAccount              []byte
	RewardAccountCredentialTag uint8
	DepositAmount              types.Uint64
}

func (PoolRetirement) TableName() string {
	return "pool_retirement"
}
