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

type Pool struct {
	Margin        *types.Rat
	PoolKeyHash   []byte `gorm:"uniqueIndex"`
	VrfKeyHash    []byte
	RewardAccount []byte
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

type PoolRegistration struct {
	Margin        *types.Rat
	Pool          *Pool // Belongs-to relationship; CASCADE is defined on Pool.Registration
	MetadataUrl   string
	VrfKeyHash    []byte
	PoolKeyHash   []byte `gorm:"index"`
	RewardAccount []byte
	MetadataHash  []byte
	Owners        []PoolRegistrationOwner `gorm:"foreignKey:PoolRegistrationID;constraint:OnDelete:CASCADE"`
	Relays        []PoolRegistrationRelay `gorm:"foreignKey:PoolRegistrationID;constraint:OnDelete:CASCADE"`
	Pledge        types.Uint64
	Cost          types.Uint64
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	PoolID        uint   `gorm:"index"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
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
	PoolKeyHash   []byte `gorm:"index"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	PoolID        uint   `gorm:"index"`
	Epoch         uint64
	AddedSlot     uint64 `gorm:"index"`
}

func (PoolRetirement) TableName() string {
	return "pool_retirement"
}
