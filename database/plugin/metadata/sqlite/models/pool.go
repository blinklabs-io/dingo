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
	"net"

	"github.com/blinklabs-io/dingo/database/types"
)

type Pool struct {
	ID            uint   `gorm:"primarykey"`
	PoolKeyHash   []byte `gorm:"uniqueIndex"`
	VrfKeyHash    []byte
	Pledge        types.Uint64
	Cost          types.Uint64
	Margin        *types.Rat
	RewardAccount []byte
	Owners        []PoolRegistrationOwner
	Relays        []PoolRegistrationRelay
	Registration  []PoolRegistration
	Retirement    []PoolRetirement
}

func (p *Pool) TableName() string {
	return "pool"
}

type PoolRegistration struct {
	ID            uint `gorm:"primarykey"`
	PoolID        uint
	PoolKeyHash   []byte `gorm:"index"`
	VrfKeyHash    []byte
	Pledge        types.Uint64
	Cost          types.Uint64
	Margin        *types.Rat
	RewardAccount []byte
	Owners        []PoolRegistrationOwner
	Relays        []PoolRegistrationRelay
	MetadataUrl   string
	MetadataHash  []byte
	AddedSlot     uint64
	DepositAmount uint64
}

func (PoolRegistration) TableName() string {
	return "pool_registration"
}

type PoolRegistrationOwner struct {
	ID                 uint `gorm:"primarykey"`
	PoolRegistrationID uint
	PoolID             uint
	KeyHash            []byte
}

func (PoolRegistrationOwner) TableName() string {
	return "pool_registration_owner"
}

type PoolRegistrationRelay struct {
	ID                 uint `gorm:"primarykey"`
	PoolRegistrationID uint
	PoolID             uint
	Port               uint
	Ipv4               *net.IP
	Ipv6               *net.IP
	Hostname           string
}

func (PoolRegistrationRelay) TableName() string {
	return "pool_registration_relay"
}

type PoolRetirement struct {
	ID          uint `gorm:"primarykey"`
	PoolID      uint
	PoolKeyHash []byte `gorm:"index"`
	Epoch       uint64
	AddedSlot   uint64
}

func (PoolRetirement) TableName() string {
	return "pool_retirement"
}
