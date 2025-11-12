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
	Owners        []PoolRegistrationOwner
	Relays        []PoolRegistrationRelay
	Registration  []PoolRegistration
	Retirement    []PoolRetirement
	ID            uint `gorm:"primarykey"`
	Pledge        types.Uint64
	Cost          types.Uint64
}

func (p *Pool) TableName() string {
	return "pool"
}

type PoolRegistration struct {
	Margin        *types.Rat
	MetadataUrl   string
	VrfKeyHash    []byte
	PoolKeyHash   []byte `gorm:"index"`
	RewardAccount []byte
	MetadataHash  []byte
	Owners        []PoolRegistrationOwner
	Relays        []PoolRegistrationRelay
	Pledge        types.Uint64
	Cost          types.Uint64
	CertificateID uint `gorm:"index"`
	ID            uint `gorm:"primarykey"`
	PoolID        uint `gorm:"index"`
	AddedSlot     uint64
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
	AddedSlot     uint64
}

func (PoolRetirement) TableName() string {
	return "pool_retirement"
}
