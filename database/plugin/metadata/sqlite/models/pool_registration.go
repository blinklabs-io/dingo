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
	"database/sql/driver"
	"fmt"
	"math/big"
	"net"
	"strconv"
)

type PoolRegistration struct {
	ID            uint   `gorm:"primarykey"`
	PoolKeyHash   []byte `gorm:"index"`
	VrfKeyHash    []byte
	Pledge        Uint64
	Cost          Uint64
	Margin        *Rat
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
	KeyHash            []byte
}

func (PoolRegistrationOwner) TableName() string {
	return "pool_registration_owner"
}

type PoolRegistrationRelay struct {
	ID                 uint `gorm:"primarykey"`
	PoolRegistrationID uint
	Port               uint
	Ipv4               *net.IP
	Ipv6               *net.IP
	Hostname           string
}

func (PoolRegistrationRelay) TableName() string {
	return "pool_registration_relay"
}

//nolint:recvcheck
type Rat struct {
	*big.Rat
}

func (r Rat) Value() (driver.Value, error) {
	if r.Rat == nil {
		return "", nil
	}
	return r.Rat.String(), nil
}

func (r *Rat) Scan(val any) error {
	if r.Rat == nil {
		r.Rat = new(big.Rat)
	}
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf(
			"value was not expected type, wanted string, got %T",
			val,
		)
	}
	if _, ok := r.SetString(v); !ok {
		return fmt.Errorf("failed to set big.Rat value from string: %s", v)
	}
	return nil
}

//nolint:recvcheck
type Uint64 uint64

func (u Uint64) Value() (driver.Value, error) {
	return strconv.FormatUint(uint64(u), 10), nil
}

func (u *Uint64) Scan(val any) error {
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf(
			"value was not expected type, wanted string, got %T",
			val,
		)
	}
	tmpUint, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return err
	}
	*u = Uint64(tmpUint)
	return nil
}
