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

package database

import (
	"net"

	"github.com/blinklabs-io/dingo/database/types"
)

// Pool contains the current pool state plus historical registration/retirement entries
type Pool struct {
	PoolKeyHash   []byte
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

type PoolRegistration struct {
	MetadataUrl  string
	MetadataHash []byte
	AddedSlot    uint64
}

type PoolRegistrationOwner struct {
	KeyHash []byte
}

type PoolRegistrationRelay struct {
	Port     uint
	Ipv4     *net.IP
	Ipv6     *net.IP
	Hostname string
}

type PoolRetirement struct {
	Epoch uint64
}

// GetPool returns a pool by its key hash
func (d *Database) GetPool(
	pkh []byte,
	txn *Txn,
) (Pool, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	modelPool, err := d.metadata.GetPool(pkh, txn.Metadata())
	if err != nil {
		return Pool{}, err
	}
	// Map model to database-level struct
	ret := Pool{
		PoolKeyHash:   modelPool.PoolKeyHash,
		VrfKeyHash:    modelPool.VrfKeyHash,
		Pledge:        modelPool.Pledge,
		Cost:          modelPool.Cost,
		Margin:        modelPool.Margin,
		RewardAccount: modelPool.RewardAccount,
	}
	// Owners
	for _, o := range modelPool.Owners {
		ret.Owners = append(ret.Owners, PoolRegistrationOwner{KeyHash: o.KeyHash})
	}
	// Relays
	for _, r := range modelPool.Relays {
		ret.Relays = append(ret.Relays, PoolRegistrationRelay{
			Port:     r.Port,
			Ipv4:     r.Ipv4,
			Ipv6:     r.Ipv6,
			Hostname: r.Hostname,
		})
	}
	// Registration entries
	for _, reg := range modelPool.Registration {
		ret.Registration = append(ret.Registration, PoolRegistration{
			MetadataUrl:  reg.MetadataUrl,
			MetadataHash: reg.MetadataHash,
			AddedSlot:    reg.AddedSlot,
		})
	}
	// Retirement entries
	for _, r := range modelPool.Retirement {
		ret.Retirement = append(ret.Retirement, PoolRetirement{Epoch: r.Epoch})
	}
	return ret, nil
}
