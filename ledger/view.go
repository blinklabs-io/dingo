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

package ledger

import (
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type LedgerView struct {
	ls  *LedgerState
	txn *database.Txn
}

func (lv *LedgerView) NetworkId() uint {
	genesis := lv.ls.config.CardanoNodeConfig.ShelleyGenesis()
	if genesis == nil {
		// no config, definitely not mainnet
		return 0
	}
	networkName := genesis.NetworkId
	if networkName == "Mainnet" {
		return 1
	}
	return 0
}

func (lv *LedgerView) UtxoById(
	utxoId lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	utxo, err := lv.ls.db.UtxoByRef(
		utxoId.Id().Bytes(),
		utxoId.Index(),
		lv.txn,
	)
	if err != nil {
		return lcommon.Utxo{}, err
	}
	tmpOutput, err := utxo.Decode()
	if err != nil {
		return lcommon.Utxo{}, err
	}
	return lcommon.Utxo{
		Id:     utxoId,
		Output: tmpOutput,
	}, nil
}

func (lv *LedgerView) PoolRegistration(
	pkh []byte,
) ([]lcommon.PoolRegistrationCertificate, error) {
	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pkh))
	return lv.ls.db.GetPoolRegistrations(poolKeyHash, lv.txn)
}

func (lv *LedgerView) StakeRegistration(
	stakingKey []byte,
) ([]lcommon.StakeRegistrationCertificate, error) {
	// stakingKey = lcommon.NewBlake2b224(stakingKey)
	return lv.ls.db.GetStakeRegistrations(stakingKey, lv.txn)
}

// It returns the most recent active pool registration certificate
// and the epoch of any pending retirement for the given pool key hash.
func (lv *LedgerView) PoolCurrentState(
	pkh []byte,
) (*lcommon.PoolRegistrationCertificate, *uint64, error) {
	pool, err := lv.ls.db.GetPool(pkh, lv.txn)
	if err != nil {
		return nil, nil, err
	}
	var currentReg *lcommon.PoolRegistrationCertificate
	if len(pool.Registration) > 0 {
		var latestIdx int
		var latestSlot uint64
		for i, reg := range pool.Registration {
			if reg.AddedSlot >= latestSlot {
				latestSlot = reg.AddedSlot
				latestIdx = i
			}
		}
		reg := pool.Registration[latestIdx]
		tmp := lcommon.PoolRegistrationCertificate{
			CertType:   lcommon.CertificateTypePoolRegistration,
			Operator:   lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool.PoolKeyHash)),
			VrfKeyHash: lcommon.VrfKeyHash(lcommon.NewBlake2b256(pool.VrfKeyHash)),
			Pledge:     uint64(pool.Pledge),
			Cost:       uint64(pool.Cost),
		}
		if pool.Margin != nil {
			tmp.Margin = cbor.Rat{Rat: pool.Margin.Rat}
		}
		tmp.RewardAccount = lcommon.AddrKeyHash(lcommon.NewBlake2b224(pool.RewardAccount))
		for _, owner := range pool.Owners {
			tmp.PoolOwners = append(tmp.PoolOwners, lcommon.AddrKeyHash(lcommon.NewBlake2b224(owner.KeyHash)))
		}
		for _, relay := range pool.Relays {
			r := lcommon.PoolRelay{}
			if relay.Port != 0 {
				port := uint32(relay.Port) // #nosec G115
				r.Port = &port
			}
			if relay.Hostname != "" {
				r.Type = lcommon.PoolRelayTypeSingleHostName
				hostname := relay.Hostname
				r.Hostname = &hostname
			} else if relay.Ipv4 != nil || relay.Ipv6 != nil {
				r.Type = lcommon.PoolRelayTypeSingleHostAddress
				r.Ipv4 = relay.Ipv4
				r.Ipv6 = relay.Ipv6
			}
			tmp.Relays = append(tmp.Relays, r)
		}
		if reg.MetadataUrl != "" {
			tmp.PoolMetadata = &lcommon.PoolMetadata{
				Url:  reg.MetadataUrl,
				Hash: lcommon.PoolMetadataHash(lcommon.NewBlake2b256(reg.MetadataHash)),
			}
		}
		currentReg = &tmp
	}
	var pendingEpoch *uint64
	if len(pool.Retirement) > 0 {
		var latestEpoch uint64
		var found bool
		for _, r := range pool.Retirement {
			if !found || r.Epoch > latestEpoch {
				latestEpoch = r.Epoch
				found = true
			}
		}
		if found {
			pendingEpoch = &latestEpoch
		}
	}
	return currentReg, pendingEpoch, nil
}
