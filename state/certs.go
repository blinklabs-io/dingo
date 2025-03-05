// Copyright 2024 Blink Labs Software
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

package state

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/state/models"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (ls *LedgerState) processTransactionCertificates(
	txn *database.Txn,
	blockPoint pcommon.Point,
	tx lcommon.Transaction,
) error {
	for _, tmpCert := range tx.Certificates() {
		certDeposit, err := ls.currentEra.CertDepositFunc(tmpCert, ls.currentPParams)
		if err != nil {
			return err
		}
		switch cert := tmpCert.(type) {
		case *lcommon.PoolRegistrationCertificate:
			tmpItem := models.PoolRegistration{
				PoolKeyHash:   cert.Operator[:],
				VrfKeyHash:    cert.VrfKeyHash[:],
				Pledge:        database.Uint64(cert.Pledge),
				Cost:          database.Uint64(cert.Cost),
				Margin:        &database.Rat{Rat: cert.Margin.Rat},
				AddedSlot:     blockPoint.Slot,
				DepositAmount: certDeposit,
			}
			if cert.PoolMetadata != nil {
				tmpItem.MetadataUrl = cert.PoolMetadata.Url
				tmpItem.MetadataHash = cert.PoolMetadata.Hash[:]
			}
			for _, owner := range cert.PoolOwners {
				tmpItem.Owners = append(
					tmpItem.Owners,
					models.PoolRegistrationOwner{
						KeyHash: owner[:],
					},
				)
			}
			for _, relay := range cert.Relays {
				tmpRelay := models.PoolRegistrationRelay{
					Ipv4: relay.Ipv4,
					Ipv6: relay.Ipv6,
				}
				if relay.Port != nil {
					tmpRelay.Port = uint(*relay.Port)
				}
				if relay.Hostname != nil {
					tmpRelay.Hostname = *relay.Hostname
				}
				tmpItem.Relays = append(
					tmpItem.Relays,
					tmpRelay,
				)
			}
			if result := txn.Metadata().Create(&tmpItem); result.Error != nil {
				return result.Error
			}
		case *lcommon.PoolRetirementCertificate:
			tmpItem := models.PoolRetirement{
				PoolKeyHash: cert.PoolKeyHash[:],
				Epoch:       uint(cert.Epoch),
				AddedSlot:   blockPoint.Slot,
			}
			if result := txn.Metadata().Create(&tmpItem); result.Error != nil {
				return result.Error
			}
		case *lcommon.StakeRegistrationCertificate:
			tmpItem := models.StakeRegistration{
				StakingKey:    cert.StakeRegistration.Credential,
				AddedSlot:     blockPoint.Slot,
				DepositAmount: certDeposit,
			}
			if result := txn.Metadata().Create(&tmpItem); result.Error != nil {
				return result.Error
			}
		case *lcommon.StakeDeregistrationCertificate:
			tmpItem := models.StakeDeregistration{
				StakingKey: cert.StakeDeregistration.Credential,
				AddedSlot:  blockPoint.Slot,
			}
			if result := txn.Metadata().Create(&tmpItem); result.Error != nil {
				return result.Error
			}
		case *lcommon.StakeDelegationCertificate:
			tmpItem := models.StakeDelegation{
				StakingKey:  cert.StakeCredential.Credential,
				PoolKeyHash: cert.PoolKeyHash[:],
				AddedSlot:   blockPoint.Slot,
			}
			if result := txn.Metadata().Create(&tmpItem); result.Error != nil {
				return result.Error
			}
		default:
			ls.config.Logger.Warn(
				fmt.Sprintf("ignoring unsupported certificate type %T", cert),
			)
		}
	}
	return nil
}
