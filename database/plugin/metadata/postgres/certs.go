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

package postgres

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GetStakeRegistrations returns stake registration certificates
func (d *MetadataStorePostgres) GetStakeRegistrations(
	stakingKey []byte,
	txn types.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	ret := []lcommon.StakeRegistrationCertificate{}
	certs := []models.StakeRegistration{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return ret, err
	}
	result := db.Where("staking_key = ?", stakingKey).
		Order("id DESC").
		Find(&certs)
	if result.Error != nil {
		return ret, result.Error
	}
	var tmpCert lcommon.StakeRegistrationCertificate
	for _, cert := range certs {
		tmpCert = lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(cert.StakingKey),
			},
		}
		ret = append(ret, tmpCert)
	}
	return ret, nil
}
