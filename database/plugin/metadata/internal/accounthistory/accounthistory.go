// Copyright 2026 Blink Labs Software
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

package accounthistory

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

func QueryDelegationHistory(
	db *gorm.DB,
	stakingKey []byte,
) ([]models.AccountDelegationHistoryRow, error) {
	ret := make([]models.AccountDelegationHistoryRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}

	tables := []string{
		"stake_delegation",
		"stake_registration_delegation",
		"stake_vote_delegation",
		"stake_vote_registration_delegation",
	}
	for _, table := range tables {
		var tmp []models.AccountDelegationHistoryRow
		if err := db.Table(table).
			Select(
				table+".added_slot, certs.cert_index, tx.hash AS tx_hash, "+
					table+".pool_key_hash",
			).
			Joins(
				"INNER JOIN certs ON certs.certificate_id = "+table+".id",
			).
			Joins(
				transactionJoinClause(db),
			).
			Where(
				table+".staking_key = ? AND certs.cert_type IN ?",
				stakingKey,
				DelegationCertTypes(),
			).
			Scan(&tmp).Error; err != nil {
			return nil, fmt.Errorf(
				"get account delegation history from %s: %w",
				table,
				err,
			)
		}
		ret = append(ret, tmp...)
	}

	slices.SortFunc(ret, CompareDelegationRows)
	return ret, nil
}

func QueryRegistrationHistory(
	db *gorm.DB,
	stakingKey []byte,
) ([]models.AccountRegistrationHistoryRow, error) {
	ret := make([]models.AccountRegistrationHistoryRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}

	for table, action := range RegistrationHistoryTables() {
		var tmp []models.AccountRegistrationHistoryRow
		if err := db.Table(table).
			Select(
				table+".added_slot, certs.cert_index, tx.hash AS tx_hash",
			).
			Joins(
				"INNER JOIN certs ON certs.certificate_id = "+table+".id",
			).
			Joins(
				transactionJoinClause(db),
			).
			Where(
				table+".staking_key = ? AND certs.cert_type IN ?",
				stakingKey,
				RegistrationTableCertTypes(table),
			).
			Scan(&tmp).Error; err != nil {
			return nil, fmt.Errorf(
				"get account registration history from %s: %w",
				table,
				err,
			)
		}
		for i := range tmp {
			tmp[i].Action = action
		}
		ret = append(ret, tmp...)
	}

	slices.SortFunc(ret, CompareRegistrationRows)
	return ret, nil
}

func transactionJoinClause(db *gorm.DB) string {
	switch strings.ToLower(db.Name()) {
	case "mysql":
		return "INNER JOIN `transaction` tx ON tx.id = certs.transaction_id"
	default:
		return `INNER JOIN "transaction" tx ON tx.id = certs.transaction_id`
	}
}

func DelegationCertTypes() []uint {
	return []uint{
		uint(lcommon.CertificateTypeStakeDelegation),
		uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		uint(lcommon.CertificateTypeStakeVoteDelegation),
		uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
	}
}

func RegistrationHistoryTables() map[string]string {
	return map[string]string{
		"deregistration":                     "deregistered",
		"registration":                       "registered",
		"stake_deregistration":               "deregistered",
		"stake_registration":                 "registered",
		"stake_registration_delegation":      "registered",
		"stake_vote_registration_delegation": "registered",
		"vote_registration_delegation":       "registered",
	}
}

func RegistrationTableCertTypes(
	table string,
) []uint {
	switch table {
	case "stake_registration":
		return []uint{
			uint(lcommon.CertificateTypeStakeRegistration),
		}
	case "stake_registration_delegation":
		return []uint{
			uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		}
	case "stake_vote_registration_delegation":
		return []uint{
			uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
		}
	case "vote_registration_delegation":
		return []uint{
			uint(lcommon.CertificateTypeVoteRegistrationDelegation),
		}
	case "registration":
		return []uint{
			uint(lcommon.CertificateTypeRegistration),
		}
	case "stake_deregistration":
		return []uint{
			uint(lcommon.CertificateTypeStakeDeregistration),
		}
	case "deregistration":
		return []uint{
			uint(lcommon.CertificateTypeDeregistration),
		}
	default:
		return nil
	}
}

func CompareDelegationRows(
	a, b models.AccountDelegationHistoryRow,
) int {
	switch {
	case a.AddedSlot < b.AddedSlot:
		return 1
	case a.AddedSlot > b.AddedSlot:
		return -1
	case a.CertIndex < b.CertIndex:
		return 1
	case a.CertIndex > b.CertIndex:
		return -1
	case bytes.Compare(a.TxHash, b.TxHash) < 0:
		return 1
	case bytes.Compare(a.TxHash, b.TxHash) > 0:
		return -1
	default:
		return 0
	}
}

func CompareRegistrationRows(
	a, b models.AccountRegistrationHistoryRow,
) int {
	switch {
	case a.AddedSlot < b.AddedSlot:
		return 1
	case a.AddedSlot > b.AddedSlot:
		return -1
	case a.CertIndex < b.CertIndex:
		return 1
	case a.CertIndex > b.CertIndex:
		return -1
	case bytes.Compare(a.TxHash, b.TxHash) < 0:
		return 1
	case bytes.Compare(a.TxHash, b.TxHash) > 0:
		return -1
	case a.Action < b.Action:
		return 1
	case a.Action > b.Action:
		return -1
	default:
		return 0
	}
}
