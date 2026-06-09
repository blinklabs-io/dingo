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
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

func QueryDelegationHistory(
	db *gorm.DB,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
) ([]models.AccountDelegationHistoryRow, error) {
	return QueryDelegationHistoryByCredential(
		db,
		0,
		stakingKey,
		limit,
		offset,
		order,
	)
}

func QueryDelegationHistoryByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
) ([]models.AccountDelegationHistoryRow, error) {
	ret := make([]models.AccountDelegationHistoryRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}

	query, args := delegationHistoryUnionQuery(
		db,
		credentialTag,
		stakingKey,
	)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY added_slot ASC, block_index ASC, cert_index ASC, tx_hash ASC"
	} else {
		query += " ORDER BY added_slot DESC, block_index DESC, cert_index DESC, tx_hash DESC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	if err := db.Raw(query, args...).Scan(&ret).Error; err != nil {
		return nil, fmt.Errorf("get account delegation history: %w", err)
	}
	return ret, nil
}

func CountDelegationHistory(
	db *gorm.DB,
	stakingKey []byte,
) (int, error) {
	return CountDelegationHistoryByCredential(db, 0, stakingKey)
}

func CountDelegationHistoryByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	query, args := delegationHistoryUnionQuery(
		db,
		credentialTag,
		stakingKey,
	)
	var count int64
	if err := db.Raw(
		"SELECT COUNT(*) AS count FROM ("+query+") AS delegation_history",
		args...,
	).Scan(&count).Error; err != nil {
		return 0, fmt.Errorf("count account delegation history: %w", err)
	}
	return int(count), nil
}

func delegationHistoryUnionQuery(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	tables := []string{
		"stake_delegation",
		"stake_registration_delegation",
		"stake_vote_delegation",
		"stake_vote_registration_delegation",
	}
	parts := make([]string, 0, len(tables))
	args := make([]any, 0, len(tables)*2)
	for _, table := range tables {
		certType, ok := DelegationTableCertType(table)
		if !ok {
			continue
		}
		parts = append(parts, fmt.Sprintf(
			`SELECT %[1]s.added_slot AS added_slot,
				tx.block_index AS block_index,
				certs.cert_index AS cert_index,
				tx.hash AS tx_hash,
				%[1]s.pool_key_hash AS pool_key_hash
			FROM %[1]s
			INNER JOIN certs
				ON certs.id = %[1]s.certificate_id
				AND certs.cert_type = ?
			%[2]s
				WHERE %[1]s.credential_tag = ?
					AND %[1]s.staking_key = ?`,
			table,
			transactionJoinClause(db),
		))
		args = append(
			args,
			certType,
			credentialTag,
			stakingKey,
		)
	}
	return strings.Join(parts, " UNION ALL "), args
}

func QueryRegistrationHistory(
	db *gorm.DB,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
) ([]models.AccountRegistrationHistoryRow, error) {
	return QueryRegistrationHistoryByCredential(
		db,
		0,
		stakingKey,
		limit,
		offset,
		order,
	)
}

func QueryRegistrationHistoryByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
) ([]models.AccountRegistrationHistoryRow, error) {
	ret := make([]models.AccountRegistrationHistoryRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}

	query, args := registrationHistoryUnionQuery(
		db,
		credentialTag,
		stakingKey,
	)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY added_slot ASC, block_index ASC, cert_index ASC, tx_hash ASC, action ASC"
	} else {
		query += " ORDER BY added_slot DESC, block_index DESC, cert_index DESC, tx_hash DESC, action DESC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	if err := db.Raw(query, args...).Scan(&ret).Error; err != nil {
		return nil, fmt.Errorf("get account registration history: %w", err)
	}
	return ret, nil
}

func transactionJoinClause(db *gorm.DB) string {
	if db == nil {
		return `INNER JOIN "transaction" tx ON tx.id = certs.transaction_id`
	}
	switch strings.ToLower(db.Name()) {
	case "mysql":
		return "INNER JOIN `transaction` tx ON tx.id = certs.transaction_id"
	default:
		return `INNER JOIN "transaction" tx ON tx.id = certs.transaction_id`
	}
}

func DelegationTableCertType(table string) (uint, bool) {
	switch table {
	case "stake_delegation":
		return uint(lcommon.CertificateTypeStakeDelegation), true
	case "stake_registration_delegation":
		return uint(lcommon.CertificateTypeStakeRegistrationDelegation), true
	case "stake_vote_delegation":
		return uint(lcommon.CertificateTypeStakeVoteDelegation), true
	case "stake_vote_registration_delegation":
		return uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation), true
	default:
		return 0, false
	}
}

func CountRegistrationHistory(
	db *gorm.DB,
	stakingKey []byte,
) (int, error) {
	return CountRegistrationHistoryByCredential(db, 0, stakingKey)
}

func CountRegistrationHistoryByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	query, args := registrationHistoryUnionQuery(
		db,
		credentialTag,
		stakingKey,
	)
	var count int64
	if err := db.Raw(
		"SELECT COUNT(*) AS count FROM ("+query+") AS registration_history",
		args...,
	).Scan(&count).Error; err != nil {
		return 0, fmt.Errorf("count account registration history: %w", err)
	}
	return int(count), nil
}

type registrationHistorySource struct {
	table  string
	action string
}

func registrationHistoryUnionQuery(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	sources := registrationHistorySources()
	parts := make([]string, 0, len(sources))
	args := make([]any, 0, len(sources)*3)
	for _, source := range sources {
		certType, ok := RegistrationTableCertType(source.table)
		if !ok {
			continue
		}
		parts = append(parts, fmt.Sprintf(
			`SELECT %[1]s.added_slot AS added_slot,
				tx.block_index AS block_index,
				certs.cert_index AS cert_index,
				tx.hash AS tx_hash,
				? AS action
			FROM %[1]s
			INNER JOIN certs
				ON certs.id = %[1]s.certificate_id
				AND certs.cert_type = ?
			%[2]s
				WHERE %[1]s.credential_tag = ?
					AND %[1]s.staking_key = ?`,
			source.table,
			transactionJoinClause(db),
		))
		args = append(
			args,
			source.action,
			certType,
			credentialTag,
			stakingKey,
		)
	}
	return strings.Join(parts, " UNION ALL "), args
}

func registrationHistorySources() []registrationHistorySource {
	return []registrationHistorySource{
		{table: "deregistration", action: "deregistered"},
		{table: "registration", action: "registered"},
		{table: "stake_deregistration", action: "deregistered"},
		{table: "stake_registration", action: "registered"},
		{table: "stake_registration_delegation", action: "registered"},
		{table: "stake_vote_registration_delegation", action: "registered"},
		{table: "vote_registration_delegation", action: "registered"},
	}
}

func RegistrationTableCertType(table string) (uint, bool) {
	switch table {
	case "stake_registration":
		return uint(lcommon.CertificateTypeStakeRegistration), true
	case "stake_registration_delegation":
		return uint(lcommon.CertificateTypeStakeRegistrationDelegation), true
	case "stake_vote_registration_delegation":
		return uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation), true
	case "vote_registration_delegation":
		return uint(lcommon.CertificateTypeVoteRegistrationDelegation), true
	case "registration":
		return uint(lcommon.CertificateTypeRegistration), true
	case "stake_deregistration":
		return uint(lcommon.CertificateTypeStakeDeregistration), true
	case "deregistration":
		return uint(lcommon.CertificateTypeDeregistration), true
	default:
		return 0, false
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
	case a.BlockIndex < b.BlockIndex:
		return 1
	case a.BlockIndex > b.BlockIndex:
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
	case a.BlockIndex < b.BlockIndex:
		return 1
	case a.BlockIndex > b.BlockIndex:
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
