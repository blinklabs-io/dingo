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

//nolint:rowserrcheck,sqlclosecheck // Cursors are explicitly closed and close errors are propagated before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type accountHistorySource struct {
	table         string
	certType      uint
	action        string
	depositColumn string
}

var accountDelegationHistorySources = []accountHistorySource{
	{"stake_delegation", uint(lcommon.CertificateTypeStakeDelegation), "", ""},
	{
		"stake_registration_delegation",
		uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		"",
		"",
	},
	{
		"stake_vote_delegation",
		uint(lcommon.CertificateTypeStakeVoteDelegation),
		"",
		"",
	},
	{
		"stake_vote_registration_delegation",
		uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
		"",
		"",
	},
}

var accountRegistrationHistorySources = []accountHistorySource{
	{
		"deregistration",
		uint(lcommon.CertificateTypeDeregistration),
		"deregistered",
		"amount",
	},
	{
		"registration",
		uint(lcommon.CertificateTypeRegistration),
		"registered",
		"deposit_amount",
	},
	{
		"stake_deregistration",
		uint(lcommon.CertificateTypeStakeDeregistration),
		"deregistered",
		"",
	},
	{
		"stake_registration",
		uint(lcommon.CertificateTypeStakeRegistration),
		"registered",
		"deposit_amount",
	},
	{
		"stake_registration_delegation",
		uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		"registered",
		"deposit_amount",
	},
	{
		"stake_vote_registration_delegation",
		uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
		"registered",
		"deposit_amount",
	},
	{
		"vote_registration_delegation",
		uint(lcommon.CertificateTypeVoteRegistrationDelegation),
		"registered",
		"deposit_amount",
	},
}

var accountWitnessTables = []string{
	"stake_registration",
	"stake_registration_delegation",
	"stake_vote_registration_delegation",
	"vote_registration_delegation",
	"registration",
	"stake_deregistration",
	"deregistration",
	"stake_delegation",
	"stake_vote_delegation",
	"vote_delegation",
}

type accountCertificatePosition struct {
	slot       uint64
	blockIndex uint64
	certIndex  uint64
}

var accountRegistrationStateTables = []string{
	"stake_registration",
	"stake_registration_delegation",
	"stake_vote_registration_delegation",
	"vote_registration_delegation",
	"registration",
}

var accountDeregistrationStateTables = []string{
	"stake_deregistration",
	"deregistration",
}

func (s *Store) GetAccountDelegationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountDelegationHistoryRow, error) {
	ret := []models.AccountDelegationHistoryRow{}
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve read DB for account delegation history: %w",
			err,
		)
	}
	query, args := accountDelegationHistoryQuery(credentialTag, stakingKey)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY added_slot ASC, block_index ASC, cert_index ASC, tx_hash ASC"
	} else {
		query += " ORDER BY added_slot DESC, block_index DESC, cert_index DESC, tx_hash DESC"
	}
	query, args = addLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query account delegation history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.AccountDelegationHistoryRow
		if err := rows.Scan(
			&row.AddedSlot,
			&row.BlockIndex,
			&row.CertIndex,
			&row.TxHash,
			&row.PoolKeyHash,
			&row.TxSlot,
			&row.BlockHash,
		); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) CountAccountDelegationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	query, args := accountDelegationHistoryQuery(credentialTag, stakingKey)
	var count int
	err = db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM ("+query+") delegation_history",
		args...,
	).Scan(&count)
	return count, err
}

func (s *Store) GetAccountRegistrationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	ret := []models.AccountRegistrationHistoryRow{}
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve read DB for account registration history: %w",
			err,
		)
	}
	query, args := accountRegistrationHistoryQuery(credentialTag, stakingKey)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY added_slot ASC, block_index ASC, cert_index ASC, tx_hash ASC, action ASC"
	} else {
		query += " ORDER BY added_slot DESC, block_index DESC, cert_index DESC, tx_hash DESC, action DESC"
	}
	query, args = addLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query account registration history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.AccountRegistrationHistoryRow
		var deposit sql.NullString
		if err := rows.Scan(
			&row.AddedSlot,
			&row.BlockIndex,
			&row.CertIndex,
			&row.TxHash,
			&row.Action,
			&deposit,
			&row.TxSlot,
			&row.BlockHash,
		); err != nil {
			return nil, err
		}
		row.Deposit, err = parseNullUint64(
			"account registration deposit",
			deposit,
		)
		if err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) CountAccountRegistrationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	query, args := accountRegistrationHistoryQuery(credentialTag, stakingKey)
	var count int
	err = db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM ("+query+") registration_history",
		args...,
	).Scan(&count)
	return count, err
}

func (s *Store) GetAccountWithdrawalHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountWithdrawalHistoryRow, error) {
	ret := make([]models.AccountWithdrawalHistoryRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query, args := withdrawalHistoryQuery(credentialTag, stakingKey)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY tx_slot ASC, block_index ASC, tx_hash ASC"
	} else {
		query += " ORDER BY tx_slot DESC, block_index DESC, tx_hash DESC"
	}
	query, args = addLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("get account withdrawal history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			row    models.AccountWithdrawalHistoryRow
			amount string
		)
		if err := rows.Scan(
			&row.TxHash,
			&amount,
			&row.TxSlot,
			&row.BlockIndex,
			&row.BlockHash,
		); err != nil {
			return nil, err
		}
		row.Amount, err = parseUint64("withdrawal amount", amount)
		if err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) CountAccountWithdrawalHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	query, args := withdrawalHistoryQuery(credentialTag, stakingKey)
	var count int
	err = db.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			"SELECT COUNT(*) FROM ("+query+") withdrawal_history",
		),
		args...,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count account withdrawal history: %w", err)
	}
	return count, nil
}

func withdrawalHistoryQuery(
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	return `
SELECT ard.tx_hash, ard.amount, tx.slot, tx.block_index, tx.block_hash
FROM account_reward_delta ard
JOIN "transaction" tx ON tx.hash = ard.tx_hash
WHERE ard.withdrawal = TRUE
  AND ard.credential_tag = ?
  AND ard.staking_key = ?`, []any{credentialTag, stakingKey}
}

func (s *Store) GetStakeRegistrationsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	ret := []lcommon.StakeRegistrationCertificate{}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return ret, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT sr.credential_tag, sr.staking_key
FROM stake_registration sr
LEFT JOIN certs c ON c.id = sr.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE sr.credential_tag = ? AND sr.staking_key = ?
ORDER BY sr.added_slot DESC, COALESCE(t.block_index, 0) DESC,
         COALESCE(c.cert_index, 0) DESC`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tag uint8
		var key []byte
		if err := rows.Scan(&tag, &key); err != nil {
			return nil, err
		}
		credentialType := uint(lcommon.CredentialTypeAddrKeyHash)
		if tag == 1 {
			credentialType = lcommon.CredentialTypeScriptHash
		}
		ret = append(ret, lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				CredType:   credentialType,
				Credential: lcommon.CredentialHash(key),
			},
		})
	}
	return ret, rows.Err()
}

func (s *Store) AccountLastWitnessSlots(
	refs []models.StakeCredentialRef,
	maxSlot uint64,
	txn types.Txn,
) (map[string]uint64, error) {
	ret := make(map[string]uint64, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	requested := make(map[string]struct{}, len(refs))
	keys := make([][]byte, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		requested[ref.MapKey()] = struct{}{}
		if _, ok := seen[string(ref.Key)]; ok {
			continue
		}
		seen[string(ref.Key)] = struct{}{}
		keys = append(keys, ref.Key)
	}
	sources := append(append([]string{}, accountWitnessTables...),
		"account_withdrawal_witness")
	for start := 0; start < len(keys); start += 400 {
		end := min(start+400, len(keys))
		for _, table := range sources {
			if err := mergeWitnessSlots(
				ctx,
				db,
				table,
				false,
				keys[start:end],
				maxSlot,
				requested,
				ret,
			); err != nil {
				return nil, err
			}
		}
		if err := mergeWitnessSlots(
			ctx,
			db,
			"account_reward_delta",
			true,
			keys[start:end],
			maxSlot,
			requested,
			ret,
		); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (s *Store) AccountsWitnessedAfterSlot(
	slot uint64,
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, len(accountWitnessTables)+2)
	args := make([]any, 0, len(accountWitnessTables)+2)
	for _, table := range accountWitnessTables {
		parts = append(parts,
			"SELECT credential_tag, staking_key FROM "+table+
				" WHERE added_slot > ?")
		args = append(args, slot)
	}
	parts = append(parts, `
SELECT credential_tag, staking_key FROM account_withdrawal_witness
WHERE added_slot > ?`)
	args = append(args, slot)
	parts = append(parts, `
SELECT credential_tag, staking_key FROM account_reward_delta
WHERE withdrawal = TRUE AND added_slot > ?`)
	args = append(args, slot)
	rows, err := db.QueryContext(ctx, `
SELECT credential_tag, staking_key
FROM (`+strings.Join(parts, " UNION ALL ")+`) witnesses
GROUP BY credential_tag, staking_key`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []models.StakeCredentialRef{}
	for rows.Next() {
		var ref models.StakeCredentialRef
		if err := rows.Scan(&ref.Tag, &ref.Key); err != nil {
			return nil, err
		}
		ret = append(ret, ref)
	}
	return ret, rows.Err()
}

func (s *Store) GetAccountsActiveAtSlot(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn types.Txn,
) (map[string]struct{}, error) {
	ret := make(map[string]struct{}, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	requested := make(map[string]models.StakeCredentialRef, len(refs))
	keys := make([][]byte, 0, len(refs))
	seenKeys := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		requested[ref.MapKey()] = ref
		if _, ok := seenKeys[string(ref.Key)]; ok {
			continue
		}
		seenKeys[string(ref.Key)] = struct{}{}
		keys = append(keys, ref.Key)
	}
	registrations := make(map[string]accountCertificatePosition, len(refs))
	deregistrations := make(
		map[string]accountCertificatePosition,
		len(refs),
	)
	for start := 0; start < len(keys); start += 400 {
		end := min(start+400, len(keys))
		chunk := keys[start:end]
		for _, table := range accountRegistrationStateTables {
			if err := mergeAccountCertificatePositions(
				ctx,
				db,
				table,
				chunk,
				slot,
				requested,
				registrations,
			); err != nil {
				return nil, err
			}
		}
		for _, table := range accountDeregistrationStateTables {
			if err := mergeAccountCertificatePositions(
				ctx,
				db,
				table,
				chunk,
				slot,
				requested,
				deregistrations,
			); err != nil {
				return nil, err
			}
		}
	}
	fallback := make([]models.StakeCredentialRef, 0)
	for _, ref := range refs {
		key := ref.MapKey()
		registration, hasRegistration := registrations[key]
		if !hasRegistration {
			fallback = append(fallback, ref)
			continue
		}
		deregistration, hasDeregistration := deregistrations[key]
		if !hasDeregistration ||
			compareCertificatePosition(registration, deregistration) > 0 {
			ret[key] = struct{}{}
		}
	}
	if len(fallback) == 0 {
		return ret, nil
	}
	everDeregistered := make(map[string]struct{}, len(fallback))
	fallbackStates := make(map[string]struct {
		createdSlot uint64
		active      bool
	}, len(fallback))
	for start := 0; start < len(fallback); start += 200 {
		end := min(start+200, len(fallback))
		predicate, args := credentialPredicate(fallback[start:end])
		rows, err := db.QueryContext(ctx, `
SELECT credential_tag, staking_key, created_slot, active
FROM account WHERE `+predicate,
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tag uint8
			var key []byte
			var state struct {
				createdSlot uint64
				active      bool
			}
			if err := rows.Scan(
				&tag,
				&key,
				&state.createdSlot,
				&state.active,
			); err != nil {
				rows.Close()
				return nil, err
			}
			fallbackStates[models.NewStakeCredentialRef(tag, key).MapKey()] = state
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		for _, table := range accountDeregistrationStateTables {
			rows, err := db.QueryContext(ctx, `
SELECT credential_tag, staking_key FROM `+table+`
WHERE `+predicate+`
GROUP BY credential_tag, staking_key`,
				args...,
			)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var tag uint8
				var key []byte
				if err := rows.Scan(&tag, &key); err != nil {
					rows.Close()
					return nil, err
				}
				everDeregistered[models.NewStakeCredentialRef(tag, key).MapKey()] = struct{}{}
			}
			if err := rows.Close(); err != nil {
				return nil, err
			}
			if err := rows.Err(); err != nil {
				return nil, err
			}
		}
	}
	for _, ref := range fallback {
		key := ref.MapKey()
		state, exists := fallbackStates[key]
		if !exists || state.createdSlot > slot {
			continue
		}
		if _, hasDeregistration := deregistrations[key]; hasDeregistration {
			continue
		}
		if _, wasDeregistered := everDeregistered[key]; !state.active &&
			!wasDeregistered {
			continue
		}
		ret[key] = struct{}{}
	}
	return ret, nil
}

func accountDelegationHistoryQuery(
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	parts := make([]string, 0, len(accountDelegationHistorySources))
	args := make([]any, 0, len(accountDelegationHistorySources)*3)
	for _, source := range accountDelegationHistorySources {
		parts = append(parts, fmt.Sprintf(`
SELECT %[1]s.added_slot AS added_slot,
       tx.block_index AS block_index,
       certs.cert_index AS cert_index,
       tx.hash AS tx_hash,
       %[1]s.pool_key_hash AS pool_key_hash,
       tx.slot AS tx_slot,
       tx.block_hash AS block_hash
FROM %[1]s
JOIN certs ON certs.id = %[1]s.certificate_id AND certs.cert_type = ?
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE %[1]s.credential_tag = ? AND %[1]s.staking_key = ?`,
			source.table,
		))
		args = append(args, source.certType, credentialTag, stakingKey)
	}
	return strings.Join(parts, " UNION ALL "), args
}

func accountRegistrationHistoryQuery(
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	parts := make([]string, 0, len(accountRegistrationHistorySources))
	args := make([]any, 0, len(accountRegistrationHistorySources)*4)
	for _, source := range accountRegistrationHistorySources {
		// A source with no deposit column selects the text literal '0':
		// that certificate type carries no deposit field at all. A source
		// with a column selects it raw, without COALESCE, so a NULL
		// survives to the caller as an unknown deposit rather than being
		// erased into zero.
		//
		// The literal is quoted because the deposit columns are TEXT
		// decimal strings in every dialect (the schema translation rewrites
		// integer and blob but leaves text alone). A bare 0 makes Postgres
		// reject the whole union with "UNION types integer and text cannot
		// be matched", and the COALESCE this replaced failed the same way
		// with "COALESCE types text and integer cannot be matched".
		deposit := "'0'"
		if source.depositColumn != "" {
			deposit = source.table + "." + source.depositColumn
		}
		parts = append(parts, fmt.Sprintf(`
SELECT %[1]s.added_slot AS added_slot,
       tx.block_index AS block_index,
       certs.cert_index AS cert_index,
       tx.hash AS tx_hash,
       ? AS action,
       %[2]s AS deposit,
       tx.slot AS tx_slot,
       tx.block_hash AS block_hash
FROM %[1]s
JOIN certs ON certs.id = %[1]s.certificate_id AND certs.cert_type = ?
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE %[1]s.credential_tag = ? AND %[1]s.staking_key = ?`,
			source.table,
			deposit,
		))
		args = append(
			args,
			source.action,
			source.certType,
			credentialTag,
			stakingKey,
		)
	}
	return strings.Join(parts, " UNION ALL "), args
}

func addLimitOffset(
	query string,
	args []any,
	limit int,
	offset int,
) (string, []any) {
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	} else if offset > 0 {
		query += " LIMIT -1"
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	return query, args
}

func mergeWitnessSlots(
	ctx context.Context,
	db queryer,
	table string,
	withdrawalsOnly bool,
	keys [][]byte,
	maxSlot uint64,
	requested map[string]struct{},
	ret map[string]uint64,
) error {
	args := make([]any, 0, len(keys)+1)
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, maxSlot)
	withdrawal := ""
	if withdrawalsOnly {
		withdrawal = "withdrawal = TRUE AND "
	}
	rows, err := db.QueryContext(ctx, `
SELECT credential_tag, staking_key, MAX(added_slot)
FROM `+table+`
WHERE `+withdrawal+`staking_key IN (`+bindPlaceholders(len(keys))+`)
  AND added_slot <= ?
GROUP BY credential_tag, staking_key`,
		args...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tag uint8
		var key []byte
		var last uint64
		if err := rows.Scan(&tag, &key, &last); err != nil {
			return err
		}
		mapKey := models.NewStakeCredentialRef(tag, key).MapKey()
		if _, ok := requested[mapKey]; !ok {
			continue
		}
		if current, ok := ret[mapKey]; !ok || last > current {
			ret[mapKey] = last
		}
	}
	return rows.Err()
}

func mergeAccountCertificatePositions(
	ctx context.Context,
	db queryer,
	table string,
	keys [][]byte,
	slot uint64,
	requested map[string]models.StakeCredentialRef,
	positions map[string]accountCertificatePosition,
) error {
	args := make([]any, 0, len(keys)+1)
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, slot)
	rows, err := db.QueryContext(ctx, `
SELECT source.credential_tag, source.staking_key, source.added_slot,
       COALESCE(tx.block_index, 0), COALESCE(c.cert_index, 0)
FROM `+table+` source
JOIN certs c ON c.id = source.certificate_id
LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE source.staking_key IN (`+bindPlaceholders(len(keys))+`)
  AND source.added_slot <= ?`,
		args...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tag uint8
		var key []byte
		var position accountCertificatePosition
		if err := rows.Scan(
			&tag,
			&key,
			&position.slot,
			&position.blockIndex,
			&position.certIndex,
		); err != nil {
			return err
		}
		mapKey := models.NewStakeCredentialRef(tag, key).MapKey()
		if _, ok := requested[mapKey]; !ok {
			continue
		}
		if current, ok := positions[mapKey]; !ok ||
			compareCertificatePosition(position, current) > 0 {
			positions[mapKey] = position
		}
	}
	return rows.Err()
}

func compareCertificatePosition(
	left accountCertificatePosition,
	right accountCertificatePosition,
) int {
	switch {
	case left.slot < right.slot:
		return -1
	case left.slot > right.slot:
		return 1
	case left.blockIndex < right.blockIndex:
		return -1
	case left.blockIndex > right.blockIndex:
		return 1
	case left.certIndex < right.certIndex:
		return -1
	case left.certIndex > right.certIndex:
		return 1
	default:
		return 0
	}
}

func credentialPredicate(refs []models.StakeCredentialRef) (string, []any) {
	parts := make([]string, len(refs))
	args := make([]any, 0, len(refs)*2)
	for i, ref := range refs {
		parts[i] = "(credential_tag = ? AND staking_key = ?)"
		args = append(args, ref.Tag, ref.Key)
	}
	return strings.Join(parts, " OR "), args
}
