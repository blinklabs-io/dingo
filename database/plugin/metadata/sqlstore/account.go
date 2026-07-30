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

//nolint:gosec,rowserrcheck,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

const sqliteAccountColumns = "staking_key, credential_tag, pool, drep, id, " +
	"added_slot, created_slot, certificate_id, reward, drep_type, active, " +
	"expiration_epoch"

func (s *Store) CreateAccount(
	txn types.Txn,
	account *models.Account,
) error {
	if account == nil {
		return errors.New("create account: account is nil")
	}
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			q, err := s.sqliteQueries(db)
			if err != nil {
				return err
			}
			params, err := accountParams(account)
			if err != nil {
				return err
			}
			id, err := q.CreateAccount(
				context.Background(),
				sqlitequery.CreateAccountParams(params),
			)
			if err != nil {
				return fmt.Errorf("create account: %w", err)
			}
			account.ID = uint(id)
			account.Active = params.Active.Bool
			return s.refreshRewardLiveStakeAggregate(
				db,
				models.NewStakeCredentialRef(
					account.CredentialTag,
					account.StakingKey,
				),
				account.AddedSlot,
			)
		},
	)
}

func (s *Store) ImportAccount(
	account *models.Account,
	txn types.Txn,
) error {
	if account == nil {
		return errors.New("import account: account is nil")
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	params, err := accountParams(account)
	if err != nil {
		return err
	}
	id, err := q.ImportAccount(
		context.Background(),
		sqlitequery.ImportAccountParams(params),
	)
	if err != nil {
		return fmt.Errorf("import account: %w", err)
	}
	account.ID = uint(id)
	return nil
}

func (s *Store) GetAccountByCredential(
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Account, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	params := sqlitequery.GetActiveAccountByCredentialParams{
		CredentialTag: int64(credentialTag),
		StakingKey:    stakeKey,
	}
	var row sqlitequery.Account
	if includeInactive {
		row, err = q.GetAccountByCredential(
			context.Background(),
			sqlitequery.GetAccountByCredentialParams(params),
		)
	} else {
		row, err = q.GetActiveAccountByCredential(
			context.Background(),
			params,
		)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return accountFromSQLite(row)
}

func (s *Store) GetAccountsByCredential(
	refs []models.StakeCredentialRef,
	includeInactive bool,
	txn types.Txn,
) (map[string]*models.Account, error) {
	ret := make(map[string]*models.Account, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	chunkSize := s.dialect.ParameterLimit() / 2
	for start := 0; start < len(refs); start += chunkSize {
		end := min(start+chunkSize, len(refs))
		chunk := refs[start:end]
		predicates := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*2)
		for _, ref := range chunk {
			predicates = append(
				predicates,
				"(credential_tag = ? AND staking_key = ?)",
			)
			args = append(args, ref.Tag, ref.Key)
		}
		query := "SELECT " + sqliteAccountColumns + " FROM account WHERE (" +
			strings.Join(predicates, " OR ") + ")"
		if !includeInactive {
			query += " AND active = TRUE"
		}
		rows, err := db.QueryContext(
			context.Background(),
			s.dialect.Rebind(query),
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			row, err := scanSQLiteAccount(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			account, err := accountFromSQLite(row)
			if err != nil {
				rows.Close()
				return nil, err
			}
			key := models.NewStakeCredentialRef(
				account.CredentialTag,
				account.StakingKey,
			).MapKey()
			ret[key] = account
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (s *Store) RenewAccountExpirations(
	refs []models.StakeCredentialRef,
	expirationEpoch uint64,
	txn types.Txn,
) error {
	if len(refs) == 0 {
		return nil
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	expiration, err := checkedInt64(expirationEpoch)
	if err != nil {
		return err
	}
	chunkSize := (s.dialect.ParameterLimit() - 1) / 2
	for start := 0; start < len(refs); start += chunkSize {
		end := min(start+chunkSize, len(refs))
		predicates := make([]string, 0, end-start)
		args := make([]any, 0, 1+(end-start)*2)
		args = append(args, expiration)
		for _, ref := range refs[start:end] {
			predicates = append(
				predicates,
				"(credential_tag = ? AND staking_key = ?)",
			)
			args = append(args, ref.Tag, ref.Key)
		}
		query := "UPDATE account SET expiration_epoch = ? WHERE " +
			strings.Join(predicates, " OR ")
		if _, err := db.ExecContext(
			context.Background(),
			s.dialect.Rebind(query),
			args...,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) StampAllActiveAccountExpirations(
	expirationEpoch uint64,
	txn types.Txn,
) (int64, error) {
	expiration, err := checkedInt64(expirationEpoch)
	if err != nil {
		return 0, err
	}
	var affected int64
	err = s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			if _, err := db.ExecContext(context.Background(), `
INSERT INTO account_inactivity_activation (credential_tag, staking_key)
SELECT credential_tag, staking_key
FROM account
WHERE active = TRUE
ON CONFLICT (credential_tag, staking_key) DO NOTHING`); err != nil {
				return err
			}
			result, err := db.ExecContext(context.Background(), `
UPDATE account SET expiration_epoch = ? WHERE active = TRUE`,
				expiration,
			)
			if err != nil {
				return err
			}
			affected, err = result.RowsAffected()
			return err
		},
	)
	return affected, err
}

func (s *Store) AccountInactivityActivationMembership(
	refs []models.StakeCredentialRef,
	txn types.Txn,
) (map[string]struct{}, error) {
	ret := make(map[string]struct{})
	if len(refs) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	chunkSize := s.dialect.ParameterLimit() / 2
	for start := 0; start < len(refs); start += chunkSize {
		end := min(start+chunkSize, len(refs))
		predicates := make([]string, 0, end-start)
		args := make([]any, 0, (end-start)*2)
		for _, ref := range refs[start:end] {
			predicates = append(
				predicates,
				"(credential_tag = ? AND staking_key = ?)",
			)
			args = append(args, ref.Tag, ref.Key)
		}
		rows, err := db.QueryContext(
			context.Background(),
			s.dialect.Rebind(`
SELECT credential_tag, staking_key
FROM account_inactivity_activation
WHERE `+strings.Join(predicates, " OR ")),
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tag int64
			var key []byte
			if err := rows.Scan(&tag, &key); err != nil {
				rows.Close()
				return nil, err
			}
			ref := models.NewStakeCredentialRef(uint8(tag), key)
			ret[ref.MapKey()] = struct{}{}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (s *Store) ResetAccountExpirationActivation(
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	ret := []models.StakeCredentialRef{}
	err := s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			rows, err := db.QueryContext(context.Background(), `
SELECT credential_tag, staking_key
FROM account_inactivity_activation`)
			if err != nil {
				return err
			}
			for rows.Next() {
				var tag int64
				var key []byte
				if err := rows.Scan(&tag, &key); err != nil {
					rows.Close()
					return err
				}
				ret = append(
					ret,
					models.NewStakeCredentialRef(uint8(tag), key),
				)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if _, err := db.ExecContext(context.Background(), `
UPDATE account
SET expiration_epoch = 0
WHERE EXISTS (
    SELECT 1 FROM account_inactivity_activation activation
    WHERE activation.credential_tag = account.credential_tag
      AND activation.staking_key = account.staking_key
)`); err != nil {
				return err
			}
			_, err = db.ExecContext(
				context.Background(),
				"DELETE FROM account_inactivity_activation",
			)
			return err
		},
	)
	return ret, err
}

func (s *Store) GetActiveAccountCredentials(
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActiveAccountCredentials: resolve db: %w",
			err,
		)
	}
	rows, err := db.QueryContext(context.Background(), `
SELECT credential_tag, staking_key FROM account WHERE active = TRUE`)
	if err != nil {
		return nil, fmt.Errorf("GetActiveAccountCredentials: %w", err)
	}
	defer rows.Close()
	ret := []models.StakeCredentialRef{}
	for rows.Next() {
		var tag int64
		var key []byte
		if err := rows.Scan(&tag, &key); err != nil {
			return nil, err
		}
		ret = append(ret, models.NewStakeCredentialRef(uint8(tag), key))
	}
	return ret, rows.Err()
}

func (s *Store) DeactivateAccounts(
	txn types.Txn,
	refs []models.StakeCredentialRef,
) error {
	if len(refs) == 0 {
		return nil
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("DeactivateAccounts: resolve db: %w", err)
	}
	chunkSize := s.dialect.ParameterLimit() / 2
	for start := 0; start < len(refs); start += chunkSize {
		end := min(start+chunkSize, len(refs))
		predicates := make([]string, 0, end-start)
		args := make([]any, 0, (end-start)*2)
		for _, ref := range refs[start:end] {
			predicates = append(
				predicates,
				"(credential_tag = ? AND staking_key = ?)",
			)
			args = append(args, ref.Tag, ref.Key)
		}
		if _, err := db.ExecContext(
			context.Background(),
			s.dialect.Rebind(`
UPDATE account SET active = FALSE
WHERE active = TRUE AND (`+strings.Join(predicates, " OR ")+")"),
			args...,
		); err != nil {
			return fmt.Errorf("DeactivateAccounts: %w", err)
		}
	}
	return nil
}

func (s *Store) GetAccountSumsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (models.AccountSums, error) {
	var ret models.AccountSums
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return ret, fmt.Errorf("resolve read DB for account sums: %w", err)
	}
	sum := func(query string, args ...any) (uint64, error) {
		var value int64
		if err := db.QueryRowContext(
			context.Background(),
			query,
			args...,
		).Scan(&value); err != nil {
			return 0, err
		}
		return uint64(value), nil
	}
	ret.WithdrawalsSum, err = sum(`
SELECT CAST(COALESCE(SUM(CAST(amount AS INTEGER)), 0) AS INTEGER)
FROM account_reward_delta
WHERE withdrawal = TRUE AND credential_tag = ? AND staking_key = ?`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return models.AccountSums{}, fmt.Errorf(
			"query account sums: sum withdrawals: %w",
			err,
		)
	}
	ret.ReservesSum, err = sum(`
SELECT CAST(COALESCE(SUM(CAST(reward.amount AS INTEGER)), 0) AS INTEGER)
FROM move_instantaneous_rewards_reward reward
JOIN move_instantaneous_rewards mir ON mir.id = reward.mir_id
WHERE mir.pot = 0 AND reward.credential_tag = ?
  AND reward.credential = ?`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return models.AccountSums{}, fmt.Errorf(
			"query account sums: sum reserves MIR: %w",
			err,
		)
	}
	ret.TreasurySum, err = sum(`
SELECT CAST(COALESCE(SUM(CAST(reward.amount AS INTEGER)), 0) AS INTEGER)
FROM move_instantaneous_rewards_reward reward
JOIN move_instantaneous_rewards mir ON mir.id = reward.mir_id
WHERE mir.pot = 1 AND reward.credential_tag = ?
  AND reward.credential = ?`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return models.AccountSums{}, fmt.Errorf(
			"query account sums: sum treasury MIR: %w",
			err,
		)
	}
	return ret, nil
}

func (s *Store) RestoreAccountStateAtSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			rows, err := db.QueryContext(context.Background(), `
SELECT credential_tag, staking_key, created_slot
FROM account WHERE added_slot > ?`,
				slot,
			)
			if err != nil {
				return err
			}
			type affectedAccount struct {
				tag         uint8
				key         []byte
				createdSlot uint64
			}
			accounts := []affectedAccount{}
			for rows.Next() {
				var account affectedAccount
				if err := rows.Scan(
					&account.tag,
					&account.key,
					&account.createdSlot,
				); err != nil {
					rows.Close()
					return err
				}
				accounts = append(accounts, account)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			refs := make([]models.StakeCredentialRef, 0, len(accounts))
			for _, account := range accounts {
				registration, hasRegistration, err := latestAccountEvent(
					db,
					accountRegistrationStateTables,
					account.tag,
					account.key,
					slot,
					"",
				)
				if err != nil {
					return err
				}
				ref := models.NewStakeCredentialRef(account.tag, account.key)
				refs = append(refs, ref)
				if !hasRegistration {
					if account.createdSlot > slot {
						if _, err := db.ExecContext(context.Background(), `
DELETE FROM account
WHERE credential_tag = ? AND staking_key = ?`,
							account.tag,
							account.key,
						); err != nil {
							return err
						}
					} else {
						if _, err := db.ExecContext(context.Background(), `
UPDATE account SET added_slot = ?
WHERE credential_tag = ? AND staking_key = ?`,
							slot,
							account.tag,
							account.key,
						); err != nil {
							return err
						}
					}
					continue
				}
				deregistration, hasDeregistration, err := latestAccountEvent(
					db,
					accountDeregistrationStateTables,
					account.tag,
					account.key,
					slot,
					"",
				)
				if err != nil {
					return err
				}
				pool, hasPool, err := latestAccountEvent(
					db,
					[]string{
						"stake_delegation",
						"stake_registration_delegation",
						"stake_vote_delegation",
						"stake_vote_registration_delegation",
					},
					account.tag,
					account.key,
					slot,
					"pool_key_hash",
				)
				if err != nil {
					return err
				}
				drep, hasDrep, err := latestAccountEvent(
					db,
					[]string{
						"vote_delegation",
						"vote_registration_delegation",
						"stake_vote_delegation",
						"stake_vote_registration_delegation",
					},
					account.tag,
					account.key,
					slot,
					"drep",
				)
				if err != nil {
					return err
				}
				active := !hasDeregistration ||
					compareCertificatePosition(
						registration.position,
						deregistration.position,
					) > 0
				if !active || hasDeregistration &&
					hasPool &&
					compareCertificatePosition(
						deregistration.position,
						pool.position,
					) > 0 {
					pool.value = nil
				}
				if !active || hasDeregistration &&
					hasDrep &&
					compareCertificatePosition(
						deregistration.position,
						drep.position,
					) > 0 {
					drep.value = nil
					drep.valueType = 0
				}
				latestSlot := registration.position.slot
				for _, event := range []accountRestoreEvent{
					deregistration,
					pool,
					drep,
				} {
					if event.position.slot > latestSlot {
						latestSlot = event.position.slot
					}
				}
				if _, err := db.ExecContext(context.Background(), `
UPDATE account
SET pool = ?, drep = ?, drep_type = ?, active = ?, added_slot = ?
WHERE credential_tag = ? AND staking_key = ?`,
					nullBytes(pool.value),
					nullBytes(drep.value),
					drep.valueType,
					active,
					latestSlot,
					account.tag,
					account.key,
				); err != nil {
					return err
				}
			}
			return s.refreshRewardLiveStakeRefs(db, refs, slot)
		},
	)
}

type accountRestoreEvent struct {
	position  accountCertificatePosition
	value     []byte
	valueType uint64
}

func latestAccountEvent(
	db queryer,
	tables []string,
	tag uint8,
	key []byte,
	slot uint64,
	valueColumn string,
) (accountRestoreEvent, bool, error) {
	var latest accountRestoreEvent
	found := false
	for _, table := range tables {
		valueExpr := "NULL"
		typeExpr := "0"
		if valueColumn != "" {
			valueExpr = "event." + valueColumn
		}
		if valueColumn == "drep" {
			typeExpr = "event.drep_type"
		}
		var event accountRestoreEvent
		err := db.QueryRowContext(context.Background(), `
SELECT event.added_slot, COALESCE(tx.block_index, 0),
       COALESCE(certs.cert_index, 0), `+valueExpr+`, `+typeExpr+`
FROM `+table+` event
LEFT JOIN certs ON certs.id = event.certificate_id
LEFT JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE event.credential_tag = ? AND event.staking_key = ?
  AND event.added_slot <= ?
ORDER BY event.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
         COALESCE(certs.cert_index, 0) DESC, event.id DESC
LIMIT 1`,
			tag,
			key,
			slot,
		).Scan(
			&event.position.slot,
			&event.position.blockIndex,
			&event.position.certIndex,
			&event.value,
			&event.valueType,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return accountRestoreEvent{}, false, err
		}
		if !found || compareCertificatePosition(
			event.position,
			latest.position,
		) > 0 {
			latest = event
			found = true
		}
	}
	return latest, found, nil
}

func (s *Store) AddAccountRewardByCredential(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn types.Txn,
) error {
	if amount == 0 {
		return nil
	}
	if sourceHash == nil {
		sourceHash = []byte{}
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			var accountID int64
			var reward sql.NullString
			err := db.QueryRowContext(context.Background(), `
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`,
				credentialTag,
				stakeKey,
			).Scan(&accountID, &reward)
			if errors.Is(err, sql.ErrNoRows) {
				return models.ErrAccountNotFound
			}
			if err != nil {
				return err
			}
			current, err := parseNullUint64("account reward", reward)
			if err != nil {
				return err
			}
			if current > ^uint64(0)-amount {
				return fmt.Errorf(
					"account reward overflow for stake key %x",
					stakeKey,
				)
			}
			result, err := db.ExecContext(context.Background(), `
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal
) VALUES (?, ?, ?, ?, NULL, ?, FALSE)
ON CONFLICT (
    withdrawal, tx_hash, credential_tag, staking_key, added_slot
) DO NOTHING`,
				stakeKey,
				credentialTag,
				sourceHash,
				strconv.FormatUint(amount, 10),
				slotValue,
			)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				return nil
			}
			result, err = db.ExecContext(context.Background(), `
UPDATE account SET reward = ? WHERE id = ?`,
				strconv.FormatUint(current+amount, 10),
				accountID,
			)
			if err != nil {
				return err
			}
			affected, err = result.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				return models.ErrAccountNotFound
			}
			return s.refreshRewardLiveStakeAggregate(
				db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				slot,
			)
		},
	)
}

func (s *Store) ApplyAccountRewardWithdrawal(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	txHash []byte,
	txn types.Txn,
) error {
	if amount == 0 {
		return nil
	}
	if txHash == nil {
		txHash = []byte{}
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			var accountID int64
			var reward sql.NullString
			err := db.QueryRowContext(context.Background(), `
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`,
				credentialTag,
				stakeKey,
			).Scan(&accountID, &reward)
			if errors.Is(err, sql.ErrNoRows) {
				return models.ErrAccountNotFound
			}
			if err != nil {
				return err
			}
			var exists bool
			if err := db.QueryRowContext(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM account_reward_delta
    WHERE withdrawal = TRUE AND tx_hash = ?
      AND credential_tag = ? AND staking_key = ?
)`,
				txHash,
				credentialTag,
				stakeKey,
			).Scan(&exists); err != nil {
				return err
			}
			if exists {
				return nil
			}
			current, err := parseNullUint64("account reward", reward)
			if err != nil {
				return err
			}
			if _, err := db.ExecContext(context.Background(), `
UPDATE account SET reward = '0' WHERE id = ?`,
				accountID,
			); err != nil {
				return err
			}
			result, err := db.ExecContext(context.Background(), `
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal
) VALUES (?, ?, ?, ?, ?, ?, TRUE)
ON CONFLICT (
    withdrawal, tx_hash, credential_tag, staking_key, added_slot
) DO NOTHING`,
				stakeKey,
				credentialTag,
				txHash,
				strconv.FormatUint(amount, 10),
				strconv.FormatUint(current, 10),
				slotValue,
			)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				return nil
			}
			return s.refreshRewardLiveStakeAggregate(
				db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				slot,
			)
		},
	)
}

func (s *Store) DeleteAccountRewardsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			rows, err := db.QueryContext(context.Background(), `
SELECT staking_key, credential_tag, amount, previous_reward, withdrawal
FROM account_reward_delta
WHERE added_slot > ?
ORDER BY added_slot DESC, id DESC`,
				slotValue,
			)
			if err != nil {
				return err
			}
			type delta struct {
				key        []byte
				tag        uint8
				amount     uint64
				previous   uint64
				withdrawal bool
			}
			deltas := []delta{}
			for rows.Next() {
				var item delta
				var tag int64
				var amount string
				var previous sql.NullString
				if err := rows.Scan(
					&item.key,
					&tag,
					&amount,
					&previous,
					&item.withdrawal,
				); err != nil {
					rows.Close()
					return err
				}
				item.tag = uint8(tag)
				item.amount, err = parseUint64("reward delta amount", amount)
				if err != nil {
					rows.Close()
					return err
				}
				item.previous, err = parseNullUint64(
					"reward delta previous reward",
					previous,
				)
				if err != nil {
					rows.Close()
					return err
				}
				deltas = append(deltas, item)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}
			refs := make(map[string]models.StakeCredentialRef)
			for _, item := range deltas {
				var id int64
				var reward sql.NullString
				err := db.QueryRowContext(context.Background(), `
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ?`,
					item.tag,
					item.key,
				).Scan(&id, &reward)
				ref := models.NewStakeCredentialRef(item.tag, item.key)
				refs[ref.MapKey()] = ref
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				value := item.previous
				if !item.withdrawal {
					current, err := parseNullUint64(
						"account reward",
						reward,
					)
					if err != nil {
						return err
					}
					if current < item.amount {
						return fmt.Errorf(
							"account reward rollback underflow for stake key %x",
							item.key,
						)
					}
					value = current - item.amount
				}
				if _, err := db.ExecContext(context.Background(), `
UPDATE account SET reward = ? WHERE id = ?`,
					strconv.FormatUint(value, 10),
					id,
				); err != nil {
					return err
				}
			}
			if _, err := db.ExecContext(context.Background(), `
DELETE FROM account_reward_delta WHERE added_slot > ?`,
				slotValue,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(context.Background(), `
DELETE FROM account_withdrawal_witness WHERE added_slot > ?`,
				slotValue,
			); err != nil {
				return err
			}
			for _, ref := range refs {
				if err := s.refreshRewardLiveStakeAggregate(
					db,
					ref,
					slot,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type accountQueryParams struct {
	StakingKey      []byte
	CredentialTag   int64
	Pool            []byte
	Drep            []byte
	AddedSlot       sql.NullInt64
	CreatedSlot     int64
	CertificateID   sql.NullInt64
	Reward          sql.NullString
	DrepType        sql.NullInt64
	Active          sql.NullBool
	ExpirationEpoch sql.NullInt64
}

func accountParams(account *models.Account) (accountQueryParams, error) {
	addedSlot, err := checkedInt64(account.AddedSlot)
	if err != nil {
		return accountQueryParams{}, err
	}
	createdSlot, err := checkedInt64(account.CreatedSlot)
	if err != nil {
		return accountQueryParams{}, err
	}
	expirationEpoch, err := checkedInt64(account.ExpirationEpoch)
	if err != nil {
		return accountQueryParams{}, err
	}
	drepType, err := checkedInt64(account.DrepType)
	if err != nil {
		return accountQueryParams{}, err
	}
	return accountQueryParams{
		StakingKey:    account.StakingKey,
		CredentialTag: int64(account.CredentialTag),
		Pool:          account.Pool,
		Drep:          account.Drep,
		AddedSlot:     sql.NullInt64{Int64: addedSlot, Valid: true},
		CreatedSlot:   createdSlot,
		CertificateID: sql.NullInt64{
			Int64: int64(account.CertificateID),
			Valid: true,
		},
		Reward: sql.NullString{
			String: decimalUint64(account.Reward),
			Valid:  true,
		},
		DrepType: sql.NullInt64{
			Int64: drepType,
			Valid: true,
		},
		Active: sql.NullBool{
			Bool:  account.Active,
			Valid: true,
		},
		ExpirationEpoch: sql.NullInt64{
			Int64: expirationEpoch,
			Valid: true,
		},
	}, nil
}

func accountFromSQLite(row sqlitequery.Account) (*models.Account, error) {
	reward := uint64(0)
	var err error
	if row.Reward.Valid {
		reward, err = parseUint64("account reward", row.Reward.String)
		if err != nil {
			return nil, err
		}
	}
	return &models.Account{
		StakingKey:      row.StakingKey,
		CredentialTag:   uint8(row.CredentialTag),
		Pool:            row.Pool,
		Drep:            row.Drep,
		ID:              uint(row.ID),
		AddedSlot:       uint64(row.AddedSlot.Int64),
		CreatedSlot:     uint64(row.CreatedSlot),
		CertificateID:   uint(row.CertificateID.Int64),
		Reward:          types.Uint64(reward),
		DrepType:        uint64(row.DrepType.Int64),
		Active:          row.Active.Bool,
		ExpirationEpoch: uint64(row.ExpirationEpoch.Int64),
	}, nil
}

func scanSQLiteAccount(rows *sql.Rows) (sqlitequery.Account, error) {
	var row sqlitequery.Account
	err := rows.Scan(
		&row.StakingKey,
		&row.CredentialTag,
		&row.Pool,
		&row.Drep,
		&row.ID,
		&row.AddedSlot,
		&row.CreatedSlot,
		&row.CertificateID,
		&row.Reward,
		&row.DrepType,
		&row.Active,
		&row.ExpirationEpoch,
	)
	return row, err
}
