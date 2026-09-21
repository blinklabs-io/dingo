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
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
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
		txn,
		func(db queryer, ctx context.Context) error {
			q := s.operationalQueries(db)
			params, err := accountParams(account)
			if err != nil {
				return err
			}
			id, err := q.CreateAccount(
				ctx,
				sqlitequery.CreateAccountParams(params),
			)
			if err != nil {
				return fmt.Errorf("create account: %w", err)
			}
			account.ID = uint(id)
			account.Active = params.Active.Bool
			return s.refreshRewardLiveStakeAggregate(
				ctx,
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

// ImportAccount writes a snapshot-imported or genesis-delegated account row
// together with the baseline a later rollback restores it to. Both writes share
// one transaction: an account row committed without its baseline leaves
// RestoreAccountStateAtSlot deriving the pre-fix state for that credential, and
// nothing rewrites the baseline afterwards unless the account is imported
// again.
func (s *Store) ImportAccount(
	account *models.Account,
	txn types.Txn,
) error {
	if account == nil {
		return errors.New("import account: account is nil")
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			params, err := accountParams(account)
			if err != nil {
				return err
			}
			id, err := s.operationalQueries(db).ImportAccount(
				ctx,
				sqlitequery.ImportAccountParams(params),
			)
			if err != nil {
				return fmt.Errorf("import account: %w", err)
			}
			if err := writeAccountImportBaseline(ctx, db, account); err != nil {
				return fmt.Errorf("import account: %w", err)
			}
			account.ID = uint(id)
			return nil
		},
	)
}

// accountImportBaseline is the account state a Mithril snapshot import or a
// Shelley genesis stake delegation established. It stands in for the
// registration certificate this database does not hold, so a rollback can
// restore the pre-certificate state instead of keeping the state a rolled-away
// certificate wrote.
type accountImportBaseline struct {
	position accountCertificatePosition
	pool     []byte
	drep     []byte
	drepType uint64
	active   bool
}

// requireAccountBaselineTransaction refuses a baseline write issued on the
// autocommit handle. A baseline is only meaningful in the same transaction as
// the account row it describes, so a caller that resolved its handle through
// dbFromTxn with a nil txn would commit the two independently and leave a
// rollback deriving state that contradicts the account row. Failing here turns
// that split into an error rather than silent divergence.
func requireAccountBaselineTransaction(db queryer) error {
	for {
		if _, ok := db.(*sql.Tx); ok {
			return nil
		}
		// Both wrapper types unwrap the same way (a queryer field holding the
		// handle underneath); countingQueryer has to be checked here for the
		// same reason stmtForQueryer in prepared_stmt.go does: whenever
		// Config.PromRegistry is set, Store.instrumentedQueryer wraps every
		// handle it hands out in countingQueryer, including the *sql.Tx a
		// real write transaction passes down to this check. Without this
		// case, a real write transaction's *sql.Tx would never be seen
		// (countingQueryer, not dialectQueryer, is the outermost type in the
		// sqlite case), and this would reject every account-baseline write
		// as "outside a write transaction" on a metrics-enabled Store.
		if wrapped, ok := db.(countingQueryer); ok {
			db = wrapped.queryer
			continue
		}
		wrapped, ok := db.(dialectQueryer)
		if !ok {
			return errors.New(
				"account import baseline write outside a write transaction",
			)
		}
		db = wrapped.queryer
	}
}

// writeAccountImportBaseline records the baseline for an imported account.
// Re-importing an account (a second bootstrap into the same database) replaces
// it, because the newer snapshot is then the earliest state this database can
// reach.
func writeAccountImportBaseline(
	ctx context.Context,
	db queryer,
	account *models.Account,
) error {
	if err := requireAccountBaselineTransaction(db); err != nil {
		return err
	}
	// The baseline is read back by equality on the credential, which no NULL
	// or empty key can match, and its primary key rejects NULL outright.
	if len(account.StakingKey) == 0 {
		return errors.New(
			"write account import baseline: empty staking key",
		)
	}
	addedSlot, err := checkedInt64(account.AddedSlot)
	if err != nil {
		return err
	}
	drepType, err := checkedInt64(account.DrepType)
	if err != nil {
		return err
	}
	var deposit any
	if account.ImportDeposit != nil {
		deposit = decimalUint64(*account.ImportDeposit)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO account_import_baseline (
    credential_tag, staking_key, pool, drep, drep_type, active, added_slot,
    deposit_amount
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool = excluded.pool,
    drep = excluded.drep,
    drep_type = excluded.drep_type,
    active = excluded.active,
    added_slot = excluded.added_slot,
    deposit_amount = excluded.deposit_amount`,
		account.CredentialTag,
		account.StakingKey,
		nullBytes(account.Pool),
		nullBytes(account.Drep),
		drepType,
		account.Active,
		addedSlot,
		deposit,
	); err != nil {
		return fmt.Errorf("write account import baseline: %w", err)
	}
	return nil
}

// GetAccountImportRegistrationByCredential returns the virtual registration
// established by an import or genesis baseline. A nil Deposit means the
// baseline predates deposit preservation; callers must not substitute the
// current protocol-parameter value for an unknown historical deposit.
func (s *Store) GetAccountImportRegistrationByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (*models.AccountImportRegistration, error) {
	if len(stakingKey) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	var (
		active    bool
		addedSlot int64
		raw       sql.NullString
	)
	err = db.QueryRowContext(ctx, `
SELECT active, added_slot, deposit_amount
FROM account_import_baseline
WHERE credential_tag = ? AND staking_key = ?`,
		credentialTag,
		stakingKey,
	).Scan(&active, &addedSlot, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read account import registration: %w", err)
	}
	if !active {
		return nil, nil
	}
	slot, err := checkedUint64(addedSlot)
	if err != nil {
		return nil, fmt.Errorf("read account import registration: %w", err)
	}
	ret := &models.AccountImportRegistration{AddedSlot: slot}
	if raw.Valid {
		deposit, err := parseUint64("account import deposit", raw.String)
		if err != nil {
			return nil, err
		}
		ret.Deposit = &deposit
	}
	return ret, nil
}

func readAccountImportBaseline(
	ctx context.Context,
	db queryer,
	credentialTag uint8,
	stakingKey []byte,
) (accountImportBaseline, bool, error) {
	var (
		baseline  accountImportBaseline
		drepType  sql.NullInt64
		addedSlot int64
	)
	err := db.QueryRowContext(ctx, `
SELECT pool, drep, drep_type, active, added_slot
FROM account_import_baseline
WHERE credential_tag = ? AND staking_key = ?`,
		credentialTag,
		stakingKey,
	).Scan(
		&baseline.pool,
		&baseline.drep,
		&drepType,
		&baseline.active,
		&addedSlot,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return accountImportBaseline{}, false, nil
	}
	if err != nil {
		return accountImportBaseline{}, false, fmt.Errorf(
			"read account import baseline: %w",
			err,
		)
	}
	if addedSlot < 0 || drepType.Int64 < 0 {
		return accountImportBaseline{}, false, fmt.Errorf(
			"read account import baseline: negative added_slot %d or drep_type %d",
			addedSlot,
			drepType.Int64,
		)
	}
	baseline.drepType = uint64(drepType.Int64)
	baseline.position = accountCertificatePosition{slot: uint64(addedSlot)}
	return baseline, true, nil
}

func deleteAccountImportBaseline(
	ctx context.Context,
	db queryer,
	credentialTag uint8,
	stakingKey []byte,
) error {
	if err := requireAccountBaselineTransaction(db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
DELETE FROM account_import_baseline
WHERE credential_tag = ? AND staking_key = ?`,
		credentialTag,
		stakingKey,
	); err != nil {
		return fmt.Errorf("delete account import baseline: %w", err)
	}
	return nil
}

func (s *Store) GetAccountByCredential(
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Account, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	params := sqlitequery.GetActiveAccountByCredentialParams{
		CredentialTag: int64(credentialTag),
		StakingKey:    stakeKey,
	}
	var row sqlitequery.Account
	if includeInactive {
		row, err = q.GetAccountByCredential(
			ctx,
			sqlitequery.GetAccountByCredentialParams(params),
		)
	} else {
		row, err = q.GetActiveAccountByCredential(
			ctx,
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

// accountsByCredentialChunkQuery builds the SELECT for one credential_tag
// chunk of GetAccountsByCredential: a single-column staking_key IN (...)
// predicate against idx_account_credential, never a per-ref
// (credential_tag = ? AND staking_key = ?) OR ... predicate. Split out so a
// test can pin the query's shape directly — an EXPLAIN QUERY PLAN assertion
// is not durable here, since the chosen plan depends on table size and
// whether ANALYZE has run, not on which of the two predicate forms is used.
func accountsByCredentialChunkQuery(
	tag uint8,
	keys [][]byte,
	includeInactive bool,
) (string, []any) {
	args := make([]any, 0, len(keys)+1)
	args = append(args, tag)
	for _, key := range keys {
		args = append(args, key)
	}
	query := "SELECT " + sqliteAccountColumns +
		" FROM account WHERE credential_tag = ? AND staking_key IN (" +
		bindPlaceholders(len(keys)) + ")"
	if !includeInactive {
		query += " AND active = TRUE"
	}
	return query, args
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
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	// Grouped by credential_tag and queried as a single-column staking_key IN
	// (...), matching the unique index idx_account_credential(credential_tag,
	// staking_key) so each chunk is a single index range scan.
	//
	// (credential_tag = ? AND staking_key = ?) OR ... per ref is drivable
	// from the same index via SQLite's multi-index OR optimization, but only
	// once sqlite_stat1 exists. Without statistics the AND active = TRUE
	// conjunct leads the planner to idx_account_active_pool_staking_key
	// (active=?) instead, and the whole OR chain is evaluated per row:
	// O(active rows x refs) per chunk. ANALYZE only runs at Mithril sync and
	// before backfill, so a genesis-synced node is in exactly that state.
	byTag := make(map[uint8][][]byte)
	for _, ref := range refs {
		byTag[ref.Tag] = append(byTag[ref.Tag], ref.Key)
	}
	// One bound parameter is reserved for credential_tag; the rest of the
	// chunk is the staking_key IN list.
	chunkSize := max(1, s.dialect.ParameterLimit()-1)
	for tag, keys := range byTag {
		for start := 0; start < len(keys); start += chunkSize {
			end := min(start+chunkSize, len(keys))
			query, args := accountsByCredentialChunkQuery(
				tag,
				keys[start:end],
				includeInactive,
			)
			rows, err := db.QueryContext(
				ctx,
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
	db, ctx, err := s.dbFromTxn(txn)
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
			ctx,
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
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(ctx, `
INSERT INTO account_inactivity_activation (credential_tag, staking_key)
SELECT credential_tag, staking_key
FROM account
WHERE active = TRUE
ON CONFLICT (credential_tag, staking_key) DO NOTHING`); err != nil {
				return err
			}
			result, err := db.ExecContext(ctx, `
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
	db, ctx, err := s.readDBFromTxn(txn)
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
			ctx,
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
		txn,
		func(db queryer, ctx context.Context) error {
			rows, err := db.QueryContext(ctx, `
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
			if _, err := db.ExecContext(ctx, `
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
				ctx,
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
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActiveAccountCredentials: resolve db: %w",
			err,
		)
	}
	rows, err := db.QueryContext(ctx, `
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

// clearAccountImportBaselines tombstones the baselines of the credentials the
// predicate matches. Mithril reconciliation deactivates a credential precisely
// because the newer snapshot's live set does not hold it, which is a statement
// about the imported baseline and not about any certificate. Leaving the
// baseline active would let a later rollback restore the account the caller
// just tombstoned.
func (s *Store) clearAccountImportBaselines(
	ctx context.Context,
	db queryer,
	predicate string,
	args ...any,
) error {
	if err := requireAccountBaselineTransaction(db); err != nil {
		return err
	}
	if _, err := db.ExecContext(
		ctx,
		s.dialect.Rebind(`
UPDATE account_import_baseline SET active = FALSE, pool = NULL, drep = NULL,
    drep_type = 0
WHERE active = TRUE AND (`+predicate+")"),
		args...,
	); err != nil {
		return fmt.Errorf("clear account import baselines: %w", err)
	}
	return nil
}

// ClearDelegationsToRetiredPool removes every account delegation pointing at a
// pool reaped at an epoch boundary, the delegation half of the Shelley POOLREAP
// transition (domain-restrict the delegation map by the retired pools, Shelley
// spec Fig. 41).
//
// Called from ledger.applyPoolRetirements with the same boundary slot the
// deposit refund is written at. Stamping added_slot with that slot is what
// makes the clear rollback-safe: RestoreAccountStateAtSlot only revisits
// accounts whose added_slot is past the rollback target, and re-derives the
// delegation from the certificates surviving there — so a rollback to before
// the reap restores the delegation, and one to after it leaves the account
// cleared. Without the stamp the account is never revisited and stays
// un-delegated with no certificate saying so.
//
// The reward_live_stake aggregate carries the same attribution and is cleared
// with it: it mirrors account.pool only when refreshRewardLiveStakeAggregate
// runs for the credential, which a reap does not trigger, and it is what the
// boundary snapshot actually reads.
//
// The import baseline is deliberately left alone. It records the delegation a
// Mithril snapshot observed at its anchor, which is a statement about a slot
// before this boundary; a rollback past the reap must restore exactly that.
func (s *Store) ClearDelegationsToRetiredPool(
	poolKeyHash []byte,
	boundarySlot uint64,
	txn types.Txn,
) error {
	if len(poolKeyHash) == 0 {
		return nil
	}
	slotValue, err := checkedInt64(boundarySlot)
	if err != nil {
		return fmt.Errorf("clear delegations to retired pool: %w", err)
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(ctx, `
UPDATE account SET pool = NULL, added_slot = ?
WHERE pool = ?`,
				slotValue,
				poolKeyHash,
			); err != nil {
				return fmt.Errorf(
					"clear delegations to retired pool: %w",
					err,
				)
			}
			// reward_live_stake mirrors account.pool, but only when
			// refreshRewardLiveStakeAggregate runs for the credential, and a
			// reap triggers no such refresh. It is the aggregate the boundary
			// snapshot reads (GetLiveStakeInputsForPools selects on
			// pool_key_hash), so leaving it behind would keep feeding the
			// stake distribution the delegation just removed. The values match
			// what a refresh would compute for an account with no pool.
			if _, err := db.ExecContext(ctx, `
UPDATE reward_live_stake
SET pool_key_hash = NULL, pool_delegation_slot = 0,
    pool_delegation_block_index = 0, pool_delegation_cert_index = 0,
    updated_slot = ?
WHERE pool_key_hash = ?`,
				slotValue,
				poolKeyHash,
			); err != nil {
				return fmt.Errorf(
					"clear live stake attribution to retired pool: %w",
					err,
				)
			}
			return nil
		},
	)
}

// DeactivateAccounts tombstones the given credentials and their import
// baselines. The two writes and every chunk of them share one transaction: an
// account tombstoned while its baseline stays active is contradictory state
// that lets a later rollback restore exactly the account this call removed.
func (s *Store) DeactivateAccounts(
	txn types.Txn,
	refs []models.StakeCredentialRef,
) error {
	if len(refs) == 0 {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
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
				predicate := strings.Join(predicates, " OR ")
				if _, err := db.ExecContext(
					ctx,
					s.dialect.Rebind(`
UPDATE account SET active = FALSE
WHERE active = TRUE AND (`+predicate+")"),
					args...,
				); err != nil {
					return fmt.Errorf("DeactivateAccounts: %w", err)
				}
				if err := s.clearAccountImportBaselines(
					ctx,
					db,
					predicate,
					args...,
				); err != nil {
					return fmt.Errorf("DeactivateAccounts: %w", err)
				}
			}
			return nil
		},
	)
}

func (s *Store) GetAccountSumsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (models.AccountSums, error) {
	// The two MIR totals are non-nil on every return, including the failure
	// ones, so AccountSums never hands a caller a nil *big.Int.
	ret := models.NewAccountSums()
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return ret, fmt.Errorf("resolve read DB for account sums: %w", err)
	}
	sum := func(query string, args ...any) (uint64, error) {
		return sumUint64Rows(ctx, db, s.dialect.Rebind(query), args...)
	}
	// MIR amounts are delta_coin, so the two pot totals are summed as signed
	// values. A withdrawal is coin and stays unsigned.
	sumSigned := func(query string, args ...any) (*big.Int, error) {
		return sumSignedRows(ctx, db, s.dialect.Rebind(query), args...)
	}
	// reconciled_amount IS NULL excludes ReconcileAccountRewardBalance's
	// synthetic withdrawal-shaped rows (see that method's doc comment
	// above): their "amount" is the pre-correction balance the correction
	// proved wrong, not coin a delegator actually withdrew, so it must not
	// be summed into Blockfrost's withdrawals_sum.
	ret.WithdrawalsSum, err = sum(`
SELECT amount
FROM account_reward_delta
WHERE withdrawal = TRUE AND reconciled_amount IS NULL
  AND credential_tag = ? AND staking_key = ?`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return models.NewAccountSums(), fmt.Errorf(
			"query account sums: sum withdrawals: %w",
			err,
		)
	}
	ret.ReservesSum, err = sumSigned(`
SELECT reward.amount
FROM move_instantaneous_rewards_reward reward
JOIN move_instantaneous_rewards mir ON mir.id = reward.mir_id
WHERE mir.pot = 0 AND reward.credential_tag = ?
  AND reward.credential = ?`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return models.NewAccountSums(), fmt.Errorf(
			"query account sums: sum reserves MIR: %w",
			err,
		)
	}
	ret.TreasurySum, err = sumSigned(`
SELECT reward.amount
FROM move_instantaneous_rewards_reward reward
JOIN move_instantaneous_rewards mir ON mir.id = reward.mir_id
WHERE mir.pot = 1 AND reward.credential_tag = ?
  AND reward.credential = ?`,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return models.NewAccountSums(), fmt.Errorf(
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
		txn,
		func(db queryer, ctx context.Context) error {
			rows, err := db.QueryContext(ctx, `
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
			if err := rows.Err(); err != nil {
				return err
			}
			refs := make([]models.StakeCredentialRef, 0, len(accounts))
			for _, account := range accounts {
				registration, hasRegistration, err := latestAccountEvent(
					ctx,
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
				var baseline accountImportBaseline
				hasBaseline := false
				if !hasRegistration {
					if account.createdSlot > slot {
						if _, err := db.ExecContext(ctx, `
DELETE FROM account
WHERE credential_tag = ? AND staking_key = ?`,
							account.tag,
							account.key,
						); err != nil {
							return err
						}
						if err := deleteAccountImportBaseline(
							ctx,
							db,
							account.tag,
							account.key,
						); err != nil {
							return err
						}
						continue
					}
					// No registration certificate is reachable at or before
					// the rollback slot, so the account predates every
					// certificate this database holds: an imported or
					// genesis-delegated account. Its import baseline stands in
					// for the missing registration certificate, and the same
					// derivation below then applies whichever certificates do
					// survive the rollback.
					baseline, hasBaseline, err = readAccountImportBaseline(
						ctx,
						db,
						account.tag,
						account.key,
					)
					if err != nil {
						return err
					}
					registration = accountRestoreEvent{
						position: baseline.position,
					}
				}
				deregistration, hasDeregistration, err := latestAccountEvent(
					ctx,
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
					ctx,
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
					ctx,
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
				// An account with a baseline but no delegation certificate at
				// or before the rollback slot delegates exactly as the
				// snapshot recorded; without a baseline there is nothing to
				// derive the pool or DRep from, and the live values are left
				// alone.
				if hasBaseline {
					if !hasPool {
						pool = accountRestoreEvent{
							position: baseline.position,
							value:    baseline.pool,
						}
						hasPool = len(pool.value) > 0
					}
					if !hasDrep {
						drep = accountRestoreEvent{
							position:  baseline.position,
							value:     baseline.drep,
							valueType: baseline.drepType,
						}
						hasDrep = len(drep.value) > 0 || drep.valueType != 0
					}
				}
				// A registration certificate proves the account was registered
				// at its position; a baseline carries whatever the snapshot
				// recorded. Without either, absence of a surviving
				// deregistration is the only evidence available.
				priorActive := true
				if hasBaseline {
					priorActive = baseline.active
				}
				active := priorActive &&
					(!hasDeregistration ||
						compareCertificatePosition(
							registration.position,
							deregistration.position,
						) > 0)
				// A delegation certificate is not the last word on the
				// delegation: POOLREAP removes the delegations pointing at a
				// pool reaped at an epoch boundary and writes no certificate
				// of its own, so a certificate predating a reap that still
				// stands at the rollback slot must not put the account back on
				// that pool. Rolling back past the reap is the other
				// direction, and the boundary check below excludes it, so the
				// certificate is authoritative again there.
				if hasPool && len(pool.value) > 0 {
					reaped, err := poolReapedAfterDelegation(
						ctx,
						db,
						pool.value,
						pool.position.slot,
						slot,
					)
					if err != nil {
						return err
					}
					if reaped {
						pool.value = nil
					}
				}
				// Only rewrite pool/drep when their value at the rollback slot
				// is actually known: from a certificate, from the baseline, or
				// from the account being deregistered.
				setPool := hasRegistration || hasBaseline || hasPool || !active
				setDrep := hasRegistration || hasBaseline || hasDrep || !active
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
				// A baseline established after the rollback target (a rollback
				// to before the snapshot slot) must not leave the row claiming
				// a modification slot ahead of the tip.
				if latestSlot > slot {
					latestSlot = slot
				}
				assignments := []string{"active = ?", "added_slot = ?"}
				args := []any{active, latestSlot}
				if setPool {
					assignments = append(assignments, "pool = ?")
					args = append(args, nullBytes(pool.value))
				}
				if setDrep {
					assignments = append(
						assignments,
						"drep = ?",
						"drep_type = ?",
					)
					args = append(
						args,
						nullBytes(drep.value),
						drep.valueType,
					)
				}
				args = append(args, account.tag, account.key)
				if _, err := db.ExecContext(ctx,
					"UPDATE account SET "+strings.Join(assignments, ", ")+
						" WHERE credential_tag = ? AND staking_key = ?",
					args...,
				); err != nil {
					return err
				}
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

type accountRestoreEvent struct {
	position  accountCertificatePosition
	value     []byte
	valueType uint64
}

// poolReapedAfterDelegation reports whether poolKeyHash was reaped at an epoch
// boundary strictly after delegationSlot and at or before slot.
//
// A reap removes the delegations pointing at the pool (POOLREAP; see
// ClearDelegationsToRetiredPool) but writes no certificate, so the certificate
// derivation RestoreAccountStateAtSlot performs cannot see it: the pre-reap
// delegation certificate is still the latest one at the rollback slot and would
// put the account straight back on the reaped pool, returning the stake the
// reap removed to the pool distribution.
//
// The reap boundary is the first slot of the retirement certificate's epoch,
// and the certificate only takes effect there if it is the pool's latest
// certificate before that boundary. Both a later retirement (which moves the
// retirement out to its own epoch) and a later registration (which cancels it)
// supersede it. That is the same rule GetPoolsRetiringAtEpoch encodes by
// selecting the latest retirement and registration before the boundary and
// requiring the retirement to win and to name that epoch: later added_slot
// wins, then later block index, then later certificate index.
//
// Rows above the rollback slot are excluded, so this sees exactly the
// certificate history that survives the rollback.
func poolReapedAfterDelegation(
	ctx context.Context,
	db queryer,
	poolKeyHash []byte,
	delegationSlot uint64,
	slot uint64,
) (bool, error) {
	if len(poolKeyHash) == 0 {
		return false, nil
	}
	var reaped bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pool_retirement rt
    JOIN pool p ON p.id = rt.pool_id
    JOIN epoch e ON e.epoch_id = rt.epoch
    LEFT JOIN certs c ON c.id = rt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE p.pool_key_hash = ?
      AND rt.added_slot <= ?
      AND e.start_slot > ?
      AND e.start_slot <= ?
      AND NOT EXISTS (
          SELECT 1
          FROM pool_registration pr
          LEFT JOIN certs c2 ON c2.id = pr.certificate_id
          LEFT JOIN "transaction" t2 ON t2.id = c2.transaction_id
          WHERE pr.pool_id = rt.pool_id
            AND pr.added_slot < e.start_slot
            AND (
                pr.added_slot > rt.added_slot
                OR (pr.added_slot = rt.added_slot
                    AND COALESCE(t2.block_index, 0) > COALESCE(t.block_index, 0))
                OR (pr.added_slot = rt.added_slot
                    AND COALESCE(t2.block_index, 0) = COALESCE(t.block_index, 0)
                    AND COALESCE(c2.cert_index, 0) > COALESCE(c.cert_index, 0))
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pool_retirement rt2
          LEFT JOIN certs c3 ON c3.id = rt2.certificate_id
          LEFT JOIN "transaction" t3 ON t3.id = c3.transaction_id
          WHERE rt2.pool_id = rt.pool_id
            AND rt2.id <> rt.id
            AND rt2.added_slot < e.start_slot
            AND (
                rt2.added_slot > rt.added_slot
                OR (rt2.added_slot = rt.added_slot
                    AND COALESCE(t3.block_index, 0) > COALESCE(t.block_index, 0))
                OR (rt2.added_slot = rt.added_slot
                    AND COALESCE(t3.block_index, 0) = COALESCE(t.block_index, 0)
                    AND COALESCE(c3.cert_index, 0) > COALESCE(c.cert_index, 0))
            )
      )
)`,
		poolKeyHash,
		slot,
		delegationSlot,
		slot,
	).Scan(&reaped); err != nil {
		return false, fmt.Errorf("check pool reaped after delegation: %w", err)
	}
	return reaped, nil
}

func latestAccountEvent(
	ctx context.Context,
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
		err := db.QueryRowContext(ctx, `
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
		txn,
		func(db queryer, ctx context.Context) error {
			var accountID int64
			var reward, rewardDeficit sql.NullString
			err := db.QueryRowContext(ctx, `
SELECT id, reward, reward_deficit FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`,
				credentialTag,
				stakeKey,
			).Scan(&accountID, &reward, &rewardDeficit)
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
			deficit, err := parseNullUint64(
				"account reward deficit",
				rewardDeficit,
			)
			if err != nil {
				return err
			}
			result, err := db.ExecContext(ctx, `
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
			// netRewardDeficit absorbs any outstanding reward_deficit (left
			// behind by a past DeleteAccountRewardsAfterSlot rollback that
			// could not fully explain a balance gap; see that method and the
			// v22 migration's doc comment) into this credit, instead of
			// crediting current+amount unconditionally and letting the
			// discarded shortfall stay permanently baked into the balance.
			newReward, newDeficit := netRewardDeficit(current+amount, deficit)
			result, err = db.ExecContext(ctx, `
UPDATE account SET reward = ?, reward_deficit = ? WHERE id = ?`,
				strconv.FormatUint(newReward, 10),
				nullableRewardDeficit(newDeficit),
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
				ctx,
				db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				slot,
			)
		},
	)
}

// netRewardDeficit nets an outstanding reward_deficit against a newly
// credited balance. AddAccountRewardByCredential is the only path that ever
// credits an account additively without also re-anchoring it to an
// externally-sourced true balance (contrast ApplyAccountRewardWithdrawal and
// ReconcileAccountRewardBalance, which both clear reward_deficit to zero
// because they establish that anchor directly) -- so it is the only path
// that can safely absorb a deficit: doing so anywhere else would apply the
// correction against a balance that is about to be overwritten by ground
// truth anyway. It returns the balance to store and the deficit remaining
// after this credit, which is always <= the deficit passed in.
func netRewardDeficit(newTotal, deficit uint64) (uint64, uint64) {
	if deficit == 0 {
		return newTotal, 0
	}
	if deficit >= newTotal {
		return 0, deficit - newTotal
	}
	return newTotal - deficit, 0
}

// nullableRewardDeficit renders a reward_deficit value for storage: zero is
// stored as NULL (the v22 migration's "no outstanding deficit" sentinel)
// rather than the string "0", so existing reads via parseNullUint64 keep
// treating "no row ever recorded a deficit" and "the deficit nets to zero"
// identically.
func nullableRewardDeficit(deficit uint64) any {
	if deficit == 0 {
		return nil
	}
	return strconv.FormatUint(deficit, 10)
}

func (s *Store) AddPostSnapshotAccountRewardByCredential(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn types.Txn,
) error {
	if err := s.AddAccountRewardByCredential(
		credentialTag, stakeKey, amount, slot, sourceHash, txn,
	); err != nil {
		return err
	}
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
		txn,
		func(db queryer, ctx context.Context) error {
			_, err := db.ExecContext(ctx, `
UPDATE account_reward_delta
SET post_snapshot = TRUE
WHERE withdrawal = FALSE AND tx_hash = ?
  AND credential_tag = ? AND staking_key = ? AND added_slot = ?`,
				sourceHash, credentialTag, stakeKey, slotValue,
			)
			return err
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
		txn,
		func(db queryer, ctx context.Context) error {
			var accountID int64
			var reward sql.NullString
			err := db.QueryRowContext(ctx, `
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
			if err := db.QueryRowContext(ctx, `
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
			// A withdrawal re-anchors the balance to a fresh,
			// externally-sourced true value (zero), which makes any
			// reward_deficit accumulated before it (see
			// DeleteAccountRewardsAfterSlot and netRewardDeficit) moot --
			// clear it rather than let a stale deficit apply against
			// whatever this credential accrues next.
			if _, err := db.ExecContext(ctx, `
UPDATE account SET reward = '0', reward_deficit = NULL WHERE id = ?`,
				accountID,
			); err != nil {
				return err
			}
			result, err := db.ExecContext(ctx, `
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
				ctx,
				db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				slot,
			)
		},
	)
}

// ReconcileAccountRewardBalance overwrites an account's reward balance with
// correctedAmount, without treating the change as a withdrawal: the balance
// is not cleared to zero, and the recorded account_reward_delta row exists
// only so a later rollback (DeleteAccountRewardsAfterSlot, including via
// `dingo database truncate`) can invert this exact overwrite -- it is not a
// real on-chain event and carries no withdrawal semantics of its own.
//
// Before this delta row existed, a rollback/truncate crossing this slot had
// no record that the balance had been overwritten out-of-band: it would
// walk the delta chain as if this account's history were unbroken, and
// eventually try to undo an earlier, legitimate accrual delta against a
// current balance the overwrite had silently changed, failing with
// "account reward rollback underflow" (dingo#4510 follow-up, discovered
// while trying to truncate a database that had exercised this reconciliation
// path). Recording a withdrawal-shaped delta with previous_reward set to the
// pre-overwrite balance lets DeleteAccountRewardsAfterSlot's existing
// withdrawal branch (which restores previous_reward verbatim, ignoring
// amount) correctly restore that balance regardless of what correctedAmount
// was.
//
// The same delta row is also read by historicalRewardsBatch
// (historical_stake.go), which reconstructs a credential's reward balance as
// of an arbitrary past slot by walking every withdrawal-shaped row on or
// after that slot and resolving to the row's balance-before-the-row (dingo
// #4529): for an ordinary withdrawal that is exactly previous_reward, the
// balance a real withdrawal cleared to zero. Using previous_reward the same
// way here would resolve every boundary before this correction's slot to
// the pre-correction balance -- the very value this reconciliation exists to
// say was wrong -- reproducing the same small, persistent understatement in
// every historical/epoch-boundary stake read that predates the correction.
// reconciled_amount records correctedAmount separately so
// historicalRewardsBatch can resolve those boundaries to the corrected
// balance instead, while previous_reward keeps meaning exactly what
// DeleteAccountRewardsAfterSlot has always used it for. It is NULL for every
// ordinary withdrawal row and set only here.
//
// This exists for a narrow, explicitly opt-in reward-balance recovery path
// (dingo#4152 follow-up; not present in this codebase as of this change --
// see dingo#4560's review discussion): a validated peer's block proves the
// locally reconstructed reward balance for one credential disagrees with the
// chain by some small amount, and the operator has chosen to trust the
// peer's figure over the node's own reconstruction rather than halt the
// ledger pipeline indefinitely on shelley.IncorrectWithdrawalAmountError.
// This method and the reconciled_amount column it writes are retained as
// independently valid historical-reconstruction/rollback-repair
// infrastructure even without that recovery path wired up to a caller;
// wiring a caller back in is out of scope here and would need real
// independent-corroboration evidence, not a bare redelivery-based trust
// flag. Correcting the
// balance to the withdrawal's own Provided amount, rather than to zero,
// deliberately does not apply the withdrawal itself -- it only makes the
// account's balance agree with what the pending block's own validation
// rule expects, so replay recovery's ordinary rewind-and-retry can
// re-validate and apply that same block normally afterward, through the
// same path a correct reconstruction would have taken.
//
// slot and txHash identify the block/transaction whose validation triggered
// this reconciliation. The row this writes is keyed on a discriminator
// derived from txHash (reconciliationDiscriminator), not txHash itself: the
// real transaction is expected to be re-validated and applied normally on
// the very next retry (see above), through ApplyAccountRewardWithdrawal,
// whose own idempotency check looks up a withdrawal row by the identical
// (tx_hash, credential_tag, staking_key) triple. Keying this row on the
// unmodified txHash would make that lookup find THIS row instead of the
// real withdrawal's own, short-circuit as "already applied", and never
// zero the balance the retried withdrawal is supposed to withdraw --
// leaving account.reward permanently stuck at correctedAmount. The
// discriminator keeps this row idempotent against a redelivery of the same
// reconciliation event while staying distinct from any real tx_hash.
func (s *Store) ReconcileAccountRewardBalance(
	credentialTag uint8,
	stakeKey []byte,
	correctedAmount uint64,
	slot uint64,
	txHash []byte,
	txn types.Txn,
) error {
	discriminator := reconciliationDiscriminator(txHash)
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var accountID int64
			var reward sql.NullString
			err := db.QueryRowContext(ctx, `
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
			// The idempotency-journal insert must run, and be checked,
			// before the balance write -- not after. A redelivered/retried
			// identical reconciliation call is expected (see this method's
			// doc comment on discriminator-based idempotency); running the
			// balance UPDATE unconditionally before this insert clobbered
			// any legitimate accrual that landed between the original
			// delivery and the redelivery back to the stale reconciled
			// value, because the redelivery's UPDATE always ran even though
			// its INSERT then correctly no-opped on the conflict. Checking
			// rows-affected first makes the whole correction a no-op on
			// redelivery, balance write included.
			result, err := db.ExecContext(ctx, `
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal, reconciled_amount
) VALUES (?, ?, ?, ?, ?, ?, TRUE, ?)
ON CONFLICT (
    withdrawal, tx_hash, credential_tag, staking_key, added_slot
) DO NOTHING`,
				stakeKey,
				credentialTag,
				discriminator,
				strconv.FormatUint(current, 10),
				strconv.FormatUint(current, 10),
				slotValue,
				strconv.FormatUint(correctedAmount, 10),
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
			// The correction is itself a fresh, externally-sourced true
			// balance, which makes any reward_deficit accumulated before it
			// (see DeleteAccountRewardsAfterSlot and netRewardDeficit) moot.
			if _, err := db.ExecContext(ctx, `
UPDATE account SET reward = ?, reward_deficit = NULL WHERE id = ?`,
				strconv.FormatUint(correctedAmount, 10),
				accountID,
			); err != nil {
				return err
			}
			// Every other account.reward mutator in this file
			// (AddAccountRewardByCredential, AddPostSnapshotAccountRewardByCredential,
			// ApplyAccountRewardWithdrawal) refreshes reward_live_stake as its
			// last step. Omitting it here left reward_live_stake -- the
			// incrementally maintained aggregate the live/fast mark-snapshot
			// path reads -- silently stale after every
			// ReconcileAccountRewardBalance correction, while
			// account.reward held the corrected value. That is a real
			// divergence between the two supposedly-equivalent reward
			// aggregates in its own right, independent of whether it is the
			// specific mechanism behind any one observed stake-drift report
			// (see dingo #4529, still under investigation as of this fix).
			return s.refreshRewardLiveStakeAggregate(
				ctx,
				db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				slot,
			)
		},
	)
}

// reconciliationDiscriminatorPrefix marks an account_reward_delta row as a
// ReconcileAccountRewardBalance correction rather than a real withdrawal,
// mirroring mirRewardSourceHash's synthetic-discriminator pattern in
// ledger/mir.go for the same reason: this table's tx_hash column doubles as
// a generic per-writer discriminator wherever no natural one applies (or, as
// here, where the natural one must not be reused -- see
// reconciliationDiscriminator).
const reconciliationDiscriminatorPrefix = "dingo:reconcile:"

// reconciliationDiscriminator derives the account_reward_delta identity for
// a ReconcileAccountRewardBalance correction from the real transaction hash
// that triggered it, without reusing that hash directly.
// ApplyAccountRewardWithdrawal's own idempotency check looks up a withdrawal
// row by the exact (tx_hash, credential_tag, staking_key) triple the real
// withdrawal will also use once replay recovery re-validates and applies it
// (see ReconcileAccountRewardBalance's doc comment); keying this row on the
// unmodified hash would make that lookup match this row instead, report the
// real withdrawal as already applied, and never zero the balance it is
// supposed to withdraw. Prefixing keeps this row idempotent against a
// redelivery of the same reconciliation event (same txHash in, same
// discriminator out) while guaranteeing it can never equal a real 32-byte
// transaction hash.
func reconciliationDiscriminator(txHash []byte) []byte {
	out := make([]byte, 0, len(reconciliationDiscriminatorPrefix)+len(txHash))
	out = append(out, reconciliationDiscriminatorPrefix...)
	out = append(out, txHash...)
	return out
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
		txn,
		func(db queryer, ctx context.Context) error {
			rows, err := db.QueryContext(ctx, `
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
				var reward, rewardDeficit sql.NullString
				err := db.QueryRowContext(ctx, `
SELECT id, reward, reward_deficit FROM account
WHERE credential_tag = ? AND staking_key = ?`,
					item.tag,
					item.key,
				).Scan(&id, &reward, &rewardDeficit)
				ref := models.NewStakeCredentialRef(item.tag, item.key)
				refs[ref.MapKey()] = ref
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				deficit, err := parseNullUint64(
					"account reward deficit",
					rewardDeficit,
				)
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
						// The delta chain assumes an unbroken history: every
						// balance change between here and the current value
						// is accounted for by exactly one row this loop will
						// walk. A gap -- most commonly an out-of-band balance
						// correction that intentionally records no delta of
						// its own (see ReconcileAccountRewardBalance's doc
						// comment) predating this database's own fix for
						// that -- breaks that assumption and would otherwise
						// make this a hard failure for every caller of
						// DeleteAccountRewardsAfterSlot, including
						// `dingo database truncate`, whose own doc comment
						// promises exactly the disaster-recovery scenario an
						// unbroken delta chain cannot always guarantee.
						// Clamping to zero and continuing accepts an
						// imprecise reward balance for this one credential
						// rather than refusing the entire rollback: the
						// alternative is a truncate that can never succeed
						// against a database old enough to carry such a gap.
						// Unlike before, the discarded shortfall is not lost:
						// it is added to reward_deficit, which
						// AddAccountRewardByCredential (via netRewardDeficit)
						// nets out of the next real accrual this credential
						// sees post-rollback, converging the balance back
						// toward the canonical figure instead of a node that
						// hit this clamp diverging from one that never saw
						// the gap forever.
						shortfall := item.amount - current
						if deficit > ^uint64(0)-shortfall {
							// Deficit accumulation itself overflowing is
							// astronomically implausible (it would require
							// more clamped shortfall than the entire supply
							// can express), but saturate rather than wrap if
							// it ever happens.
							deficit = ^uint64(0)
						} else {
							deficit += shortfall
						}
						if s.logger != nil {
							s.logger.Warn(
								"account reward rollback underflow; clamping to zero, recording deficit, and continuing",
								"component",
								"database",
								"staking_key",
								hex.EncodeToString(item.key),
								"credential_tag",
								item.tag,
								"current_reward",
								current,
								"delta_amount",
								item.amount,
								"recorded_deficit",
								deficit,
							)
						}
						value = 0
					} else {
						value = current - item.amount
					}
				}
				if _, err := db.ExecContext(ctx, `
UPDATE account SET reward = ?, reward_deficit = ? WHERE id = ?`,
					strconv.FormatUint(value, 10),
					nullableRewardDeficit(deficit),
					id,
				); err != nil {
					return err
				}
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM account_reward_delta WHERE added_slot > ?`,
				slotValue,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM account_withdrawal_witness WHERE added_slot > ?`,
				slotValue,
			); err != nil {
				return err
			}
			for _, ref := range refs {
				if err := s.refreshRewardLiveStakeAggregate(
					ctx,
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
