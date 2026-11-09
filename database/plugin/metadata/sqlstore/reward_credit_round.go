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

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetPendingRewardCreditRounds(
	txn types.Txn,
) ([]models.RewardCreditRound, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	return s.pendingRewardCreditRounds(ctx, db)
}

func (s *Store) HasPendingRewardCreditRounds(txn types.Txn) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	return pendingRewardCreditOutputsExist(ctx, db)
}

func (s *Store) HasUnfoldedRewardCreditsThroughEpoch(
	snapshotEpoch uint64,
	txn types.Txn,
) (bool, error) {
	epoch, err := checkedInt64(snapshotEpoch)
	if err != nil {
		return false, fmt.Errorf("reward credit epoch: %w", err)
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	var exists bool
	if err := db.QueryRowContext(ctx, s.dialect.Rebind(`
SELECT EXISTS (
    SELECT 1 FROM reward_credit_round rcr
    WHERE rcr.snapshot_epoch <= ?
      AND EXISTS (
          SELECT 1 FROM reward_account_output rao
          WHERE rao.spendable = TRUE AND rao.guarded = FALSE
            AND rao.folded = FALSE AND rao.epoch = rcr.snapshot_epoch
      )
)`), epoch).Scan(&exists); err != nil {
		return false, fmt.Errorf("check unfolded reward credits through epoch %d: %w", snapshotEpoch, err)
	}
	return exists, nil
}

func pendingRewardCreditOutputsExist(ctx context.Context, db queryer) (bool, error) {
	var exists bool
	// Driven from the few credited rounds: each probes the pending index for
	// its epoch. Driven from the outputs instead, the probe walks every
	// unfolded row of the round precomputed for the next boundary, which is
	// not credited yet, on every call.
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM reward_credit_round rcr
    WHERE EXISTS (
        SELECT 1 FROM reward_account_output rao
        WHERE rao.spendable = TRUE AND rao.guarded = FALSE
          AND rao.folded = FALSE AND rao.epoch = rcr.snapshot_epoch
    )
)`).Scan(&exists); err != nil {
		return false, fmt.Errorf("check pending reward credit outputs: %w", err)
	}
	return exists, nil
}

func (s *Store) SetPendingRewardCreditRounds(
	rounds []models.RewardCreditRound,
	txn types.Txn,
) error {
	return s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		if _, err := db.ExecContext(ctx, `DELETE FROM reward_credit_round`); err != nil {
			return fmt.Errorf("replace pending reward credit rounds: %w", err)
		}
		for _, round := range rounds {
			if err := insertRewardCreditRound(ctx, db, s.dialect, round); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) AddAppliedRewardCreditRound(
	round models.RewardCreditRound,
	txn types.Txn,
) error {
	epoch, err := checkedInt64(round.SnapshotEpoch)
	if err != nil {
		return fmt.Errorf("applied reward credit epoch: %w", err)
	}
	return s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		if _, err := db.ExecContext(ctx, s.dialect.Rebind(
			`DELETE FROM reward_credit_round WHERE snapshot_epoch = ?`,
		), epoch); err != nil {
			return fmt.Errorf("replace applied reward credit round: %w", err)
		}
		return insertRewardCreditRound(ctx, db, s.dialect, round)
	})
}

func (s *Store) HasAppliedRewardCreditRound(
	epoch uint64,
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return false, fmt.Errorf("applied reward credit epoch: %w", err)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, s.dialect.Rebind(
		`SELECT EXISTS (SELECT 1 FROM reward_credit_round WHERE snapshot_epoch = ?)`,
	), sqlEpoch).Scan(&exists); err != nil {
		return false, fmt.Errorf("check applied reward credit round: %w", err)
	}
	return exists, nil
}

func insertRewardCreditRound(
	ctx context.Context,
	db queryer,
	dialect Dialect,
	round models.RewardCreditRound,
) error {
	epoch, err := checkedInt64(round.SnapshotEpoch)
	if err != nil {
		return fmt.Errorf("pending reward credit epoch: %w", err)
	}
	boundarySlot, err := checkedInt64(round.BoundarySlot)
	if err != nil {
		return fmt.Errorf("pending reward credit boundary slot: %w", err)
	}
	if _, err := db.ExecContext(ctx, dialect.Rebind(
		`INSERT INTO reward_credit_round (snapshot_epoch, boundary_slot) VALUES (?, ?)`,
	), epoch, boundarySlot); err != nil {
		return fmt.Errorf("save pending reward credit round: %w", err)
	}
	return nil
}

// pendingRewardCreditRounds reads the pending rounds through the caller's own
// handle, so a read inside a boundary transaction sees the round that
// transaction just applied.
func (s *Store) pendingRewardCreditRounds(
	ctx context.Context,
	db queryer,
) ([]models.RewardCreditRound, error) {
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(
		`SELECT snapshot_epoch, boundary_slot FROM reward_credit_round ORDER BY snapshot_epoch`,
	))
	if err != nil {
		return nil, fmt.Errorf("list pending reward credit rounds: %w", err)
	}
	defer rows.Close()
	var rounds []models.RewardCreditRound
	for rows.Next() {
		var epoch, slot int64
		if err := rows.Scan(&epoch, &slot); err != nil {
			return nil, err
		}
		if epoch < 0 || slot < 0 {
			return nil, fmt.Errorf(
				"invalid pending reward credit round epoch %d slot %d",
				epoch,
				slot,
			)
		}
		rounds = append(rounds, models.RewardCreditRound{
			SnapshotEpoch: uint64(epoch),
			BoundarySlot:  uint64(slot),
		})
	}
	return rounds, rows.Err()
}

// pendingCreditsForCredentials totals, per credential, the unfolded credits
// visible at a historical slot.
func (s *Store) pendingCreditsForCredentials(
	ctx context.Context,
	db queryer,
	visibleAt uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
	ret := make(map[historicalRewardKey]uint64)
	if len(selected) == 0 {
		return ret, nil
	}
	keys := make([]historicalRewardKey, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	batch := max(1, (s.dialect.ParameterLimit()-1)/2)
	for start := 0; start < len(keys); start += batch {
		end := min(start+batch, len(keys))
		batchSelected := make(map[historicalRewardKey]struct{}, end-start)
		for _, key := range keys[start:end] {
			batchSelected[key] = struct{}{}
		}
		predicate, predicateArgs := historicalRewardCredentialPredicate(
			batchSelected,
		)
		args := make([]any, 0, 1+len(predicateArgs))
		args = append(args, visibleAt)
		args = append(args, predicateArgs...)
		if err := func() error {
			rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT rao.credential_tag, rao.staking_key,
       SUM(CAST(rao.amount AS `+s.pendingCreditCastType()+`))
FROM reward_account_output rao
WHERE rao.spendable = TRUE AND rao.guarded = FALSE AND NOT rao.folded
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr
      WHERE rcr.snapshot_epoch = rao.epoch AND rcr.boundary_slot <= ?
  )
  AND (`+predicate+`)
GROUP BY rao.credential_tag, rao.staking_key`), args...)
			if err != nil {
				return fmt.Errorf("get unfolded reward credits: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var tag uint8
				var key []byte
				var amount int64
				if err := rows.Scan(&tag, &key, &amount); err != nil {
					return err
				}
				if amount < 0 {
					return fmt.Errorf("negative unfolded reward credit %d", amount)
				}
				ret[historicalRewardKey{tag: tag, key: string(key)}] += uint64(amount)
			}
			return rows.Err()
		}(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// credentialUnfoldedRewardCreditPredicate selects the rows of a stake
// credential's credits not yet in account.reward, for a query that looks up
// one stake credential. Written with NOT folded, the flag tests stop matching
// the (spendable, guarded, folded, epoch) index as equalities, so SQLite looks
// the rows up on the credential index instead of walking every unfolded row
// for each stake credential.
const credentialUnfoldedRewardCreditPredicate = `spendable = TRUE AND guarded = FALSE AND NOT folded`

// FoldRewardAccountOutputs marks a credential's unfolded credits of the given
// rounds as included in account.reward, in the transaction establishing it.
func (s *Store) FoldRewardAccountOutputs(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) error {
	if len(epochs) == 0 || len(stakingKey) == 0 {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			args := make([]any, 0, 2+len(epochs))
			args = append(args, credentialTag, stakingKey)
			for _, epoch := range epochs {
				args = append(args, epoch)
			}
			if _, err := db.ExecContext(ctx, s.dialect.Rebind(`
UPDATE reward_account_output SET folded = TRUE
WHERE credential_tag = ? AND staking_key = ?
  AND epoch IN (`+bindPlaceholders(len(epochs))+`)
  AND `+credentialUnfoldedRewardCreditPredicate), args...); err != nil {
				return fmt.Errorf("fold reward account outputs: %w", err)
			}
			return nil
		},
	)
}

func (s *Store) FoldPendingRewardAccountOutputs(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) error {
	if len(stakingKey) == 0 {
		return nil
	}
	return s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		if _, err := db.ExecContext(ctx, s.dialect.Rebind(`
UPDATE reward_account_output SET folded = TRUE
WHERE credential_tag = ? AND staking_key = ?
  AND `+credentialUnfoldedRewardCreditPredicate+`
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr
      WHERE rcr.snapshot_epoch = reward_account_output.epoch
  )`), credentialTag, stakingKey); err != nil {
			return fmt.Errorf("fold pending reward account outputs: %w", err)
		}
		return nil
	})
}

// A claim marks outputs folded one row at a time with a conditional update,
// so a concurrent claim of the same row finds it already folded and skips it:
// no credit is returned, and so written, twice.
const (
	claimRewardOutputSQL = `-- name: ClaimRewardAccountOutput :exec
UPDATE reward_account_output SET folded = TRUE WHERE id = ? AND folded = FALSE`

	pendingRewardOutputsForCredentialSQL = `-- name: GetPendingRewardOutputsForClaim :many
SELECT ` + rewardAccountOutputColumns + `
FROM reward_account_output rao
WHERE rao.credential_tag = ? AND rao.staking_key = ?
  AND rao.spendable = TRUE AND rao.guarded = FALSE AND NOT rao.folded
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr
      WHERE rcr.snapshot_epoch = rao.epoch
  )
ORDER BY rao.id`

	claimAccountIDSQL = `-- name: GetClaimAccountID :one
SELECT id FROM account WHERE credential_tag = ? AND staking_key = ?`
)

// buildUnfoldedRewardOutputsSQL builds the statement that selects a round's unfolded outputs
// in account.id order, starting at an account. Folding a round in this order
// makes its journal rows and the account rows it updates sequential. SQLite
// is told to drive from account: left to itself it can drive from the outputs'
// epoch index and sort the whole round for the ORDER BY.
func buildUnfoldedRewardOutputsSQL(dialect string) string {
	join := "JOIN reward_account_output o"
	if dialect == "sqlite" {
		// Naming the index keeps the probe on (epoch, credential): the
		// planner otherwise picks the pending-round index, which has no
		// credential column and would read every unfolded output of the
		// round once per account.
		join = "CROSS JOIN reward_account_output o INDEXED BY " +
			"idx_reward_account_output_epoch_cred_pool_type"
	}
	return `-- name: GetUnfoldedRewardOutputsInAccountOrder :many
SELECT a.id, ` + rewardAccountOutputColumnsOf("o") + `
FROM account a ` + join + `
  ON o.credential_tag = a.credential_tag AND o.staking_key = a.staking_key
WHERE a.id >= ? AND o.epoch = ? AND o.spendable = TRUE
  AND o.guarded = FALSE AND o.folded = FALSE
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr WHERE rcr.snapshot_epoch = o.epoch
  )
ORDER BY a.id, o.id
LIMIT ?`
}

var (
	unfoldedRewardOutputsSQLite = buildUnfoldedRewardOutputsSQL("sqlite")
	unfoldedRewardOutputsShared = buildUnfoldedRewardOutputsSQL("")
)

func (s *Store) unfoldedRewardOutputsSQL() string {
	if s.dialect.Name() == "sqlite" {
		return unfoldedRewardOutputsSQLite
	}
	return unfoldedRewardOutputsShared
}

// unaccountedRewardOutputsSQL selects a round's unfolded outputs whose
// credential has no account row. The account-ordered scan cannot reach them,
// but a claim that skipped them would leave the round due forever; claiming
// them lets the caller's write report the missing account, or an import
// acknowledge them.
var unaccountedRewardOutputsSQL = `-- name: GetUnaccountedRewardOutputs :many
SELECT 0, ` + rewardAccountOutputColumnsOf("o") + `
FROM reward_account_output o
WHERE o.epoch = ? AND o.spendable = TRUE AND o.guarded = FALSE
  AND o.folded = FALSE
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr WHERE rcr.snapshot_epoch = o.epoch
  )
  AND NOT EXISTS (
      SELECT 1 FROM account a
      WHERE a.credential_tag = o.credential_tag AND a.staking_key = o.staking_key
  )
ORDER BY o.id
LIMIT ?`

func rewardAccountOutputColumnsOf(alias string) string {
	return alias + `.staking_key, ` + alias + `.pool_key_hash, ` + alias +
		`.reward_type, ` + alias + `.id, ` + alias + `.epoch, ` + alias +
		`.credential_tag, ` + alias + `.amount, ` + alias + `.spendable, ` +
		alias + `.guarded, ` + alias + `.captured_slot, ` + alias +
		`.boundary_slot`
}

// claimOutputs marks each output folded and returns those this call folded.
func (s *Store) claimOutputs(
	ctx context.Context,
	db queryer,
	outputs []*models.RewardAccountOutput,
) ([]*models.RewardAccountOutput, error) {
	claimed := outputs[:0]
	for _, output := range outputs {
		result, err := s.execCached(ctx, db, claimRewardOutputSQL, output.ID)
		if err != nil {
			return nil, fmt.Errorf("fold claimed reward credit: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected == 1 {
			claimed = append(claimed, output)
		}
	}
	return claimed, nil
}

// ClaimPendingRewardCreditsForCredential claims every unfolded credit of one
// stake credential in the credited rounds.
func (s *Store) ClaimPendingRewardCreditsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	if len(stakingKey) == 0 {
		return nil, nil
	}
	var ret []*models.RewardAccountOutput
	err := s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		var accountID int64
		err := s.queryRowCached(ctx, db, claimAccountIDSQL,
			credentialTag, stakingKey,
		).Scan(&accountID)
		if errors.Is(err, sql.ErrNoRows) {
			accountID = 0
		} else if err != nil {
			return err
		}
		rows, err := db.QueryContext(
			ctx, pendingRewardOutputsForCredentialSQL, credentialTag, stakingKey,
		)
		if err != nil {
			return fmt.Errorf("claim unfolded reward credits: %w", err)
		}
		outputs, err := scanRewardAccountOutputRows(rows)
		if err != nil {
			return err
		}
		for _, output := range outputs {
			output.AccountID = uint(accountID) //nolint:gosec
		}
		ret, err = s.claimOutputs(ctx, db, outputs)
		return err
	})
	return ret, err
}

// ClaimUnfoldedRewardCredits claims up to limit unfolded credits of one
// credited round in account order, starting at account fromAccountID. A caller
// resumes at the last returned output's AccountID: the account is read again
// and its outputs already folded are skipped, so the folded flag, not the
// cursor, is the record of progress.
func (s *Store) ClaimUnfoldedRewardCredits(
	snapshotEpoch uint64,
	fromAccountID uint,
	limit int,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	epoch, err := checkedInt64(snapshotEpoch)
	if err != nil {
		return nil, fmt.Errorf("claim reward credits epoch: %w", err)
	}
	if limit <= 0 {
		return nil, nil
	}
	var ret []*models.RewardAccountOutput
	err = s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		rows, err := db.QueryContext(
			ctx, s.unfoldedRewardOutputsSQL(), int64(fromAccountID), epoch, limit,
		)
		if err != nil {
			return fmt.Errorf("select unfolded reward credits: %w", err)
		}
		outputs, err := scanAccountOrderedRewardOutputs(rows)
		if err != nil {
			return err
		}
		if len(outputs) < limit {
			rows, err := db.QueryContext(
				ctx, unaccountedRewardOutputsSQL, epoch, limit-len(outputs),
			)
			if err != nil {
				return fmt.Errorf("select unaccounted reward credits: %w", err)
			}
			orphans, err := scanAccountOrderedRewardOutputs(rows)
			if err != nil {
				return err
			}
			outputs = append(outputs, orphans...)
		}
		ret, err = s.claimOutputs(ctx, db, outputs)
		return err
	})
	return ret, err
}

func scanAccountOrderedRewardOutputs(
	rows *sql.Rows,
) ([]*models.RewardAccountOutput, error) {
	defer rows.Close()
	var ret []*models.RewardAccountOutput
	for rows.Next() {
		var output models.RewardAccountOutput
		var accountID, id, epoch, tag, captured, boundary int64
		var amount string
		if err := rows.Scan(
			&accountID, &output.StakingKey, &output.PoolKeyHash,
			&output.RewardType, &id, &epoch, &tag, &amount,
			&output.Spendable, &output.Guarded, &captured, &boundary,
		); err != nil {
			return nil, err
		}
		value, err := parseUint64("reward account amount", amount)
		if err != nil {
			return nil, err
		}
		output.AccountID = uint(accountID)     //nolint:gosec
		output.ID = uint(id)                   //nolint:gosec
		output.Epoch = uint64(epoch)           //nolint:gosec
		output.CredentialTag = uint8(tag)      //nolint:gosec
		output.CapturedSlot = uint64(captured) //nolint:gosec
		output.BoundarySlot = uint64(boundary) //nolint:gosec
		output.Amount = types.Uint64(value)
		ret = append(ret, &output)
	}
	return ret, rows.Err()
}

const rollbackUnfoldRewardAccountOutputsSQL = `
UPDATE reward_account_output SET folded = FALSE
WHERE folded = TRUE
  AND epoch IN (
      SELECT snapshot_epoch FROM reward_credit_round WHERE boundary_slot > ?
  )`

// deleteRewardCreditRoundsAfterSlot drops rounds applied at a boundary a
// rollback undoes, and the fold progress recorded for them.
func (s *Store) deleteRewardCreditRoundsAfterSlot(
	ctx context.Context,
	db queryer,
	slot uint64,
) error {
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("rollback slot: %w", err)
	}
	// A rollback removes account deltas through the same transaction. Outputs
	// that survive because their snapshot predates the rollback must be
	// unfolded before their applied-round marker is removed.
	if _, err := db.ExecContext(ctx, s.dialect.Rebind(rollbackUnfoldRewardAccountOutputsSQL), sqlSlot); err != nil {
		return fmt.Errorf("unfold reward account outputs: %w", err)
	}
	if _, err := db.ExecContext(ctx, s.dialect.Rebind(
		`DELETE FROM reward_credit_round WHERE boundary_slot > ?`,
	), sqlSlot); err != nil {
		return fmt.Errorf("delete rolled-back reward credit rounds: %w", err)
	}
	return nil
}

func (s *Store) pendingCreditCastType() string {
	switch s.dialect.Name() {
	case "postgres":
		return "BIGINT"
	case "mysql":
		return "UNSIGNED"
	default:
		return "INTEGER"
	}
}

// pendingRewardCreditTotals returns every credential's pending credit total:
// the rows pendingRewardCreditSubquery sums, read in one pass and grouped by
// credential. The subquery is right for a lookup of a few credentials, but a
// read of every delegator pays it once per row, which is one index probe plus
// one fetch per unfolded output across every credited round. On
// idx_reward_account_output_credential_pending the single pass is a scan of
// the index in group order with no table access at all.
//
// The key is the credential tag byte followed by the staking key. A credential
// absent from the map has no pending credit, which is the subquery's
// COALESCE(..., 0).
func (s *Store) pendingRewardCreditTotals(
	ctx context.Context,
	db queryer,
) (map[string]uint64, error) {
	rows, err := db.QueryContext(ctx, s.pendingRewardCreditTotalsSQL())
	if err != nil {
		return nil, fmt.Errorf("get pending reward credit totals: %w", err)
	}
	defer rows.Close()
	ret := make(map[string]uint64)
	for rows.Next() {
		var tag uint8
		var key []byte
		var raw sql.NullString
		if err := rows.Scan(&tag, &key, &raw); err != nil {
			return nil, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		amount, err := parseUint64("pending reward credit total", raw.String)
		if err != nil {
			return nil, err
		}
		ret[pendingRewardCreditKey(tag, key)] = amount
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get pending reward credit totals: %w", err)
	}
	return ret, nil
}

// pendingRewardCreditTotalsSQL is the query behind pendingRewardCreditTotals.
// The flag tests are written as boolean expressions rather than equalities so
// they match no index prefix: as equalities they fit the (spendable, guarded,
// folded, epoch) index, which SQLite picks without current planner stats and
// which walks every output of those flags with a table fetch each and a sort
// for the grouping. With nothing to search by, the covering credential index
// is the cheapest access path whatever the stats say: it is scanned once, in
// group order, with no table access.
func (s *Store) pendingRewardCreditTotalsSQL() string {
	return `
SELECT prc.credential_tag, prc.staking_key,
       SUM(CAST(prc.amount AS ` + s.pendingCreditCastType() + `))
FROM reward_account_output prc
WHERE prc.spendable AND NOT prc.guarded AND NOT prc.folded
  AND EXISTS (SELECT 1 FROM reward_credit_round rcr
              WHERE rcr.snapshot_epoch = prc.epoch)
GROUP BY prc.credential_tag, prc.staking_key`
}

func pendingRewardCreditKey(tag uint8, stakingKey []byte) string {
	return string([]byte{tag}) + string(stakingKey)
}

// pendingRewardCreditSubquery is a scalar subquery totalling the pending
// rounds' spendable, unguarded outputs for the credential named by tagCol and
// keyCol. The indexed round lookup keeps the query's bind count independent
// of how many rounds remain unfolded.
func (s *Store) pendingRewardCreditSubquery(
	tagCol, keyCol string,
) string {
	return `(SELECT COALESCE(SUM(CAST(prc.amount AS ` + s.pendingCreditCastType() +
		`)), 0) FROM reward_account_output prc WHERE prc.credential_tag = ` +
		tagCol + ` AND prc.staking_key = ` + keyCol +
		` AND prc.spendable = TRUE AND prc.guarded = FALSE AND NOT prc.folded` +
		` AND EXISTS (SELECT 1 FROM reward_credit_round rcr` +
		` WHERE rcr.snapshot_epoch = prc.epoch))`
}

func scanRewardAccountOutputRows(
	rows *sql.Rows,
) ([]*models.RewardAccountOutput, error) {
	defer rows.Close()
	var ret []*models.RewardAccountOutput
	for rows.Next() {
		var output models.RewardAccountOutput
		var id, epoch, tag, captured, boundary int64
		var amount string
		if err := rows.Scan(
			&output.StakingKey, &output.PoolKeyHash, &output.RewardType,
			&id, &epoch, &tag, &amount, &output.Spendable, &output.Guarded,
			&captured, &boundary,
		); err != nil {
			return nil, err
		}
		value, err := parseUint64("reward account amount", amount)
		if err != nil {
			return nil, err
		}
		output.ID = uint(id)                   //nolint:gosec
		output.Epoch = uint64(epoch)           //nolint:gosec
		output.CredentialTag = uint8(tag)      //nolint:gosec
		output.CapturedSlot = uint64(captured) //nolint:gosec
		output.BoundarySlot = uint64(boundary) //nolint:gosec
		output.Amount = types.Uint64(value)
		ret = append(ret, &output)
	}
	return ret, rows.Err()
}

const rewardAccountOutputColumns = `staking_key, pool_key_hash, reward_type,
    id, epoch, credential_tag, amount, spendable, guarded, captured_slot,
    boundary_slot`

func (s *Store) GetRewardAccountOutputsForCredential(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	return s.rewardAccountOutputsForCredential(
		epochs, credentialTag, stakingKey, txn, true,
	)
}

func (s *Store) GetRewardAccountOutputsForEligibility(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	return s.rewardAccountOutputsForCredential(
		epochs, credentialTag, stakingKey, txn, false,
	)
}

func (s *Store) rewardAccountOutputsForCredential(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
	spendableOnly bool,
) ([]*models.RewardAccountOutput, error) {
	if len(epochs) == 0 || len(stakingKey) == 0 {
		return nil, nil
	}
	outputPredicate := "NOT folded"
	if spendableOnly {
		outputPredicate = credentialUnfoldedRewardCreditPredicate
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	args := []any{credentialTag, stakingKey}
	for _, epoch := range epochs {
		args = append(args, epoch)
	}
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT `+rewardAccountOutputColumns+`
FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ?
  AND epoch IN (`+bindPlaceholders(len(epochs))+`)
  AND `+outputPredicate+`
ORDER BY epoch, pool_key_hash, reward_type`), args...)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward account outputs for credential: %w", err,
		)
	}
	return scanRewardAccountOutputRows(rows)
}

func (s *Store) GetPendingRewardAccountOutputsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	if len(stakingKey) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT `+rewardAccountOutputColumns+`
FROM reward_account_output rao
WHERE rao.credential_tag = ? AND rao.staking_key = ?
  AND rao.spendable = TRUE AND rao.guarded = FALSE AND NOT rao.folded
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr
      WHERE rcr.snapshot_epoch = rao.epoch
  )
ORDER BY rao.epoch, rao.pool_key_hash, rao.reward_type`),
		credentialTag, stakingKey,
	)
	if err != nil {
		return nil, fmt.Errorf("get pending reward account outputs for credential: %w", err)
	}
	return scanRewardAccountOutputRows(rows)
}

func (s *Store) GetRewardAccountOutputsInPoolKeyHashRange(
	epoch uint64,
	lo, hi []byte,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT `+rewardAccountOutputColumns+`
FROM reward_account_output
WHERE epoch = ? AND pool_key_hash >= ? AND pool_key_hash <= ?
ORDER BY pool_key_hash, credential_tag, staking_key, reward_type`),
		epoch, lo, hi,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward account outputs in pool range: %w", err,
		)
	}
	return scanRewardAccountOutputRows(rows)
}

// pendingDRepCredits totals the pending rounds' spendable, unguarded outputs
// by the credited account's DRep delegation, for accounts whose filterCol
// (drep or drep_type) is one of values.
func (s *Store) pendingDRepCredits(
	ctx context.Context,
	db queryer,
	expiryEpoch uint64,
	filterCol string,
	values []any,
) (map[string]uint64, map[uint64]uint64, error) {
	byCredential := make(map[string]uint64)
	byType := make(map[uint64]uint64)
	if len(values) == 0 {
		return byCredential, byType, nil
	}
	hasPending, err := pendingRewardCreditOutputsExist(ctx, db)
	if err != nil {
		return nil, nil, fmt.Errorf("check pending reward credit outputs for DRep power: %w", err)
	}
	if !hasPending {
		return byCredential, byType, nil
	}
	fixedArgs := 0
	if expiryEpoch > 0 {
		fixedArgs++
	}
	chunkSize := s.dialect.ParameterLimit() - fixedArgs
	if chunkSize <= 0 {
		return nil, nil, fmt.Errorf("dialect parameter limit %d cannot fit pending DRep filters", s.dialect.ParameterLimit())
	}
	for start := 0; start < len(values); start += chunkSize {
		end := min(start+chunkSize, len(values))
		pendingExpr := s.pendingRewardCreditSubquery(
			"a.credential_tag", "a.staking_key",
		)
		args := make([]any, 0, end-start+1)
		args = append(args, values[start:end]...)
		expiry := ""
		if expiryEpoch > 0 {
			expiry = " AND (a.expiration_epoch = 0 OR a.expiration_epoch >= ?)"
			args = append(args, expiryEpoch)
		}
		// Driven from the DRep's delegators, with one indexed lookup of
		// each delegator's outputs, like the base voting-power query.
		query := `
SELECT a.drep, a.drep_type, SUM(` + pendingExpr + `)
FROM account a
WHERE a.` + filterCol + ` IN (` + bindPlaceholders(end-start) + `)
  AND a.active = TRUE` + expiry + `
GROUP BY a.drep, a.drep_type`
		if err := func() error {
			rows, err := db.QueryContext(
				ctx, s.dialect.Rebind(query), args...,
			)
			if err != nil {
				return fmt.Errorf("get pending drep credits: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var drep []byte
				var drepType, amount int64
				if err := rows.Scan(&drep, &drepType, &amount); err != nil {
					return err
				}
				if drepType < 0 || amount < 0 {
					return fmt.Errorf(
						"invalid pending drep credit type %d amount %d",
						drepType, amount,
					)
				}
				if drepType <= 1 {
					key := models.NewStakeCredentialRef(
						uint8(drepType), drep, //nolint:gosec
					).MapKey()
					byCredential[key] += uint64(amount)
				}
				byType[uint64(drepType)] += uint64(amount)
			}
			return rows.Err()
		}(); err != nil {
			return nil, nil, err
		}
	}
	return byCredential, byType, nil
}

type rewardEligibilityRef struct {
	Tag uint8  `json:"tag"`
	Key []byte `json:"key"`
}

func (s *Store) recordRewardEligibilityRecheck(
	ctx context.Context,
	db queryer,
	refs []models.StakeCredentialRef,
) error {
	if len(refs) == 0 {
		return nil
	}
	var raw string
	err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(`SELECT value FROM sync_state WHERE sync_key = ?`),
		models.RewardEligibilityRecheckKey,
	).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read reward eligibility recheck: %w", err)
	}
	var current []rewardEligibilityRef
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &current); err != nil {
			return fmt.Errorf("decode reward eligibility recheck: %w", err)
		}
	}
	seen := make(map[string]struct{}, len(current)+len(refs))
	for _, ref := range current {
		mapKey := models.NewStakeCredentialRef(ref.Tag, ref.Key).MapKey()
		seen[mapKey] = struct{}{}
	}
	for _, ref := range refs {
		if _, ok := seen[ref.MapKey()]; ok {
			continue
		}
		seen[ref.MapKey()] = struct{}{}
		current = append(
			current,
			rewardEligibilityRef{Tag: ref.Tag, Key: ref.Key},
		)
	}
	encoded, err := json.Marshal(current)
	if err != nil {
		return err
	}
	return s.upsertSyncState(ctx, db, models.RewardEligibilityRecheckKey,
		string(encoded))
}

func (s *Store) upsertSyncState(
	ctx context.Context,
	db queryer,
	key, value string,
) error {
	_, err := db.ExecContext(ctx, s.dialect.Rebind(`-- name: UpsertSyncState :exec
INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`), key, value)
	return err
}

func (s *Store) TakeRewardEligibilityRecheck(
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	raw, err := s.GetSyncState(models.RewardEligibilityRecheckKey, txn)
	if err != nil || raw == "" {
		return nil, err
	}
	var current []rewardEligibilityRef
	if err := json.Unmarshal([]byte(raw), &current); err != nil {
		return nil, fmt.Errorf("decode reward eligibility recheck: %w", err)
	}
	if err := s.DeleteSyncState(
		models.RewardEligibilityRecheckKey, txn,
	); err != nil {
		return nil, err
	}
	refs := make([]models.StakeCredentialRef, 0, len(current))
	for _, ref := range current {
		refs = append(refs, models.NewStakeCredentialRef(ref.Tag, ref.Key))
	}
	return refs, nil
}

// GetStakeCredentialsWithRegistrationEvents returns the credentials with a
// registration or deregistration certificate in the inclusive slot range.
func (s *Store) GetStakeCredentialsWithRegistrationEvents(
	fromSlot, toSlot uint64,
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	tables := append(
		append([]string(nil), accountRegistrationStateTables...),
		accountDeregistrationStateTables...,
	)
	selects := make([]string, 0, len(tables))
	args := make([]any, 0, len(tables)*2)
	for _, table := range tables {
		selects = append(selects, `SELECT credential_tag, staking_key FROM `+
			table+` WHERE added_slot >= ? AND added_slot <= ?`)
		args = append(args, fromSlot, toSlot)
	}
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(
		strings.Join(selects, " UNION ")), args...)
	if err != nil {
		return nil, fmt.Errorf("get registration events in slot range: %w", err)
	}
	defer rows.Close()
	var refs []models.StakeCredentialRef
	for rows.Next() {
		var tag uint8
		var key []byte
		if err := rows.Scan(&tag, &key); err != nil {
			return nil, err
		}
		refs = append(refs, models.NewStakeCredentialRef(tag, key))
	}
	return refs, rows.Err()
}
