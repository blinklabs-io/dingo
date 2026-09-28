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

func pendingRewardCreditOutputsExist(ctx context.Context, db queryer) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM reward_account_output rao
    WHERE rao.spendable = TRUE AND rao.guarded = FALSE AND rao.folded = FALSE
      AND EXISTS (
          SELECT 1 FROM reward_credit_round rcr
          WHERE rcr.snapshot_epoch = rao.epoch
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
WHERE rao.spendable = TRUE AND rao.guarded = FALSE AND rao.folded = FALSE
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

// unfoldedRewardCreditPredicate selects a credited round's reward_account_output
// rows that are part of their account's balance but not yet in
// account.reward.
const unfoldedRewardCreditPredicate = `spendable = TRUE AND guarded = FALSE AND folded = FALSE`

// FoldRewardAccountOutputs marks a credential's unfolded credits of the given
// rounds as added to account.reward, in the transaction that adds them.
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
  AND `+unfoldedRewardCreditPredicate), args...); err != nil {
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
  AND `+unfoldedRewardCreditPredicate+`
  AND EXISTS (
      SELECT 1 FROM reward_credit_round rcr
      WHERE rcr.snapshot_epoch = reward_account_output.epoch
  )`), credentialTag, stakingKey); err != nil {
			return fmt.Errorf("fold pending reward account outputs: %w", err)
		}
		return nil
	})
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
		` AND prc.spendable = TRUE AND prc.guarded = FALSE AND prc.folded = FALSE` +
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
	outputPredicate := "folded = FALSE"
	if spendableOnly {
		outputPredicate = unfoldedRewardCreditPredicate
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
  AND rao.spendable = TRUE AND rao.guarded = FALSE AND rao.folded = FALSE
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
	_, err := db.ExecContext(ctx, s.dialect.Rebind(`
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

// RewardCreditsAlreadyApplied reports, for each credit, whether
// AddAccountRewardByCredential has already journaled it.
func (s *Store) RewardCreditsAlreadyApplied(
	credits []models.AccountRewardCredit,
	txn types.Txn,
) ([]bool, error) {
	ret := make([]bool, len(credits))
	if len(credits) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	hashes := make([]any, 0, len(credits))
	seen := make(map[string]struct{}, len(credits))
	for _, credit := range credits {
		hash := credit.SourceHash
		if hash == nil {
			hash = []byte{}
		}
		if _, ok := seen[string(hash)]; ok {
			continue
		}
		seen[string(hash)] = struct{}{}
		hashes = append(hashes, hash)
	}
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT tx_hash, credential_tag, staking_key, added_slot
FROM account_reward_delta
WHERE withdrawal = FALSE AND tx_hash IN (`+bindPlaceholders(len(hashes))+`)`),
		hashes...,
	)
	if err != nil {
		return nil, fmt.Errorf("check reward credits applied: %w", err)
	}
	defer rows.Close()
	applied := make(map[rewardCreditJournalKey]struct{})
	for rows.Next() {
		var hash, key []byte
		var tag, slot int64
		if err := rows.Scan(&hash, &tag, &key, &slot); err != nil {
			return nil, err
		}
		applied[rewardCreditJournalKey{
			sourceHash:    string(hash),
			credentialTag: uint8(tag), //nolint:gosec
			stakingKey:    string(key),
			slot:          uint64(slot), //nolint:gosec
		}] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, credit := range credits {
		hash := credit.SourceHash
		if hash == nil {
			hash = []byte{}
		}
		_, ret[i] = applied[rewardCreditJournalKey{
			sourceHash:    string(hash),
			credentialTag: credit.CredentialTag,
			stakingKey:    string(credit.StakingKey),
			slot:          credit.Slot,
		}]
	}
	return ret, nil
}
