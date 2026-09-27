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

func decodeRewardCreditRounds(raw string) ([]models.RewardCreditRound, error) {
	if raw == "" {
		return nil, nil
	}
	var rounds []models.RewardCreditRound
	if err := json.Unmarshal([]byte(raw), &rounds); err != nil {
		return nil, fmt.Errorf("decode pending reward credit rounds: %w", err)
	}
	return rounds, nil
}

func (s *Store) GetPendingRewardCreditRounds(
	txn types.Txn,
) ([]models.RewardCreditRound, error) {
	raw, err := s.GetSyncState(models.PendingRewardCreditRoundsKey, txn)
	if err != nil {
		return nil, err
	}
	return decodeRewardCreditRounds(raw)
}

func (s *Store) SetPendingRewardCreditRounds(
	rounds []models.RewardCreditRound,
	txn types.Txn,
) error {
	if len(rounds) == 0 {
		return s.DeleteSyncState(models.PendingRewardCreditRoundsKey, txn)
	}
	raw, err := json.Marshal(rounds)
	if err != nil {
		return fmt.Errorf("encode pending reward credit rounds: %w", err)
	}
	return s.SetSyncState(models.PendingRewardCreditRoundsKey, string(raw), txn)
}

// pendingRewardCreditEpochs reads the pending rounds' snapshot epochs through
// the caller's own handle, so a read inside a boundary transaction sees the
// round that transaction just applied.
func (s *Store) pendingRewardCreditEpochs(
	ctx context.Context,
	db queryer,
) ([]uint64, error) {
	var raw string
	err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(`SELECT value FROM sync_state WHERE sync_key = ?`),
		models.PendingRewardCreditRoundsKey,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pending reward credit rounds: %w", err)
	}
	rounds, err := decodeRewardCreditRounds(raw)
	if err != nil {
		return nil, err
	}
	epochs := make([]uint64, 0, len(rounds))
	for _, round := range rounds {
		epochs = append(epochs, round.SnapshotEpoch)
	}
	return epochs, nil
}

// deleteRewardCreditRoundsAfterSlot drops rounds applied at a boundary a
// rollback undoes, and the fold progress recorded for them.
func (s *Store) deleteRewardCreditRoundsAfterSlot(
	ctx context.Context,
	db queryer,
	slot uint64,
) error {
	var raw string
	err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(`SELECT value FROM sync_state WHERE sync_key = ?`),
		models.PendingRewardCreditRoundsKey,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	rounds, err := decodeRewardCreditRounds(raw)
	if err != nil {
		return err
	}
	kept := rounds[:0]
	for _, round := range rounds {
		if round.BoundarySlot > slot {
			continue
		}
		kept = append(kept, round)
	}
	if len(kept) == len(rounds) {
		return nil
	}
	if len(kept) == 0 {
		_, err := db.ExecContext(
			ctx,
			s.dialect.Rebind(`DELETE FROM sync_state WHERE sync_key = ?`),
			models.PendingRewardCreditRoundsKey,
		)
		return err
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(
		ctx,
		s.dialect.Rebind(`UPDATE sync_state SET value = ? WHERE sync_key = ?`),
		string(encoded),
		models.PendingRewardCreditRoundsKey,
	)
	return err
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
// keyCol. Its bind arguments are the epochs.
func (s *Store) pendingRewardCreditSubquery(
	tagCol, keyCol string,
	epochs []uint64,
) (string, []any) {
	args := make([]any, len(epochs))
	for i, epoch := range epochs {
		args[i] = epoch
	}
	return `(SELECT COALESCE(SUM(CAST(prc.amount AS ` + s.pendingCreditCastType() +
		`)), 0) FROM reward_account_output prc WHERE prc.credential_tag = ` +
		tagCol + ` AND prc.staking_key = ` + keyCol +
		` AND prc.spendable = TRUE AND prc.guarded = FALSE AND prc.epoch IN (` +
		bindPlaceholders(
			len(epochs),
		) + `))`, args
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
	if len(epochs) == 0 || len(stakingKey) == 0 {
		return nil, nil
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
ORDER BY epoch, pool_key_hash, reward_type`), args...)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward account outputs for credential: %w", err,
		)
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
	epochs []uint64,
	expiryEpoch uint64,
	filterCol string,
	values []any,
) (map[string]uint64, map[uint64]uint64, error) {
	byCredential := make(map[string]uint64)
	byType := make(map[uint64]uint64)
	if len(epochs) == 0 || len(values) == 0 {
		return byCredential, byType, nil
	}
	chunkSize := max(1, s.dialect.ParameterLimit()-len(epochs)-1)
	for start := 0; start < len(values); start += chunkSize {
		end := min(start+chunkSize, len(values))
		pendingExpr, args := s.pendingRewardCreditSubquery(
			"a.credential_tag", "a.staking_key", epochs,
		)
		args = append(args, values[start:end]...)
		expiry := ""
		if expiryEpoch > 0 {
			expiry = " AND (a.expiration_epoch = 0 OR a.expiration_epoch >= ?)"
			args = append(args, expiryEpoch)
		}
		// Driven from the DRep's delegators, with one indexed lookup of
		// each delegator's outputs, like the base voting-power query.
		var sb strings.Builder
		sb.WriteString(`
SELECT a.drep, a.drep_type, SUM(` + pendingExpr + `)
FROM account a
WHERE a.` + filterCol + ` IN (` + bindPlaceholders(end-start) + `)
  AND a.active = TRUE` + expiry + `
GROUP BY a.drep, a.drep_type`)
		if err := func() error {
			rows, err := db.QueryContext(
				ctx, s.dialect.Rebind(sb.String()), args...,
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
				if drepType <= 1 {
					key := models.NewStakeCredentialRef(
						uint8(drepType), drep, //nolint:gosec
					).MapKey()
					byCredential[key] += uint64(amount) //nolint:gosec
				}
				byType[uint64(drepType)] += uint64(amount) //nolint:gosec
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
