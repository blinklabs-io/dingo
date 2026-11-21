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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.RewardFoldStore = (*Store)(nil)

// UnfoldedRewardCreditRounds implements metadata.RewardFoldStore.
func (s *Store) UnfoldedRewardCreditRounds(
	ctx context.Context,
	txn types.Txn,
) ([]models.RewardCreditRound, error) {
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	if txn == nil {
		txCtx = ctx
	}
	rows, err := db.QueryContext(txCtx, `
SELECT rcr.snapshot_epoch, rcr.boundary_slot FROM reward_credit_round rcr
WHERE EXISTS (
    SELECT 1 FROM reward_account_output rao
    WHERE rao.spendable = TRUE AND rao.guarded = FALSE
      AND rao.folded = FALSE AND rao.epoch = rcr.snapshot_epoch
)
ORDER BY rcr.snapshot_epoch`)
	if err != nil {
		return nil, fmt.Errorf("list unfolded reward credit rounds: %w", err)
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
				"invalid reward credit round epoch %d slot %d", epoch, slot,
			)
		}
		rounds = append(rounds, models.RewardCreditRound{
			SnapshotEpoch: uint64(epoch),
			BoundarySlot:  uint64(slot),
		})
	}
	return rounds, rows.Err()
}

// FoldRewardOutputsForCredits implements metadata.RewardFoldStore.
func (s *Store) FoldRewardOutputsForCredits(
	credits []models.AccountRewardCredit,
	txn types.Txn,
) error {
	type credential struct {
		key string
		tag uint8
	}
	byCredential := make(map[credential]map[string]struct{})
	for _, c := range credits {
		id := credential{tag: c.CredentialTag, key: string(c.StakingKey)}
		if byCredential[id] == nil {
			byCredential[id] = make(map[string]struct{})
		}
		byCredential[id][string(c.SourceHash)] = struct{}{}
	}
	if len(byCredential) == 0 {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for id, hashes := range byCredential {
				fold, err := s.unfoldedOutputsFor(ctx, db, id.tag, []byte(id.key), hashes)
				if err != nil {
					return err
				}
				chunk := max(1, s.dialect.ParameterLimit())
				for start := 0; start < len(fold); start += chunk {
					end := min(start+chunk, len(fold))
					if _, err := db.ExecContext(ctx, s.dialect.Rebind(
						`UPDATE reward_account_output SET folded = TRUE WHERE id IN (`+
							bindPlaceholders(end-start)+`)`,
					), fold[start:end]...); err != nil {
						return fmt.Errorf("fold copied reward outputs: %w", err)
					}
				}
			}
			return nil
		},
	)
}

// unfoldedOutputsFor returns the ids of a credential's unfolded outputs whose
// credit source hash is in hashes.
func (s *Store) unfoldedOutputsFor(
	ctx context.Context,
	db queryer,
	tag uint8,
	key []byte,
	hashes map[string]struct{},
) ([]any, error) {
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT id, epoch, pool_key_hash, reward_type FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ? AND `+
		credentialUnfoldedRewardCreditPredicate), tag, key)
	if err != nil {
		return nil, fmt.Errorf("read unfolded reward outputs: %w", err)
	}
	defer rows.Close()
	var ids []any
	for rows.Next() {
		var id, epoch int64
		var pool []byte
		var rewardType string
		if err := rows.Scan(&id, &epoch, &pool, &rewardType); err != nil {
			return nil, err
		}
		if epoch < 0 {
			return nil, fmt.Errorf("invalid reward output epoch %d", epoch)
		}
		hash := models.StakeRewardSourceHash(
			uint64(epoch), pool, tag, key, rewardType,
		)
		if _, ok := hashes[string(hash)]; ok {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}
