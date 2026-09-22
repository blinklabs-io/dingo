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
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// ErrRewardCreditWithdrawnSince is returned by CorrectAccountRewardCredit
// when the credit being corrected has a later real withdrawal on the same
// credential. The credit row's amount is still corrected (for audit
// purposes and for any boundary that historicalRewardsBatch resolves via a
// no-withdrawal-found scan of individual credits), but a boundary between
// the correction and that withdrawal's own slot is resolved by
// historicalRewardsBatch via the withdrawal row's own previous_reward
// field, not by summing credit rows -- and this codebase has no mechanism
// (equivalent to a hypothetical reconciled_amount column) to override that
// field without also changing what a rollback across the withdrawal
// restores it to. A credential in this state needs a separate, dedicated
// fix; callers should surface it rather than treat the credit correction as
// complete for every historical read.
var ErrRewardCreditWithdrawnSince = errors.New(
	"reward credit has a later withdrawal; historical reads before it are not corrected",
)

// CorrectAccountRewardCredit overwrites a single, already-applied stake
// reward credit's amount, for repairing a credit that was computed from a
// historically wrong stake input (dingo #4529: pools
// 2bf19282e11384ccf60c9a3b0f5b6e00e74aed2cbd8bc9837ccc0966 and
// 881f9bc5415bb7381dc4b6571ab662eff5277d78d778d05614cdf0d4 each retired and
// later re-registered before the dc8e29bd (#4528) reap-after-delegation
// guard existed, wrongly resurrecting their stale delegators' stake into the
// mark[646] snapshot's total for the epoch this credit's calculation used).
// It is not a withdrawal and is not a ReconcileAccountRewardBalance-style
// out-of-band overwrite: it corrects the ordinary credit row itself
// (identified by credentialTag, stakeKey, addedSlot, and the reward-source
// tx_hash) to the amount a correct calculation would have produced, using
// the exact identity AddAccountRewardByCredential wrote it under.
//
// If no real withdrawal has cleared this credential's balance since
// addedSlot, account.reward is still a pure running sum of every credit
// since the last reset and is adjusted by the same delta so it stays
// consistent with the corrected row, and historicalRewardsBatch's
// no-withdrawal-found path (which sums individual credit rows) sees the
// correction for every boundary.
//
// If a later withdrawal exists, the real on-chain withdrawal amount is
// already the source of truth for the current live balance, so account.reward
// is left untouched. The credit row's amount is still corrected, but see
// ErrRewardCreditWithdrawnSince: a boundary before that withdrawal's own
// slot is resolved via the withdrawal row's previous_reward, which this
// method does not have a safe way to adjust, so it is returned wrapped in
// ErrRewardCreditWithdrawnSince alongside a nil error for the write itself.
func (s *Store) CorrectAccountRewardCredit(
	credentialTag uint8,
	stakeKey []byte,
	addedSlot uint64,
	sourceHash []byte,
	correctedAmount uint64,
	txn types.Txn,
) error {
	if sourceHash == nil {
		sourceHash = []byte{}
	}
	slotValue, err := checkedInt64(addedSlot)
	if err != nil {
		return err
	}
	var withdrawnSince bool
	writeErr := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var rowID int64
			var rawAmount string
			err := db.QueryRowContext(ctx, `
SELECT id, amount FROM account_reward_delta
WHERE withdrawal = FALSE AND credential_tag = ? AND staking_key = ?
  AND added_slot = ? AND tx_hash = ?`,
				credentialTag, stakeKey, slotValue, sourceHash,
			).Scan(&rowID, &rawAmount)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf(
					"%w: no credit row for tag=%d key=%x slot=%d",
					models.ErrAccountNotFound,
					credentialTag,
					stakeKey,
					addedSlot,
				)
			}
			if err != nil {
				return err
			}
			original, err := parseUint64(
				"original reward credit amount",
				rawAmount,
			)
			if err != nil {
				return err
			}
			if original == correctedAmount {
				return nil
			}

			var existsLaterWithdrawal bool
			if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM account_reward_delta
    WHERE withdrawal = TRUE AND credential_tag = ? AND staking_key = ?
      AND added_slot > ?
)`,
				credentialTag, stakeKey, slotValue,
			).Scan(&existsLaterWithdrawal); err != nil {
				return err
			}

			if _, err := db.ExecContext(ctx, `
UPDATE account_reward_delta SET amount = ? WHERE id = ?`,
				strconv.FormatUint(correctedAmount, 10), rowID,
			); err != nil {
				return err
			}

			if existsLaterWithdrawal {
				// The real withdrawal already established the true live
				// balance for every boundary at or after its own slot; a
				// boundary before it is resolved via that withdrawal row's
				// previous_reward, which is intentionally left untouched
				// here -- see ErrRewardCreditWithdrawnSince.
				withdrawnSince = true
				return nil
			}

			var accountID int64
			var currentRaw sql.NullString
			if err := db.QueryRowContext(ctx, `
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`,
				credentialTag, stakeKey,
			).Scan(&accountID, &currentRaw); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return models.ErrAccountNotFound
				}
				return err
			}
			current, err := parseNullUint64("account reward", currentRaw)
			if err != nil {
				return err
			}
			var newReward uint64
			if correctedAmount > original {
				delta := correctedAmount - original
				if ^uint64(0)-current < delta {
					return fmt.Errorf(
						"account reward overflow correcting credit for stake key %x",
						stakeKey,
					)
				}
				newReward = current + delta
			} else {
				delta := original - correctedAmount
				if current < delta {
					return fmt.Errorf(
						"account reward underflow correcting credit for stake key %x: current=%d delta=%d",
						stakeKey, current, delta,
					)
				}
				newReward = current - delta
			}
			if _, err := db.ExecContext(ctx, `
UPDATE account SET reward = ? WHERE id = ?`,
				strconv.FormatUint(newReward, 10), accountID,
			); err != nil {
				return err
			}
			return s.refreshRewardLiveStakeAggregate(
				ctx, db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				addedSlot,
			)
		},
	)
	if writeErr != nil {
		return writeErr
	}
	if withdrawnSince {
		return ErrRewardCreditWithdrawnSince
	}
	return nil
}
