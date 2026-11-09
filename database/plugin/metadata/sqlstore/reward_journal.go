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

//nolint:gosec // G101 reads "cred" in credit statement names; ids are non-negative SQL integers
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// The reward journal records every change to an account's reward balance that
// the account row alone cannot undo or explain, in three tables:
//
//   - reward_credit: stake-reward credits, one per reward_account_output row
//     applied to an account, keyed (boundary_slot, account_id, output_id), with
//     reward_credit_slot listing the slots that hold rows. A round is folded in
//     account order, so its rows append at the end of its key range. output_id
//     is opaque: the output row may be pruned, and rows migrated from the
//     former journal carry a negative id. The key is the slot and not the
//     round's snapshot epoch because the two bootstrap boundaries apply the
//     same snapshot epoch at different slots.
//   - reward_event_credit: credits outside a round (pool-retirement refunds,
//     MIR, governance, direct deposits), identified by reward_source.hash.
//     after_withdrawal records that the account already had a withdrawal at
//     the credit's slot when it was applied.
//   - reward_withdrawal: withdrawals, keyed by the withdrawing transaction.
//
// Rollback and historical reads need each credit's order relative to the
// account's withdrawals within a slot. Stake-reward credits precede every
// withdrawal of their slot: a credential's pending credits are folded into the
// journal before any withdrawal reads its balance. An event credit can follow
// one, because a Dijkstra direct deposit is applied after its transaction's
// withdrawals; after_withdrawal is what places it.
const (
	getActiveAccountRewardByIDSQL = `-- name: GetActiveAccountRewardByID :one
SELECT reward FROM account WHERE id = ? AND active = TRUE`

	getAccountIDByCredentialSQL = `-- name: GetAccountIDByCredential :one
SELECT id FROM account WHERE credential_tag = ? AND staking_key = ?`

	getAccountIdentityRewardSQL = `-- name: GetAccountIdentityReward :one
SELECT credential_tag, staking_key, reward FROM account WHERE id = ?`

	insertRewardCreditSQL = `-- name: InsertRewardCredit :exec
INSERT INTO reward_credit (boundary_slot, account_id, output_id, amount)
VALUES (?, ?, ?, ?)
ON CONFLICT (boundary_slot, account_id, output_id) DO NOTHING`

	insertRewardCreditSlotSQL = `-- name: InsertRewardCreditSlot :exec
INSERT INTO reward_credit_slot (boundary_slot) VALUES (?)
ON CONFLICT (boundary_slot) DO NOTHING`

	insertRewardSourceSQL = `-- name: InsertRewardSource :exec
INSERT INTO reward_source (hash) VALUES (?) ON CONFLICT (hash) DO NOTHING`

	getRewardSourceIDSQL = `-- name: GetRewardSourceID :one
SELECT id FROM reward_source WHERE hash = ?`

	insertRewardEventCreditSQL = `-- name: InsertRewardEventCredit :exec
INSERT INTO reward_event_credit (
    source_id, account_id, slot, amount, post_snapshot, after_withdrawal
) VALUES (?, ?, ?, ?, ?, EXISTS (
    SELECT 1 FROM reward_withdrawal WHERE account_id = ? AND slot = ?
))
ON CONFLICT (source_id, account_id, slot) DO NOTHING`

	markRewardEventCreditPostSnapshotSQL = `-- name: MarkRewardEventCreditPostSnapshot :exec
UPDATE reward_event_credit SET post_snapshot = TRUE
WHERE source_id = ? AND account_id = ? AND slot = ?`

	rewardWithdrawalExistsSQL = `-- name: RewardWithdrawalExists :one
SELECT EXISTS (
    SELECT 1 FROM reward_withdrawal WHERE transaction_id = ? AND account_id = ?
)`

	insertRewardWithdrawalSQL = `-- name: InsertRewardWithdrawal :exec
INSERT INTO reward_withdrawal (
    transaction_id, account_id, slot, amount, previous_reward
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (transaction_id, account_id) DO NOTHING`

	getTransactionIDByHashSQL = `-- name: GetWithdrawalTransactionID :one
SELECT id FROM "transaction" WHERE hash = ?`

	rollbackRewardEventAccountsSQL = `
SELECT e.account_id, e.slot, e.amount, e.after_withdrawal FROM reward_event_credit e
WHERE e.slot > ?`

	rollbackRewardWithdrawalsSQL = `
SELECT account_id, slot, transaction_id, previous_reward FROM reward_withdrawal
WHERE slot > ?`

	rollbackRewardCreditsOfSlotSQL = `
SELECT account_id, amount FROM reward_credit WHERE boundary_slot = ?`

	rolledBackRewardSlotsSQL = `
SELECT boundary_slot FROM reward_credit_slot
WHERE boundary_slot > ? ORDER BY boundary_slot`

	deleteRewardCreditsOfSlotSQL = `DELETE FROM reward_credit WHERE boundary_slot = ?`

	deleteRewardCreditSlotsAfterSlotSQL = `DELETE FROM reward_credit_slot WHERE boundary_slot > ?`

	deleteRewardEventCreditsAfterSlotSQL = `DELETE FROM reward_event_credit WHERE slot > ?`

	deleteRewardWithdrawalsAfterSlotSQL = `DELETE FROM reward_withdrawal WHERE slot > ?`

	deleteAccountRewardCreditsAfterSlotSQL = `-- name: DeleteAccountRewardCreditsAfterSlot :exec
DELETE FROM reward_credit
WHERE account_id = ? AND boundary_slot IN (
    SELECT boundary_slot FROM reward_credit_slot WHERE boundary_slot > ?
)`

	deleteAccountRewardEventCreditsAfterSlotSQL = `-- name: DeleteAccountRewardEventCreditsAfterSlot :exec
DELETE FROM reward_event_credit WHERE account_id = ? AND slot > ?`

	deleteAccountRewardWithdrawalsAfterSlotSQL = `-- name: DeleteAccountRewardWithdrawalsAfterSlot :exec
DELETE FROM reward_withdrawal WHERE account_id = ? AND slot > ?`

	deleteAccountWithdrawalWitnessAfterSlotSQL = `-- name: DeleteAccountWithdrawalWitnessAfterSlot :exec
DELETE FROM account_withdrawal_witness
WHERE added_slot > ? AND credential_tag = ? AND staking_key = ?`
)

// rewardJournalHotStatements lists the journal statements prepared at Start.
var rewardJournalHotStatements = []string{
	getActiveAccountRewardByIDSQL,
	getAccountIdentityRewardSQL,
	getAccountIDByCredentialSQL,
	getTransactionIDByHashSQL,
	insertRewardCreditSQL,
	insertRewardCreditSlotSQL,
	insertRewardSourceSQL,
	getRewardSourceIDSQL,
	insertRewardEventCreditSQL,
	markRewardEventCreditPostSnapshotSQL,
	rewardWithdrawalExistsSQL,
	claimRewardOutputSQL,
	claimAccountIDSQL,
	insertRewardWithdrawalSQL,
	deleteAccountRewardCreditsAfterSlotSQL,
	deleteAccountRewardEventCreditsAfterSlotSQL,
	deleteAccountRewardWithdrawalsAfterSlotSQL,
	deleteAccountWithdrawalWitnessAfterSlotSQL,
}

// AddAccountRewardByCredential journals one event credit and adds it to the
// account's reward balance. sourceHash names the event; a repeat of the same
// (source, account, slot) is ignored.
func (s *Store) AddAccountRewardByCredential(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn types.Txn,
) error {
	return s.addAccountRewardEvent(
		credentialTag, stakeKey, amount, slot, sourceHash, false, txn,
	)
}

// AddPostSnapshotAccountRewardByCredential is AddAccountRewardByCredential
// for a credit applied after the epoch's SNAP point.
func (s *Store) AddPostSnapshotAccountRewardByCredential(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn types.Txn,
) error {
	return s.addAccountRewardEvent(
		credentialTag, stakeKey, amount, slot, sourceHash, true, txn,
	)
}

func (s *Store) addAccountRewardEvent(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	postSnapshot bool,
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
			var reward sql.NullString
			err := s.queryRowCached(ctx, db, getActiveAccountRewardSQL,
				credentialTag, stakeKey,
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
					"account reward overflow for stake key %x", stakeKey,
				)
			}
			if _, err := s.execCached(
				ctx, db, insertRewardSourceSQL, sourceHash,
			); err != nil {
				return err
			}
			var sourceID int64
			if err := s.queryRowCached(
				ctx, db, getRewardSourceIDSQL, sourceHash,
			).Scan(&sourceID); err != nil {
				return err
			}
			result, err := s.execCached(
				ctx, db, insertRewardEventCreditSQL,
				sourceID, accountID, slotValue,
				strconv.FormatUint(amount, 10), postSnapshot,
				accountID, slotValue,
			)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				// A replay still marks the credit when this call is the one
				// that knows it applies after SNAP.
				if postSnapshot {
					_, err = s.execCached(
						ctx, db, markRewardEventCreditPostSnapshotSQL,
						sourceID, accountID, slotValue,
					)
				}
				return err
			}
			return s.creditAccountReward(
				ctx, db, txn, accountID, current+amount,
				models.NewStakeCredentialRef(credentialTag, stakeKey), slot,
			)
		},
	)
}

// creditAccountReward stores an account's new balance and refreshes its live
// stake aggregate.
func (s *Store) creditAccountReward(
	ctx context.Context,
	db queryer,
	txn types.Txn,
	accountID int64,
	balance uint64,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	result, err := s.execCached(
		ctx, db, updateAccountRewardSQL,
		strconv.FormatUint(balance, 10), accountID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return models.ErrAccountNotFound
	}
	pending, err := pendingUtxoStakeDelta(ctx, db, txn, nil, ref)
	if err != nil {
		return err
	}
	return s.refreshRewardLiveStakeAggregateNetOfPending(
		ctx, db, ref, slot, pending,
	)
}

// AddAccountRewardsByCredential journals stake-reward credits and adds each
// account's newly journaled total to its reward balance, once per account. A
// credit whose (snapshot_epoch, account, output) is already journaled is
// skipped. Credits arrive in account order from the claim; the work for one
// account is one balance read and one update however many credits it holds.
func (s *Store) AddAccountRewardsByCredential(
	credits []models.AccountRewardCredit,
	txn types.Txn,
) error {
	if len(credits) == 0 {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			type accountTotal struct {
				id     int64
				ref    models.StakeCredentialRef
				amount uint64
				slot   uint64
			}
			var order []*accountTotal
			byID := make(map[int64]*accountTotal)
			slots := make(map[int64]struct{})
			for _, credit := range credits {
				if credit.Amount == 0 {
					continue
				}
				accountID := int64(credit.AccountID)
				if accountID == 0 {
					var reward sql.NullString
					err := s.queryRowCached(ctx, db, getActiveAccountRewardSQL,
						credit.CredentialTag, credit.StakingKey,
					).Scan(&accountID, &reward)
					if errors.Is(err, sql.ErrNoRows) {
						return models.ErrAccountNotFound
					}
					if err != nil {
						return err
					}
				}
				slot, err := checkedInt64(credit.Slot)
				if err != nil {
					return err
				}
				if _, known := slots[slot]; !known {
					if _, err := s.execCached(
						ctx, db, insertRewardCreditSlotSQL, slot,
					); err != nil {
						return fmt.Errorf("journal reward credit slot: %w", err)
					}
					slots[slot] = struct{}{}
				}
				result, err := s.execCached(
					ctx, db, insertRewardCreditSQL,
					slot, accountID, int64(credit.OutputID),
					strconv.FormatUint(credit.Amount, 10),
				)
				if err != nil {
					return fmt.Errorf("journal reward credit: %w", err)
				}
				affected, err := result.RowsAffected()
				if err != nil {
					return err
				}
				if affected == 0 {
					continue
				}
				entry, ok := byID[accountID]
				if !ok {
					entry = &accountTotal{
						id: accountID,
						ref: models.NewStakeCredentialRef(
							credit.CredentialTag, credit.StakingKey,
						),
					}
					byID[accountID] = entry
					order = append(order, entry)
				}
				if entry.amount > ^uint64(0)-credit.Amount {
					return fmt.Errorf(
						"account reward overflow for stake key %x",
						credit.StakingKey,
					)
				}
				entry.amount += credit.Amount
				// The live-stake refresh stamps the last credit's slot.
				entry.slot = credit.Slot
			}
			for _, entry := range order {
				var reward sql.NullString
				err := s.queryRowCached(
					ctx, db, getActiveAccountRewardByIDSQL, entry.id,
				).Scan(&reward)
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
				if current > ^uint64(0)-entry.amount {
					return fmt.Errorf(
						"account reward overflow for stake key %x",
						entry.ref.Key,
					)
				}
				if err := s.creditAccountReward(
					ctx, db, txn, entry.id, current+entry.amount,
					entry.ref, entry.slot,
				); err != nil {
					return fmt.Errorf("credit account reward: %w", err)
				}
			}
			return nil
		},
	)
}

// ApplyAccountRewardWithdrawal clears a registered reward account after a
// withdrawal by the transaction with hash txHash. The transaction row must
// already exist, and models.ErrWithdrawalTransactionNotFound is returned
// otherwise: the real transaction insert keeps a pre-existing row's type, fee,
// ttl, metadata and valid columns, so a placeholder row would stay incomplete.
// The block path records withdrawals through applyTransactionWithdrawals.
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
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var accountID int64
			var reward sql.NullString
			err := s.queryRowCached(ctx, db, getActiveAccountRewardSQL,
				credentialTag, stakeKey,
			).Scan(&accountID, &reward)
			if errors.Is(err, sql.ErrNoRows) {
				return models.ErrAccountNotFound
			}
			if err != nil {
				return err
			}
			var transactionID int64
			err = s.queryRowCached(ctx, db, getTransactionIDByHashSQL, txHash).
				Scan(&transactionID)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf(
					"%w: %x", models.ErrWithdrawalTransactionNotFound, txHash,
				)
			}
			if err != nil {
				return err
			}
			var exists bool
			if err := s.queryRowCached(
				ctx, db, rewardWithdrawalExistsSQL, transactionID, accountID,
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
			if _, err := s.execCached(
				ctx, db, updateAccountRewardSQL, "0", accountID,
			); err != nil {
				return err
			}
			result, err := s.execCached(
				ctx, db, insertRewardWithdrawalSQL,
				transactionID, accountID, slotValue,
				strconv.FormatUint(amount, 10),
				strconv.FormatUint(current, 10),
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
				ctx, db,
				models.NewStakeCredentialRef(credentialTag, stakeKey),
				slot,
			)
		},
	)
}

// rewardRollbackAccount accumulates what a rollback undoes for one account.
type rewardRollbackAccount struct {
	// credits is the total of every credit after the rollback slot.
	credits uint64
	// beforeWithdrawal is the part of credits that precedes the account's
	// earliest withdrawal after the slot.
	beforeWithdrawal        uint64
	hasWithdrawal           bool
	withdrawalSlot          int64
	withdrawalTransactionID int64
	previous                uint64
}

// DeleteAccountRewardsAfterSlot reverts the journal above slot and each
// affected account's balance to what it was at slot.
//
// An account with a withdrawal after slot returns to the earliest such
// withdrawal's previous balance less the credits between slot and that
// withdrawal. Any other account loses every credit after slot. Neither result
// depends on the order rows are read in.
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
			accounts := make(map[int64]*rewardRollbackAccount)
			account := func(id int64) *rewardRollbackAccount {
				entry, ok := accounts[id]
				if !ok {
					entry = &rewardRollbackAccount{}
					accounts[id] = entry
				}
				return entry
			}
			if err := s.collectRollbackWithdrawals(
				ctx, db, slotValue, account,
			); err != nil {
				return err
			}
			if err := s.collectRollbackCredits(
				ctx, db, slotValue, account,
			); err != nil {
				return err
			}
			ids := make([]int64, 0, len(accounts))
			for id := range accounts {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			refs := make([]models.StakeCredentialRef, 0, len(ids))
			for _, id := range ids {
				entry := accounts[id]
				var tag uint8
				var key []byte
				var reward sql.NullString
				err := s.queryRowCached(
					ctx, db, getAccountIdentityRewardSQL, id,
				).Scan(&tag, &key, &reward)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				refs = append(refs, models.NewStakeCredentialRef(tag, key))
				value, err := rewardAfterRollback(entry, reward, key)
				if err != nil {
					return err
				}
				if _, err := s.execCached(
					ctx, db, updateAccountRewardSQL,
					strconv.FormatUint(value, 10), id,
				); err != nil {
					return err
				}
			}
			creditSlots, err := s.rolledBackRewardSlots(ctx, db, slotValue)
			if err != nil {
				return err
			}
			for _, creditSlot := range creditSlots {
				if _, err := db.ExecContext(
					ctx, deleteRewardCreditsOfSlotSQL, creditSlot,
				); err != nil {
					return err
				}
			}
			for _, statement := range []string{
				deleteRewardCreditSlotsAfterSlotSQL,
				deleteRewardEventCreditsAfterSlotSQL,
				deleteRewardWithdrawalsAfterSlotSQL,
				`DELETE FROM account_withdrawal_witness WHERE added_slot > ?`,
			} {
				if _, err := db.ExecContext(ctx, statement, slotValue); err != nil {
					return err
				}
			}
			for _, ref := range refs {
				if err := s.refreshRewardLiveStakeAggregate(
					ctx, db, ref, slot,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func rewardAfterRollback(
	entry *rewardRollbackAccount,
	reward sql.NullString,
	key []byte,
) (uint64, error) {
	if entry.hasWithdrawal {
		if entry.beforeWithdrawal > entry.previous {
			return 0, fmt.Errorf(
				"account reward rollback underflow for stake key %x", key,
			)
		}
		return entry.previous - entry.beforeWithdrawal, nil
	}
	current, err := parseNullUint64("account reward", reward)
	if err != nil {
		return 0, err
	}
	if current < entry.credits {
		return 0, fmt.Errorf(
			"account reward rollback underflow for stake key %x", key,
		)
	}
	return current - entry.credits, nil
}

func (s *Store) rolledBackRewardSlots(
	ctx context.Context,
	db queryer,
	slot int64,
) ([]int64, error) {
	rows, err := db.QueryContext(ctx, rolledBackRewardSlotsSQL, slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var slots []int64
	for rows.Next() {
		var creditSlot int64
		if err := rows.Scan(&creditSlot); err != nil {
			return nil, err
		}
		slots = append(slots, creditSlot)
	}
	return slots, rows.Err()
}

func (s *Store) collectRollbackWithdrawals(
	ctx context.Context,
	db queryer,
	slot int64,
	account func(int64) *rewardRollbackAccount,
) error {
	rows, err := db.QueryContext(ctx, rollbackRewardWithdrawalsSQL, slot)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, withdrawnAt, transactionID int64
		var raw sql.NullString
		if err := rows.Scan(&id, &withdrawnAt, &transactionID, &raw); err != nil {
			return err
		}
		previous, err := parseNullUint64(
			"reward withdrawal previous reward",
			raw,
		)
		if err != nil {
			return err
		}
		entry := account(id)
		if entry.hasWithdrawal &&
			(withdrawnAt > entry.withdrawalSlot ||
				(withdrawnAt == entry.withdrawalSlot && transactionID > entry.withdrawalTransactionID)) {
			continue
		}
		entry.hasWithdrawal = true
		entry.withdrawalSlot = withdrawnAt
		entry.withdrawalTransactionID = transactionID
		entry.previous = previous
	}
	return rows.Err()
}

func (s *Store) collectRollbackCredits(
	ctx context.Context,
	db queryer,
	slot int64,
	account func(int64) *rewardRollbackAccount,
) error {
	add := func(id, at int64, raw string, afterWithdrawal bool) error {
		amount, err := parseUint64("reward credit amount", raw)
		if err != nil {
			return err
		}
		entry := account(id)
		if entry.credits > ^uint64(0)-amount {
			return errors.New("reward rollback credit total overflows")
		}
		entry.credits += amount
		if entry.hasWithdrawal && creditPrecedesWithdrawal(
			at, afterWithdrawal, entry.withdrawalSlot,
		) {
			entry.beforeWithdrawal += amount
		}
		return nil
	}
	creditSlots, err := s.rolledBackRewardSlots(ctx, db, slot)
	if err != nil {
		return err
	}
	for _, creditSlot := range creditSlots {
		if err := func() error {
			rows, err := db.QueryContext(ctx, rollbackRewardCreditsOfSlotSQL, creditSlot)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				var raw string
				if err := rows.Scan(&id, &raw); err != nil {
					return err
				}
				if err := add(id, creditSlot, raw, false); err != nil {
					return err
				}
			}
			return rows.Err()
		}(); err != nil {
			return err
		}
	}
	rows, err := db.QueryContext(ctx, rollbackRewardEventAccountsSQL, slot)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, at int64
		var raw string
		var afterWithdrawal bool
		if err := rows.Scan(&id, &at, &raw, &afterWithdrawal); err != nil {
			return err
		}
		if err := add(id, at, raw, afterWithdrawal); err != nil {
			return err
		}
	}
	return rows.Err()
}

// creditPrecedesWithdrawal reports whether a credit at slot came before a
// withdrawal at withdrawalSlot that is the account's earliest one from that
// slot on. At the withdrawal's slot, a credit flagged after_withdrawal follows
// some withdrawal of that slot and so follows the earliest one too.
func creditPrecedesWithdrawal(
	slot int64,
	afterWithdrawal bool,
	withdrawalSlot int64,
) bool {
	return slot < withdrawalSlot || (slot == withdrawalSlot && !afterWithdrawal)
}

// deleteAccountRewardJournalAfterSlot removes one account's journal rows above
// slot, and its withdrawal witnesses, without touching its balance.
func (s *Store) deleteAccountRewardJournalAfterSlot(
	ctx context.Context,
	db queryer,
	accountID int64,
	tag uint8,
	key []byte,
	slot int64,
) error {
	if _, err := s.execCached(
		ctx, db, deleteAccountRewardCreditsAfterSlotSQL, accountID, slot,
	); err != nil {
		return err
	}
	if _, err := s.execCached(
		ctx, db, deleteAccountRewardEventCreditsAfterSlotSQL, accountID, slot,
	); err != nil {
		return err
	}
	if _, err := s.execCached(
		ctx, db, deleteAccountRewardWithdrawalsAfterSlotSQL, accountID, slot,
	); err != nil {
		return err
	}
	_, err := s.execCached(
		ctx, db, deleteAccountWithdrawalWitnessAfterSlotSQL, slot, tag, key,
	)
	return err
}

// DeleteAccountRewardJournalForCredentialsAfterSlot deletes the journal and
// withdrawal-witness rows above slot for exactly the given credentials,
// without reversing any balance.
//
// The caller has just overwritten account.reward from the snapshot, so the
// current balance is not the one the journal rows were applied against, and
// reversing them by subtraction can underflow. Clear only the stale journal
// rows; replay inserts fresh ones against the snapshot balance.
//
// A credential cert-state import did not write this run must not appear in
// refs: nothing else touches its account.reward, so deleting its journal
// here would let replay double-apply a credit already reflected in its
// current balance.
func (s *Store) DeleteAccountRewardJournalForCredentialsAfterSlot(
	slot uint64,
	refs []models.StakeCredentialRef,
	txn types.Txn,
) error {
	if len(refs) == 0 {
		return nil
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for _, ref := range refs {
				var accountID int64
				err := s.queryRowCached(ctx, db, getAccountIDByCredentialSQL,
					ref.Tag, ref.Key,
				).Scan(&accountID)
				if errors.Is(err, sql.ErrNoRows) {
					// No account row, no journal row: ids are never reused.
					if _, err := s.execCached(
						ctx, db, deleteAccountWithdrawalWitnessAfterSlotSQL,
						slotValue, ref.Tag, ref.Key,
					); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}
				if err := s.deleteAccountRewardJournalAfterSlot(
					ctx, db, accountID, ref.Tag, ref.Key, slotValue,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}
