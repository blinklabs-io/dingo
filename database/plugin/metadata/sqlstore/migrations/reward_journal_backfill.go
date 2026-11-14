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
package migrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// rewardJournalBatchSize is the number of legacy account_reward_delta ids one
// backfill transaction copies. Ids are not dense, so a batch holds at most this
// many rows.
const rewardJournalBatchSize = 100_000

const (
	rewardJournalPhaseRows   = "rows"
	rewardJournalPhaseVerify = "verify"
	rewardJournalPhaseDone   = "done"
)

type rewardJournalCursor struct {
	Phase string `json:"phase"`
	After int64  `json:"after"`
	Max   int64  `json:"max"`
}

// rewardJournalStatements holds every statement of the backfill for one
// dialect, built once when the migration is registered.
type rewardJournalStatements struct {
	maxLegacyID     string
	copySlots       string
	copyCredits     string
	copySources     string
	copyEvents      string
	copyWithdrawals string
	expectCredits   string
	matchCredits    string
	expectEvents    string
	matchEvents     string
	expectWithdraws string
	matchWithdraws  string
	countLegacy     string
	countCredits    string
	countEvents     string
	countWithdraws  string
}

// legacyRoundCreditPredicate is the class of legacy journal rows that are
// stake-reward credits: not a withdrawal, not marked post-snapshot, a 32-byte
// source hash that is no transaction's hash, and credited at the first slot of
// an epoch. A stake-reward source is derived from the reward, while a Dijkstra
// direct deposit's source is its transaction's hash. Every other credit is an
// event credit. A pre-snapshot MIR or governance credit at an epoch's first
// slot matches too and is stored as a round credit; that keeps its place
// before the slot's withdrawals, because the epoch transition that applies it
// runs before the transactions of the block at that slot.
func legacyRoundCreditPredicate(lengthFn, tx string) string {
	return "d.withdrawal = FALSE AND " + legacyRoundSource(lengthFn, tx)
}

func legacyEventCreditPredicate(lengthFn, tx string) string {
	return "d.withdrawal = FALSE AND NOT (" + legacyRoundSource(lengthFn, tx) + ")"
}

func legacyRoundSource(lengthFn, tx string) string {
	return "d.post_snapshot = FALSE AND " + lengthFn + "(d.tx_hash) = 32" +
		" AND EXISTS (SELECT 1 FROM epoch e0 WHERE e0.start_slot = d.added_slot)" +
		" AND NOT EXISTS (SELECT 1 FROM " + tx + " t0 WHERE t0.hash = d.tx_hash)"
}

func newRewardJournalStatements(dialect string) rewardJournalStatements {
	lengthFn := "LENGTH"
	if dialect == "postgres" {
		lengthFn = "OCTET_LENGTH"
	}
	tx := `"transaction"`
	if dialect == "mysql" {
		tx = "`transaction`"
	}
	nothing := func(columns, first string) string {
		if dialect == "mysql" {
			return "ON DUPLICATE KEY UPDATE " + first + " = " + first
		}
		return "ON CONFLICT (" + columns + ") DO NOTHING"
	}
	// SQLite answers DISTINCT added_slot by walking the whole added_slot
	// index rather than the batch's rowid range, which reads every legacy row
	// once per batch. NOT INDEXED still allows the rowid range.
	slotsAccess := ""
	if dialect == "sqlite" {
		slotsAccess = " NOT INDEXED"
	}
	const accountJoin = " JOIN account a ON a.credential_tag = d.credential_tag" +
		" AND a.staking_key = d.staking_key"
	const idRange = " d.id > ? AND d.id <= ?"
	round := legacyRoundCreditPredicate(lengthFn, tx)
	event := legacyEventCreditPredicate(lengthFn, tx)
	const eventSource = " JOIN reward_source s ON s.hash = d.tx_hash"
	// A legacy credit follows a withdrawal of its slot when the withdrawal's
	// row id is lower: the legacy journal was written in application order.
	const afterWithdrawal = "EXISTS (SELECT 1 FROM account_reward_delta w" +
		" WHERE w.withdrawal = TRUE AND w.credential_tag = d.credential_tag" +
		" AND w.staking_key = d.staking_key AND w.added_slot = d.added_slot" +
		" AND w.id < d.id)"
	const eventMatch = eventSource + " JOIN reward_event_credit e ON" +
		" e.source_id = s.id AND e.account_id = a.id AND e.slot = d.added_slot" +
		" AND e.amount = d.amount AND e.post_snapshot = d.post_snapshot" +
		" AND e.after_withdrawal = " + afterWithdrawal
	withdrawalJoin := accountJoin + " JOIN " + tx + " tx ON tx.hash = d.tx_hash"
	return rewardJournalStatements{
		maxLegacyID: "SELECT COALESCE(MAX(id), 0) FROM account_reward_delta",
		copySlots: "INSERT INTO reward_credit_slot (boundary_slot) SELECT DISTINCT d.added_slot" +
			" FROM account_reward_delta d" + slotsAccess + " WHERE" + idRange + " AND " + round +
			" " + nothing(
			"boundary_slot",
			"boundary_slot",
		),
		copyCredits: "INSERT INTO reward_credit (boundary_slot, account_id, output_id, amount)" +
			" SELECT d.added_slot, a.id, -d.id, d.amount FROM account_reward_delta d" +
			accountJoin + " WHERE" + idRange + " AND " + round +
			" ORDER BY d.added_slot, a.id, d.id " +
			nothing(
				"boundary_slot, account_id, output_id",
				"boundary_slot",
			),
		copySources: "INSERT INTO reward_source (hash) SELECT DISTINCT d.tx_hash" +
			" FROM account_reward_delta d WHERE" + idRange + " AND " + event +
			" " + nothing(
			"hash",
			"hash",
		),
		copyEvents: "INSERT INTO reward_event_credit (source_id, account_id, slot, amount, post_snapshot, after_withdrawal)" +
			" SELECT s.id, a.id, d.added_slot, d.amount, d.post_snapshot, " + afterWithdrawal +
			" FROM account_reward_delta d" +
			accountJoin + eventSource + " WHERE" + idRange + " AND " + event +
			" ORDER BY d.id " + nothing(
			"source_id, account_id, slot",
			"source_id",
		),
		copyWithdrawals: "INSERT INTO reward_withdrawal (tx_id, account_id, slot, amount, previous_reward)" +
			" SELECT tx.id, a.id, d.added_slot, d.amount, COALESCE(d.previous_reward, '0')" +
			" FROM account_reward_delta d" + withdrawalJoin +
			" WHERE" + idRange + " AND d.withdrawal = TRUE ORDER BY tx.id, a.id " +
			nothing(
				"tx_id, account_id",
				"tx_id",
			),
		expectCredits: "SELECT COUNT(*) FROM account_reward_delta d WHERE" + idRange + " AND " + round,
		matchCredits: "SELECT COUNT(*) FROM account_reward_delta d" + accountJoin +
			" JOIN reward_credit c ON c.boundary_slot = d.added_slot" +
			" AND c.account_id = a.id AND c.output_id = -d.id AND c.amount = d.amount" +
			" WHERE" + idRange + " AND " + round,
		expectEvents: "SELECT COUNT(*) FROM account_reward_delta d WHERE" + idRange + " AND " + event,
		matchEvents: "SELECT COUNT(*) FROM account_reward_delta d" + accountJoin + eventMatch +
			" WHERE" + idRange + " AND " + event,
		expectWithdraws: "SELECT COUNT(*) FROM account_reward_delta d WHERE" + idRange +
			" AND d.withdrawal = TRUE",
		matchWithdraws: "SELECT COUNT(*) FROM account_reward_delta d" + withdrawalJoin +
			" JOIN reward_withdrawal w ON w.tx_id = tx.id AND w.account_id = a.id" +
			" AND w.slot = d.added_slot AND w.amount = d.amount" +
			" AND w.previous_reward = COALESCE(d.previous_reward, '0')" +
			" WHERE" + idRange + " AND d.withdrawal = TRUE",
		countLegacy:    "SELECT COUNT(*) FROM account_reward_delta",
		countCredits:   "SELECT COUNT(*) FROM reward_credit WHERE output_id < 0",
		countEvents:    "SELECT COUNT(*) FROM reward_event_credit",
		countWithdraws: "SELECT COUNT(*) FROM reward_withdrawal",
	}
}

// rewardJournalBackfill moves account_reward_delta into reward_credit,
// reward_event_credit and reward_withdrawal. Every batch is idempotent and
// verifies, row by row through the new tables' keys, that each legacy row of
// its id range is present with the same amount; the final phase compares row
// counts so that rows in the new tables with no legacy counterpart fail too.
// The legacy table is dropped only by the contract phase, after this returns.
func rewardJournalBackfill(dialect string) Backfill {
	stmts := newRewardJournalStatements(dialect)
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		cursor := rewardJournalCursor{}
		if batch.Cursor != "" {
			if err := json.Unmarshal([]byte(batch.Cursor), &cursor); err != nil {
				return BatchResult{}, fmt.Errorf(
					"decode reward journal cursor: %w",
					err,
				)
			}
		}
		next := func(c rewardJournalCursor, rows int64, done bool) (BatchResult, error) {
			raw, err := json.Marshal(c)
			if err != nil {
				return BatchResult{}, err
			}
			return BatchResult{Cursor: string(raw), Rows: rows, Done: done}, nil
		}
		switch cursor.Phase {
		case "":
			var maxID int64
			if err := batch.Tx.QueryRowContext(
				ctx, batch.Rebind(stmts.maxLegacyID),
			).Scan(&maxID); err != nil {
				return BatchResult{}, fmt.Errorf(
					"read legacy reward journal bound: %w",
					err,
				)
			}
			return next(rewardJournalCursor{
				Phase: rewardJournalPhaseRows, Max: maxID,
			}, 0, false)
		case rewardJournalPhaseRows:
			upper := min(cursor.After+int64(max(batch.Limit, 1)), cursor.Max)
			if upper > cursor.After {
				if err := copyRewardJournalRange(ctx, batch, stmts, cursor.After, upper); err != nil {
					return BatchResult{}, err
				}
			}
			copied := upper - cursor.After
			cursor.After = upper
			if upper >= cursor.Max {
				cursor.Phase = rewardJournalPhaseVerify
			}
			return next(cursor, copied, false)
		case rewardJournalPhaseVerify:
			var legacy, credits, events, withdrawals int64
			for statement, target := range map[string]*int64{
				stmts.countLegacy:    &legacy,
				stmts.countCredits:   &credits,
				stmts.countEvents:    &events,
				stmts.countWithdraws: &withdrawals,
			} {
				if err := batch.Tx.QueryRowContext(
					ctx, batch.Rebind(statement),
				).Scan(target); err != nil {
					return BatchResult{}, fmt.Errorf(
						"count reward journal rows: %w",
						err,
					)
				}
			}
			if legacy != credits+events+withdrawals {
				return BatchResult{}, fmt.Errorf(
					"reward journal digest mismatch: %d legacy rows, %d credits + %d events + %d withdrawals",
					legacy,
					credits,
					events,
					withdrawals,
				)
			}
			cursor.Phase = rewardJournalPhaseDone
			return next(cursor, 0, true)
		case rewardJournalPhaseDone:
			return next(cursor, 0, true)
		default:
			return BatchResult{}, errors.New(
				"unknown reward journal backfill phase " + strings.TrimSpace(
					cursor.Phase,
				),
			)
		}
	}
}

func copyRewardJournalRange(
	ctx context.Context,
	batch Batch,
	stmts rewardJournalStatements,
	after, upper int64,
) error {
	for _, step := range []struct {
		name      string
		statement string
	}{
		{"credit slots", stmts.copySlots},
		{"credits", stmts.copyCredits},
		{"event sources", stmts.copySources},
		{"event credits", stmts.copyEvents},
		{"withdrawals", stmts.copyWithdrawals},
	} {
		if _, err := batch.Tx.ExecContext(
			ctx, batch.Rebind(step.statement), after, upper,
		); err != nil {
			return fmt.Errorf("copy reward journal %s: %w", step.name, err)
		}
	}
	for _, check := range []struct {
		name          string
		expect, match string
	}{
		{"credits", stmts.expectCredits, stmts.matchCredits},
		{"event credits", stmts.expectEvents, stmts.matchEvents},
		{"withdrawals", stmts.expectWithdraws, stmts.matchWithdraws},
	} {
		var expected, matched int64
		if err := batch.Tx.QueryRowContext(
			ctx, batch.Rebind(check.expect), after, upper,
		).Scan(&expected); err != nil {
			return fmt.Errorf(
				"count legacy reward journal %s: %w",
				check.name,
				err,
			)
		}
		if err := batch.Tx.QueryRowContext(
			ctx, batch.Rebind(check.match), after, upper,
		).Scan(&matched); err != nil {
			return fmt.Errorf("verify reward journal %s: %w", check.name, err)
		}
		if expected != matched {
			return fmt.Errorf(
				"reward journal digest mismatch for %s of legacy ids (%d, %d]: %d legacy rows, %d migrated",
				check.name,
				after,
				upper,
				expected,
				matched,
			)
		}
	}
	return nil
}
