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

package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// lateRewardFixture is a ledger store and an IndexerDB whose writer has
// already passed the slot the ledger dates a reward precompute at.
type lateRewardFixture struct {
	src, dst       *database.Database
	srcRaw, dstRaw *sql.DB
	f              *MetadataFacts
}

func newLateRewardFixture(t *testing.T) *lateRewardFixture {
	t.Helper()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f, err := NewMetadataFacts(src, dst)
	require.NoError(t, err)
	return &lateRewardFixture{
		src: src, dst: dst, srcRaw: srcRaw, dstRaw: dstRaw, f: f,
	}
}

// restart is a writer restart: the facts source is rebuilt over the same
// stores, so only what the IndexerDB persisted survives.
func (l *lateRewardFixture) restart(t *testing.T) {
	t.Helper()
	f, err := NewMetadataFacts(l.src, l.dst)
	require.NoError(t, err)
	l.f = f
}

func (l *lateRewardFixture) copyRange(t *testing.T, from, through uint64) {
	t.Helper()
	writeFacts(t, l.f, func(txn *database.Txn) error {
		return l.f.CopyFacts(context.Background(), txn, from, through)
	})
}

func cursorJSON(epoch uint64, done bool, fingerprint string, slot uint64) string {
	return fmt.Sprintf(
		`{"format_version":2,"snapshot_epoch":%d,"done":%t,`+
			`"output_fingerprint":%q,"captured_slot":%d}`,
		epoch, done, fingerprint, slot,
	)
}

// ledgerPrecomputes is the ledger finishing a precompute round: the pool and
// account outputs dated at the slot the round captured, then the done cursor.
func (l *lateRewardFixture) ledgerPrecomputes(
	t *testing.T,
	epoch, slot uint64,
	fingerprint string,
	amount string,
) {
	t.Helper()
	_, err := l.srcRaw.Exec(`INSERT OR REPLACE INTO reward_pool_output
		(apparent_performance, pool_key_hash, epoch, optimal_reward,
		 total_reward, leader_reward, member_reward_total, owner_stake,
		 undistributed, unspendable, captured_slot, boundary_slot)
		VALUES ('1', ?, ?, '9', '8', '3', '5', '1', '0', '0', ?, ?)`,
		rewardTestPool, epoch, slot, slot)
	require.NoError(t, err)
	_, err = l.srcRaw.Exec(`INSERT OR REPLACE INTO reward_account_output
		(staking_key, pool_key_hash, reward_type, epoch, credential_tag,
		 amount, spendable, guarded, captured_slot, boundary_slot, folded)
		VALUES (?, ?, 'member', ?, 0, ?, 1, 0, ?, ?, 0)`,
		rewardTestKey, rewardTestPool, epoch, amount, slot, slot)
	require.NoError(t, err)
	_, err = l.srcRaw.Exec(`INSERT OR REPLACE INTO reward_ada_pots
		(epoch, treasury, reserves, fees, rewards, captured_slot)
		VALUES (?, '1', '2', '3', '8', ?)`, epoch, slot)
	require.NoError(t, err)
	_, err = l.srcRaw.Exec(
		`INSERT OR REPLACE INTO sync_state (sync_key, value) VALUES (?, ?)`,
		models.RewardPrecomputeCursorPrefix+fmt.Sprint(epoch),
		cursorJSON(epoch, true, fingerprint, slot),
	)
	require.NoError(t, err)
}

func (l *lateRewardFixture) poolRows(t *testing.T, epoch uint64) int {
	t.Helper()
	return scalar[int](
		t, l.dstRaw,
		`SELECT COUNT(*) FROM reward_pool_output WHERE epoch = ?`, epoch,
	)
}

func (l *lateRewardFixture) accountAmount(t *testing.T, epoch uint64) string {
	t.Helper()
	var amount sql.NullString
	err := l.dstRaw.QueryRow(
		`SELECT amount FROM reward_account_output WHERE epoch = ?`, epoch,
	).Scan(&amount)
	if err == sql.ErrNoRows {
		return ""
	}
	require.NoError(t, err)
	return amount.String
}

// The ledger finishes a precompute after the writer has copied past the slot
// it dates the round at: the slot range never covers those rows again, so the
// copy has to pick the epoch up by key.
func TestMetadataFactsCopiesRewardOutputsFinishedAfterTheWriterPassedThem(
	t *testing.T,
) {
	t.Parallel()
	l := newLateRewardFixture(t)

	l.copyRange(t, 0, 100)
	require.Zero(t, l.poolRows(t, 12))

	l.ledgerPrecomputes(t, 12, 90, "fp-a", "500")
	l.copyRange(t, 101, 200)

	require.Equal(t, 1, l.poolRows(t, 12),
		"a pool output dated before the writer's position must be copied")
	require.Equal(t, "500", l.accountAmount(t, 12),
		"an account output dated before the writer's position must be copied")
	require.Equal(t, "8", scalar[string](t, l.dstRaw,
		`SELECT rewards FROM reward_ada_pots WHERE epoch = 12`),
		"the completed round's rewards total must reach the pots row")

	// A later segment does not read the epoch again: a fold the writer
	// applied survives and nothing is rewritten.
	_, err := l.dstRaw.Exec(`UPDATE reward_account_output SET folded = 1`)
	require.NoError(t, err)
	_, err = l.dstRaw.Exec(`UPDATE reward_pool_output SET total_reward = '77'`)
	require.NoError(t, err)
	l.copyRange(t, 201, 300)
	require.Equal(t, "77", scalar[string](
		t, l.dstRaw, `SELECT total_reward FROM reward_pool_output`),
		"an epoch already copied is not copied again")
	require.True(t, scalar[bool](
		t, l.dstRaw, `SELECT folded FROM reward_account_output`))
}

// The record of what was copied is durable: a restarted writer neither loses
// an epoch it still owes nor repeats one it copied.
func TestMetadataFactsLateRewardOutputsSurviveAWriterRestart(t *testing.T) {
	t.Parallel()
	l := newLateRewardFixture(t)

	l.copyRange(t, 0, 100)
	l.ledgerPrecomputes(t, 12, 90, "fp-a", "500")
	l.restart(t)
	l.copyRange(t, 101, 200)
	require.Equal(t, "500", l.accountAmount(t, 12),
		"a writer restarted before the copy still copies the epoch")

	_, err := l.dstRaw.Exec(`UPDATE reward_pool_output SET total_reward = '77'`)
	require.NoError(t, err)
	l.restart(t)
	l.copyRange(t, 201, 300)
	require.Equal(t, "77", scalar[string](
		t, l.dstRaw, `SELECT total_reward FROM reward_pool_output`),
		"a restarted writer does not copy a finished epoch again")
}

// A round the ledger recomputes (a rollback below its inputs deleted the
// outputs, the precompute ran again) is copied again, over the earlier rows.
func TestMetadataFactsCopiesARecomputedRewardRound(t *testing.T) {
	t.Parallel()
	l := newLateRewardFixture(t)

	l.copyRange(t, 0, 100)
	l.ledgerPrecomputes(t, 12, 90, "fp-a", "500")
	l.copyRange(t, 101, 200)
	require.Equal(t, "500", l.accountAmount(t, 12))

	l.ledgerPrecomputes(t, 12, 90, "fp-b", "640")
	l.copyRange(t, 201, 300)
	require.Equal(t, "640", l.accountAmount(t, 12))
	require.Equal(t, 1, l.poolRows(t, 12))
}

// A rollback to a slot below the round's captured slot removes its rows, and
// the record that they were copied with them.
func TestMetadataFactsRollbackForgetsCopiedRewardOutputs(t *testing.T) {
	t.Parallel()
	l := newLateRewardFixture(t)

	l.copyRange(t, 0, 100)
	l.ledgerPrecomputes(t, 12, 90, "fp-a", "500")
	l.copyRange(t, 101, 200)
	require.Equal(t, 1, l.poolRows(t, 12))

	writeFacts(t, l.f, func(txn *database.Txn) error {
		return l.f.RollbackFacts(context.Background(), txn, pointAt(50))
	})
	require.Zero(t, l.poolRows(t, 12))

	// The ledger rolled back too and computed the identical round again.
	l.copyRange(t, 51, 120)
	require.Equal(t, 1, l.poolRows(t, 12),
		"the same round after a rollback is copied, not skipped as copied")
	require.Equal(t, "500", l.accountAmount(t, 12))
}

// A round still being written is not marked copied: its remaining chunks reach
// the IndexerDB once the cursor is done.
func TestMetadataFactsWaitsForAnUnfinishedRewardRound(t *testing.T) {
	t.Parallel()
	l := newLateRewardFixture(t)

	l.copyRange(t, 0, 100)
	l.ledgerPrecomputes(t, 12, 90, "", "500")
	_, err := l.srcRaw.Exec(
		`UPDATE sync_state SET value = ? WHERE sync_key = ?`,
		cursorJSON(12, false, "", 90),
		models.RewardPrecomputeCursorPrefix+"12",
	)
	require.NoError(t, err)
	l.copyRange(t, 101, 200)
	require.Zero(t, l.poolRows(t, 12))

	_, err = l.srcRaw.Exec(
		`UPDATE sync_state SET value = ? WHERE sync_key = ?`,
		cursorJSON(12, true, "fp-a", 90),
		models.RewardPrecomputeCursorPrefix+"12",
	)
	require.NoError(t, err)
	l.copyRange(t, 201, 300)
	require.Equal(t, 1, l.poolRows(t, 12))
}

// The boundary that credits a round rechecks the eligibility of its outputs in
// place, after the writer copied them; the segment carrying the boundary
// copies the corrected rows, and a credit the writer already folded stays
// folded.
func TestMetadataFactsCopiesEligibilityRecheckedRewardOutputs(t *testing.T) {
	t.Parallel()
	l := newLateRewardFixture(t)

	l.copyRange(t, 0, 100)
	l.ledgerPrecomputes(t, 12, 90, "fp-a", "500")
	l.copyRange(t, 101, 200)
	require.Equal(t, "1", scalar[string](t, l.dstRaw,
		`SELECT CAST(spendable AS TEXT) FROM reward_account_output`))

	_, err := l.dstRaw.Exec(`UPDATE reward_account_output SET folded = 1`)
	require.NoError(t, err)
	_, err = l.srcRaw.Exec(`UPDATE reward_account_output SET spendable = 0`)
	require.NoError(t, err)
	_, err = l.srcRaw.Exec(`INSERT INTO reward_credit_round
		(snapshot_epoch, boundary_slot) VALUES (12, 250)`)
	require.NoError(t, err)
	l.copyRange(t, 201, 300)

	require.Equal(t, "0", scalar[string](t, l.dstRaw,
		`SELECT CAST(spendable AS TEXT) FROM reward_account_output`),
		"the recheck's correction must reach the IndexerDB")
	require.True(t, scalar[bool](
		t, l.dstRaw, `SELECT folded FROM reward_account_output`),
		"copying an output again must not unfold it")
}

// The production writer copies facts for an earlier segment before the ledger
// finishes a reward round at the tip captured by that segment.
func TestWriterCopiesRewardRoundFinishedBehindItsTip(t *testing.T) {
	t.Parallel()
	l := newLateRewardFixture(t)
	store, err := indexerdb.NewStore(l.dst, l.dst, 10)
	require.NoError(t, err)
	chain := newFakeChain(3, "late-reward")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 1).WithFacts(l.f)
	w.afterBatch = func(tip indexerdb.Tip) {
		if tip.BlockNumber == 0 {
			l.ledgerPrecomputes(t, 12, tip.Slot, "fp-a", "500")
		}
	}

	require.NoError(t, w.CatchUp(t.Context()))

	require.Equal(t, 1, l.poolRows(t, 12),
		"the live writer passed the captured slot before the reward round finished")
	require.Equal(t, "500", l.accountAmount(t, 12))
	require.Equal(t, "8", scalar[string](t, l.dstRaw,
		`SELECT total_reward FROM reward_pool_output WHERE epoch = 12`))
}
