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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

var (
	rewardTestKey  = []byte("stake-credential-hash-0123456789")
	rewardTestPool = []byte("pool-key-hash-0123456789abcdef0")
)

// rewardFixture is a ledger store and an IndexerDB holding one credited
// round whose single output the ledger has not folded yet.
type rewardFixture struct {
	f        *MetadataFacts
	srcRaw   *sql.DB
	dstRaw   *sql.DB
	sourceID []byte
}

func newRewardFixture(t *testing.T) *rewardFixture {
	t.Helper()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f, err := NewMetadataFacts(src, dst)
	require.NoError(t, err)
	_, err = dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 10, 10, '40', 1)`, rewardTestKey)
	require.NoError(t, err)
	_, err = srcRaw.Exec(`INSERT INTO reward_account_output
		(staking_key, pool_key_hash, reward_type, epoch, credential_tag,
		 amount, spendable, guarded, captured_slot, boundary_slot, folded)
		VALUES (?, ?, 'member', 10, 0, '500', 1, 0, 50, 100, 0)`,
		rewardTestKey, rewardTestPool)
	require.NoError(t, err)
	_, err = srcRaw.Exec(`INSERT INTO reward_credit_round
		(snapshot_epoch, boundary_slot) VALUES (10, 100)`)
	require.NoError(t, err)
	return &rewardFixture{
		f: f, srcRaw: srcRaw, dstRaw: dstRaw,
		sourceID: models.StakeRewardSourceHash(
			10, rewardTestPool, 0, rewardTestKey, "member",
		),
	}
}

func (r *rewardFixture) copyRange(t *testing.T, from, through uint64) {
	t.Helper()
	writeFacts(t, r.f, func(txn *database.Txn) error {
		return r.f.CopyFacts(context.Background(), txn, from, through)
	})
}

// ledgerFolds is what the ledger does when it next touches the credential:
// it journals the credit under the round's boundary slot and marks the
// output folded.
func (r *rewardFixture) ledgerFolds(t *testing.T) {
	t.Helper()
	_, err := r.srcRaw.Exec(`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, 0, ?, '500', 100, 0, 0)`, rewardTestKey, r.sourceID)
	require.NoError(t, err)
	_, err = r.srcRaw.Exec(`UPDATE reward_account_output SET folded = 1`)
	require.NoError(t, err)
}

func (r *rewardFixture) reward(t *testing.T) string {
	t.Helper()
	return scalar[string](
		t, r.dstRaw, `SELECT reward FROM account WHERE staking_key = ?`,
		rewardTestKey,
	)
}

func (r *rewardFixture) journalRows(t *testing.T) int {
	t.Helper()
	return scalar[int](
		t, r.dstRaw, `SELECT COUNT(*) FROM account_reward_delta`,
	)
}

func (r *rewardFixture) outputFolded(t *testing.T) bool {
	t.Helper()
	return scalar[bool](
		t, r.dstRaw, `SELECT folded FROM reward_account_output`,
	)
}

func TestMetadataFactsCopyCreditsFoldedAfterTheWriterPassedTheRound(
	t *testing.T,
) {
	t.Parallel()
	r := newRewardFixture(t)

	// The writer copies the round while the ledger has not folded it: the
	// output is pending in the IndexerDB and the journal is empty.
	r.copyRange(t, 0, 150)
	require.Zero(t, r.journalRows(t))
	require.False(t, r.outputFolded(t))
	require.Equal(t, "40", r.reward(t))

	// The ledger folds later, journaling at slot 100, behind the writer.
	r.ledgerFolds(t)
	r.copyRange(t, 151, 300)

	require.Equal(t, 1, r.journalRows(t),
		"a credit folded after the writer passed its slot must be copied")
	require.Equal(t, "540", r.reward(t))
	require.True(t, r.outputFolded(t),
		"a copied credit must stop counting as a pending output")

	r.copyRange(t, 301, 400)
	require.Equal(t, "540", r.reward(t), "a later segment must not credit again")
	require.Equal(t, 1, r.journalRows(t))
}

func TestMetadataFactsFoldsAnOutputCopiedBeforeItsLedgerFold(t *testing.T) {
	t.Parallel()
	r := newRewardFixture(t)

	// The output is copied unfolded in an earlier segment; the ledger folds
	// before the segment that carries the round's slot reads the journal.
	r.copyRange(t, 0, 60)
	require.False(t, r.outputFolded(t))
	r.ledgerFolds(t)
	r.copyRange(t, 61, 150)

	require.Equal(t, "540", r.reward(t))
	require.True(t, r.outputFolded(t),
		"a credit in the account must not also count as pending")
}

func TestMetadataFactsKeepsAnUnfoldedOutputPending(t *testing.T) {
	t.Parallel()
	r := newRewardFixture(t)

	r.copyRange(t, 0, 150)
	r.copyRange(t, 151, 300)

	require.Equal(t, "40", r.reward(t))
	require.Zero(t, r.journalRows(t))
	require.False(t, r.outputFolded(t))
}

func TestMetadataFactsDefersStakeInputsTheLedgerStillWrites(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f, err := NewMetadataFacts(src, dst)
	require.NoError(t, err)

	_, err = srcRaw.Exec(`INSERT INTO reward_stake_input
		(epoch, pool_key_hash, credential_tag, staking_key, stake, owner,
		 registered, captured_slot, boundary_slot)
		VALUES (10, ?, 0, ?, '7', 0, 1, 100, 100),
		       (9, ?, 0, ?, '5', 0, 1, 80, 80)`,
		rewardTestPool, rewardTestKey, rewardTestPool, rewardTestKey)
	require.NoError(t, err)
	_, err = srcRaw.Exec(`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)`,
		models.RewardStakeInputsPendingKey,
		`{"entries":[{"epoch":10,"boundary_slot":100}]}`)
	require.NoError(t, err)

	copyRange := func(from, through uint64) {
		writeFacts(t, f, func(txn *database.Txn) error {
			return f.CopyFacts(context.Background(), txn, from, through)
		})
	}
	count := func(epoch int) int {
		return scalar[int](
			t, dstRaw,
			`SELECT COUNT(*) FROM reward_stake_input WHERE epoch = ?`, epoch,
		)
	}

	copyRange(0, 150)
	require.Equal(t, 1, count(9), "a finished snapshot is copied")
	require.Zero(t, count(10), "a snapshot still being written is not")

	copyRange(151, 200)
	require.Zero(t, count(10), "still pending in a later segment")

	_, err = srcRaw.Exec(`UPDATE sync_state SET value = ? WHERE sync_key = ?`,
		`{"entries":[]}`, models.RewardStakeInputsPendingKey)
	require.NoError(t, err)
	copyRange(201, 250)
	require.Equal(t, 1, count(10),
		"the deferred snapshot is copied once the ledger finishes it")
	require.Equal(t, 1, count(9))
}

// countingRows counts the rows each read of the reward journal returns.
type countingRows struct {
	metadata.FactRowStore
	journalRows int
}

func (c *countingRows) ReadFactRows(
	ctx context.Context,
	table metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) (metadata.FactRows, int64, error) {
	rows, next, err := c.FactRowStore.ReadFactRows(
		ctx, table, from, through, cursor, limit, txn,
	)
	if table.Name == metadata.AccountRewardCreditTable.Name && from == 100 &&
		through == 100 {
		c.journalRows += len(rows.Rows)
	}
	return rows, next, err
}

func TestMetadataFactsReadsAFoldedCreditOnceAcrossLaterSegments(t *testing.T) {
	t.Parallel()
	r := newRewardFixture(t)
	// A dormant delegator's output stays unfolded, keeping the round listed.
	_, err := r.srcRaw.Exec(`INSERT INTO reward_account_output
		(staking_key, pool_key_hash, reward_type, epoch, credential_tag,
		 amount, spendable, guarded, captured_slot, boundary_slot, folded)
		VALUES (?, ?, 'member', 10, 0, '7', 1, 0, 50, 100, 0)`,
		[]byte("dormant-credential-hash-012345678"), rewardTestPool)
	require.NoError(t, err)
	counter := &countingRows{FactRowStore: r.f.srcRows}
	r.f.srcRows = counter

	r.copyRange(t, 0, 150)
	r.ledgerFolds(t)
	for i := range uint64(10) {
		r.copyRange(t, 151+i*100, 250+i*100)
	}

	require.Equal(t, "540", r.reward(t))
	require.Equal(t, 1, counter.journalRows,
		"later segments must read only credits journaled since the last read")
}
