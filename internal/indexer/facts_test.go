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
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newFactsDB opens an IndexerDB, whose SQL fact tables the tests seed and
// compare on both sides of a copy.
func newFactsDB(t *testing.T) (*database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Role:    database.RoleIndexer,
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return db, raw
}

func writeFacts(
	t *testing.T,
	f *MetadataFacts,
	fn func(*database.Txn) error,
) {
	t.Helper()
	txn := f.dst.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(fn))
}

func scalar[T any](t *testing.T, raw *sql.DB, query string, args ...any) T {
	t.Helper()
	var v T
	require.NoError(t, raw.QueryRow(query, args...).Scan(&v))
	return v
}

func TestMetadataFactsReplaceByNaturalKeyNotRowID(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	// The same epoch under different row ids and an older boundary slot in
	// the destination, as left by an earlier copy of a row the ledger has
	// since rewritten. The destination also holds a row of its own.
	_, err := srcRaw.Exec(`INSERT INTO epoch_summary
		(id, epoch, total_active_stake, total_pool_count, total_delegators, boundary_slot, snapshot_ready)
		VALUES (900, 5, '700', 3, 4, 120, 1), (901, 6, '800', 3, 4, 200, 1)`)
	require.NoError(t, err)
	_, err = dstRaw.Exec(`INSERT INTO epoch_summary
		(id, epoch, total_active_stake, total_pool_count, total_delegators, boundary_slot, snapshot_ready)
		VALUES (1, 5, '1', 1, 1, 100, 0), (2, 4, '9', 1, 1, 40, 1)`)
	require.NoError(t, err)

	writeFacts(t, f, func(txn *database.Txn) error {
		return f.CopyFacts(context.Background(), txn, 110, 150)
	})
	// Only the in-range row crossed; it replaced the destination's row for
	// epoch 5, and the row outside the range stayed out.
	require.Equal(
		t,
		"700",
		scalar[string](
			t,
			dstRaw,
			`SELECT total_active_stake FROM epoch_summary WHERE epoch = 5`,
		),
	)
	require.Equal(
		t,
		1,
		scalar[int](
			t,
			dstRaw,
			`SELECT COUNT(*) FROM epoch_summary WHERE epoch = 5`,
		),
	)
	require.Equal(
		t,
		0,
		scalar[int](
			t,
			dstRaw,
			`SELECT COUNT(*) FROM epoch_summary WHERE epoch = 6`,
		),
	)
	require.Equal(
		t,
		"9",
		scalar[string](
			t,
			dstRaw,
			`SELECT total_active_stake FROM epoch_summary WHERE epoch = 4`,
		),
	)
	require.NotEqual(t, 900,
		scalar[int](t, dstRaw, `SELECT id FROM epoch_summary WHERE epoch = 5`),
		"the source's row id must not be copied")
}

func TestMetadataFactsCreditsRewardsOncePerJournalEntry(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	key := []byte("stake-credential-hash-0123456789")
	_, err := dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 10, 10, '40', 1)`, key)
	require.NoError(t, err)
	_, err = srcRaw.Exec(`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, 0, ?, '500', 100, 0, 0),
		       (?, 0, ?, '77', 100, 1, 0)`,
		key, []byte("credit-source"), key, []byte("withdrawal-tx"))
	require.NoError(t, err)

	copyRange := func(from, through uint64) {
		writeFacts(t, f, func(txn *database.Txn) error {
			return f.CopyFacts(context.Background(), txn, from, through)
		})
	}
	reward := func() string {
		return scalar[string](
			t,
			dstRaw,
			`SELECT reward FROM account WHERE staking_key = ?`,
			key,
		)
	}
	copyRange(0, 200)
	// The credit lands; the withdrawal is the indexer's own block-derived
	// effect and is not replayed from the ledger's journal.
	require.Equal(t, "540", reward())
	copyRange(0, 200)
	require.Equal(t, "540", reward(), "a repeated range must not credit twice")
	copyRange(101, 200)
	require.Equal(t, "540", reward(), "a range past the credit's slot skips it")
}

func TestMetadataFactsPatchLeavesBlockDerivedColumns(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	held, absent := []byte(
		"held-credential-0123456789abcdef",
	), []byte(
		"absent-credential-0123456789abcd",
	)
	pool := []byte("pool-key-hash-0123456789")
	_, err := srcRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, pool, added_slot, created_slot, reward, active, expiration_epoch)
		VALUES (?, 0, ?, 120, 10, '999', 0, 7), (?, 0, ?, 130, 10, '5', 1, 0)`,
		held, pool, absent, pool)
	require.NoError(t, err)
	_, err = dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 10, 10, '40', 1)`, held)
	require.NoError(t, err)

	writeFacts(t, f, func(txn *database.Txn) error {
		return f.PatchFacts(context.Background(), txn, 100, 200)
	})
	// The ledger's delegation, activity and expiry outcome is applied; the
	// reward balance, which only credits and withdrawals may change, is not,
	// and an account the indexer has not written is not invented.
	require.Equal(
		t,
		pool,
		scalar[[]byte](
			t,
			dstRaw,
			`SELECT pool FROM account WHERE staking_key = ?`,
			held,
		),
	)
	require.Equal(
		t,
		0,
		scalar[int](
			t,
			dstRaw,
			`SELECT active FROM account WHERE staking_key = ?`,
			held,
		),
	)
	require.Equal(
		t,
		7,
		scalar[int](
			t,
			dstRaw,
			`SELECT expiration_epoch FROM account WHERE staking_key = ?`,
			held,
		),
	)
	require.Equal(
		t,
		"40",
		scalar[string](
			t,
			dstRaw,
			`SELECT reward FROM account WHERE staking_key = ?`,
			held,
		),
	)
	require.Equal(t, 1,
		scalar[int](t, dstRaw, `SELECT COUNT(*) FROM account`))
}

// A live-stake row mirrors its account's pool, so a delegation the ledger
// patches onto an account, or clears from one, must reach the row the way it
// does when the ledger itself writes the account.
func TestMetadataFactsPatchKeepsLiveStakeAttributionInStep(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	delegated, cleared, absent := []byte("delegated-credential-0123456789a"),
		[]byte("cleared-credential-0123456789abc"),
		[]byte("absent-credential-0123456789abcd")
	pool := []byte("pool-key-hash-0123456789")
	_, err := srcRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, pool, added_slot, created_slot, reward, active, expiration_epoch)
		VALUES (?, 0, ?, 120, 10, '0', 1, 0), (?, 0, NULL, 150, 10, '0', 1, 0)`,
		delegated, pool, cleared)
	require.NoError(t, err)
	_, err = dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, pool, added_slot, created_slot, reward, active)
		VALUES (?, 0, NULL, 10, 10, '0', 1), (?, 0, ?, 10, 10, '0', 1)`,
		delegated, cleared, pool)
	require.NoError(t, err)
	_, err = dstRaw.Exec(`INSERT INTO reward_live_stake
		(credential_tag, staking_key, pool_key_hash, utxo_stake, reward_stake, total_stake,
		 registered, pool_delegation_slot, pool_delegation_block_index,
		 pool_delegation_cert_index, updated_slot, calculation_version)
		VALUES (0, ?, NULL, '500', '0', '500', 1, 0, 0, 0, 10, 3),
		       (0, ?, ?, '700', '0', '700', 1, 10, 0, 0, 10, 3)`,
		delegated, cleared, pool)
	require.NoError(t, err)

	writeFacts(t, f, func(txn *database.Txn) error {
		return f.PatchFacts(context.Background(), txn, 100, 200)
	})

	type live struct {
		Pool       []byte
		Delegation int64
		Updated    int64
		Utxo       string
	}
	read := func(key []byte) live {
		var l live
		require.NoError(t, dstRaw.QueryRow(
			`SELECT pool_key_hash, pool_delegation_slot, updated_slot, utxo_stake
			FROM reward_live_stake WHERE staking_key = ?`, key,
		).Scan(&l.Pool, &l.Delegation, &l.Updated, &l.Utxo))
		return l
	}
	require.Equal(t, live{pool, 120, 120, "500"}, read(delegated),
		"the delegation reaches the live-stake row at the slot it was made")
	require.Equal(t, live{nil, 0, 150, "700"}, read(cleared),
		"a cleared delegation leaves the live-stake row")
	require.Zero(t, scalar[int](t, dstRaw,
		`SELECT COUNT(*) FROM reward_live_stake WHERE staking_key = ?`, absent),
		"an account the indexer does not hold gains no live-stake row")
}

func TestMetadataFactsRollbackRemovesFactsAfterPoint(t *testing.T) {
	t.Parallel()
	src, _ := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)
	_, err := dstRaw.Exec(`INSERT INTO reward_ada_pots
		(epoch, treasury, reserves, fees, rewards, captured_slot)
		VALUES (3, '1', '1', '1', '1', 100), (4, '1', '1', '1', '1', 200)`)
	require.NoError(t, err)
	writeFacts(t, f, func(txn *database.Txn) error {
		return f.RollbackFacts(context.Background(), txn, pointAt(150))
	})
	require.Equal(
		t,
		1,
		scalar[int](t, dstRaw, `SELECT COUNT(*) FROM reward_ada_pots`),
	)
	require.Equal(
		t,
		3,
		scalar[int](t, dstRaw, `SELECT epoch FROM reward_ada_pots`),
	)
}

func pointAt(slot uint64) ocommon.Point { return ocommon.Point{Slot: slot} }

func TestCopyAnchorStateRefusesAJournalThatExceedsTheBalance(t *testing.T) {
	t.Parallel()
	src, _ := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)

	key := []byte("stake-credential-hash-0123456789")
	_, err := dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 10, 10, '0', 1)`, key)
	require.NoError(t, err)
	// A credit of 30 past the anchor cannot have been paid into a balance of
	// 20: the journal and the account disagree.
	f, err := NewMetadataFacts(movingLedger{
		storeLedger: newStoreLedger(t, src),
		moving: &movingAnchorState{
			key: key, balance: 20,
			journal: []metadata.RewardJournalEntry{
				{StakingKey: key, Amount: 30},
			},
		},
	}, dst)
	require.NoError(t, err)

	txn := dst.Transaction(context.Background(), true)
	err = txn.Do(func(txn *database.Txn) error {
		return f.CopyAnchorState(context.Background(), txn, 100)
	})
	require.ErrorContains(t, err, "cannot undo journal movement")
	require.Equal(
		t,
		"0",
		scalar[string](
			t,
			dstRaw,
			`SELECT reward FROM account WHERE staking_key = ?`,
			key,
		),
		"a refused copy leaves the balance",
	)
	require.Zero(t,
		scalar[int](t, dstRaw, `SELECT COUNT(*) FROM account_import_baseline`),
		"a refused copy writes no baseline")
}

func TestCopyAnchorStateRefusesARewardBeyondInt64(t *testing.T) {
	t.Parallel()
	src, _ := newFactsDB(t)
	dst, _ := newFactsDB(t)
	f, err := NewMetadataFacts(movingLedger{
		storeLedger: newStoreLedger(t, src),
		moving: &movingAnchorState{
			key:     []byte("stake-credential-hash-0123456789"),
			balance: 1 << 63,
		},
	}, dst)
	require.NoError(t, err)

	txn := dst.Transaction(context.Background(), true)
	err = txn.Do(func(txn *database.Txn) error {
		return f.CopyAnchorState(context.Background(), txn, 100)
	})
	require.ErrorContains(t, err, "exceeds the int64 range")
}

// A proposal ratified in the range and enacted after it is copied ratified
// only; the enactment arrives with the range that contains it.
func TestMetadataFactsPatchProposalOutcomesAsOfTheRange(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	hash := []byte("proposal-tx-hash-0123456789abcdef")
	insert := `INSERT INTO governance_proposal
		(tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
		 deposit, added_slot, ratified_epoch, ratified_slot,
		 enacted_epoch, enacted_slot)
		VALUES (?, 0, 0, 1, 9, 0, 50, ?, ?, ?, ?)`
	_, err := srcRaw.Exec(insert, hash, 3, 120, 4, 180)
	require.NoError(t, err)
	_, err = dstRaw.Exec(insert, hash, nil, nil, nil, nil)
	require.NoError(t, err)

	patch := func(from, through uint64) {
		writeFacts(t, f, func(txn *database.Txn) error {
			return f.PatchFacts(context.Background(), txn, from, through)
		})
	}
	patch(100, 150)
	require.Equal(
		t,
		3,
		scalar[int](
			t,
			dstRaw,
			`SELECT ratified_epoch FROM governance_proposal`,
		),
	)
	require.Equal(t, 120,
		scalar[int](t, dstRaw, `SELECT ratified_slot FROM governance_proposal`))
	require.Nil(t,
		scalar[any](t, dstRaw, `SELECT enacted_epoch FROM governance_proposal`),
		"an enactment past the range must not be copied ahead of its slot")
	require.Nil(t,
		scalar[any](t, dstRaw, `SELECT enacted_slot FROM governance_proposal`))

	patch(151, 200)
	require.Equal(t, 4,
		scalar[int](t, dstRaw, `SELECT enacted_epoch FROM governance_proposal`))
	require.Equal(t, 180,
		scalar[int](t, dstRaw, `SELECT enacted_slot FROM governance_proposal`))
	require.Equal(
		t,
		3,
		scalar[int](
			t,
			dstRaw,
			`SELECT ratified_epoch FROM governance_proposal`,
		),
	)
}

// The ledger renews a DRep's expiry without touching its row's slot, so the
// row alone only says where the expiry stands at the ledger's tip. Its expiry
// history says where it stood at the range's end.
func TestMetadataFactsPatchDrepExpiryAsOfTheRange(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	cred := []byte("drep-credential-0123456789abcdef")
	_, err := srcRaw.Exec(`INSERT INTO drep
		(credential_tag, credential, added_slot, last_activity_epoch, expiry_epoch, active)
		VALUES (0, ?, 120, 30, 50, 1)`, cred)
	require.NoError(t, err)
	_, err = srcRaw.Exec(`INSERT INTO drep_expiry_history
		(credential_tag, credential, added_slot, last_activity_epoch, expiry_epoch)
		VALUES (0, ?, 120, 10, 20), (0, ?, 180, 30, 50)`, cred, cred)
	require.NoError(t, err)
	_, err = dstRaw.Exec(`INSERT INTO drep
		(credential_tag, credential, added_slot, last_activity_epoch, expiry_epoch, active)
		VALUES (0, ?, 120, 10, 20, 1)`, cred)
	require.NoError(t, err)

	step := func(from, through uint64) {
		writeFacts(t, f, func(txn *database.Txn) error {
			if err := f.CopyFacts(context.Background(), txn, from, through); err != nil {
				return err
			}
			return f.PatchFacts(context.Background(), txn, from, through)
		})
	}
	expiry := func() int {
		return scalar[int](t, dstRaw, `SELECT expiry_epoch FROM drep`)
	}
	step(100, 150)
	require.Equal(t, 20, expiry(), "the renewal at slot 180 is not yet due")
	require.Equal(t, 10,
		scalar[int](t, dstRaw, `SELECT last_activity_epoch FROM drep`))
	step(151, 200)
	require.Equal(
		t,
		50,
		expiry(),
		"the renewal applies once its range is copied",
	)
	require.Equal(t, 30,
		scalar[int](t, dstRaw, `SELECT last_activity_epoch FROM drep`))
}

// The ledger records the drop of an expired proposal in a companion row that
// points at the proposal by row id, which differs between the two stores.
func TestMetadataFactsCopyProposalDropsByProposalKey(t *testing.T) {
	t.Parallel()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)

	held := []byte("held-proposal-tx-hash-0123456789a")
	missing := []byte("missing-proposal-tx-hash-01234567")
	insert := `INSERT INTO governance_proposal
		(id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
		 deposit, added_slot)
		VALUES (?, ?, 0, 0, 1, 9, 0, 50)`
	// Different row ids in the two stores, and a proposal the destination
	// never received.
	_, err := srcRaw.Exec(insert, 11, held)
	require.NoError(t, err)
	_, err = srcRaw.Exec(insert, 12, missing)
	require.NoError(t, err)
	_, err = dstRaw.Exec(insert, 7, held)
	require.NoError(t, err)
	_, err = srcRaw.Exec(`INSERT INTO governance_proposal_drop
		(proposal_id, dropped_epoch, dropped_slot)
		VALUES (11, 6, 300), (12, 6, 310)`)
	require.NoError(t, err)

	run := func(from, through uint64) {
		writeFacts(t, f, func(txn *database.Txn) error {
			return f.CopyFacts(context.Background(), txn, from, through)
		})
		writeFacts(t, f, func(txn *database.Txn) error {
			return f.PatchFacts(context.Background(), txn, from, through)
		})
	}
	run(100, 299)
	require.Equal(t, 0, scalar[int](
		t, dstRaw, `SELECT COUNT(*) FROM governance_proposal_drop`),
		"a drop dated past the range is not copied ahead of its slot")

	run(300, 400)
	require.Equal(t, 1, scalar[int](
		t, dstRaw, `SELECT COUNT(*) FROM governance_proposal_drop`),
		"the proposal the destination does not hold is skipped")
	require.Equal(t, 7, scalar[int](
		t, dstRaw, `SELECT proposal_id FROM governance_proposal_drop`),
		"the row follows the proposal's key, not the source's row id")
	require.Equal(t, 6, scalar[int](
		t, dstRaw, `SELECT dropped_epoch FROM governance_proposal_drop`))
	require.Equal(t, 300, scalar[int](
		t, dstRaw, `SELECT dropped_slot FROM governance_proposal_drop`))

	// Copying the same range again replaces the row instead of duplicating it.
	run(300, 400)
	require.Equal(t, 1, scalar[int](
		t, dstRaw, `SELECT COUNT(*) FROM governance_proposal_drop`))

	writeFacts(t, f, func(txn *database.Txn) error {
		return f.RollbackFacts(context.Background(), txn, pointAt(299))
	})
	require.Equal(t, 0, scalar[int](
		t, dstRaw, `SELECT COUNT(*) FROM governance_proposal_drop`),
		"a rollback removes drops dated after the point")
}
