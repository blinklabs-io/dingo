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
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// BenchmarkInsertUtxoModel is the before/after timing counterpart:
// legacyInsertUtxo reproduces insertUtxoModel's pre-cache one-shot
// QueryRowContext call (the exact query text and argument order that used
// to be inlined directly in insertUtxoModel), run against the same migrated
// schema and connection insertUtxoModel itself uses via the hot-statement
// cache.
func legacyInsertUtxo(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRowContext(ctx, insertUtxoQueryIgnoreConflict,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	return id, err
}

// utxoForBenchmarkIteration builds a UTxO with a unique tx_id per i, so each
// benchmark iteration inserts a genuinely new row (the realistic sync
// workload) instead of repeatedly hitting the ON CONFLICT DO NOTHING branch.
func utxoForBenchmarkIteration(i uint64) *models.Utxo {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], i)
	return &models.Utxo{
		TxId:       txID,
		OutputIdx:  0,
		PaymentKey: bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize),
		AddedSlot:  1,
		Amount:     types.Uint64(1_000_000 + i),
	}
}

func BenchmarkInsertUtxoModel(b *testing.B) {
	ctx := context.Background()

	b.Run("one_shot_uncached", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		i := uint64(0)
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := legacyInsertUtxo(ctx, store.writeDB, utxo); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		i := uint64(0)
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if err := store.insertUtxoModel(ctx, store.writeDB, utxo, true); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// legacyImportUtxoStatement reproduces importUtxos' former generated sqlc
// call. The benchmark keeps both paths in one transaction so the measured
// difference is statement preparation/reuse, not transaction setup.
func legacyImportUtxoStatement(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	return sqlitequery.New(db).CreateUtxoIfAbsent(
		ctx,
		sqlitequery.CreateUtxoIfAbsentParams(params),
	)
}

func cachedImportUtxoStatement(
	ctx context.Context,
	store *Store,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	var id int64
	err = store.queryRowCached(ctx, db, insertUtxoQueryIgnoreConflict,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	return id, err
}

// BenchmarkImportUtxoStatement measures the exact importer insert before and
// after transaction-scoped prepared-statement reuse. Unique transaction IDs
// keep every iteration on the insert path rather than the conflict fallback.
func BenchmarkImportUtxoStatement(b *testing.B) {
	ctx := context.Background()

	b.Run("one_shot_uncached", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		txn := store.Transaction(ctx)
		db, txCtx, err := store.dbFromTxn(txn)
		require.NoError(b, err)
		i := uint64(0)
		b.ResetTimer()
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := legacyImportUtxoStatement(txCtx, db, utxo); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.NoError(b, txn.Commit())
	})

	b.Run("transaction_scoped_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		txn := store.Transaction(ctx)
		db, txCtx, err := store.dbFromTxn(txn)
		require.NoError(b, err)
		i := uint64(0)
		b.ResetTimer()
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := cachedImportUtxoStatement(
				txCtx,
				store,
				db,
				utxo,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.NoError(b, txn.Commit())
	})
}

// BenchmarkLatestPoolOpCertSequence is the timing counterpart: pairing
// MAX(sequence) with COUNT(*) (the legacy form) defeats SQLite's min/max
// optimization on idx_pool_opcert_sequence_pool_sequence, forcing a scan of
// every row recorded for the pool instead of a single index descent to the
// largest one. n scales with how many blocks a single pool has produced by
// the time a from-genesis sync reaches it.
func BenchmarkLatestPoolOpCertSequence(b *testing.B) {
	for _, n := range []int{1_000, 50_000, 200_000} {
		store := newMigratedSQLiteStore(b)
		hot := make([]byte, 28)
		hot[0] = 0xAA
		seedPoolOpCertSequence(b, store, hot, n)
		pkh := lcommon.PoolKeyHash(hot)

		b.Run(fmt.Sprintf("n=%d/legacy_max_and_count", n), func(b *testing.B) {
			for b.Loop() {
				var seq, count int64
				if err := store.writeDB.QueryRow(
					legacyLatestPoolOpCertSequenceQuery, hot,
				).Scan(&seq, &count); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/max_only", n), func(b *testing.B) {
			for b.Loop() {
				if _, _, err := store.LatestPoolOpCertSequence(pkh, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRefreshRewardLiveStakeAggregateAccountAndUpsert is the timing
// counterpart to BenchmarkSumCredentialUtxoStake: it isolates just the two
// newly cached queries (a registered credential with no live UTxOs, so
// sumCredentialUtxoStake's own cost is negligible and constant across both
// variants) to measure the one-shot-vs-cached parse-cost delta in isolation.
func BenchmarkRefreshRewardLiveStakeAggregateAccountAndUpsert(b *testing.B) {
	pool := make([]byte, 28)
	pool[0] = 0xBB

	b.Run("one_shot", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		seedRewardLiveStakeAccount(b, store, ref, pool, 1_000_000)
		for n := 0; b.Loop(); n++ {
			err := store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return oneShotRefreshRewardLiveStakeAggregate(
						ctx, store, db, ref, uint64(n+1),
					)
				},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		seedRewardLiveStakeAccount(b, store, ref, pool, 1_000_000)
		for n := 0; b.Loop(); n++ {
			err := store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return store.refreshRewardLiveStakeAggregate(
						ctx, db, ref, uint64(n+1),
					)
				},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRefreshRewardLiveStakeAggregateDeltaAtScale is the direct
// before/after comparison of refreshRewardLiveStakeAggregate's
// full sumCredentialUtxoStake rescan against
// refreshRewardLiveStakeAggregateDelta's O(1) running-total update, for a
// credential holding as many live UTxOs as the 20,003-UTxO case the issue
// measured on a live node. Both benchmarks touch the same warmed-up
// credential; only the incremental one is expected to stay flat as n grows.
func BenchmarkRefreshRewardLiveStakeAggregateDeltaAtScale(b *testing.B) {
	for _, n := range []int{100, 1_000, 20_003} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/full_scan", n), func(b *testing.B) {
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregate(
							ctx, db, ref, 1,
						)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/incremental_delta", n), func(b *testing.B) {
			establishRunningTotal(b, store, ref, 1)
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregateDelta(
							ctx, db, ref, 1, 0,
						)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// seedRewardLiveStakeScaleFixture writes credentials directly through SQL
// rather than the model importers: the finalizer's cost is a property of the
// row populations it reads, and the importers' per-row bookkeeping would
// dominate the setup at these sizes.
//
// delegatedShare is the percentage of credentials that hold an active account
// with a pool. deadAssignments adds stake-assignment rows for credentials
// with no account row at all, which is what a long-lived chain accumulates as
// stake keys deregister: that population grows with the chain's age while the
// live credential population does not, and it is the shape the finalizer's
// ranked query must not spend time on.
func seedRewardLiveStakeScaleFixture(
	tb testing.TB,
	store *Store,
	credentials int,
	assignmentsEach int,
	delegatedShare int,
	deadAssignments int,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	prepare := func(query string) *sql.Stmt {
		stmt, err := tx.Prepare(query)
		require.NoError(tb, err)
		return stmt
	}
	accountStmt := prepare(`
INSERT INTO account
    (staking_key, credential_tag, pool, added_slot, created_slot, reward,
     active)
VALUES (?, 0, ?, ?, ?, ?, ?)`)
	utxoStmt := prepare(`
INSERT INTO utxo
    (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot,
     amount)
VALUES (?, 0, ?, 0, ?, 0, ?)`)
	totalStmt := prepare(`
INSERT INTO reward_live_stake
    (credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
     registered, updated_slot, calculation_version)
VALUES (0, ?, ?, '0', ?, false, 1, 0)`)
	transactionStmt := prepare(`
INSERT INTO "transaction" (id, slot, block_index)
VALUES (?, ?, ?)`)
	certStmt := prepare(`
INSERT INTO certs (id, transaction_id, slot, cert_index)
VALUES (?, ?, ?, ?)`)
	delegationStmt := prepare(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	registrationDelegationStmt := prepare(`
INSERT INTO stake_registration_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	voteDelegationStmt := prepare(`
INSERT INTO stake_vote_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	voteRegistrationDelegationStmt := prepare(`
INSERT INTO stake_vote_registration_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	delegationStmts := []*sql.Stmt{
		delegationStmt,
		registrationDelegationStmt,
		voteDelegationStmt,
		voteRegistrationDelegationStmt,
	}

	pool := func(index int) []byte {
		buf := make([]byte, 28)
		binary.BigEndian.PutUint64(buf, uint64(index%64))
		return buf
	}
	var eventID int64
	for index := range credentials {
		key := make([]byte, 28)
		binary.BigEndian.PutUint64(key, uint64(index))
		active := index%100 < delegatedShare
		slot := int64(index + 1)
		var accountPool any
		if active {
			accountPool = pool(index)
		}
		_, err := accountStmt.Exec(key, accountPool, slot, slot, "0", active)
		require.NoError(tb, err)
		txID := make([]byte, 32)
		binary.BigEndian.PutUint64(txID, uint64(index))
		_, err = utxoStmt.Exec(txID, key, slot, "1000000")
		require.NoError(tb, err)
		_, err = totalStmt.Exec(key, "1000000", "1000000")
		require.NoError(tb, err)
		for assignment := range assignmentsEach {
			eventID++
			assignmentSlot := slot + int64(assignment)
			assignmentPool := pool(index + assignment)
			if assignment == assignmentsEach-1 {
				assignmentPool = pool(index)
			}
			_, err := transactionStmt.Exec(eventID, assignmentSlot, assignment)
			require.NoError(tb, err)
			_, err = certStmt.Exec(eventID, eventID, assignmentSlot, 0)
			require.NoError(tb, err)
			_, err = delegationStmts[assignment%len(delegationStmts)].Exec(
				key, assignmentPool, eventID, assignmentSlot,
			)
			require.NoError(tb, err)
		}
	}
	for index := range deadAssignments {
		eventID++
		key := make([]byte, 28)
		binary.BigEndian.PutUint64(key, uint64(credentials+index/4))
		assignmentSlot := int64(index + 1)
		_, err := transactionStmt.Exec(eventID, assignmentSlot, 0)
		require.NoError(tb, err)
		_, err = certStmt.Exec(eventID, eventID, assignmentSlot, 0)
		require.NoError(tb, err)
		_, err = delegationStmts[index%len(delegationStmts)].Exec(
			key, pool(index), eventID, assignmentSlot,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, tx.Commit())
}

func logRewardLiveStakeBatchPlan(b *testing.B, store *Store) {
	b.Helper()
	lastKey := make([]byte, 28)
	binary.BigEndian.PutUint64(lastKey, rewardLiveStakeRebuildBatch-1)
	query, args := rewardLiveStakeCredentialQuery(true, stakeKeyRange{
		hi: &stakeKeyBound{tag: 0, key: lastKey},
	})
	rows, err := store.writeDB.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(b, err)
	defer rows.Close()
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(b, rows.Scan(&id, &parent, &notUsed, &detail))
		b.Logf("query_plan=%s", detail)
	}
	require.NoError(b, rows.Err())
}

func rewardLiveStakeBatchQuery(
	keys stakeKeyRange,
	forceCredentialIndex bool,
	constrainDelegationRange bool,
	forceAccountFirst bool,
) (string, []any, error) {
	query, args := rewardLiveStakeCredentialQuery(true, keys)
	if constrainDelegationRange {
		accountRange, rangeArgs := keys.predicate(
			"a.credential_tag",
			"a.staking_key",
		)
		searchStart := 0
		for index, alias := range []string{"sd", "srd", "svd", "svrd"} {
			needle := "AND " + accountRange
			position := strings.Index(query[searchStart:], needle)
			if position < 0 {
				return "", nil, fmt.Errorf(
					"find account key range for delegation alias %s",
					alias,
				)
			}
			position += searchStart
			delegationRange, delegationArgs := keys.predicate(
				alias+".credential_tag",
				alias+".staking_key",
			)
			if len(delegationArgs) != len(rangeArgs) {
				return "", nil, fmt.Errorf(
					"delegation range argument count differs for alias %s",
					alias,
				)
			}
			insertAt := position + len(needle)
			query = query[:insertAt] + " AND " + delegationRange + query[insertAt:]
			searchStart = insertAt + len(" AND "+delegationRange)

			// The new placeholders follow this branch's account range in SQL order.
			argPosition := (2*index + 1) * len(rangeArgs)
			if argPosition > len(args) {
				return "", nil, fmt.Errorf(
					"delegation range argument position %d exceeds %d arguments",
					argPosition,
					len(args),
				)
			}
			updatedArgs := make([]any, 0, len(args)+len(delegationArgs))
			updatedArgs = append(updatedArgs, args[:argPosition]...)
			updatedArgs = append(updatedArgs, delegationArgs...)
			updatedArgs = append(updatedArgs, args[argPosition:]...)
			args = updatedArgs
		}
	}
	if forceAccountFirst {
		var err error
		query, err = sqliteRewardLiveStakeAccountFirstQuery(query)
		if err != nil {
			return "", nil, err
		}
	} else if forceCredentialIndex {
		query = strings.ReplaceAll(
			query,
			"FROM account a\n",
			"FROM account a INDEXED BY idx_account_credential\n",
		)
	}
	return query, args, nil
}

func scanRewardLiveStakeBatches(
	ctx context.Context,
	store *Store,
	forceCredentialIndex bool,
	constrainDelegationRange bool,
	forceAccountFirst bool,
) (int64, error) {
	var processed int64
	var lo *stakeKeyBound
	for {
		hi, err := nextRewardLiveStakeBatchEnd(
			ctx,
			store.writeDB,
			lo,
			rewardLiveStakeRebuildBatch,
		)
		if err != nil {
			return 0, err
		}
		query, args, err := rewardLiveStakeBatchQuery(
			stakeKeyRange{lo: lo, hi: hi},
			forceCredentialIndex,
			constrainDelegationRange,
			forceAccountFirst,
		)
		if err != nil {
			return 0, err
		}
		rows, err := store.writeDB.QueryContext(ctx, query, args...)
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			processed++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, err
		}
		if err := rows.Close(); err != nil {
			return 0, err
		}
		if hi == nil {
			return processed, nil
		}
		lo = hi
	}
}

func logRewardLiveStakeRangeQueryPlan(
	b *testing.B,
	store *Store,
	forceCredentialIndex bool,
	constrainDelegationRange bool,
	forceAccountFirst bool,
) {
	b.Helper()
	lastKey := make([]byte, 28)
	binary.BigEndian.PutUint64(lastKey, rewardLiveStakeRebuildBatch-1)
	query, args, err := rewardLiveStakeBatchQuery(
		stakeKeyRange{hi: &stakeKeyBound{tag: 0, key: lastKey}},
		forceCredentialIndex,
		constrainDelegationRange,
		forceAccountFirst,
	)
	require.NoError(b, err)
	if forceCredentialIndex {
		b.Log("query plan with credential-key index forced")
	} else if constrainDelegationRange {
		b.Log("query plan with delegation key range pushed down")
	} else if forceAccountFirst {
		b.Log("query plan with account-first credential-key index forced")
	} else {
		b.Log("current query plan")
	}
	rows, err := store.writeDB.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(b, err)
	defer rows.Close()
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(b, rows.Scan(&id, &parent, &notUsed, &detail))
		b.Logf("query_plan=%s", detail)
	}
	require.NoError(b, rows.Err())
}

// BenchmarkRebuildRewardLiveStakeFromRunningTotals measures the Mithril
// bootstrap finalizer over live key histories and deregistered-key history.
func BenchmarkRebuildRewardLiveStakeFromRunningTotals(b *testing.B) {
	for _, scenario := range []struct {
		credentials     int
		assignmentsEach int
		deadAssignments int
	}{
		{credentials: 100_000, assignmentsEach: 3},
		{credentials: 100_000, assignmentsEach: 3, deadAssignments: 1_200_000},
		{credentials: 100_000, assignmentsEach: 20},
	} {
		name := fmt.Sprintf(
			"keys=%d/assignments=%d/dead=%d",
			scenario.credentials,
			scenario.assignmentsEach,
			scenario.deadAssignments,
		)
		b.Run(name, func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			seedRewardLiveStakeScaleFixture(
				b,
				store,
				scenario.credentials,
				scenario.assignmentsEach,
				80,
				scenario.deadAssignments,
			)
			logRewardLiveStakeBatchPlan(b, store)
			b.ResetTimer()
			for b.Loop() {
				require.NoError(
					b,
					store.RebuildRewardLiveStakeFromRunningTotals(1_000_000, nil),
				)
			}
		})
	}
}

func BenchmarkRewardLiveStakeRangeQuery(b *testing.B) {
	store := newMigratedSQLiteStore(b)
	seedRewardLiveStakeScaleFixture(b, store, 100_000, 20, 80, 0)
	logRewardLiveStakeRangeQueryPlan(b, store, false, false, false)
	logRewardLiveStakeRangeQueryPlan(b, store, true, false, false)
	logRewardLiveStakeRangeQueryPlan(b, store, false, true, false)
	logRewardLiveStakeRangeQueryPlan(b, store, false, false, true)
	currentRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		false,
		false,
		false,
	)
	require.NoError(b, err)
	credentialIndexRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		true,
		false,
		false,
	)
	require.NoError(b, err)
	require.Equal(b, currentRows, credentialIndexRows)
	delegationRangeRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		false,
		true,
		false,
	)
	require.NoError(b, err)
	require.Equal(b, currentRows, delegationRangeRows)
	accountFirstRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		false,
		false,
		true,
	)
	require.NoError(b, err)
	require.Equal(b, currentRows, accountFirstRows)
	for _, variant := range []struct {
		name                     string
		forceCredentialIndex     bool
		constrainDelegationRange bool
		forceAccountFirst        bool
	}{
		{name: "current"},
		{name: "credential-key-index", forceCredentialIndex: true},
		{name: "delegation-key-range", constrainDelegationRange: true},
		{name: "account-first-credential-key-index", forceAccountFirst: true},
	} {
		b.Run(variant.name, func(b *testing.B) {
			for b.Loop() {
				rows, err := scanRewardLiveStakeBatches(
					context.Background(),
					store,
					variant.forceCredentialIndex,
					variant.constrainDelegationRange,
					variant.forceAccountFirst,
				)
				require.NoError(b, err)
				require.Equal(b, currentRows, rows)
			}
		})
	}
}

// BenchmarkRebuildRewardLiveStakeFinalizers compares the two finalization
// paths on a 20,000-live-UTxO fixture. Reproduce with:
//
//	go test ./database/plugin/metadata/sqlstore -run '^$' \
//	  -bench BenchmarkRebuildRewardLiveStakeFinalizers -benchtime=1x -count=1
func BenchmarkRebuildRewardLiveStakeFinalizers(b *testing.B) {
	for _, path := range []struct {
		name string
		fast bool
	}{
		{name: "authoritative"},
		{name: "running_totals", fast: true},
	} {
		b.Run(path.name, func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			ref := models.NewStakeCredentialRef(0, []byte{1, 2, 3})
			amounts := make([]uint64, 20_000)
			for i := range amounts {
				amounts[i] = uint64(i + 1)
			}
			seedCredentialUtxos(b, store, 99, ref, amounts, nil)
			require.NoError(b, store.ImportAccount(&models.Account{
				StakingKey: ref.Key, CredentialTag: ref.Tag,
				AddedSlot: 1, CreatedSlot: 1, Reward: types.Uint64(100), Active: true,
			}, nil))
			require.NoError(b, store.RebuildRewardLiveStake(100, nil))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if path.fast {
					err = store.RebuildRewardLiveStakeFromRunningTotals(100, nil)
				} else {
					err = store.RebuildRewardLiveStake(100, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// oneShotSumCredentialUtxoStake reproduces sumCredentialUtxoStake's SQL
// exactly (sumCredentialUtxoStakeQuery, defined in live_stake.go) but issues
// it as a plain QueryRowContext call instead of going through Store's
// cachedStmt -- i.e. it is what sumCredentialUtxoStake looked like before it
// became a Store method backed by the prepared-statement cache. Kept as the
// direct before/after benchmark comparison point for that cache (see
// prepared_stmt.go and BenchmarkSumCredentialUtxoStake).
func oneShotSumCredentialUtxoStake(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
) (uint64, error) {
	var total sql.NullInt64
	err := db.QueryRowContext(
		ctx,
		sumCredentialUtxoStakeQuery,
		ref.Tag, ref.Key,
	).Scan(&total)
	if err != nil {
		return 0, err
	}
	if !total.Valid {
		return 0, nil
	}
	if total.Int64 < 0 {
		return 0, fmt.Errorf(
			"negative reward live stake UTxO sum for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	return uint64(total.Int64), nil
}

// BenchmarkSumCredentialUtxoStake is the timing counterpart: a stake
// credential with many live UTxOs (a heavily used address; profiling a
// synced node found one holding 6,794) forces refreshRewardLiveStakeAggregate
// to fetch every one of them into Go and sum there on every touch. n scales
// with how many live UTxOs a credential has accumulated by the time it is
// next touched.
func BenchmarkSumCredentialUtxoStake(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100, 1_000, 7_000} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/legacy_row_sum", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := legacySumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/sql_aggregate_one_shot", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := oneShotSumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/sql_aggregate_prepared_cache", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
