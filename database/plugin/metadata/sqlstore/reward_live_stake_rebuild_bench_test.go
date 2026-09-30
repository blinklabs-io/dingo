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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
