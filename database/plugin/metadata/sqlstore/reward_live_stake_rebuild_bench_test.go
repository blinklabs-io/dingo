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
	"database/sql"
	"encoding/binary"
	"fmt"
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
	delegationStmt := prepare(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, NULL, ?)`)

	pool := func(index int) []byte {
		buf := make([]byte, 28)
		binary.BigEndian.PutUint64(buf, uint64(index%64))
		return buf
	}
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
			_, err := delegationStmt.Exec(
				key, pool(index+assignment), slot+int64(assignment),
			)
			require.NoError(tb, err)
		}
	}
	for index := range deadAssignments {
		key := make([]byte, 28)
		binary.BigEndian.PutUint64(key, uint64(credentials+index/4))
		_, err := delegationStmt.Exec(key, pool(index), int64(index))
		require.NoError(tb, err)
	}
	require.NoError(tb, tx.Commit())
}

// BenchmarkRebuildRewardLiveStakeFromRunningTotals measures the Mithril
// bootstrap finalizer at stake-identity populations of the order the live
// path reaches. The dead=0 case is the worst case for the ranked query's
// account restriction, where every assignment belongs to a live delegator;
// the dead=1200000 case is the shape a long-lived chain actually has. This is
// the scale regression guard for #4610.
func BenchmarkRebuildRewardLiveStakeFromRunningTotals(b *testing.B) {
	for _, dead := range []int{0, 1_200_000} {
		b.Run(fmt.Sprintf("dead=%d", dead), func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			seedRewardLiveStakeScaleFixture(b, store, 100_000, 3, 80, dead)
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
