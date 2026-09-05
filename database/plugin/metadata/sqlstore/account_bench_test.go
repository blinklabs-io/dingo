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

package sqlstore_test

import (
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// seedAccountsForBench bulk-inserts n account rows directly against the
// real migrated schema (dbtest.NewDatabase, not a hand-rolled CREATE TABLE)
// so the benchmark exercises the production idx_account_credential index,
// and returns a StakeCredentialRef for every row -- mirroring
// GetAccountsByCredential's real caller, rewardActiveAccounts, which looks
// up every pool/delegator credential in the reward snapshot, not a random
// subset.
func seedAccountsForBench(
	b *testing.B,
	n int,
) (*database.Database, []models.StakeCredentialRef) {
	b.Helper()
	cfg := *database.DefaultConfig
	cfg.DataDir = b.TempDir()
	db, err := dbtest.NewDatabase(b, &cfg)
	require.NoError(b, err)

	raw, err := dbtest.RawSQLiteMetadata(b, db)
	require.NoError(b, err)

	tx, err := raw.Begin()
	require.NoError(b, err)
	stmt, err := tx.Prepare(
		"INSERT INTO account (staking_key, credential_tag, active) VALUES (?, ?, TRUE)",
	)
	require.NoError(b, err)
	refs := make([]models.StakeCredentialRef, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 28)
		key[0] = byte(i >> 16)
		key[1] = byte(i >> 8)
		key[2] = byte(i)
		_, err := stmt.Exec(key, 0)
		require.NoError(b, err)
		refs[i] = models.NewStakeCredentialRef(0, key)
	}
	require.NoError(b, stmt.Close())
	require.NoError(b, tx.Commit())
	return db, refs
}

// BenchmarkGetAccountsByCredential measures the production query (chunked
// OR-of-AND-pairs, chunk size ParameterLimit()/2) against the real migrated
// schema at scales spanning a small pool's delegator count up to a sizeable
// fraction of mainnet's ~1.5M active accounts, so a candidate fix has a real
// before/after number instead of a guess about SQLite's OR-optimization
// planner.
func BenchmarkGetAccountsByCredential(b *testing.B) {
	for _, n := range []int{1_000, 50_000, 300_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db, refs := seedAccountsForBench(b, n)
			meta := db.Metadata()
			b.ResetTimer()
			for range b.N {
				accounts, err := meta.GetAccountsByCredential(refs, false, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(accounts) != n {
					b.Fatalf("got %d accounts, want %d", len(accounts), n)
				}
			}
		})
	}
}
