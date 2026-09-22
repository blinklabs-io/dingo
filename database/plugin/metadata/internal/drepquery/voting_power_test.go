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

package drepquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVotingPowerByTypeSQLDialectShapes asserts the account-first shape
// (blinklabs-io/dingo#4364) holds for every dialect this file supports, not
// just sqlite, since only sqlite's shape gets an execution test: the inner
// subquery joins outward from account instead of running a correlated
// EXISTS against it, and no dialect leaks another dialect's join hint or
// cast type.
func TestVotingPowerByTypeSQLDialectShapes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dialect      string
		castType     string
		active       string
		expectedJoin string
	}{
		{"postgres", "BIGINT", "true", "JOIN utxo\n\t\t\t\t     ON"},
		{"mysql", "UNSIGNED", "1", "JOIN utxo\n\t\t\t\t     ON"},
		{"sqlite", "INTEGER", "1", "JOIN utxo INDEXED BY " +
			sqliteUtxoStakingLiveAmountIndex + "\n\t\t\t\t     ON"},
	}
	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			t.Parallel()
			sql := VotingPowerByTypeSQL(tc.dialect, 0)

			// The account-first join this fix introduces.
			require.Contains(t, sql, "FROM account ax")
			require.Contains(t, sql, tc.expectedJoin)
			require.Contains(
				t,
				sql,
				"ON utxo.credential_tag = ax.credential_tag",
			)
			require.Contains(t, sql, "AND utxo.deleted_slot = 0")
			require.Contains(
				t,
				sql,
				"GROUP BY ax.credential_tag, ax.staking_key",
			)

			// The pre-fix correlated-EXISTS shape must be gone.
			require.NotContains(t, sql, "EXISTS")
			require.NotContains(t, sql, "FROM utxo")

			require.Contains(
				t,
				sql,
				"CAST(utxo.amount AS "+tc.castType+")",
			)
			require.Contains(t, sql, "ax.active = "+tc.active)

			// Postgres and mysql must never see sqlite's INDEXED BY hint,
			// which is invalid syntax for both.
			if tc.dialect != "sqlite" {
				require.NotContains(t, sql, "INDEXED BY")
			}
		})
	}
}

func TestVotingPowerSQLPostgresPlaceholderReuse(t *testing.T) {
	t.Parallel()
	credential := []byte("credential")
	offSQL, offArgs := VotingPowerSQL("postgres", 1, credential, 0)
	require.NotContains(t, offSQL, "expiration_epoch")
	require.Equal(t, []any{credential, uint8(1)}, offArgs)

	onSQL, onArgs := VotingPowerSQL("postgres", 1, credential, 5)
	require.Equal(t, 2, strings.Count(onSQL, "expiration_epoch >= $3"))
	require.NotContains(t, onSQL, "expiration_epoch >= ?")
	require.Equal(t, []any{credential, uint8(1), uint64(5)}, onArgs)
}

func TestCollectionArgsExpiryOrder(t *testing.T) {
	values := []uint64{2, 3}
	require.Equal(t, []any{values, values}, CollectionArgs(values, 0))
	require.Equal(
		t,
		[]any{uint64(5), values, uint64(5), values},
		CollectionArgs(values, 5),
	)
}
