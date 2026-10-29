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
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// legacyLatestOpCertSQL is the shape these lookups had before COUNT(*) was
// dropped: found is derived from a second aggregate.
func legacyLatestOpCertSQL(where string) string {
	return "SELECT COALESCE(MAX(sequence), 0), COUNT(*) " +
		"FROM pool_opcert_sequence WHERE " + where
}

func opCertQueryPlan(
	t *testing.T,
	store *Store,
	query string,
	args ...any,
) string {
	t.Helper()
	rows, err := store.readDB.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(details, "\n")
}

// The pool_slot index is a full-range scan for a pool with long history; the
// pool_sequence index lets MAX(sequence) descend to one row. The plan is
// asserted from the statements the store issues.
func TestLatestPoolOpCertSequenceQueriesAvoidCountScan(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{9}
	for slot := uint64(1); slot <= 50; slot++ {
		require.NoError(t, store.UpdatePoolOpCertSequence(pool, slot, slot*10, nil))
	}
	plan := opCertQueryPlan(
		t, store, latestPoolOpCertSequenceAtOrBeforeSQL,
		pool.Bytes(), int64(400),
	)
	require.Contains(
		t, plan, "idx_pool_opcert_sequence_pool_sequence",
		"plan: %s", plan,
	)
	require.NotContains(t, plan, "idx_pool_opcert_sequence_pool_slot", plan)
	for _, query := range []string{
		latestPoolOpCertSequenceAfterSQL,
		latestPoolOpCertSequenceAtOrBeforeSQL,
		latestPoolOpCertSequenceInRangeSQL,
	} {
		require.NotContains(t, query, "COUNT(")
	}
}

// Results must match the legacy two-aggregate query for every combination,
// including no row versus a stored sequence that encodes to zero.
func TestLatestPoolOpCertSequenceNoCountMatchesLegacy(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{7}
	absent := lcommon.PoolKeyHash{8}
	seqs := []uint64{5, 3, 9, 9, 0, 1 << 63, 4}
	for i, seq := range seqs {
		require.NoError(t, store.UpdatePoolOpCertSequence(pool, seq, uint64(100+i*10), nil))
	}
	legacy := func(where string, args ...any) (uint64, bool) {
		var seq, count int64
		require.NoError(t, store.readDB.QueryRow(
			legacyLatestOpCertSQL(where), args...,
		).Scan(&seq, &count))
		if count == 0 {
			return 0, false
		}
		return decodeOpCertSequence(seq), true
	}
	for _, p := range []lcommon.PoolKeyHash{pool, absent} {
		for _, a := range []uint64{0, 99, 100, 105, 120, 160, 1000} {
			got, found, err := store.LatestPoolOpCertSequenceAtOrBefore(p, a, nil)
			require.NoError(t, err)
			want, wantFound := legacy("pool_key_hash = ? AND slot <= ?", p.Bytes(), int64(a))
			require.Equal(t, wantFound, found, "atOrBefore %d", a)
			require.Equal(t, want, got, "atOrBefore %d", a)

			got, found, err = store.LatestPoolOpCertSequenceAfter(p, a, nil)
			require.NoError(t, err)
			want, wantFound = legacy("pool_key_hash = ? AND slot > ?", p.Bytes(), a)
			require.Equal(t, wantFound, found, "after %d", a)
			require.Equal(t, want, got, "after %d", a)

			for _, b := range []uint64{0, 110, 150, 1000} {
				got, found, err = store.LatestPoolOpCertSequenceInRange(p, a, b, nil)
				require.NoError(t, err)
				if a > b {
					require.False(t, found)
					continue
				}
				want, wantFound = legacy(
					"pool_key_hash = ? AND slot >= ? AND slot <= ?",
					p.Bytes(), int64(a), int64(b),
				)
				require.Equal(t, wantFound, found, "range %d-%d", a, b)
				require.Equal(t, want, got, "range %d-%d", a, b)
			}
		}
	}
}

// A row whose stored sequence is the minimum value 0 after decoding must be
// reported as found, not conflated with "no row".
func TestLatestPoolOpCertSequenceZeroIsFound(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{6}
	require.NoError(t, store.UpdatePoolOpCertSequence(pool, 0, 10, nil))
	got, found, err := store.LatestPoolOpCertSequenceAtOrBefore(pool, 10, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(0), got)
	_, found, err = store.LatestPoolOpCertSequenceAtOrBefore(pool, 9, nil)
	require.NoError(t, err)
	require.False(t, found)
}
