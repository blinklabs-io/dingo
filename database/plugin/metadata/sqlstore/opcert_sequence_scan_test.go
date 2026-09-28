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
	"fmt"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// legacyLatestPoolOpCertSequenceQuery reproduces LatestPoolOpCertSequence's
// pre-fix statement: MAX(sequence) paired with COUNT(*) in the same
// aggregate query so "found" could be read off COUNT(*) > 0. Kept here as
// the comparison point this file benchmarks and correctness-checks the
// MAX-only rewrite against.
const legacyLatestPoolOpCertSequenceQuery = `
SELECT COALESCE(MAX(sequence), 0), COUNT(*)
FROM pool_opcert_sequence
WHERE pool_key_hash = ?`

// seedPoolOpCertSequence inserts n rows for a single pool, one per
// (slot, sequence) pair -- the shape a long-producing pool leaves in
// pool_opcert_sequence after a from-genesis sync on a network with a
// concentrated stake distribution (a handful of pools producing almost
// every block).
func seedPoolOpCertSequence(
	tb testing.TB,
	store *Store,
	poolKeyHash []byte,
	n int,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence) " +
			"VALUES (?, ?, ?)",
	)
	require.NoError(tb, err)
	for i := range n {
		_, err := stmt.Exec(poolKeyHash, int64(i)*20, int64(i))
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// TestLatestPoolOpCertSequenceNoRowsReturnsNotFound covers the case the
// MAX-only rewrite has to keep exact: a pool with no pool_opcert_sequence
// rows at all. The old query read "found" off COUNT(*) > 0; the new one
// reads it off MAX(sequence) being non-NULL, which UpdatePoolOpCertSequence
// (the table's only writer, always inserting a concrete sequence) makes
// equivalent.
func TestLatestPoolOpCertSequenceNoRowsReturnsNotFound(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	other := make([]byte, 28)
	other[0] = 0x01
	seedPoolOpCertSequence(t, store, other, 5)

	missing := make([]byte, 28)
	missing[0] = 0x02
	sequence, found, err := store.LatestPoolOpCertSequence(
		lcommon.PoolKeyHash(missing),
		nil,
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, sequence)
}

// TestLatestPoolOpCertSequenceMatchesLegacyQuery proves the MAX-only rewrite
// returns the same (sequence, found) pair the COUNT(*)-paired legacy query
// did, for both a populated and an absent pool.
func TestLatestPoolOpCertSequenceMatchesLegacyQuery(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	hot := make([]byte, 28)
	hot[0] = 0xAA
	seedPoolOpCertSequence(t, store, hot, 1_000)

	for _, pkh := range [][]byte{hot, bytes.Repeat([]byte{0xFF}, 28)} {
		var legacySeq, legacyCount int64
		require.NoError(t, store.writeDB.QueryRow(
			legacyLatestPoolOpCertSequenceQuery, pkh,
		).Scan(&legacySeq, &legacyCount))

		gotSeq, gotFound, err := store.LatestPoolOpCertSequence(
			lcommon.PoolKeyHash(pkh), nil,
		)
		require.NoError(t, err)
		require.Equal(t, legacyCount > 0, gotFound)
		require.Equal(t, uint64(legacySeq), gotSeq)
	}
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
