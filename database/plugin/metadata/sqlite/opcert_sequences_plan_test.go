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

package sqlite

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poolOpCertSequenceIndex is the index migration v2 declares for the counter
// aggregate. Named here rather than in the store: nothing in the query
// mentions it, since which index answers a statement is the planner's choice,
// and that choice is exactly what this test checks.
const poolOpCertSequenceIndex = "idx_pool_opcert_sequence_pool_sequence"

// latestPoolOpCertSequencesSQL is the statement LatestPoolOpCertSequences
// issues. Restated here because the store builds it inline; the assertion
// below is only meaningful while the two agree, so a change to the store's
// SQL that this misses would show up as a plan that no longer reads the index.
const latestPoolOpCertSequencesSQL = `
SELECT pool_key_hash, MAX(sequence)
FROM pool_opcert_sequence
GROUP BY pool_key_hash`

// TestLatestPoolOpCertSequencesReadsIndexOnly pins the read plan of the
// op-cert counter aggregate.
//
// pool_opcert_sequence takes a row per block minted and is never pruned, so on
// a synced mainnet database this aggregate covers millions of rows to produce
// one entry per pool that has ever minted -- a few thousand. There is no slot
// bound available to narrow it: every row the table holds is at or below the
// tip, so restricting to the tip would exclude nothing. What keeps it off the
// table itself is an index carrying both columns it reads, which lets the
// aggregate run without touching a single row.
//
// The plan is asserted rather than the index's mere existence: an index no
// planner chooses is a write cost with no read benefit.
func TestLatestPoolOpCertSequencesReadsIndexOnly(t *testing.T) {
	t.Parallel()
	store, db := newSharedSQLStore(t)

	// Rows for two pools, so the group-by has something to fold.
	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)
	pkhB := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xB2}, 28)),
	)
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 9, 20, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 1, 15, nil))

	rows, err := db.Query(
		"EXPLAIN QUERY PLAN " + latestPoolOpCertSequencesSQL,
	)
	require.NoError(t, err)
	defer rows.Close()

	var details string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details += detail + "\n"
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, details, "the planner must describe the aggregate")

	assert.Contains(t, details, "COVERING INDEX "+poolOpCertSequenceIndex,
		"the counter aggregate must read the index alone, not the table:\n%s",
		details,
	)
}
