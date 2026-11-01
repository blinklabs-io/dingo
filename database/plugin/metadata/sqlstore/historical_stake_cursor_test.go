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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// vdbeShape is the size of a prepared SQLite statement: its VDBE program
// length and the highest cursor number it opens.
type vdbeShape struct {
	ops       int
	maxCursor int
}

// explainShape prepares query through EXPLAIN and measures its program.
func explainShape(
	t *testing.T,
	db *sql.DB,
	query string,
	args []any,
) vdbeShape {
	t.Helper()
	rows, err := db.Query("EXPLAIN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()
	shape := vdbeShape{maxCursor: -1}
	for rows.Next() {
		var addr, p1, p2, p3, p5 int64
		var opcode string
		var p4, comment sql.NullString
		require.NoError(
			t,
			rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment),
		)
		shape.ops++
		switch opcode {
		case "OpenRead", "OpenWrite", "OpenEphemeral", "OpenAutoindex",
			"OpenPseudo", "OpenDup", "SorterOpen", "ReopenIdx":
			shape.maxCursor = max(shape.maxCursor, int(p1))
		}
	}
	require.NoError(t, rows.Err())
	return shape
}

// historicalStakeCursorCeiling bounds the cursors the historical stake
// statement may open. SQLite 3.54 refuses to prepare a statement whose
// cursors reach SQLITE_LIMIT_EXPR_DEPTH (1000 as compiled into
// modernc.org/sqlite). Joining each CIP-0163 witness source to
// active_delegation, which copies that CTE once per source, takes this
// statement past 3000.
const historicalStakeCursorCeiling = 600

// TestHistoricalStakeQueryCursorsStayBounded prepares the historical stake
// statement under every shape historicalStakeCTE can emit and checks it opens
// a bounded number of cursors and runs.
func TestHistoricalStakeQueryCursorsStayBounded(t *testing.T) {
	t.Parallel()
	for _, pointer := range []bool{false, true} {
		for _, expiry := range []bool{false, true} {
			for _, activation := range []bool{false, true} {
				name := fmt.Sprintf(
					"pointer=%v/expiry=%v/activation=%v",
					pointer, expiry, activation,
				)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					store := newDepositHeldStore(t)
					// Babbage counts pointer stake; Conway does not.
					era := uint(6)
					if pointer {
						era = 5
					}
					for epoch := range uint64(3) {
						require.NoError(t, store.SetEpoch(
							epoch*depositHeldEpochLength, epoch,
							nil, nil, nil, nil,
							era, 1, depositHeldEpochLength, nil,
						))
					}
					if activation {
						_, err := store.writeDB.Exec(`INSERT INTO sync_state
							(sync_key, value)
							VALUES ('delegator_inactivity_activated', '1')`)
						require.NoError(t, err)
					}
					var expiryEpoch, period uint64
					if expiry {
						expiryEpoch, period = 2, 1
					}
					query, args, err := store.historicalStakeCTE(
						t.Context(), store.writeDB, 1_999, 2_000,
						nil, expiryEpoch, period,
						"active_delegation.pool_key_hash IN (?)",
						[]any{[]byte{1}},
					)
					require.NoError(t, err)
					query += `
SELECT pool_key_hash, credential_tag, staking_key, utxo_amount
FROM active_delegator_stake`
					shape := explainShape(t, store.writeDB, query, args)
					require.Less(
						t,
						shape.maxCursor,
						historicalStakeCursorCeiling,
						"historical stake statement opens %d cursors",
						shape.maxCursor,
					)
					rows, err := store.writeDB.Query(query, args...)
					require.NoError(t, err)
					for rows.Next() {
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
				})
			}
		}
	}
}
