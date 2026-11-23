//go:build dingo_extra_plugins

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

package ledger

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// backendRows counts query's rows on a server backend, binding the statement
// and quoting the reserved table name for its dialect.
func backendRows(
	t *testing.T,
	raw *sql.DB,
	backend string,
	query string,
	args ...any,
) int {
	t.Helper()
	switch backend {
	case "postgres":
		var out strings.Builder
		n := 0
		for _, r := range query {
			if r == '?' {
				n++
				fmt.Fprintf(&out, "$%d", n)
				continue
			}
			out.WriteRune(r)
		}
		query = out.String()
	case "mysql":
		query = strings.ReplaceAll(query, `"transaction"`, "`transaction`")
	}
	var n int
	require.NoError(t, raw.QueryRow(query, args...).Scan(&n))
	return n
}

// TestRepairZeroValueByronGenesisOnServerBackends runs the repair of a
// core-mode store on PostgreSQL and MySQL, where the statements' placeholders,
// quoting and column comparisons run on the engine.
func TestRepairZeroValueByronGenesisOnServerBackends(t *testing.T) {
	t.Parallel()
	for _, backend := range rewardBatchBackends() {
		if backend.name == "sqlite" {
			continue
		}
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			f := newZeroGenesisFixture(t)
			db, raw, name := backend.open(t)
			seedUnfilteredGenesis(t, db, f.nodeCfg, f.all())
			count := func(u lcommon.Utxo) (int, int) {
				return backendRows(t, raw, name, `
SELECT COUNT(*) FROM utxo x JOIN "transaction" t ON t.id = x.tx_id
WHERE t.hash = ? AND x.output_idx = ?`,
						u.Id.Id().Bytes(), u.Id.Index()),
					backendRows(t, raw, name, `
SELECT COUNT(*) FROM "transaction" WHERE hash = ?`, u.Id.Id().Bytes())
			}
			for _, u := range f.all() {
				utxos, txs := count(u)
				require.Equal(t, 1, utxos)
				require.Equal(t, 1, txs)
			}

			repairFixture(t, db, f)

			for _, u := range f.zeroByron {
				utxos, txs := count(u)
				require.Zero(t, utxos, "output %s", u.Id.Id())
				require.Zero(t, txs, "transaction %s", u.Id.Id())
			}
			for _, u := range []lcommon.Utxo{f.fundedByron, f.zeroShelley} {
				utxos, txs := count(u)
				require.Equal(t, 1, utxos)
				require.Equal(t, 1, txs)
			}
			marker, err := db.GetSyncState(ByronGenesisZeroRepairSyncKey, nil)
			require.NoError(t, err)
			require.NotEmpty(t, marker)
		})
	}
}
