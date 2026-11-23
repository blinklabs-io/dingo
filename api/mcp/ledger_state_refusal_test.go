// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mcp

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// ledgerStateToolSession serves the SQLite and Cardano tools over db with no
// ledger state, outsideSQL saying whether the node keeps its ledger state
// in the LedgerDB.
func ledgerStateToolSession(
	t *testing.T,
	db *sql.DB,
	outsideSQL bool,
) *mcp.ClientSession {
	t.Helper()
	limits := newReadLimits()
	limits.ledgerStateOutsideSQL = outsideSQL
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	)
	registerSQLiteTools(server, db, 5*time.Second, 100, limits)
	registerCardanoTools(
		server, db, nil, nil, "preview", 5*time.Second, limits,
	)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// Caller SQL that names a ledger-state table is refused on a node that keeps
// its ledger state in the LedgerDB, since the table is empty there.
func TestSQLiteToolsRefuseLedgerStateTables(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		query      string
		outsideSQL bool
		refused    bool
	}{
		{"nonces table", "SELECT count(*) FROM block_nonce", true, true},
		{
			"quoted and aliased",
			"SELECT n.slot FROM \"block_nonce\" AS n", true, true,
		},
		{"comma join", "SELECT 1 FROM tip t, block_nonce n", true, true},
		{"network state", "SELECT * FROM network_state", true, true},
		{"the tip table", "SELECT count(*) FROM tip", true, false},
		{
			"ledger state in SQL",
			"SELECT count(*) FROM block_nonce", false, false,
		},
	} {
		for _, tool := range []string{"sqlite_query", "sqlite_explain"} {
			t.Run(tc.name+"/"+tool, func(t *testing.T) {
				t.Parallel()
				cs := ledgerStateToolSession(t, newSchemaDB(t), tc.outsideSQL)

				text := callTool(
					t, cs, tool, map[string]any{"query": tc.query}, tc.refused,
				)

				if tc.refused {
					require.Contains(t, text, "Query refused")
					require.Contains(t, text, "does not hold")
				}
			})
		}
	}
}

func TestEpochSummaryWithoutLedgerStateRefusesSQLReads(t *testing.T) {
	t.Parallel()
	cs := ledgerStateToolSession(t, newSchemaDB(t), true)

	text := callTool(t, cs, "get_epoch_summary", map[string]any{}, true)

	require.Contains(t, text, "does not hold")
	for _, family := range []nodesettings.Family{
		nodesettings.FamilyNonces,
		nodesettings.FamilyEpochs,
		nodesettings.FamilySummaries,
	} {
		require.Contains(t, text, family.String())
	}
}

// With the ledger state in SQL the SQL path answers: a database with no
// epochs reports the absent row, not a refusal.
func TestEpochSummaryWithoutLedgerStateAnswersFromSQL(t *testing.T) {
	t.Parallel()
	cs := ledgerStateToolSession(t, newSchemaDB(t), false)

	text := callTool(t, cs, "get_epoch_summary", map[string]any{}, true)

	require.NotContains(t, text, "does not hold")
	require.Contains(t, text, "read latest epoch")
}

// Without a ledger state, get_governance_proposal reads the SQL tables, so it
// is refused, for a listing and for one proposal, when the ledger state is in
// the LedgerDB, and answered when it is in SQL.
func TestGovernanceProposalWithoutLedgerStateRefusesSQLReads(t *testing.T) {
	t.Parallel()
	hash := "0x" + strings.Repeat("ab", 32)
	for _, args := range []map[string]any{
		{"status": "all"},
		{"tx_hash": hash},
	} {
		cs := ledgerStateToolSession(t, newSchemaDB(t), true)

		text := callTool(t, cs, "get_governance_proposal", args, true)

		require.Contains(t, text, "does not hold")
		require.Contains(t, text, nodesettings.FamilyGovernance.String())
	}
	cs := ledgerStateToolSession(t, newSchemaDB(t), false)
	text := callTool(
		t, cs, "get_governance_proposal", map[string]any{"status": "all"}, true,
	)
	require.NotContains(t, text, "does not hold")
	require.Contains(t, text, "governance_proposal")
}

