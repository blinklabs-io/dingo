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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// drainedToolSession serves the SQLite and Cardano tools over db with the
// given families reported drained and no ledger state, as a node without a
// ledger would.
func drainedToolSession(
	t *testing.T,
	db *sql.DB,
	families ...nodesettings.Family,
) *mcp.ClientSession {
	t.Helper()
	var mask nodesettings.FamilyMask
	for _, family := range families {
		mask = mask.With(family)
	}
	limits := newReadLimits()
	limits.drained = func() nodesettings.FamilyMask { return mask }
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

func TestSQLiteToolsRefuseDrainedTables(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		query   string
		drained []nodesettings.Family
		refused bool
	}{
		{
			"drained nonces table",
			"SELECT count(*) FROM block_nonce",
			[]nodesettings.Family{nodesettings.FamilyNonces}, true,
		},
		{
			"quoted and aliased",
			"SELECT n.slot FROM \"block_nonce\" AS n",
			[]nodesettings.Family{nodesettings.FamilyNonces}, true,
		},
		{
			"comma join",
			"SELECT 1 FROM epoch e, block_nonce n",
			[]nodesettings.Family{nodesettings.FamilyNonces}, true,
		},
		{
			"drained network donations",
			"SELECT * FROM network_state",
			[]nodesettings.Family{nodesettings.FamilyNetworkDonations}, true,
		},
		{
			"another table of a drained database",
			"SELECT count(*) FROM epoch",
			[]nodesettings.Family{nodesettings.FamilyNonces}, false,
		},
		{
			"the table when its family is not drained",
			"SELECT count(*) FROM block_nonce",
			[]nodesettings.Family{nodesettings.FamilyNetworkDonations}, false,
		},
		{
			"the table when nothing is drained",
			"SELECT count(*) FROM block_nonce", nil, false,
		},
	} {
		for _, tool := range []string{"sqlite_query", "sqlite_explain"} {
			t.Run(tc.name+"/"+tool, func(t *testing.T) {
				t.Parallel()
				cs := drainedToolSession(t, newSchemaDB(t), tc.drained...)

				text := callTool(
					t, cs, tool, map[string]any{"query": tc.query}, tc.refused,
				)

				if tc.refused {
					require.Contains(t, text, "Query refused")
					require.Contains(t, text, "no longer holds")
				}
			})
		}
	}
}

func TestEpochSummaryWithoutLedgerStateRefusesDrainedFamilies(t *testing.T) {
	t.Parallel()
	for _, family := range []nodesettings.Family{
		nodesettings.FamilyNonces,
		nodesettings.FamilyEpochs,
		nodesettings.FamilySummaries,
	} {
		t.Run(family.String(), func(t *testing.T) {
			t.Parallel()
			cs := drainedToolSession(t, newSchemaDB(t), family)

			text := callTool(t, cs, "get_epoch_summary", map[string]any{}, true)

			require.Contains(t, text, "no longer holds")
			require.Contains(t, text, family.String())
		})
	}
}

// With nothing drained the SQL path answers as before: a database with no
// epochs reports the absent row, not a drain refusal.
func TestEpochSummaryWithoutLedgerStateAnswersWhenNothingIsDrained(
	t *testing.T,
) {
	t.Parallel()
	cs := drainedToolSession(
		t,
		newSchemaDB(t),
		nodesettings.FamilyNetworkDonations,
	)

	text := callTool(t, cs, "get_epoch_summary", map[string]any{}, true)

	require.NotContains(t, text, "no longer holds")
	require.Contains(t, text, "read latest epoch")
}

// NewMCPServer reads the drained families from the database and the ledger
// state it was given.
func TestNewMCPServerRefusesTablesTheDatabaseDrained(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, nodeDB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
	server, _, err := NewMCPServer(
		t.Context(),
		DefaultProviderConfig(),
		ProviderDependencies{Database: nodeDB, SQLDB: newSchemaDB(t)},
	)
	require.NoError(t, err)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	refused := callTool(
		t, cs, "sqlite_query",
		map[string]any{"query": "SELECT * FROM block_nonce"}, true,
	)
	require.Contains(t, refused, "Query refused")
	callTool(
		t, cs, "sqlite_query",
		map[string]any{"query": "SELECT * FROM epoch"}, false,
	)
}

// Without a ledger state, get_governance_proposal reads the SQL tables, so a
// drained governance family is refused, for a listing and for one proposal,
// and answered when the family is not drained.
func TestGovernanceProposalWithoutLedgerStateRefusesADrainedFamily(
	t *testing.T,
) {
	t.Parallel()
	hash := "0x" + strings.Repeat("ab", 32)
	for _, args := range []map[string]any{
		{"status": "all"},
		{"tx_hash": hash},
	} {
		cs := drainedToolSession(
			t, newSchemaDB(t), nodesettings.FamilyGovernance,
		)

		text := callTool(t, cs, "get_governance_proposal", args, true)

		require.Contains(t, text, "no longer holds")
		require.Contains(t, text, nodesettings.FamilyGovernance.String())
	}
	cs := drainedToolSession(t, newSchemaDB(t), nodesettings.FamilyNonces)
	text := callTool(
		t, cs, "get_governance_proposal", map[string]any{"status": "all"}, true,
	)
	require.NotContains(t, text, "no longer holds")
	require.Contains(t, text, "governance_proposal")
}

// get_pool_performance joins reward_pool_input, which the set serves only
// through the ledger, so a drained reward-inputs family is refused and an
// undrained one is answered.
func TestPoolPerformanceRefusesDrainedRewardInputs(t *testing.T) {
	t.Parallel()
	pool := strings.Repeat("ab", 28)
	cs := drainedToolSession(
		t, newSchemaDB(t), nodesettings.FamilyRewardInputs,
	)

	text := callTool(
		t, cs, "get_pool_performance", map[string]any{"pool_id": pool}, true,
	)

	require.Contains(t, text, "no longer holds")
	require.Contains(t, text, nodesettings.FamilyRewardInputs.String())

	cs = drainedToolSession(t, newSchemaDB(t), nodesettings.FamilyNonces)
	text = callTool(
		t, cs, "get_pool_performance", map[string]any{"pool_id": pool}, false,
	)
	require.NotContains(t, text, "no longer holds")
}
