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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"regexp"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMCPPromptsList(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Prompts: &mcp.PromptCapabilities{ListChanged: true},
			},
		},
	)
	RegisterPrompts(server, nil, nil, "preview")

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	res, err := cs.ListPrompts(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, res)

	promptNames := make(map[string]bool)
	for _, p := range res.Prompts {
		promptNames[p.Name] = true
	}

	expectedPrompts := []string{
		"diagnose_node_health",
		"simulate_and_diagnose_tx",
		"audit_pool_rewards",
		"track_asset_portfolio",
		"conway_governance_brief",
		"investigate_address",
	}

	for _, expected := range expectedPrompts {
		assert.True(
			t,
			promptNames[expected],
			"expected prompt %s to be registered",
			expected,
		)
	}
	assert.Equal(t, len(expectedPrompts), len(res.Prompts))
}

func TestMCPPromptsGet(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Prompts: &mcp.PromptCapabilities{ListChanged: true},
			},
		},
	)
	RegisterPrompts(server, nil, nil, "preview")

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. diagnose_node_health (standard & detailed)
	resDiag, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "diagnose_node_health",
	})
	require.NoError(t, err)
	require.Len(t, resDiag.Messages, 1)
	assert.Contains(
		t,
		resDiag.Messages[0].Content.(*mcp.TextContent).Text,
		"get_cardano_tip",
	)
	assert.Contains(
		t,
		resDiag.Messages[0].Content.(*mcp.TextContent).Text,
		"BlockHeaderProtocolMinor is 69",
	)

	resDiagDetailed, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name:      "diagnose_node_health",
		Arguments: map[string]string{"detailed": "true"},
	})
	require.NoError(t, err)
	assert.Contains(
		t,
		resDiagDetailed.Messages[0].Content.(*mcp.TextContent).Text,
		"Detailed mode enabled",
	)

	// 2. simulate_and_diagnose_tx (success & missing argument)
	resSim, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "simulate_and_diagnose_tx",
		Arguments: map[string]string{
			"tx_cbor": "84a3008182582001",
			"purpose": "dex-swap",
		},
	})
	require.NoError(t, err)
	simText := resSim.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, simText, "84a3008182582001")
	assert.Contains(t, simText, "dex-swap")
	assert.Contains(t, simText, "evaluate_tx")

	_, errSimMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "simulate_and_diagnose_tx",
	})
	assert.Error(t, errSimMissing)
	assert.Contains(
		t,
		errSimMissing.Error(),
		"missing required argument 'tx_cbor'",
	)

	// 3. audit_pool_rewards (success & missing pool_id)
	resAudit, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "audit_pool_rewards",
		Arguments: map[string]string{
			"pool_id": strings.Repeat("01", 28),
			"epoch":   "450",
		},
	})
	require.NoError(t, err)
	auditText := resAudit.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, auditText, strings.Repeat("01", 28))
	assert.Contains(t, auditText, "epoch 450")
	assert.Contains(t, auditText, "get_pool_performance")
	assert.Contains(t, auditText, "pool = X'"+strings.Repeat("01", 28)+"'")
	assert.Contains(t, auditText, "active_delegators")
	assert.NotContains(t, auditText, "controlled_amount")
	assert.NotContains(t, auditText, "pool_id =")

	_, errAuditMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "audit_pool_rewards",
	})
	assert.Error(t, errAuditMissing)
	assert.Contains(
		t,
		errAuditMissing.Error(),
		"missing required argument 'pool_id'",
	)

	// 4. track_asset_portfolio (success & missing policy_id)
	resAsset, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "track_asset_portfolio",
		Arguments: map[string]string{
			"policy_id":  "a0000000000000000000000000000000000000000000000000000001",
			"asset_name": "HOSKY",
		},
	})
	require.NoError(t, err)
	assetText := resAsset.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(
		t,
		assetText,
		"a0000000000000000000000000000000000000000000000000000001",
	)
	assert.Contains(t, assetText, "484f534b59")
	assert.Contains(t, assetText, "get_utxos_by_asset")

	_, errAssetMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "track_asset_portfolio",
	})
	assert.Error(t, errAssetMissing)
	assert.Contains(
		t,
		errAssetMissing.Error(),
		"policy_id' must be a 56-character hex policy ID",
	)

	// 5. conway_governance_brief (success with and without arguments)
	resGov, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "conway_governance_brief",
		Arguments: map[string]string{
			"drep_id":       "drep1ygqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq7vlc9n",
			"stake_address": "stake_test1upugeuz3jdy0a7hncusutadavzcetdzylgxcldz39hp9n0s0xy0n5",
		},
	})
	require.NoError(t, err)
	govText := resGov.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(
		t,
		govText,
		"drep1ygqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq7vlc9n",
	)
	assert.Contains(
		t,
		govText,
		"stake_test1upugeuz3jdy0a7hncusutadavzcetdzylgxcldz39hp9n0s0xy0n5",
	)
	assert.Contains(t, govText, "get_governance_state")
	assert.Contains(t, govText, "get_governance_proposal")
	assert.NotContains(t, govText, "gov_action_proposal")

	resGovEmpty, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "conway_governance_brief",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resGovEmpty.Messages[0].Content.(*mcp.TextContent).Text)

	// 6. investigate_address (success & missing address)
	resAddr, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "investigate_address",
		Arguments: map[string]string{
			"address": "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp",
		},
	})
	require.NoError(t, err)
	addrText := resAddr.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(
		t,
		addrText,
		"addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp",
	)
	assert.Contains(t, addrText, "get_utxos")

	_, errAddrMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "investigate_address",
	})
	assert.Error(t, errAddrMissing)
	assert.Contains(
		t,
		errAddrMissing.Error(),
		"missing required argument 'address'",
	)
}

func TestAuditPoolRewardsAccountQueryPrepares(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	query := "SELECT COUNT(*) AS active_delegators FROM account WHERE active = 1 AND pool = X'" + strings.Repeat(
		"01",
		28,
	) + "'"
	stmt, err := db.Prepare(query)
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
}

func TestMCPPromptsCapabilities(t *testing.T) {
	t.Parallel()

	cfg := DefaultProviderConfig()
	deps := ProviderDependencies{
		Network: "preview",
	}
	server, _, err := NewMCPServer(cfg, deps)
	require.NoError(t, err)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// Verify server capabilities include Prompts
	initRes := cs.InitializeResult()
	require.NotNil(t, initRes)
	caps := initRes.Capabilities
	require.NotNil(t, caps)
	require.NotNil(t, caps.Prompts)
	assert.True(t, caps.Prompts.ListChanged)
	require.NotNil(t, caps.Tools)
	assert.True(t, caps.Tools.ListChanged)
	require.NotNil(t, caps.Resources)
	assert.True(t, caps.Resources.ListChanged)
}

func TestRegisterResourcesNilDB(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterResources(server, nil, nil, "preview")

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// Reading status with nil DB and nil LedgerState returns zero status
	res, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://node/status"},
	)
	require.NoError(t, err)
	assert.Contains(t, res.Contents[0].Text, "Dingo Node Status")
	assert.Contains(t, res.Contents[0].Text, "Tip Slot**: `0`")

	// Reading tables with nil DB reports unavailable schema
	tablesRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://schema/tables"},
	)
	require.NoError(t, err)
	assert.Contains(
		t,
		tablesRes.Contents[0].Text,
		"Dingo SQLite Database Tables Catalog",
	)

	assert.Contains(
		t,
		tablesRes.Contents[0].Text,
		"SQLite database is unavailable",
	)

	// Reading dingo://docs/identity returns identity content
	idRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://docs/identity"},
	)
	require.NoError(t, err)
	assert.Contains(t, idRes.Contents[0].Text, "Protocol Minor Version 69")

	// Reading dingo://docs/faq returns FAQ content
	faqRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://docs/faq"},
	)
	require.NoError(t, err)
	assert.Contains(t, faqRes.Contents[0].Text, "Frequently Asked Questions")

	// Reading dingo://docs/architecture returns architecture content
	archRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://docs/architecture"},
	)
	require.NoError(t, err)
	assert.Contains(t, archRes.Contents[0].Text, "Dingo Node Architecture")

	// Reading table schema with nil DB returns error
	_, err = cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://schema/table/blocks"},
	)
	require.Error(t, err)
}

func TestSchemaGuidanceAgainstMigratedDatabase(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	db, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	_, err = db.Exec(`
INSERT INTO "transaction" (hash, block_hash, slot, block_index, fee)
VALUES (zeroblob(32), zeroblob(32), 123, 0, '170001');
INSERT INTO utxo (tx_id, output_idx, amount, deleted_slot)
VALUES (zeroblob(32), 0, '1000001', 0), (zeroblob(32), 1, '2000002', 124);
INSERT INTO account (staking_key, credential_tag, reward, active)
VALUES (zeroblob(28), 0, '111', 1), (zeroblob(28), 1, '222', 1),
       (X'01', 0, '333', 0);
INSERT INTO datum (hash, raw_datum, added_slot)
VALUES (zeroblob(32), X'182A', 123);`)
	require.NoError(t, err)

	server, _, err := NewMCPServer(
		DefaultProviderConfig(),
		ProviderDependencies{SQLDB: db},
	)
	require.NoError(t, err)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "schema-test", Version: "1"}, nil).
		Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	read := func(uri string) string {
		t.Helper()
		result, err := cs.ReadResource(
			t.Context(),
			&mcp.ReadResourceParams{URI: uri},
		)
		require.NoError(t, err)
		require.Len(t, result.Contents, 1)
		return result.Contents[0].Text
	}
	examples := regexp.MustCompile("(?s)~~~sql\n(.*?)\n~~~").
		FindAllStringSubmatch(
			read("dingo://dbsync/cheatsheet"), -1)
	require.Len(t, examples, 4)
	for _, example := range examples {
		query := example[1]
		require.NoError(t, ValidateReadOnlyQuery(query))
		rows := readGuidanceRows(t, db, query)
		switch {
		case strings.Contains(query, `FROM "transaction"`):
			require.Len(t, rows, 1)
			require.Equal(t, "170001", rows[0]["fee_lovelace"])
			require.Equal(t, strings.Repeat("00", 32), rows[0]["tx_hash"])
		case strings.Contains(query, "FROM utxo"):
			require.Len(t, rows, 1, "spent outputs must be excluded")
			require.Equal(t, "1000001", rows[0]["lovelace"])
			require.EqualValues(t, 0, rows[0]["output_idx"])
		case strings.Contains(query, "FROM account"):
			require.Len(t, rows, 2, "inactive accounts must be excluded")
			require.EqualValues(t, 1, rows[0]["credential_tag"])
			require.EqualValues(t, 0, rows[1]["credential_tag"])
			require.Equal(
				t,
				rows[0]["stake_credential"],
				rows[1]["stake_credential"],
			)
			require.Equal(t, "222", rows[0]["reward_lovelace"])
		case strings.Contains(query, "FROM datum"):
			require.Len(t, rows, 1)
			require.Equal(t, "182A", rows[0]["datum_cbor_hex"])
		default:
			t.Fatalf("example needs semantic assertions: %s", query)
		}
		callTool(t, cs, "sqlite_query", map[string]any{"query": query}, false)
	}

	catalog := read("dingo://schema/tables")
	require.Contains(t, catalog, "output_idx INTEGER")
	require.Contains(t, catalog, "staking_key BLOB")
	require.Contains(t, catalog, "raw_datum BLOB")
	// Changes after registration must be reflected, including quoted identifiers.
	_, err = db.Exec(`CREATE TABLE "catalog's probe" (new_column TEXT)`)
	require.NoError(t, err)
	catalog = read("dingo://schema/tables")
	require.Contains(t, catalog, "catalog's probe")
	require.Contains(t, catalog, "new_column TEXT")
	_, err = db.Exec(`DROP TABLE "catalog's probe"`)
	require.NoError(t, err)
	require.NotContains(t, read("dingo://schema/tables"), "catalog's probe")
}

func readGuidanceRows(t *testing.T, db *sql.DB, query string) []map[string]any {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		row := make(map[string]any, len(cols))
		for i, name := range cols {
			row[name] = values[i]
		}
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return result
}
