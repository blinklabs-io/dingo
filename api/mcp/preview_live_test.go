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
	"encoding/hex"
	"flag"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

var previewTestURL = flag.String(
	"preview-mcp-url",
	"",
	"Preview MCP endpoint for opt-in live evaluation",
)

var previewTestDB = flag.String(
	"preview-mcp-db",
	"",
	"Local metadata database for the Preview MCP endpoint",
)

// TestPreviewLiveEvaluation evaluates unsigned templates without submitting
// transactions. Supply a Preview MCP URL and its local SQLite metadata path.
func TestPreviewLiveEvaluation(t *testing.T) {
	t.Parallel()
	endpoint := os.Getenv("DINGO_MCP_PREVIEW_TEST_URL")
	path := os.Getenv("DINGO_MCP_PREVIEW_TEST_DB")
	if *previewTestURL != "" {
		endpoint = *previewTestURL
	}
	if *previewTestDB != "" {
		path = *previewTestDB
	}
	if endpoint == "" || path == "" {
		t.Skip("set DINGO_MCP_PREVIEW_TEST_URL and DINGO_MCP_PREVIEW_TEST_DB")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := OpenReadOnlySQLite(path)
	require.NoError(t, err)
	defer db.Close()
	var network string
	require.NoError(t, db.QueryRowContext(
		ctx,
		"SELECT value FROM node_settings_gate WHERE name='network'",
	).Scan(&network))
	require.Equal(t, "preview", network)
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "preview-evaluation-test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint}, nil)
	require.NoError(t, err)
	defer cs.Close()
	require.Contains(
		t,
		callTool(t, cs, "get_cardano_tip", map[string]any{}, false),
		"preview",
	)
	var hash, payment []byte
	var idx uint32
	var amount uint64
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT tx_id, output_idx, payment_key, amount
FROM utxo u
WHERE deleted_slot=0 AND payment_script=0 AND length(payment_key)=28
  AND (datum_hash IS NULL OR length(datum_hash)=0)
  AND CAST(amount AS INTEGER)>5000000
  AND NOT EXISTS (SELECT 1 FROM asset WHERE utxo_id=u.id)
LIMIT 1`).Scan(&hash, &idx, &payment, &amount))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		0,
		payment,
		nil,
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)
	body := map[uint]any{
		0: []any{[]any{hash, idx}},
		1: []any{[]any{addressBytes, amount - 500000}},
		2: uint64(500000),
	}
	evaluate := func(witness map[uint]any) string {
		t.Helper()
		raw, err := cbor.Encode([]any{body, witness, true, nil})
		require.NoError(t, err)
		result := callTool(t, cs, "evaluate_tx", map[string]any{
			"cbor": hex.EncodeToString(raw),
		}, false)
		require.Contains(t, result, "SUCCESS")
		return result
	}
	plain := evaluate(map[uint]any{})
	require.Contains(t, plain, "Total CPU Steps**: 0")
	require.Contains(t, plain, "Total Memory**: 0")

	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{Body: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Constant{Con: &syn.Unit{}},
		}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	script, err := cbor.Encode(flat)
	require.NoError(t, err)
	policy := lcommon.PlutusV1Script(script).Hash()
	mint := map[cbor.ByteString]map[cbor.ByteString]int64{
		cbor.NewByteString(policy.Bytes()): {
			cbor.NewByteString([]byte("mcp-eval")): 1,
		},
	}
	body[9] = mint
	body[1] = []any{[]any{addressBytes, []any{amount - 500000, mint}}}
	witness := map[uint]any{
		3: []any{script},
		5: []any{
			[]any{
				uint64(1),
				uint64(0),
				uint64(0),
				[]any{uint64(1000000), uint64(100000000)},
			},
		},
	}
	first := evaluate(witness)
	require.Contains(t, first, "| mint | 0 |")
	require.NotContains(t, first, "Total CPU Steps**: 0")
	require.NotContains(t, first, "Total Memory**: 0")
	require.Equal(
		t,
		first,
		evaluate(witness),
		"same template and ledger parameters must evaluate consistently",
	)
	t.Log("unspent input", hex.EncodeToString(hash), idx)
	t.Log(strings.TrimSpace(first))
}

// TestPreviewLiveSQL exercises the current MCP implementation against a live
// node's metadata file without starting or modifying that node.
func TestPreviewLiveSQL(t *testing.T) {
	t.Parallel()
	path := os.Getenv("DINGO_MCP_PREVIEW_TEST_DB")
	if *previewTestDB != "" {
		path = *previewTestDB
	}
	if path == "" {
		t.Skip("set DINGO_MCP_PREVIEW_TEST_DB")
	}
	db, err := OpenReadOnlySQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var network string
	require.NoError(
		t,
		db.QueryRowContext(t.Context(), "SELECT value FROM node_settings_gate WHERE name='network'").
			Scan(&network),
	)
	require.Equal(t, "preview", network)
	cfg := DefaultProviderConfig()
	server, _, err := NewMCPServer(cfg, ProviderDependencies{SQLDB: db})
	require.NoError(t, err)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "preview-sql-test", Version: "1"}, nil).
		Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	guidance, err := cs.ReadResource(
		t.Context(),
		&mcp.ReadResourceParams{URI: "dingo://dbsync/cheatsheet"},
	)
	require.NoError(t, err)
	require.Len(t, guidance.Contents, 1)
	examples := regexp.MustCompile("(?s)~~~sql\n(.*?)\n~~~").
		FindAllStringSubmatch(guidance.Contents[0].Text, -1)
	require.Len(t, examples, 4)
	for _, example := range examples {
		t.Log("SQL example:", example[1])
		stmt, err := db.PrepareContext(t.Context(), example[1])
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		callTool(
			t,
			cs,
			"sqlite_query",
			map[string]any{"query": example[1]},
			false,
		)
	}
	catalog, err := cs.ReadResource(
		t.Context(),
		&mcp.ReadResourceParams{URI: "dingo://schema/tables"},
	)
	require.NoError(t, err)
	require.Contains(t, catalog.Contents[0].Text, "staking_key BLOB")
	require.Contains(t, catalog.Contents[0].Text, "output_idx INTEGER")
	callTool(
		t,
		cs,
		"sqlite_query",
		map[string]any{"query": "PRAGMA table_info(utxo)"},
		false,
	)
	var queryOnly int
	require.NoError(
		t,
		db.QueryRowContext(t.Context(), "PRAGMA query_only").Scan(&queryOnly),
	)
	require.Equal(t, 1, queryOnly)
	t.Log(
		"four resource SQL examples executed through MCP against read-only Preview metadata",
	)
}
