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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestQASQLInjectionBoundary(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(
		"CREATE TABLE sentinel(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO sentinel VALUES(1,'unchanged')",
	)
	require.NoError(t, err)
	cs := newToolSession(t, db, 50)
	forbidden := []string{
		"SELECT 1; DELETE FROM sentinel",
		"SELECT 'x;';/**/UPDATE sentinel SET value='changed'",
		"SELECT 1;-- hidden\nDROP TABLE sentinel",
		"SELECT 1\x00;DELETE FROM sentinel",
		"WITH \"SELECT\"(n) AS MATERIALIZED (VALUES(1)) DELETE FROM sentinel RETURNING *",
		"WITH RECURSIVE x(n) AS (VALUES(1)) UPDATE sentinel SET value='changed'",
		"PRAGMA query_only(0)",
		"PRAGMA main.query_only=OFF",
		"EXPLAIN PRAGMA writable_schema=ON",
		"ATTACH ':memory:' AS injected",
		"VACUUM INTO 'injected.sqlite'",
	}
	for _, query := range forbidden {
		t.Run(query, func(t *testing.T) {
			for _, tool := range []string{"sqlite_query", "sqlite_explain"} {
				callTool(t, cs, tool, map[string]any{"query": query}, true)
			}
		})
	}
	for _, query := range []string{
		"SELECT 'it''s; DELETE FROM sentinel' AS value",
		"SELECT 1 AS [; DROP TABLE sentinel]",
		"SELECT 1 AS `; DROP TABLE sentinel`",
		"SELECT 1 /* ; DELETE FROM sentinel */; -- comment",
		"\ufeffWITH x AS (SELECT ') ; DROP') SELECT * FROM x",
		"EXPLAIN DELETE FROM sentinel",
		"EXPLAIN ATTACH ':memory:' AS injected",
		"PRAGMA table_info('sentinel')",
	} {
		callTool(t, cs, "sqlite_query", map[string]any{"query": query}, false)
	}
	var value string
	require.NoError(t, db.QueryRow("SELECT value FROM sentinel WHERE id=1").Scan(&value))
	require.Equal(t, "unchanged", value)
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM sentinel").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(
		t,
		db.QueryRow("SELECT COUNT(*) FROM pragma_database_list WHERE name='injected'").Scan(&count),
	)
	require.Zero(t, count)
	require.NoError(t, db.QueryRow("PRAGMA query_only").Scan(&count))
	require.Zero(t, count)
	_, err = db.Exec("INSERT INTO sentinel VALUES(2,'pool still writable')")
	require.NoError(t, err)
}

func TestQASchemaReservedIdentifier(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	cs := newToolSession(t, db, 100)
	text := callTool(
		t,
		cs,
		"sqlite_table_schema",
		map[string]any{"table_name": "transaction"},
		false,
	)
	require.Contains(t, text, "block_hash")
	for _, name := range []string{"transaction); DROP TABLE utxo;--", "transaction' OR 1=1 --", "transaction\x00"} {
		callTool(t, cs, "sqlite_table_schema", map[string]any{"table_name": name}, true)
	}
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&count))
}

func TestQASQLCancellationRestoresConnection(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	cs := newToolSessionWithTimeout(t, db, 10, 20*time.Millisecond)
	result := callTool(
		t,
		cs,
		"sqlite_query",
		map[string]any{
			"query": "WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x) SELECT sum(n) FROM x",
		},
		true,
	)
	require.Contains(t, result, "context deadline exceeded")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, "CREATE TABLE after_cancel(n INTEGER)")
	require.NoError(t, err)
	callTool(t, cs, "sqlite_query", map[string]any{"query": "SELECT 42"}, false)
}

func TestQAQueryLengthBoundary(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateReadOnlyQuery("SELECT 1"+strings.Repeat(" ", (64<<10)-8)))
	require.Error(t, ValidateReadOnlyQuery("SELECT 1"+strings.Repeat(" ", 64<<10)))
}

func FuzzQASQLReadOnlyBoundary(f *testing.F) {
	for _, s := range []string{"", "'", "/*", "--\n", ";", "\ufeff", "\x00", ") AS (SELECT 1)"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, noise string) {
		if len(noise) > 4096 {
			t.Skip()
		}
		quoted := strings.ReplaceAll(noise, "'", "''")
		if strings.ContainsRune(quoted, 0) {
			return
		}
		require.NoError(t, ValidateReadOnlyQuery(fmt.Sprintf("SELECT '%s'", quoted)))
		require.Error(
			t,
			ValidateReadOnlyQuery(fmt.Sprintf("SELECT '%s'; DELETE FROM sentinel", quoted)),
		)
		require.Error(
			t,
			ValidateReadOnlyQuery(
				fmt.Sprintf("WITH x AS (SELECT '%s') DELETE FROM sentinel", quoted),
			),
		)
	})
}

func TestQAAssetNameCollision(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	_, err := db.Exec(
		`INSERT INTO utxo(id,tx_id,output_idx,payment_key,amount,deleted_slot) VALUES(1,zeroblob(32),0,zeroblob(28),'1',0);
 INSERT INTO asset(id,utxo_id,policy_id,name,fingerprint,amount) VALUES(1,1,zeroblob(28),x'ab','binary','7'),(2,1,zeroblob(28),x'4142','text','1000'),(3,1,zeroblob(28),x'20','space','20');`,
	)
	require.NoError(t, err)
	cs := newToolSession(t, db, 100)
	for _, tc := range []struct{ name, supply, fingerprint string }{
		{"AB", "7", "binary"}, {"4142", "1000", "text"}, {" ", "20", "space"},
	} {
		text := callTool(
			t,
			cs,
			"get_asset_info",
			map[string]any{"policy_id": strings.Repeat("0", 56), "asset_name": tc.name},
			false,
		)
		require.Contains(t, text, "**Live Circulating Supply**: "+tc.supply+" units")
		text = callTool(
			t,
			cs,
			"get_utxos_by_asset",
			map[string]any{"policy_id": strings.Repeat("0", 56), "asset_name": tc.name},
			false,
		)
		require.Contains(t, text, "Found 1 UTxOs")
		require.Contains(t, text, tc.fingerprint)
	}
}

func TestQACredentialPrefixesAndZeroHashes(t *testing.T) {
	t.Parallel()
	for _, hrp := range []string{"pool", "drep", "unrelated", "stake_bad", "addr_bad"} {
		for _, n := range []int{28, 29, 40} {
			data, err := bech32.ConvertBits(make([]byte, n), 8, 5, true)
			require.NoError(t, err)
			value, err := bech32.Encode(hrp, data)
			require.NoError(t, err)
			_, _, err = parseAddressOrCredential(value)
			require.Error(t, err, "prefix %s length %d", hrp, n)
		}
	}
	cs := newToolSession(t, nil, 100)
	for _, typ := range []uint8{lcommon.AddressTypeKeyKey, lcommon.AddressTypeScriptNone, lcommon.AddressTypeNoneKey, lcommon.AddressTypeNoneScript} {
		payment, stake := make([]byte, 28), make([]byte, 28)
		if typ == lcommon.AddressTypeScriptNone {
			stake = nil
		}
		if typ == lcommon.AddressTypeNoneKey || typ == lcommon.AddressTypeNoneScript {
			payment = nil
		}
		addr, err := lcommon.NewAddressFromParts(typ, 0, payment, stake)
		require.NoError(t, err)
		text := callTool(t, cs, "decode_address", map[string]any{"address": addr.String()}, false)
		if typ != lcommon.AddressTypeNoneKey && typ != lcommon.AddressTypeNoneScript {
			require.Contains(t, text, "**Payment Credential Hash** | `"+strings.Repeat("0", 56)+"`")
		}
		if typ != lcommon.AddressTypeScriptNone {
			require.Contains(t, text, "**Staking Credential Hash** | `"+strings.Repeat("0", 56)+"`")
		}
	}
}

func TestQAActualListenerBoundary(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{"0.0.0.0:8088", "[::]:8088", ":8088", "192.0.2.1:8088"} {
		server, err := NewServer(
			t.Context(),
			DefaultProviderConfig(),
			ProviderDependencies{},
			apiconfig.EffectiveTLS{},
			addr,
		)
		if server != nil {
			require.NoError(t, server.Stop(t.Context()))
		}
		require.Error(t, err, addr)
	}
}

func TestQAMalformedCBORAndNumericInputs(t *testing.T) {
	t.Parallel()
	cs := newToolSession(t, nil, 100)
	output := "82581d60" + strings.Repeat("00", 28) + "1a000f4240"
	for i, payload := range []string{"ff", "80", output + "ff", output[:len(output)-2], strings.Repeat("81", 1025) + "00"} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			callTool(t, cs, "calculate_min_utxo", map[string]any{"output_cbor_hex": payload}, true)
		})
	}
	callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{"coins_per_utxo_byte": uint64(math.MaxUint64)},
		true,
	)
	for _, value := range []any{-1, 1.5, "1"} {
		result, err := cs.CallTool(
			t.Context(),
			&mcp.CallToolParams{
				Name:      "calculate_min_utxo",
				Arguments: map[string]any{"coins_per_utxo_byte": value},
			},
		)
		require.True(
			t,
			err != nil || (result != nil && result.IsError),
			"invalid rate %#v accepted",
			value,
		)
	}
	callTool(t, cs, "calculate_min_utxo", map[string]any{"output_cbor_hex": output}, false)
	text := callTool(t, cs, "evaluate_tx", map[string]any{"cbor": "80ff"}, true)
	require.Contains(t, text, "trailing data")
}

func TestQAAssetSupplementalErrors(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"token_registry_entry", "asset_mint_burn", "transaction_metadata_label"} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			db := newSchemaDB(t)
			_, err := db.Exec(
				`INSERT INTO asset_mint_burn(tx_hash,policy_id,name,slot) VALUES(zeroblob(32),zeroblob(28),x'41',1)`,
			)
			require.NoError(t, err)
			_, err = db.Exec("DROP TABLE " + table)
			require.NoError(t, err)
			cs := newToolSession(t, db, 100)
			text := callTool(
				t,
				cs,
				"get_asset_info",
				map[string]any{"policy_id": strings.Repeat("0", 56), "asset_name": "A"},
				true,
			)
			require.Contains(t, text, table)
			require.NotContains(t, text, "was not found")
		})
	}
}

func TestQABoundParameterPayloads(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	cs := newToolSession(t, db, 100)
	payload := "' OR 1=1; DROP TABLE utxo;--"
	for _, tool := range []string{"get_asset_info", "get_utxos_by_asset"} {
		text := callTool(
			t,
			cs,
			tool,
			map[string]any{"policy_id": strings.Repeat("0", 56), "asset_name": payload},
			false,
		)
		require.NotContains(t, text, "SQL logic error")
	}
	for _, tc := range []struct{ tool, key string }{
		{"get_transaction", "tx_hash"}, {"get_block", "hash_or_slot"}, {"get_utxos", "address_or_credential"}, {"get_account", "stake_address_or_credential"}, {"get_pool_performance", "pool_id"}, {"get_governance_proposal", "status"},
	} {
		callTool(t, cs, tc.tool, map[string]any{tc.key: payload}, true)
	}
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&count))
	for _, tool := range []string{"get_asset_info", "get_utxos_by_asset"} {
		text := callTool(
			t,
			cs,
			tool,
			map[string]any{
				"policy_id":  strings.Repeat("0", 56),
				"asset_name": strings.Repeat("x", 33),
			},
			true,
		)
		require.Contains(t, text, "32 bytes")
	}
}

func FuzzQACredentialParsing(f *testing.F) {
	for _, s := range []string{"", strings.Repeat("0", 56), "addr_vkh1", "pool1", "\x00", "stake1", "é"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 4096 {
			t.Skip()
		}
		payment, stake, err := parseAddressOrCredential(s)
		if err == nil {
			require.True(t, len(payment) == 28 || len(stake) == 28)
			require.True(t, len(payment) == 0 || len(payment) == 28)
			require.True(t, len(stake) == 0 || len(stake) == 28)
		}
		if hash, err := parsePoolID(s); err == nil {
			require.Len(t, hash, 28)
		}
		if hash, _, err := parseDrepCredential(s); err == nil && strings.TrimSpace(s) != "" {
			require.Len(t, hash, 28)
		}
	})
}

func TestQAMintScanFailure(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	_, err := db.Exec(
		`INSERT INTO asset_mint_burn(tx_hash,policy_id,name,slot) VALUES(zeroblob(32),zeroblob(28),x'41','invalid-slot')`,
	)
	require.NoError(t, err)
	cs := newToolSession(t, db, 100)
	text := callTool(
		t,
		cs,
		"get_asset_info",
		map[string]any{"policy_id": strings.Repeat("0", 56), "asset_name": "A"},
		true,
	)
	require.Contains(t, text, "read mint history")
}
