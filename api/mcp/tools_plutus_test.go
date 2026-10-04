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
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveDatumOrScript(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE datum (
			hash BLOB NOT NULL UNIQUE,
			raw_datum BLOB NOT NULL,
			id INTEGER PRIMARY KEY,
			added_slot INTEGER NOT NULL
		);
		CREATE TABLE script (
			hash BLOB UNIQUE,
			content BLOB,
			id INTEGER PRIMARY KEY,
			created_slot INTEGER,
			type INTEGER
		);
	`)
	require.NoError(t, err)

	// Encode a Plutus Constr datum: Tag 121 (Constr 0) with fields ["bidder_address", 5000000]
	datumData := cbor.Tag{
		Number: 121,
		Content: []any{
			[]byte("bidder_address"),
			int64(5000000),
		},
	}
	datumCbor, err := cbor.Encode(datumData)
	require.NoError(t, err)

	datumHashHex := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	datumHashBytes, err := hex.DecodeString(datumHashHex)
	require.NoError(t, err)

	_, err = db.Exec(
		"INSERT INTO datum (hash, raw_datum, id, added_slot) VALUES (?, ?, ?, ?)",
		datumHashBytes,
		datumCbor,
		1,
		123456,
	)
	require.NoError(t, err)

	// Script: 28-byte hash, PlutusV2 (type 2)
	scriptHashHex := "aa112233445566778899aabbccddeeff00112233445566778899aabb"
	scriptHashBytes, err := hex.DecodeString(scriptHashHex)
	require.NoError(t, err)

	scriptContent := []byte{0x4e, 0x4d, 0x01, 0x00, 0x00, 0x22, 0x26}
	_, err = db.Exec(
		"INSERT INTO script (hash, content, id, created_slot, type) VALUES (?, ?, ?, ?, ?)",
		scriptHashBytes,
		scriptContent,
		1,
		654321,
		2,
	)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Resolve Datum
	datumRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": datumHashHex},
	})
	require.NoError(t, err)
	assert.False(t, datumRes.IsError)
	datumText := datumRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, datumText, "### Resolved Datum")
	assert.Contains(t, datumText, "Plutus Datum")
	assert.NotContains(
		t, datumText, "CIP-32",
		"index rows do not identify whether a datum was inline or witnessed",
	)
	assert.Contains(t, datumText, "123456")
	assert.Contains(t, datumText, "constructor")
	assert.Contains(t, datumText, "5000000")

	// 2. Resolve Script
	scriptRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": scriptHashHex},
	})
	require.NoError(t, err)
	assert.False(t, scriptRes.IsError)
	scriptText := scriptRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, scriptText, "### Resolved Script")
	assert.Contains(t, scriptText, "PlutusV2")
	assert.Contains(t, scriptText, "654321")
	assert.Contains(t, scriptText, "4e4d0100002226")

	// 3. Resolve Datum with corrupted CBOR (exercises fallback comment)
	corruptDatumHashHex := "0202030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	corruptHashBytes, err := hex.DecodeString(corruptDatumHashHex)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO datum (hash, raw_datum, id, added_slot) VALUES (?, ?, ?, ?)",
		corruptHashBytes,
		[]byte{0xff},
		2,
		999,
	)
	require.NoError(t, err)

	corruptRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": corruptDatumHashHex},
	})
	require.NoError(t, err)
	assert.False(t, corruptRes.IsError)
	assert.Contains(
		t,
		corruptRes.Content[0].(*mcp.TextContent).Text,
		"raw cbor parse error",
	)

	// 4. Resolve Script with content > 512 bytes and null slot/type
	longScriptHashHex := "bb112233445566778899aabbccddeeff00112233445566778899aabb"
	longScriptBytes, err := hex.DecodeString(longScriptHashHex)
	require.NoError(t, err)
	longContent := make([]byte, 600)
	for i := range longContent {
		longContent[i] = 0xab
	}
	_, err = db.Exec(
		"INSERT INTO script (hash, content, id, created_slot, type) VALUES (?, ?, ?, NULL, NULL)",
		longScriptBytes,
		longContent,
		2,
	)
	require.NoError(t, err)

	longRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": longScriptHashHex},
	})
	require.NoError(t, err)
	assert.False(t, longRes.IsError)
	longText := longRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, longText, "### Resolved Script")
	assert.Contains(t, longText, "Unknown")
	assert.Contains(t, longText, "...")
}

func TestResolveDatumOrScriptInvalidInputs(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE datum (hash BLOB NOT NULL UNIQUE, raw_datum BLOB NOT NULL, id INTEGER PRIMARY KEY, added_slot INTEGER NOT NULL);
		CREATE TABLE script (hash BLOB UNIQUE, content BLOB, id INTEGER PRIMARY KEY, created_slot INTEGER, type INTEGER);
	`)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Empty hash
	resEmpty, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": "   "},
	})
	require.NoError(t, err)
	assert.True(t, resEmpty.IsError)
	assert.Contains(
		t,
		resEmpty.Content[0].(*mcp.TextContent).Text,
		"Hash parameter is empty",
	)

	// 2. Non-hex characters
	resNonHex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": "not-valid-hex-zzz!"},
	})
	require.NoError(t, err)
	assert.True(t, resNonHex.IsError)
	assert.Contains(
		t,
		resNonHex.Content[0].(*mcp.TextContent).Text,
		"Invalid hex-encoded hash",
	)

	// 3. Invalid length (e.g. 10 hex chars = 5 bytes)
	resShort, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": "0102030405"},
	})
	require.NoError(t, err)
	assert.True(t, resShort.IsError)
	assert.Contains(
		t,
		resShort.Content[0].(*mcp.TextContent).Text,
		"Invalid hash length",
	)

	// 4. Hash not found in DB (valid 32-byte hash)
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "resolve_datum_or_script",
		Arguments: map[string]any{
			"hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
	})
	require.NoError(t, err)
	assert.True(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"was not found in the local database",
	)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"inline datum",
	)

	// 5. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "test-nil", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	clTrNil, srvTrNil := mcp.NewInMemoryTransports()
	_, err = serverNil.Connect(ctx, srvTrNil, nil)
	require.NoError(t, err)
	clientNil := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	csNil, err := clientNil.Connect(ctx, clTrNil, nil)
	require.NoError(t, err)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name: "resolve_datum_or_script",
		Arguments: map[string]any{
			"hash": "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
	assert.Contains(
		t,
		resNilDB.Content[0].(*mcp.TextContent).Text,
		"SQLite database is not available",
	)
}

func TestDatumIndexErrorsAndCoverage(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	cs := newToolSession(t, db, 100)
	args := map[string]any{"hash": strings.Repeat("ff", 32)}
	missing := callTool(t, cs, "resolve_datum_or_script", args, true)
	require.Contains(t, missing, "Core storage mode does not populate")
	require.Contains(t, missing, "both inline datum values and witness datums")
	require.Contains(
		t,
		missing,
		"does not mean the hash is absent from the blockchain",
	)
	_, err := db.Exec("ALTER TABLE datum RENAME TO saved_datum")
	require.NoError(t, err)
	failed := callTool(t, cs, "resolve_datum_or_script", args, true)
	require.Contains(t, failed, "datum index query failed")
	require.NotContains(t, failed, "was not found")
	_, err = db.Exec(
		"ALTER TABLE saved_datum RENAME TO datum; DROP TABLE script",
	)
	require.NoError(t, err)
	failed = callTool(t, cs, "resolve_datum_or_script", args, true)
	require.Contains(t, failed, "script index query failed")
	require.NotContains(t, failed, "was not found")
}

func TestEvaluateTxInvalidInputs(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

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

	// 1. Empty payload
	resEmpty, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": "   "},
	})
	require.NoError(t, err)
	assert.True(t, resEmpty.IsError)
	assert.Contains(
		t,
		resEmpty.Content[0].(*mcp.TextContent).Text,
		"Transaction payload is empty",
	)

	// 2. Non-hex non-base64
	resInvalidEnc, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": "zzz-not-cbor-!@#$"},
	})
	require.NoError(t, err)
	assert.True(t, resInvalidEnc.IsError)
	assert.Contains(
		t,
		resInvalidEnc.Content[0].(*mcp.TextContent).Text,
		"Failed to decode transaction CBOR",
	)

	// 3. Valid hex of non-transaction CBOR
	resBadType, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": "80"},
	})
	require.NoError(t, err)
	assert.True(t, resBadType.IsError)
	assert.Contains(
		t,
		resBadType.Content[0].(*mcp.TextContent).Text,
		"failed to determine transaction type",
	)

	// 4. Valid transaction CBOR, but nil LedgerState
	validTxHex := "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1" +
		"e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ce" +
		"b93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e391" +
		"4f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a" +
		"9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172" +
		"e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe17" +
		"9af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebee" +
		"f022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a40081825820" +
		"25fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409d" +
		"a928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c795" +
		"1c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581" +
		"840000187b820a0a06814746010000222601f4f6"
	resNilLS, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": validTxHex},
	})
	require.NoError(t, err)
	assert.True(t, resNilLS.IsError)
	assert.Contains(
		t,
		resNilLS.Content[0].(*mcp.TextContent).Text,
		"Ledger state is not initialized on this node",
	)
}

func TestPlutusDataAndScriptParsing(t *testing.T) {
	t.Parallel()

	// 1. scriptTypeName
	assert.Equal(t, "Native / MultiSig (Phase-1)", scriptTypeName(0))
	assert.Equal(t, "PlutusV1", scriptTypeName(1))
	assert.Equal(t, "PlutusV2", scriptTypeName(2))
	assert.Equal(t, "PlutusV3", scriptTypeName(3))
	assert.Equal(t, "Unknown (99)", scriptTypeName(99))

	// 2. redeemerPurposeString
	assert.Equal(t, "spend", redeemerPurposeString(lcommon.RedeemerTagSpend))
	assert.Equal(t, "mint", redeemerPurposeString(lcommon.RedeemerTagMint))
	assert.Equal(t, "cert", redeemerPurposeString(lcommon.RedeemerTagCert))
	assert.Equal(t, "reward", redeemerPurposeString(lcommon.RedeemerTagReward))
	assert.Equal(t, "voting", redeemerPurposeString(lcommon.RedeemerTagVoting))
	assert.Equal(
		t,
		"proposing",
		redeemerPurposeString(lcommon.RedeemerTagProposing),
	)
	assert.Equal(
		t,
		"guarding",
		redeemerPurposeString(lcommon.RedeemerTagGuarding),
	)
	assert.Equal(t, "tag(99)", redeemerPurposeString(lcommon.RedeemerTag(99)))

	// 3. cborToPrettyJSON
	validMap := map[any]any{
		"key": "val",
		123:   []any{int64(1), []byte{0xde, 0xad}},
	}
	mapCbor, err := cbor.Encode(validMap)
	require.NoError(t, err)
	pretty, err := cborToPrettyJSON(mapCbor)
	require.NoError(t, err)
	assert.Contains(t, pretty, "dead")

	// Corrupted CBOR
	_, err = cborToPrettyJSON([]byte{0xff})
	assert.Error(t, err)
}
