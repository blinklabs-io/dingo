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
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterCardanoToolsNilDB(t *testing.T) {
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

	// get_node_info works without DB
	infoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_node_info",
		Arguments: map[string]any{"topic": "identity"},
	})
	require.NoError(t, err)
	assert.False(t, infoRes.IsError)
	assert.Contains(
		t,
		infoRes.Content[0].(*mcp.TextContent).Text,
		"Protocol Minor Version 69",
	)

	faqToolRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_node_info",
		Arguments: map[string]any{"topic": "faq"},
	})
	require.NoError(t, err)
	assert.False(t, faqToolRes.IsError)
	assert.Contains(
		t,
		faqToolRes.Content[0].(*mcp.TextContent).Text,
		"Frequently Asked Questions",
	)

	archToolRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_node_info",
		Arguments: map[string]any{"topic": "architecture"},
	})
	require.NoError(t, err)
	assert.False(t, archToolRes.IsError)
	assert.Contains(
		t,
		archToolRes.Content[0].(*mcp.TextContent).Text,
		"Dingo Node Architecture",
	)

	defaultToolRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_node_info",
	})
	require.NoError(t, err)
	assert.False(t, defaultToolRes.IsError)
	assert.Contains(
		t,
		defaultToolRes.Content[0].(*mcp.TextContent).Text,
		"Dingo Node Information",
	)

	// get_cardano_tip with nil db returns error
	tipRes, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_cardano_tip"},
	)
	require.NoError(t, err)
	assert.True(t, tipRes.IsError)

	// get_block with nil db returns error
	blockRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_block",
		Arguments: map[string]any{"hash_or_slot": "1000"},
	})
	require.NoError(t, err)
	assert.True(t, blockRes.IsError)

	// get_transaction with nil db returns error
	txRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_transaction",
		Arguments: map[string]any{
			"tx_hash": "0000000000000000000000000000000000000000000000000000000000000001",
		},
	})
	require.NoError(t, err)
	assert.True(t, txRes.IsError)

	// get_utxos with nil db returns error
	utxoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_utxos",
		Arguments: map[string]any{"address_or_credential": "addr_test123"},
	})
	require.NoError(t, err)
	assert.True(t, utxoRes.IsError)

	// get_account with nil db returns error
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_account",
		Arguments: map[string]any{
			"stake_address_or_credential": "stake_test123",
		},
	})
	require.NoError(t, err)
	assert.True(t, accRes.IsError)

	// get_epoch_summary with nil db returns error
	epochRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.True(t, epochRes.IsError)
}

func TestCardanoToolsFallbacks(t *testing.T) {
	t.Parallel()

	// Database with tip table and account with credential column
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE tip (id INTEGER PRIMARY KEY, hash TEXT, slot INTEGER, block_number INTEGER);
		INSERT INTO tip VALUES (1, '0000000000000000000000000000000000000000000000000000000000000055', 5555, 55);

		CREATE TABLE account (credential TEXT, balance INTEGER, pool_id TEXT);
		INSERT INTO account VALUES ('cred_abc123', 9999999, 'pool_preview');

		CREATE TABLE epoch_summary (epoch INTEGER, epoch_nonce BLOB);
		INSERT INTO epoch_summary VALUES (43, X'abcdef123456');
		CREATE TABLE epoch (epoch_id INTEGER, start_slot INTEGER, length_in_slots INTEGER, nonce BLOB);
		INSERT INTO epoch VALUES (42, 100, 50, NULL), (43, 150, 50, X'abcdef123456');
		CREATE TABLE block_nonce (slot INTEGER, nonce BLOB);
		INSERT INTO block_nonce VALUES (151, X'abcdef123456');
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

	// 1. get_cardano_tip from tip table (source: metadata_tip)
	tipRes, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_cardano_tip"},
	)
	require.NoError(t, err)
	assert.False(t, tipRes.IsError)
	assert.Contains(
		t,
		tipRes.Content[0].(*mcp.TextContent).Text,
		"metadata_tip",
	)
	assert.Contains(t, tipRes.Content[0].(*mcp.TextContent).Text, "5555")

	// 2. get_block by slot from tip table
	blockSlotRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_block",
		Arguments: map[string]any{"hash_or_slot": "5555"},
	})
	require.NoError(t, err)
	assert.False(t, blockSlotRes.IsError)
	assert.Contains(t, blockSlotRes.Content[0].(*mcp.TextContent).Text, "5555")

	// 3. get_block by hash from tip table
	blockHashRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "0000000000000000000000000000000000000000000000000000000000000055",
		},
	})
	require.NoError(t, err)
	assert.False(t, blockHashRes.IsError)
	assert.Contains(t, blockHashRes.Content[0].(*mcp.TextContent).Text, "5555")

	// 4. get_account by credential
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_account",
		Arguments: map[string]any{"stake_address_or_credential": "cred_abc123"},
	})
	require.NoError(t, err)
	assert.False(t, accRes.IsError)
	assert.Contains(t, accRes.Content[0].(*mcp.TextContent).Text, "cred_abc123")
	assert.Contains(
		t,
		accRes.Content[0].(*mcp.TextContent).Text,
		"pool_preview",
	)

	// 5. get_epoch_summary from epoch_summary fallback
	epochRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, epochRes.IsError)
	assert.Contains(t, epochRes.Content[0].(*mcp.TextContent).Text, "43")

	// 6. get_epoch_summary with nonce from block_nonce
	epochNonceRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{"epoch": 43},
	})
	require.NoError(t, err)
	assert.False(t, epochNonceRes.IsError)
	assert.Contains(
		t,
		epochNonceRes.Content[0].(*mcp.TextContent).Text,
		"abcdef123456",
	)
}

func TestCardanoToolsTransactionFallback(t *testing.T) {
	t.Parallel()

	// Database with transaction table (no tip, no blocks)
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE "transaction" (id INTEGER PRIMARY KEY, hash BLOB, block_hash BLOB, slot INTEGER, fee INTEGER, block_index INTEGER);
		INSERT INTO "transaction" VALUES (1, X'1111111111111111111111111111111111111111111111111111111111111111', X'1111111111111111111111111111111111111111111111111111111111111111', 777, 190000, 3);

		CREATE TABLE account (staking_key BLOB, credential_tag INTEGER, balance INTEGER);
 INSERT INTO account VALUES (X'99999999999999999999999999999999999999999999999999999999', 0, 12345);

		CREATE TABLE utxo (tx_hash TEXT, transaction_id INTEGER, address TEXT, amount TEXT, tx_id BLOB);
		INSERT INTO utxo VALUES ('1111111111111111111111111111111111111111111111111111111111111111', 1, 'addr_test_paging', '5000000', X'1111111111111111111111111111111111111111111111111111111111111111');
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

	// 1. get_cardano_tip from transaction table (source: metadata_transaction)
	tipRes, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_cardano_tip"},
	)
	require.NoError(t, err)
	assert.False(t, tipRes.IsError)
	assert.Contains(
		t,
		tipRes.Content[0].(*mcp.TextContent).Text,
		"metadata_transaction",
	)
	assert.Contains(t, tipRes.Content[0].(*mcp.TextContent).Text, "777")

	// 2. get_block by slot from transaction table
	blockSlotRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_block",
		Arguments: map[string]any{"hash_or_slot": "777"},
	})
	require.NoError(t, err)
	assert.False(t, blockSlotRes.IsError)
	assert.Contains(t, blockSlotRes.Content[0].(*mcp.TextContent).Text, "777")

	// 3. get_block by hash from transaction table
	blockHashRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "1111111111111111111111111111111111111111111111111111111111111111",
		},
	})
	require.NoError(t, err)
	assert.False(t, blockHashRes.IsError)
	assert.Contains(t, blockHashRes.Content[0].(*mcp.TextContent).Text, "777")

	// 4. get_transaction with block index and UTxO outputs created
	txRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_transaction",
		Arguments: map[string]any{
			"tx_hash": "1111111111111111111111111111111111111111111111111111111111111111",
		},
	})
	require.NoError(t, err)
	assert.False(t, txRes.IsError)
	assert.Contains(
		t,
		txRes.Content[0].(*mcp.TextContent).Text,
		"Block Index**: `3`",
	)
	assert.Contains(
		t,
		txRes.Content[0].(*mcp.TextContent).Text,
		"Outputs Created**: 1",
	)

	// 5. get_account by staking_key
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_account",
		Arguments: map[string]any{
			"stake_address_or_credential": "99999999999999999999999999999999999999999999999999999999",
			"credential_type":             "key",
		},
	})
	require.NoError(t, err)
	assert.False(t, accRes.IsError)
	assert.Contains(
		t,
		accRes.Content[0].(*mcp.TextContent).Text,
		"99999999999999999999999999999999999999999999999999999999",
	)

	// 6. get_utxos with limit and offset boundaries
	utxoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "addr_test_paging",
			"limit":                 150, // exceeds 100, defaults to 20
			"offset":                -5,  // negative, clamped to 0
		},
	})
	require.NoError(t, err)
	assert.False(t, utxoRes.IsError)
	assert.Contains(
		t,
		utxoRes.Content[0].(*mcp.TextContent).Text,
		"addr_test_paging",
	)
}

func TestCardanoToolsWithMetadataSchema(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	txHash, blockHash := bytes.Repeat(
		[]byte{0x11},
		32,
	), bytes.Repeat(
		[]byte{0x22},
		32,
	)
	_, err := db.Exec(
		`INSERT INTO "transaction" (id, hash, block_hash, slot, fee, block_index) VALUES (7, ?, ?, 101, '123', 0)`,
		txHash,
		blockHash,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO utxo (transaction_id, tx_id, output_idx, amount) VALUES (7, ?, 0, '5000001'), (7, ?, 1, '7000002')`,
		txHash,
		txHash,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO epoch (epoch_id, start_slot, length_in_slots, nonce) VALUES (9, 100, 50, X'aaaa');
 INSERT INTO epoch_summary(epoch, total_active_stake, total_pool_count, total_delegators, epoch_nonce, boundary_slot) VALUES (9,'123',1,2,X'bbbb',100);
 INSERT INTO block_nonce(hash,slot,nonce) VALUES (X'01',99,X'01'),(X'02',100,X'02'),(X'03',149,X'03'),(X'04',150,X'04');`,
	)
	require.NoError(t, err)
	cs := newToolSession(t, db, 100)
	summary := callTool(
		t,
		cs,
		"get_transaction",
		map[string]any{"tx_hash": hex.EncodeToString(txHash)},
		false,
	)
	require.Contains(t, summary, "Outputs Created**: 2")
	require.Contains(t, summary, "Total Output Lovelace**: 12000003")
	for _, id := range []string{"101", hex.EncodeToString(blockHash)} {
		summary = callTool(
			t,
			cs,
			"get_block",
			map[string]any{"hash_or_slot": id},
			false,
		)
		require.Contains(t, summary, hex.EncodeToString(blockHash))
		require.NotContains(t, summary, hex.EncodeToString(txHash))
	}
	callTool(
		t,
		cs,
		"get_block",
		map[string]any{"hash_or_slot": hex.EncodeToString(txHash)},
		true,
	)
	for _, args := range []map[string]any{{"epoch": 9}, {}} {
		summary = callTool(t, cs, "get_epoch_summary", args, false)
		require.Contains(t, summary, "bbbb")
		require.Contains(t, summary, "Recorded Blocks**: 2")
	}
	_, err = db.Exec("DROP TABLE utxo")
	require.NoError(t, err)
	callTool(
		t,
		cs,
		"get_transaction",
		map[string]any{"tx_hash": hex.EncodeToString(txHash)},
		true,
	)
}

func TestCardanoIdentifierParsing(t *testing.T) {
	t.Parallel()

	// 1. parseAssetName
	raw, hexStr := parseAssetName("HOSKY")
	assert.Equal(t, []byte("HOSKY"), raw)
	assert.Equal(t, "484f534b59", hexStr)

	rawHex, hexStr2 := parseAssetName("484f534b59")
	assert.Equal(t, []byte("HOSKY"), rawHex)
	assert.Equal(t, "484f534b59", hexStr2)

	rawEmpty, hexEmpty := parseAssetName("")
	assert.Nil(t, rawEmpty)
	assert.Equal(t, "", hexEmpty)

	// 2. parsePoolID
	_, err := parsePoolID("")
	assert.Error(t, err)

	_, err = parsePoolID("short")
	assert.Error(t, err)

	validHex := "c0000000000000000000000000000000000000000000000000000003"
	parsed, err := parsePoolID(validHex)
	require.NoError(t, err)
	assert.Equal(t, 28, len(parsed))

	// 3. parseDrepCredential
	drepEmpty, _, err := parseDrepCredential("")
	require.NoError(t, err)
	assert.Nil(t, drepEmpty)

	parsedDrep, _, err := parseDrepCredential(validHex)
	require.NoError(t, err)
	assert.Equal(t, 28, len(parsedDrep))

	_, _, err = parseDrepCredential("invalid-drep!")
	assert.Error(t, err)
}

func TestParseAddressOrCredential(t *testing.T) {
	t.Parallel()

	// Empty string
	_, _, err := parseAddressOrCredential("")
	assert.Error(t, err)

	// 56-hex payment credential
	hexCred := "5e68f6d9c3203ea691221f59a3988477cfaeda716e2b7bf60b216ae0"
	pk, sk, err := parseAddressOrCredential(hexCred)
	require.NoError(t, err)
	assert.Equal(t, 28, len(pk))
	assert.Nil(t, sk)

	// Valid real testnet address
	realAddr := "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
	pkReal, skReal, err := parseAddressOrCredential(realAddr)
	require.NoError(t, err)
	assert.Equal(t, 28, len(pkReal))
	assert.Equal(t, 28, len(skReal))

	// Invalid format
	_, _, err = parseAddressOrCredential("invalid_addr_format!")
	assert.Error(t, err)
}
