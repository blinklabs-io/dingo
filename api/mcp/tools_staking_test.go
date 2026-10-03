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
	"fmt"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPoolPerformance(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE reward_pool_output (
			epoch INTEGER NOT NULL,
			pool_key_hash BLOB NOT NULL,
			apparent_performance TEXT,
			optimal_reward TEXT,
			total_reward TEXT,
			leader_reward TEXT,
			member_reward_total TEXT
		);
		CREATE TABLE reward_pool_input (
			epoch INTEGER NOT NULL,
			pool_key_hash BLOB NOT NULL,
			pledge TEXT,
			cost TEXT,
			margin TEXT,
			delegated_stake TEXT,
			blocks_produced INTEGER
		);
	`)
	require.NoError(t, err)

	poolHashHex := "c0000000000000000000000000000000000000000000000000000003"
	poolHashBytes, _ := hex.DecodeString(poolHashHex)

	conv, err := bech32.ConvertBits(poolHashBytes, 8, 5, true)
	require.NoError(t, err)
	poolBech32, err := bech32.Encode("pool", conv)
	require.NoError(t, err)

	// Insert input and output
	_, err = db.Exec(`
		INSERT INTO reward_pool_output (epoch, pool_key_hash, apparent_performance, optimal_reward, total_reward, leader_reward, member_reward_total)
		VALUES (450, ?, '0.985', '13000000', '12500000', '2500000', '10000000')
	`, poolHashBytes)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO reward_pool_input (epoch, pool_key_hash, pledge, cost, margin, delegated_stake, blocks_produced)
		VALUES (450, ?, '50000000000', '340000000', '0.02', '15000000000000', 18)
	`, poolHashBytes)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
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

	// 1. Success with Bech32 pool ID
	resBech32, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": poolBech32},
	})
	require.NoError(t, err)
	assert.False(t, resBech32.IsError)
	bText := resBech32.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, bText, "450")
	assert.Contains(t, bText, "18")
	assert.Contains(t, bText, "0.985")
	assert.Contains(t, bText, "12500000")
	assert.Contains(t, bText, "340000000")
	assert.Contains(t, bText, "0.02")

	// 2. Success with 56-hex pool ID and specific epoch
	epochNum := 450
	resHex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": poolHashHex, "epoch": epochNum},
	})
	require.NoError(t, err)
	assert.False(t, resHex.IsError)
	hText := resHex.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, hText, "450")

	// 3. Pool not found
	unknownPoolHex := "d0000000000000000000000000000000000000000000000000000004"
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": unknownPoolHex},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"No historical reward performance records found",
	)

	// 4. Invalid pool ID format
	resInvalid, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": "invalid-pool"},
	})
	require.NoError(t, err)
	assert.True(t, resInvalid.IsError)
	assert.Contains(
		t,
		resInvalid.Content[0].(*mcp.TextContent).Text,
		"Error parsing pool ID",
	)

	// 5. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "nil-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	ctNil, stNil := mcp.NewInMemoryTransports()
	_, _ = serverNil.Connect(ctx, stNil, nil)
	csNil, _ := client.Connect(ctx, ctNil, nil)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": poolHashHex},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
}

func TestAccountAndDRepCredentialTypes(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	hash := bytes.Repeat([]byte{0x33}, 28)
	for tag := range 2 {
		_, err := db.Exec(
			"INSERT INTO account(staking_key, credential_tag, reward) VALUES (?, ?, ?)",
			hash,
			tag,
			fmt.Sprint(100+tag),
		)
		require.NoError(t, err)
		_, err = db.Exec(
			"INSERT INTO drep(credential, credential_tag, anchor_url, added_slot, last_activity_epoch, expiry_epoch, active) VALUES (?, ?, ?, 1, 2, 3, TRUE)",
			hash,
			tag,
			fmt.Sprintf("https://drep%d.example", tag),
		)
		require.NoError(t, err)
	}
	cs := newToolSession(t, db, 100)
	for tag := range 2 {
		addr, err := lcommon.NewAddressFromParts(
			uint8(lcommon.AddressTypeNoneKey+tag),
			0,
			nil,
			hash,
		)
		require.NoError(t, err)
		text := callTool(
			t,
			cs,
			"get_account",
			map[string]any{"stake_address_or_credential": addr.String()},
			false,
		)
		require.Contains(t, text, fmt.Sprint(100+tag))
		bits, err := bech32.ConvertBits(
			append([]byte{byte(0x22 + tag)}, hash...),
			8,
			5,
			true,
		)
		require.NoError(t, err)
		id, err := bech32.Encode("drep", bits)
		require.NoError(t, err)
		text = callTool(
			t,
			cs,
			"get_governance_state",
			map[string]any{"drep_credential": id},
			false,
		)
		require.Contains(t, text, fmt.Sprintf("https://drep%d.example", tag))
		require.NotContains(
			t,
			text,
			fmt.Sprintf("https://drep%d.example", 1-tag),
		)
	}
	for _, tc := range []struct {
		credentialType string
		reward         string
	}{
		{credentialType: "key", reward: "100"},
		{credentialType: "script", reward: "101"},
	} {
		text := callTool(
			t,
			cs,
			"get_account",
			map[string]any{
				"stake_address_or_credential": hex.EncodeToString(hash),
				"credential_type":             tc.credentialType,
			},
			false,
		)
		require.Contains(t, text, tc.reward)
	}
	text := callTool(t, cs, "get_account", map[string]any{
		"stake_address_or_credential": hex.EncodeToString(hash),
	}, true)
	require.Contains(t, text, "credential_type is required")
	keyAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		0,
		nil,
		hash,
	)
	require.NoError(t, err)
	text = callTool(t, cs, "get_account", map[string]any{
		"stake_address_or_credential": keyAddress.String(),
		"credential_type":             "script",
	}, true)
	require.Contains(t, text, "does not match")
	for _, header := range []byte{0x02, 0x12, 0x20, 0x24, 0x32} {
		bits, err := bech32.ConvertBits(
			append([]byte{header}, hash...),
			8,
			5,
			true,
		)
		require.NoError(t, err)
		id, err := bech32.Encode("drep", bits)
		require.NoError(t, err)
		_, _, err = parseDrepCredential(id)
		require.Error(t, err)
	}
}

func TestAccountZeroStakeCredential(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	hash := make([]byte, 28)
	cs := newToolSession(t, db, 100)
	for tag := range 2 {
		_, err := db.Exec(
			"INSERT INTO account(staking_key, credential_tag, reward) VALUES (?, ?, ?)",
			hash,
			tag,
			fmt.Sprint(100+tag),
		)
		require.NoError(t, err)
		addr, err := lcommon.NewAddressFromParts(
			uint8(lcommon.AddressTypeNoneKey+tag), 0, nil, hash,
		)
		require.NoError(t, err)
		text := callTool(t, cs, "get_account", map[string]any{
			"stake_address_or_credential": addr.String(),
		}, false)
		require.Contains(t, text, fmt.Sprint(100+tag))
		require.NotContains(t, text, fmt.Sprint(101-tag))
	}
}

func TestPoolPerformanceLovelaceAmounts(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	pool := bytes.Repeat([]byte{1}, 28)
	_, err := db.Exec(
		`INSERT INTO reward_pool_input(pool_key_hash,epoch,pledge,delegated_stake,cost,delegator_count,captured_slot,boundary_slot) VALUES (?,1,'1000001','2000002','3',1,1,1)`,
		pool,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO reward_pool_output(pool_key_hash,epoch,optimal_reward,total_reward,leader_reward,member_reward_total,owner_stake,undistributed,unspendable,captured_slot,boundary_slot) VALUES (?,1,'0','0','0','0','0','0','0',1,1)`,
		pool,
	)
	require.NoError(t, err)
	text := callTool(
		t,
		newToolSession(t, db, 100),
		"get_pool_performance",
		map[string]any{"pool_id": hex.EncodeToString(pool)},
		false,
	)
	require.Contains(t, text, "Delegated Stake (lovelace)")
	require.Contains(t, text, "Pledge (lovelace)")
	require.Contains(t, text, "2000002")
	require.Contains(t, text, "1000001")
	require.NotContains(t, text, "(ADA)")
}
