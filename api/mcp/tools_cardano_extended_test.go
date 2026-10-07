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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetProtocolParameters(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

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

	// 1. Call with nil ledger state
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_protocol_parameters",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, res.IsError)
	require.NotEmpty(t, res.Content)
	text := res.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "Protocol parameters are currently unavailable")
}

func TestDecodeAddress(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

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

	// 1. Shelley Base Address (Mainnet)
	resBase, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "decode_address",
		Arguments: map[string]any{
			"address": "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
		},
	})
	require.NoError(t, err)
	assert.False(t, resBase.IsError)
	baseText := resBase.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, baseText, "Shelley Base Address (Payment + Stake)")
	assert.Contains(t, baseText, "Mainnet (ID 1)")
	assert.Contains(t, baseText, "Public Key Hash (Ed25519 VKey)")
	assert.Contains(t, baseText, "Stake Public Key Hash (VKey Delegator)")

	// 2. Stake Reward Address (derived from base address)
	baseAddrObj, err := lcommon.NewAddress(
		"addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
	)
	require.NoError(t, err)
	stakeAddrStr := baseAddrObj.StakeAddress().String()

	resStake, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "decode_address",
		Arguments: map[string]any{
			"address": stakeAddrStr,
		},
	})
	require.NoError(t, err)
	assert.False(t, resStake.IsError)
	stakeText := resStake.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, stakeText, "Shelley Reward Address")

	// 3. Invalid Address
	resInvalid, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "decode_address",
		Arguments: map[string]any{
			"address": "not_an_address",
		},
	})
	require.NoError(t, err)
	assert.True(t, resInvalid.IsError)
}

func TestCalculateMinUtxo(t *testing.T) {
	t.Parallel()

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

	clTr, srvTr := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, srvTr, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clTr, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Basic ADA-only UTxO
	resBasic, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "calculate_min_utxo",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, resBasic.IsError)
	basicText := resBasic.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, basicText, "Minimum UTxO Lovelace Calculation")
	assert.Contains(t, basicText, "4310 Lovelace/byte")
	assert.Contains(t, basicText, "Required Minimum Deposit")

	// 2. Multi-asset + Inline Datum
	resComplex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "calculate_min_utxo",
		Arguments: map[string]any{
			"coins_per_utxo_byte": 4310,
			"assets_count":        3,
			"policies_count":      2,
			"inline_datum_hex":    "d8799f010203ff",
		},
	})
	require.NoError(t, err)
	assert.True(t, resComplex.IsError)
	complexText := resComplex.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, complexText, "provide output_cbor_hex")
}

func TestMinimumOutputSizingAndHistoricalParameters(t *testing.T) {
	t.Parallel()
	cs := newToolSession(t, nil, 100)
	text := callTool(t, cs, "calculate_min_utxo", map[string]any{}, false)
	require.Contains(t, text, "857690 Lovelace")
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		0,
		bytes.Repeat([]byte{1}, 28),
		nil,
	)
	require.NoError(t, err)
	output := babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1000000},
	}
	raw, err := cbor.Encode(output)
	require.NoError(t, err)
	want, err := babbage.MinCoinTxOut(
		&output,
		&babbage.BabbageProtocolParameters{AdaPerUtxoByte: 4310},
	)
	require.NoError(t, err)
	text = callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{"output_cbor_hex": hex.EncodeToString(raw)},
		false,
	)
	require.Contains(t, text, fmt.Sprintf("%d Lovelace", want))

	assets := lcommon.NewMultiAsset[*big.Int](
		map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
			lcommon.NewBlake2b224(bytes.Repeat([]byte{2}, 28)): {
				cbor.NewByteString(bytes.Repeat([]byte{3}, 32)): new(
					big.Int,
				).SetUint64(^uint64(0)),
			},
		},
	)
	output.OutputAmount.Assets = &assets
	raw, err = cbor.Encode(output)
	require.NoError(t, err)
	want, err = babbage.MinCoinTxOut(
		&output,
		&babbage.BabbageProtocolParameters{AdaPerUtxoByte: 4310},
	)
	require.NoError(t, err)
	text = callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{"output_cbor_hex": hex.EncodeToString(raw)},
		false,
	)
	require.Contains(t, text, fmt.Sprintf("%d Lovelace", want))
	callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{
			"output_cbor_hex":     hex.EncodeToString(raw),
			"coins_per_utxo_byte": ^uint64(0),
		},
		true,
	)
	callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{"assets_count": 1},
		true,
	)
	callTool(t, cs, "get_protocol_parameters", map[string]any{"epoch": 0}, true)
}

func TestProtocolParameterEraUnits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params lcommon.ProtocolParameters
		row    string
		absent string
	}{
		{
			"Shelley",
			&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 2,
				MinFeeA:       44,
				MinFeeB:       155381,
				KeyDeposit:    2000000,
				MinUtxoValue:  1000000,
			},
			"`min_utxo_value` (min UTxO cost) | 1000000 Lovelace |",
			"coins_per_utxo_byte",
		},
		{
			"Allegra",
			&allegra.AllegraProtocolParameters{
				ProtocolMajor: 3,
				MinFeeA:       44,
				MinFeeB:       155381,
				KeyDeposit:    2000000,
				MinUtxoValue:  1000000,
			},
			"`min_utxo_value` (min UTxO cost) | 1000000 Lovelace |",
			"coins_per_utxo_byte",
		},
		{
			"Mary",
			&mary.MaryProtocolParameters{
				ProtocolMajor: 4,
				MinFeeA:       44,
				MinFeeB:       155381,
				KeyDeposit:    2000000,
				MinUtxoValue:  1000000,
			},
			"`min_utxo_value` (min UTxO cost) | 1000000 Lovelace |",
			"coins_per_utxo_byte",
		},
		{
			"Alonzo",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor:  5,
				MinFeeA:        44,
				MinFeeB:        155381,
				KeyDeposit:     2000000,
				AdaPerUtxoByte: 34482,
			},
			"`coins_per_utxo_word` (min UTxO cost) | 34482 Lovelace/word |",
			"coins_per_utxo_byte",
		},
		{
			"Babbage",
			&babbage.BabbageProtocolParameters{
				ProtocolMajor:  8,
				MinFeeA:        44,
				MinFeeB:        155381,
				KeyDeposit:     2000000,
				AdaPerUtxoByte: 4310,
			},
			"`coins_per_utxo_byte` (min UTxO cost) | 4310 Lovelace/byte |",
			"coins_per_utxo_word",
		},
		{
			"Conway",
			&conway.ConwayProtocolParameters{
				MinFeeA:        44,
				MinFeeB:        155381,
				KeyDeposit:     2000000,
				AdaPerUtxoByte: 4310,
			},
			"`coins_per_utxo_byte` (min UTxO cost) | 4310 Lovelace/byte |",
			"coins_per_utxo_word",
		},
		{
			"Dijkstra",
			&dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					MinFeeA:        44,
					MinFeeB:        155381,
					KeyDeposit:     2000000,
					AdaPerUtxoByte: 4310,
				},
			},
			"`coins_per_utxo_byte` (min UTxO cost) | 4310 Lovelace/byte |",
			"coins_per_utxo_word",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text := formatProtocolParameters(
				tc.params,
				"current active",
				"preview",
			)
			require.Contains(t, text, "Era Name | **"+tc.name+"** |")
			require.Contains(t, text, tc.row)
			require.NotContains(t, text, tc.absent)
			require.Contains(
				t,
				text,
				"`min_fee_a` (linear coefficient) | 44 Lovelace/byte |",
			)
			require.Contains(
				t,
				text,
				"`min_fee_b` (constant base fee) | 155381 Lovelace (0.155 ADA) |",
			)
			require.Contains(
				t,
				text,
				"`key_deposit` (stake registration) | 2000000 Lovelace (2.0 ADA) |",
			)
		})
	}
}

func TestProtocolParameterExecutionAndGovernanceValues(t *testing.T) {
	t.Parallel()
	pp := conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
			Minor: 1,
		},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 14000000,
			Steps:  10000000000,
		},
		CollateralPercentage: 150, MaxCollateralInputs: 3,
		DRepDeposit: 500000000, GovActionDeposit: 100000000000,
		MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(15, 1)},
		CostModels:                 map[uint][]int64{2: {1}, 0: {1}, 1: {1}},
	}
	for _, params := range []lcommon.ProtocolParameters{&pp, &dijkstra.DijkstraProtocolParameters{ConwayProtocolParameters: pp}} {
		text := formatProtocolParameters(params, "current active", "preview")
		for _, row := range []string{
			"Protocol Version | Major 9, Minor 1 |",
			"`price_mem` (Memory unit price) | 0.057700 Lovelace/unit |",
			"`price_step` (CPU step price) | 0.00007210 Lovelace/step |",
			"`max_tx_ex_mem` (Tx Memory Budget) | 14000000 units |",
			"`max_tx_ex_steps` (Tx CPU Budget) | 10000000000 steps |",
			"`collateral_percentage` | 150% |",
			"`max_collateral_inputs` | 3 |",
			"`drep_deposit` | 500000000 Lovelace (500.0 ADA) |",
			"`gov_action_deposit` | 100000000000 Lovelace (100000.0 ADA) |",
			"`min_fee_ref_script_cost_per_byte` | 15.0000 Lovelace/byte |",
			"Active Cost Models | PlutusV1, PlutusV2, PlutusV3 |",
		} {
			require.Contains(t, text, row)
		}
	}
	for _, params := range []lcommon.ProtocolParameters{
		&alonzo.AlonzoProtocolParameters{
			ExecutionCosts:       pp.ExecutionCosts,
			MaxTxExUnits:         pp.MaxTxExUnits,
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
			CostModels: map[uint][]int64{
				0: {
					1,
				},
			},
		},
		&babbage.BabbageProtocolParameters{
			ExecutionCosts:       pp.ExecutionCosts,
			MaxTxExUnits:         pp.MaxTxExUnits,
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
			CostModels: map[uint][]int64{
				0: {
					1,
				},
			},
		},
	} {
		text := formatProtocolParameters(params, "current active", "preview")
		require.Contains(
			t,
			text,
			"`price_mem` (Memory unit price) | 0.057700 Lovelace/unit |",
		)
		require.Contains(
			t,
			text,
			"`price_step` (CPU step price) | 0.00007210 Lovelace/step |",
		)
		require.Contains(
			t,
			text,
			"`max_tx_ex_mem` (Tx Memory Budget) | 14000000 units |",
		)
		require.Contains(
			t,
			text,
			"`max_tx_ex_steps` (Tx CPU Budget) | 10000000000 steps |",
		)
		require.Contains(t, text, "`collateral_percentage` | 150% |")
		require.Contains(t, text, "`max_collateral_inputs` | 3 |")
		require.Contains(t, text, "Active Cost Models | PlutusV1 |")
		require.NotContains(t, text, "drep_deposit")
		require.NotContains(t, text, "min_fee_ref_script_cost_per_byte")
	}

	require.Zero(t, ratToFloat64(nil))
	require.Zero(t, ratToFloat64(&cbor.Rat{}))
	require.Nil(t, ratPointerPtr(nil))
	require.Nil(t, ratPointerPtr(&cbor.Rat{}))
}

func TestToolRequestTypes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, tool, args string }{
		{"string limit", "sqlite_query", `{"query":"SELECT 1","limit":"1"}`},
		{"fractional limit", "sqlite_query", `{"query":"SELECT 1","limit":1.5}`},
		{"overflowing limit", "sqlite_query", `{"query":"SELECT 1","limit":18446744073709551616}`},
		{"missing address", "decode_address", `{}`},
		{"null address", "decode_address", `{"address":null}`},
		{"numeric address", "decode_address", `{"address":123}`},
		{"negative epoch", "get_protocol_parameters", `{"epoch":-1}`},
		{"fractional epoch", "get_protocol_parameters", `{"epoch":0.5}`},
		{"overflowing epoch", "get_protocol_parameters", `{"epoch":18446744073709551616}`},
		{"negative index", "get_governance_proposal", `{"tx_hash":"` + strings.Repeat("ca", 32) + `","action_index":-1}`},
		{
			"overflowing index",
			"get_governance_proposal",
			`{"tx_hash":"` + strings.Repeat("ca",
				32) + `","action_index":4294967296}`,
		},
		{"fractional index", "get_governance_proposal", `{"tx_hash":"` + strings.Repeat("ca", 32) + `","action_index":0.5}`},
		{"index without hash", "get_governance_proposal", `{"action_index":0}`},
		{"numeric status", "get_governance_proposal", `{"status":1}`},
		{"unknown status", "get_governance_proposal", `{"status":"actvie"}`},
		{"negative unit cost", "calculate_min_utxo", `{"coins_per_utxo_byte":-1}`},
		{"fractional unit cost", "calculate_min_utxo", `{"coins_per_utxo_byte":0.5}`},
		{"overflowing unit cost", "calculate_min_utxo", `{"coins_per_utxo_byte":18446744073709551616}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, _ := newGovernanceProposalDB(t)
			cs := newToolSession(t, db, 100)
			result, err := cs.CallTool(
				t.Context(),
				&mcp.CallToolParams{
					Name:      tc.tool,
					Arguments: json.RawMessage(tc.args),
				},
			)
			require.True(
				t,
				err != nil || (result != nil && result.IsError),
				"invalid input accepted: %+v",
				result,
			)
		})
	}
}

func TestProtocolParameterEpochPresence(t *testing.T) {
	t.Parallel()
	cs := newToolSession(t, nil, 100)
	text := callTool(t, cs, "get_protocol_parameters", map[string]any{}, false)
	require.Contains(t, text, "currently unavailable")
	text = callTool(
		t,
		cs,
		"get_protocol_parameters",
		map[string]any{"epoch": uint64(0)},
		true,
	)
	require.Contains(
		t,
		text,
		"historical protocol parameters are not supported",
	)
	result, err := cs.CallTool(
		t.Context(),
		&mcp.CallToolParams{
			Name:      "get_protocol_parameters",
			Arguments: json.RawMessage(`{"epoch":null}`),
		},
	)
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.Contains(
		t,
		result.Content[0].(*mcp.TextContent).Text,
		"currently unavailable",
	)
}
