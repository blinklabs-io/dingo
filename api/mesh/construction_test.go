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

package mesh

import (
	"encoding/hex"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// --- /construction/derive ----------------------------------------------

func deriveRequest(pk *PublicKey) ConstructionDeriveRequest {
	return ConstructionDeriveRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		PublicKey: pk,
	}
}

func TestConstructionDerive(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	pub, _ := testKeyPair(t, 0x11)

	rec := postJSON(t, h, "/construction/derive", deriveRequest(
		&PublicKey{
			HexBytes:  hexString(pub),
			CurveType: Edwards25519,
		},
	))

	resp := decodeResponse[ConstructionDeriveResponse](t, rec)
	require.True(
		t,
		strings.HasPrefix(
			resp.AccountIdentifier.Address, "addr_test1",
		),
		"expected a testnet address, got %q",
		resp.AccountIdentifier.Address,
	)
	// The address must be the enterprise address for blake2b-224 of
	// the key, which is what a client will re-derive locally.
	keyHash := lcommon.Blake2b224Hash(pub)
	require.Equal(
		t,
		testAddress(
			t, lcommon.AddressTypeKeyNone, keyHash[:], nil,
		),
		resp.AccountIdentifier.Address,
	)
}

// TestConstructionDeriveMainnet asserts the address network follows the
// configured network magic rather than a compile-time default.
func TestConstructionDeriveMainnet(t *testing.T) {
	h := newTestHandler(
		t,
		newTestDeps(),
		func(cfg *ServerConfig) {
			cfg.NetworkMagic = mainnetMagic
			cfg.Network = "mainnet"
		},
	)
	pub, _ := testKeyPair(t, 0x12)

	rec := postJSON(t, h, "/construction/derive",
		ConstructionDeriveRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: &NetworkIdentifier{
					Blockchain: blockchain,
					Network:    "mainnet",
				},
			},
			PublicKey: &PublicKey{
				HexBytes:  hexString(pub),
				CurveType: Edwards25519,
			},
		},
	)

	resp := decodeResponse[ConstructionDeriveResponse](t, rec)
	require.True(
		t,
		strings.HasPrefix(
			resp.AccountIdentifier.Address, "addr1",
		),
		"expected a mainnet address, got %q",
		resp.AccountIdentifier.Address,
	)
}

func TestConstructionDeriveInvalidPublicKey(t *testing.T) {
	pub, _ := testKeyPair(t, 0x13)
	tests := map[string]*PublicKey{
		"missing public key": nil,
		"unsupported curve": {
			HexBytes:  hexString(pub),
			CurveType: "secp256k1",
		},
		"non-hex bytes": {
			HexBytes:  "zzzz",
			CurveType: Edwards25519,
		},
		"wrong key length": {
			HexBytes:  hexString(testKeyHash(0x01)),
			CurveType: Edwards25519,
		},
	}
	for name, pk := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(
				t, h, "/construction/derive",
				deriveRequest(pk),
			)

			requireMeshError(
				t, rec, ErrInvalidPublicKey,
				http.StatusBadRequest,
			)
		})
	}
}

// --- /construction/preprocess ------------------------------------------

func TestConstructionPreprocess(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x14), nil,
	)
	coinID := hexString(testHash(0xa1)) + ":0"

	rec := postJSON(t, h, "/construction/preprocess",
		ConstructionPreprocessRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
			Operations: []*Operation{
				{
					OperationIdentifier: &OperationIdentifier{},
					Type:                OpInput,
					Account: &AccountIdentifier{
						Address: addr,
					},
					CoinChange: &CoinChange{
						CoinIdentifier: &CoinIdentifier{
							Identifier: coinID,
						},
						CoinAction: CoinSpent,
					},
				},
				{
					OperationIdentifier: &OperationIdentifier{
						Index: 1,
					},
					Type: OpOutput,
					Account: &AccountIdentifier{
						Address: addr,
					},
				},
			},
		},
	)

	resp := decodeResponse[ConstructionPreprocessResponse](t, rec)
	require.Equal(
		t,
		[]any{coinID},
		resp.Options["input_refs"],
	)
	// Only input operations contribute required signing keys.
	require.Equal(
		t,
		[]*AccountIdentifier{{Address: addr}},
		resp.RequiredPublicKeys,
	)
}

func TestConstructionPreprocessNoOperations(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(t, h, "/construction/preprocess",
		ConstructionPreprocessRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
		},
	)

	resp := decodeResponse[ConstructionPreprocessResponse](t, rec)
	require.Nil(t, resp.Options["input_refs"])
	require.Empty(t, resp.RequiredPublicKeys)
}

// --- /construction/metadata --------------------------------------------

func metadataRequest() ConstructionMetadataRequest {
	return ConstructionMetadataRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	}
}

func TestConstructionMetadata(t *testing.T) {
	deps := newTestDeps()
	deps.ledger.pparams = testPParams(44, 155381)
	h := newTestHandler(t, deps)

	rec := postJSON(
		t, h, "/construction/metadata", metadataRequest(),
	)

	resp := decodeResponse[ConstructionMetadataResponse](t, rec)
	require.Equal(
		t, float64(44), resp.Metadata["min_fee_coefficient"],
	)
	require.Equal(
		t, float64(155381), resp.Metadata["min_fee_constant"],
	)
	require.Len(t, resp.SuggestedFee, 1)
	require.Equal(
		t, "168581", resp.SuggestedFee[0].Value,
	)
	require.Equal(t, "ADA", resp.SuggestedFee[0].Currency.Symbol)
}

// TestConstructionMetadataUnavailable covers a node that has not yet
// loaded protocol parameters: clients must get a retriable error rather
// than a zero fee they would sign into a transaction.
func TestConstructionMetadataUnavailable(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(
		t, h, "/construction/metadata", metadataRequest(),
	)

	got := requireMeshError(
		t, rec, ErrUnavailable, http.StatusServiceUnavailable,
	)
	require.True(t, got.Retriable)
}

// --- /construction/payloads --------------------------------------------

// payloadOps builds a valid input/output operation pair.
func payloadOps(t *testing.T, addr string) []*Operation {
	t.Helper()
	return []*Operation{
		{
			OperationIdentifier: &OperationIdentifier{Index: 0},
			Type:                OpInput,
			Account:             &AccountIdentifier{Address: addr},
			Amount: &Amount{
				Value:    "-2000000",
				Currency: adaCurrency(),
			},
			CoinChange: &CoinChange{
				CoinIdentifier: &CoinIdentifier{
					Identifier: hexString(
						testHash(0xb1),
					) + ":0",
				},
				CoinAction: CoinSpent,
			},
		},
		{
			OperationIdentifier: &OperationIdentifier{Index: 0},
			Type:                OpOutput,
			Account:             &AccountIdentifier{Address: addr},
			Amount: &Amount{
				Value:    "1830000",
				Currency: adaCurrency(),
			},
		},
	}
}

func payloadsRequest(
	ops []*Operation,
	metadata map[string]any,
	keys []*PublicKey,
) ConstructionPayloadsRequest {
	return ConstructionPayloadsRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		Operations: ops,
		Metadata:   metadata,
		PublicKeys: keys,
	}
}

func TestConstructionPayloads(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x15), nil,
	)

	rec := postJSON(t, h, "/construction/payloads", payloadsRequest(
		payloadOps(t, addr),
		map[string]any{"fee": "170000"},
		nil,
	))

	resp := decodeResponse[ConstructionPayloadsResponse](t, rec)
	require.NotEmpty(t, resp.UnsignedTransaction)
	require.Len(t, resp.Payloads, 1)
	require.Equal(t, Ed25519, resp.Payloads[0].SignatureType)
	require.Nil(t, resp.Payloads[0].AccountIdentifier)

	// The unsigned transaction must be a decodable Conway body whose
	// fee and outputs match what was requested.
	bodyBytes := mustDecodeHex(t, resp.UnsignedTransaction)
	var body conway.ConwayTransactionBody
	_, err := cbor.Decode(bodyBytes, &body)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(170000), body.Fee())
	require.Len(t, body.Outputs(), 1)
	require.Equal(
		t, addr, body.Outputs()[0].Address().String(),
	)
	require.Equal(
		t,
		big.NewInt(1830000),
		body.Outputs()[0].Amount(),
	)
	require.Len(t, body.Inputs(), 1)
}

// TestConstructionPayloadsPerSignerPayloads asserts one payload is
// emitted per unique signing key, with the derived account attached so
// the client knows which key to use.
func TestConstructionPayloadsPerSignerPayloads(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x16), nil,
	)
	pubA, _ := testKeyPair(t, 0x17)
	pubB, _ := testKeyPair(t, 0x18)

	rec := postJSON(t, h, "/construction/payloads", payloadsRequest(
		payloadOps(t, addr),
		map[string]any{"fee": "170000"},
		[]*PublicKey{
			{HexBytes: hexString(pubA), CurveType: Edwards25519},
			{HexBytes: hexString(pubB), CurveType: Edwards25519},
			// Duplicate keys must not produce duplicate payloads.
			{HexBytes: hexString(pubA), CurveType: Edwards25519},
		},
	))

	resp := decodeResponse[ConstructionPayloadsResponse](t, rec)
	require.Len(t, resp.Payloads, 2)
	seen := map[string]struct{}{}
	for _, p := range resp.Payloads {
		require.NotNil(t, p.AccountIdentifier)
		require.Equal(t, Ed25519, p.SignatureType)
		// Every payload signs the same body hash.
		require.Equal(
			t, resp.Payloads[0].HexBytes, p.HexBytes,
		)
		seen[p.AccountIdentifier.Address] = struct{}{}
	}
	require.Len(t, seen, 2)
}

func TestConstructionPayloadsTTL(t *testing.T) {
	tests := map[string]struct {
		ttl     any
		wantTTL uint64
	}{
		"string ttl": {ttl: "12345", wantTTL: 12345},
		"number ttl": {ttl: float64(6789), wantTTL: 6789},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())
			addr := testAddress(
				t, lcommon.AddressTypeKeyNone,
				testKeyHash(0x19), nil,
			)

			rec := postJSON(
				t, h, "/construction/payloads",
				payloadsRequest(
					payloadOps(t, addr),
					map[string]any{
						"fee": "170000",
						"ttl": tc.ttl,
					},
					nil,
				),
			)

			resp := decodeResponse[ConstructionPayloadsResponse](
				t, rec,
			)
			var body conway.ConwayTransactionBody
			_, err := cbor.Decode(
				mustDecodeHex(t, resp.UnsignedTransaction),
				&body,
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantTTL, body.TTL())
		})
	}
}

func TestConstructionPayloadsInvalidRequest(t *testing.T) {
	validCoin := hexString(testHash(0xb2)) + ":0"

	tests := map[string]struct {
		ops      func(addr string) []*Operation
		metadata map[string]any
		wantErr  *Error
	}{
		"missing fee metadata": {
			ops:      func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{},
			wantErr:  ErrInvalidRequest,
		},
		"non-numeric fee": {
			ops:      func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{"fee": "many"},
			wantErr:  ErrInvalidRequest,
		},
		"non-string fee": {
			ops:      func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{"fee": float64(170000)},
			wantErr:  ErrInvalidRequest,
		},
		"non-numeric ttl": {
			ops: func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{
				"fee": "170000", "ttl": "soon",
			},
			wantErr: ErrInvalidRequest,
		},
		"fractional ttl": {
			ops: func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{
				"fee": "170000", "ttl": 1.5,
			},
			wantErr: ErrInvalidRequest,
		},
		"negative ttl": {
			ops: func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{
				"fee": "170000", "ttl": float64(-1),
			},
			wantErr: ErrInvalidRequest,
		},
		"unsupported ttl type": {
			ops: func(a string) []*Operation { return payloadOpsFor(a, validCoin) },
			metadata: map[string]any{
				"fee": "170000", "ttl": true,
			},
			wantErr: ErrInvalidRequest,
		},
		"input without coin change": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[0].CoinChange = nil
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"coin id without index": {
			ops: func(a string) []*Operation {
				return payloadOpsFor(a, hexString(testHash(0xb3)))
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"coin id with non-hex hash": {
			ops: func(a string) []*Operation {
				return payloadOpsFor(a, "nothex:0")
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"coin id with short hash": {
			ops: func(a string) []*Operation {
				return payloadOpsFor(
					a, hexString(testKeyHash(0x01))+":0",
				)
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"coin id with negative index": {
			ops: func(a string) []*Operation {
				return payloadOpsFor(
					a, hexString(testHash(0xb4))+":-1",
				)
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"coin id with non-numeric index": {
			ops: func(a string) []*Operation {
				return payloadOpsFor(
					a, hexString(testHash(0xb5))+":x",
				)
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"output without account": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Account = nil
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"output without amount": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Amount = nil
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"output without operation identifier": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].OperationIdentifier = nil
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"output index out of range": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].OperationIdentifier.Index = 10001
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"duplicate output index": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				dup := *ops[1]
				return append(ops, &dup)
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"invalid output address": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Account.Address = "not-an-address"
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"non-ada output currency": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Amount.Currency = nativeAssetCurrency(
					hexString(testKeyHash(0xcd)), "6162",
				)
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"missing output currency": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Amount.Currency = nil
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"non-numeric output amount": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Amount.Value = "lots"
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
		"unsupported operation type": {
			ops: func(a string) []*Operation {
				ops := payloadOpsFor(a, validCoin)
				ops[1].Type = OpStakeDelegation
				return ops
			},
			metadata: map[string]any{"fee": "170000"},
			wantErr:  ErrInvalidRequest,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())
			addr := testAddress(
				t, lcommon.AddressTypeKeyNone,
				testKeyHash(0x1a), nil,
			)

			rec := postJSON(
				t, h, "/construction/payloads",
				payloadsRequest(
					tc.ops(addr), tc.metadata, nil,
				),
			)

			requireMeshError(
				t, rec, tc.wantErr, http.StatusBadRequest,
			)
		})
	}
}

// TestConstructionPayloadsInvalidPublicKey asserts a malformed signing
// key is rejected rather than producing a payload no client can sign.
func TestConstructionPayloadsInvalidPublicKey(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x1b), nil,
	)

	rec := postJSON(t, h, "/construction/payloads", payloadsRequest(
		payloadOps(t, addr),
		map[string]any{"fee": "170000"},
		[]*PublicKey{{HexBytes: "abcd", CurveType: Edwards25519}},
	))

	requireMeshError(
		t, rec, ErrInvalidPublicKey, http.StatusBadRequest,
	)
}

// payloadOpsFor builds an input/output operation pair for a given coin
// identifier, without a *testing.T so it can be used in table entries.
func payloadOpsFor(addr, coinID string) []*Operation {
	return []*Operation{
		{
			OperationIdentifier: &OperationIdentifier{Index: 0},
			Type:                OpInput,
			Account:             &AccountIdentifier{Address: addr},
			CoinChange: &CoinChange{
				CoinIdentifier: &CoinIdentifier{
					Identifier: coinID,
				},
				CoinAction: CoinSpent,
			},
		},
		{
			OperationIdentifier: &OperationIdentifier{Index: 0},
			Type:                OpOutput,
			Account:             &AccountIdentifier{Address: addr},
			Amount: &Amount{
				Value:    "1830000",
				Currency: adaCurrency(),
			},
		},
	}
}

// --- /construction/combine ---------------------------------------------

func combineRequest(
	unsigned string,
	sigs []*Signature,
) ConstructionCombineRequest {
	return ConstructionCombineRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		UnsignedTransaction: unsigned,
		Signatures:          sigs,
	}
}

func TestConstructionCombine(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x1c), nil,
	)
	unsigned := requestPayloads(t, h, addr)
	pub, _ := testKeyPair(t, 0x1d)

	rec := postJSON(t, h, "/construction/combine", combineRequest(
		unsigned,
		[]*Signature{
			{
				PublicKey: &PublicKey{
					HexBytes:  hexString(pub),
					CurveType: Edwards25519,
				},
				SignatureType: Ed25519,
				HexBytes:      hexString(make([]byte, 64)),
			},
		},
	))

	resp := decodeResponse[ConstructionCombineResponse](t, rec)
	// The result must be a decodable transaction carrying the witness.
	txBytes := mustDecodeHex(t, resp.SignedTransaction)
	txType, err := gledger.DetermineTransactionType(txBytes)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(txType, txBytes)
	require.NoError(t, err)
	require.NotNil(t, tx.Witnesses())
	require.Len(t, tx.Witnesses().Vkey(), 1)
	require.Equal(
		t, []byte(pub), tx.Witnesses().Vkey()[0].Vkey,
	)
}

func TestConstructionCombineInvalidRequest(t *testing.T) {
	pub, _ := testKeyPair(t, 0x1e)
	validSig := hexString(make([]byte, 64))

	// Each case receives the well-formed unsigned transaction built
	// inside its own subtest, so nothing is shared between cases.
	tests := map[string]struct {
		unsigned func(valid string) string
		sigs     []*Signature
		wantErr  *Error
	}{
		"non-hex unsigned transaction": {
			unsigned: func(string) string { return "zz" },
			wantErr:  ErrInvalidTransaction,
		},
		"signature without public key": {
			unsigned: func(valid string) string { return valid },
			sigs: []*Signature{
				{HexBytes: validSig},
			},
			wantErr: ErrInvalidPublicKey,
		},
		"non-hex public key": {
			unsigned: func(valid string) string { return valid },
			sigs: []*Signature{
				{
					PublicKey: &PublicKey{HexBytes: "zz"},
					HexBytes:  validSig,
				},
			},
			wantErr: ErrInvalidPublicKey,
		},
		"short public key": {
			unsigned: func(valid string) string { return valid },
			sigs: []*Signature{
				{
					PublicKey: &PublicKey{
						HexBytes: hexString(
							testKeyHash(0x01),
						),
					},
					HexBytes: validSig,
				},
			},
			wantErr: ErrInvalidPublicKey,
		},
		"non-hex signature": {
			unsigned: func(valid string) string { return valid },
			sigs: []*Signature{
				{
					PublicKey: &PublicKey{
						HexBytes: hexString(pub),
					},
					HexBytes: "zz",
				},
			},
			wantErr: ErrInvalidTransaction,
		},
		"short signature": {
			unsigned: func(valid string) string { return valid },
			sigs: []*Signature{
				{
					PublicKey: &PublicKey{
						HexBytes: hexString(pub),
					},
					HexBytes: hexString(make([]byte, 32)),
				},
			},
			wantErr: ErrInvalidTransaction,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())
			addr := testAddress(
				t, lcommon.AddressTypeKeyNone,
				testKeyHash(0x1f), nil,
			)
			validUnsigned := requestPayloads(t, h, addr)

			rec := postJSON(
				t, h, "/construction/combine",
				combineRequest(
					tc.unsigned(validUnsigned), tc.sigs,
				),
			)

			requireMeshError(
				t, rec, tc.wantErr, http.StatusBadRequest,
			)
		})
	}
}

// --- /construction/parse -----------------------------------------------

func parseRequest(
	tx string,
	signed bool,
) ConstructionParseRequest {
	return ConstructionParseRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		Transaction: tx,
		Signed:      signed,
	}
}

func TestConstructionParseSigned(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x20), nil,
	)
	txCbor, _ := testSimpleSignedTx(t, addr)

	rec := postJSON(t, h, "/construction/parse", parseRequest(
		hexString(txCbor), true,
	))

	resp := decodeResponse[ConstructionParseResponse](t, rec)
	require.Len(t, resp.Operations, 2)
	require.Equal(t, OpInput, resp.Operations[0].Type)
	require.Equal(t, OpOutput, resp.Operations[1].Type)
	require.Equal(t, addr, resp.Operations[1].Account.Address)
	// Signers are reported as the blake2b-224 hash of each witness key.
	require.Len(t, resp.AccountIdentifierSigners, 1)
	signerKey, _ := testKeyPair(t, testSignerSeed)
	keyHash := lcommon.Blake2b224Hash(signerKey)
	require.Equal(
		t,
		hexString(keyHash[:]),
		resp.AccountIdentifierSigners[0].Address,
	)
}

func TestConstructionParseUnsigned(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x22), nil,
	)
	unsigned := requestPayloads(t, h, addr)

	rec := postJSON(t, h, "/construction/parse", parseRequest(
		unsigned, false,
	))

	resp := decodeResponse[ConstructionParseResponse](t, rec)
	require.Len(t, resp.Operations, 2)
	require.Equal(t, OpInput, resp.Operations[0].Type)
	require.Equal(t, OpOutput, resp.Operations[1].Type)
	require.Equal(t, "1830000", resp.Operations[1].Amount.Value)
	// An unsigned body carries no witnesses, so no signers.
	require.Empty(t, resp.AccountIdentifierSigners)
}

// TestConstructionParseCertificates covers the certificate shapes the
// converter recognizes: each supported certificate becomes exactly one
// operation, and unsupported ones are dropped rather than mis-typed.
func TestConstructionParseCertificates(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x23), nil,
	)
	stakeCred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.CredentialHash(testKeyHash(0x24)),
	}
	bodyCbor := testTxBodyWithCerts(
		t,
		addr,
		[]lcommon.CertificateWrapper{
			{
				Type: uint(
					lcommon.CertificateTypeStakeRegistration,
				),
				Certificate: &lcommon.StakeRegistrationCertificate{
					CertType: uint(
						lcommon.CertificateTypeStakeRegistration,
					),
					StakeCredential: stakeCred,
				},
			},
			{
				// No Mesh operation type: this one must be
				// dropped without consuming an operation index.
				Type: uint(
					lcommon.CertificateTypeAuthCommitteeHot,
				),
				Certificate: &lcommon.AuthCommitteeHotCertificate{
					CertType: uint(
						lcommon.CertificateTypeAuthCommitteeHot,
					),
					ColdCredential: stakeCred,
					HotCredential:  stakeCred,
				},
			},
			{
				Type: uint(
					lcommon.CertificateTypeStakeDelegation,
				),
				Certificate: &lcommon.StakeDelegationCertificate{
					CertType: uint(
						lcommon.CertificateTypeStakeDelegation,
					),
					StakeCredential: &stakeCred,
					PoolKeyHash: lcommon.PoolKeyHash(
						testKeyHash(0x25),
					),
				},
			},
		},
	)
	txCbor := testSignedTx(t, bodyCbor, nil)

	// Guard the fixture: the dropped certificate must survive the
	// encode/decode round trip and reach the converter, otherwise the
	// assertion below would pass for the wrong reason.
	decoded, meshErr := decodeTxCbor(hexString(txCbor))
	require.Nil(t, meshErr)
	require.NotNil(t, decoded)
	require.Len(t, decoded.Certificates(), 3)

	rec := postJSON(t, h, "/construction/parse", parseRequest(
		hexString(txCbor), true,
	))

	resp := decodeResponse[ConstructionParseResponse](t, rec)
	types := make([]string, 0, len(resp.Operations))
	for _, op := range resp.Operations {
		types = append(types, op.Type)
	}
	require.Equal(
		t,
		[]string{
			OpInput,
			OpOutput,
			OpStakeKeyRegistration,
			OpStakeDelegation,
		},
		types,
	)
	// Operation indices stay contiguous across sections.
	for i, op := range resp.Operations {
		require.Equal(
			t, int64(i), op.OperationIdentifier.Index,
		)
	}
}

func TestConstructionParseInvalid(t *testing.T) {
	tests := map[string]struct {
		tx     string
		signed bool
	}{
		"signed non-hex":     {tx: "zz", signed: true},
		"signed garbage":     {tx: "ffffff", signed: true},
		"unsigned non-hex":   {tx: "zz", signed: false},
		"unsigned garbage":   {tx: "ffffff", signed: false},
		"signed empty":       {tx: "", signed: true},
		"unsigned truncated": {tx: "a1", signed: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(
				t, h, "/construction/parse",
				parseRequest(tc.tx, tc.signed),
			)

			requireMeshError(
				t, rec, ErrInvalidTransaction,
				http.StatusBadRequest,
			)
		})
	}
}

// --- /construction/hash ------------------------------------------------

func TestConstructionHash(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x26), nil,
	)
	txCbor, tx := testSimpleSignedTx(t, addr)

	rec := postJSON(t, h, "/construction/hash",
		ConstructionHashRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
			SignedTransaction: hexString(txCbor),
		},
	)

	resp := decodeResponse[ConstructionHashResponse](t, rec)
	require.Equal(
		t,
		tx.Hash().String(),
		resp.TransactionIdentifier.Hash,
	)
}

func TestConstructionHashInvalid(t *testing.T) {
	for name, tx := range map[string]string{
		"non-hex": "zz",
		"garbage": "ffffff",
		"empty":   "",
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(t, h, "/construction/hash",
				ConstructionHashRequest{
					networkIdentifierField: networkIdentifierField{
						NetworkIdentifier: testNetworkID(),
					},
					SignedTransaction: tx,
				},
			)

			requireMeshError(
				t, rec, ErrInvalidTransaction,
				http.StatusBadRequest,
			)
		})
	}
}

// --- /construction/submit ----------------------------------------------

func submitRequest(tx string) ConstructionSubmitRequest {
	return ConstructionSubmitRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		SignedTransaction: tx,
	}
}

func TestConstructionSubmit(t *testing.T) {
	deps := newTestDeps()
	h := newTestHandler(t, deps)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x27), nil,
	)
	txCbor, tx := testSimpleSignedTx(t, addr)

	rec := postJSON(
		t, h, "/construction/submit",
		submitRequest(hexString(txCbor)),
	)

	resp := decodeResponse[ConstructionSubmitResponse](t, rec)
	require.Equal(
		t, tx.Hash().String(), resp.TransactionIdentifier.Hash,
	)
	// The exact submitted bytes must reach the mempool unmodified.
	submissions := deps.mempool.submissions()
	require.Len(t, submissions, 1)
	require.Equal(
		t, uint(gledger.TxTypeConway), submissions[0].txType,
	)
	require.Equal(t, txCbor, submissions[0].txBytes)
}

// TestConstructionSubmitRejected covers a mempool that refuses the
// transaction: the client must get the retriable submit-failed error
// with the underlying reason, not a success.
func TestConstructionSubmitRejected(t *testing.T) {
	deps := newTestDeps()
	deps.mempool.addErr = errors.New("fee too small")
	h := newTestHandler(t, deps)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x28), nil,
	)
	txCbor, _ := testSimpleSignedTx(t, addr)

	rec := postJSON(
		t, h, "/construction/submit",
		submitRequest(hexString(txCbor)),
	)

	got := requireMeshError(
		t, rec, ErrSubmitFailed, http.StatusBadRequest,
	)
	require.True(t, got.Retriable)
	require.Contains(t, got.Details["error"], "fee too small")
}

func TestConstructionSubmitInvalid(t *testing.T) {
	for name, tx := range map[string]string{
		"non-hex": "zz",
		"garbage": "ffffff",
		"empty":   "",
	} {
		t.Run(name, func(t *testing.T) {
			deps := newTestDeps()
			h := newTestHandler(t, deps)

			rec := postJSON(
				t, h, "/construction/submit",
				submitRequest(tx),
			)

			requireMeshError(
				t, rec, ErrInvalidTransaction,
				http.StatusBadRequest,
			)
			require.Empty(t, deps.mempool.submissions())
		})
	}
}

// --- round trip ---------------------------------------------------------

// TestConstructionRoundTrip walks the full Mesh construction flow and
// asserts the stages agree: the transaction produced by combine parses
// back to the operations that were requested, and hash and submit report
// the same transaction identifier.
func TestConstructionRoundTrip(t *testing.T) {
	deps := newTestDeps()
	deps.ledger.pparams = testPParams(44, 155381)
	h := newTestHandler(t, deps)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x29), nil,
	)
	pub, priv := testKeyPair(t, 0x2a)
	ops := payloadOps(t, addr)

	// preprocess -> the options a client feeds into metadata.
	preRec := postJSON(t, h, "/construction/preprocess",
		ConstructionPreprocessRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
			Operations: ops,
		},
	)
	pre := decodeResponse[ConstructionPreprocessResponse](t, preRec)
	require.NotEmpty(t, pre.RequiredPublicKeys)

	// metadata -> suggested fee used for the body.
	metaRec := postJSON(t, h, "/construction/metadata",
		ConstructionMetadataRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
			Options: pre.Options,
		},
	)
	meta := decodeResponse[ConstructionMetadataResponse](t, metaRec)
	fee := meta.SuggestedFee[0].Value

	// payloads -> unsigned body plus the payload to sign.
	payRec := postJSON(t, h, "/construction/payloads",
		payloadsRequest(
			ops,
			map[string]any{"fee": fee},
			[]*PublicKey{
				{
					HexBytes:  hexString(pub),
					CurveType: Edwards25519,
				},
			},
		),
	)
	pay := decodeResponse[ConstructionPayloadsResponse](t, payRec)
	require.Len(t, pay.Payloads, 1)

	// combine -> signed transaction.
	sig := signPayload(t, priv, pay.Payloads[0].HexBytes)
	comRec := postJSON(t, h, "/construction/combine", combineRequest(
		pay.UnsignedTransaction,
		[]*Signature{
			{
				SigningPayload: pay.Payloads[0],
				PublicKey: &PublicKey{
					HexBytes:  hexString(pub),
					CurveType: Edwards25519,
				},
				SignatureType: Ed25519,
				HexBytes:      hexString(sig),
			},
		},
	))
	com := decodeResponse[ConstructionCombineResponse](t, comRec)

	// parse(signed) -> the same operations, plus the signer.
	parseRec := postJSON(t, h, "/construction/parse", parseRequest(
		com.SignedTransaction, true,
	))
	parsed := decodeResponse[ConstructionParseResponse](t, parseRec)
	require.Len(t, parsed.Operations, len(ops))
	require.Equal(t, OpInput, parsed.Operations[0].Type)
	require.Equal(
		t,
		ops[0].CoinChange.CoinIdentifier.Identifier,
		parsed.Operations[0].CoinChange.CoinIdentifier.Identifier,
	)
	require.Equal(t, OpOutput, parsed.Operations[1].Type)
	require.Equal(
		t, addr, parsed.Operations[1].Account.Address,
	)
	require.Equal(
		t,
		ops[1].Amount.Value,
		parsed.Operations[1].Amount.Value,
	)
	keyHash := lcommon.Blake2b224Hash(pub)
	require.Equal(
		t,
		[]*AccountIdentifier{
			{Address: hexString(keyHash[:])},
		},
		parsed.AccountIdentifierSigners,
	)

	// hash and submit must agree on the transaction identifier.
	hashRec := postJSON(t, h, "/construction/hash",
		ConstructionHashRequest{
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
			SignedTransaction: com.SignedTransaction,
		},
	)
	hashed := decodeResponse[ConstructionHashResponse](t, hashRec)

	subRec := postJSON(
		t, h, "/construction/submit",
		submitRequest(com.SignedTransaction),
	)
	submitted := decodeResponse[ConstructionSubmitResponse](t, subRec)

	require.Equal(
		t,
		hashed.TransactionIdentifier.Hash,
		submitted.TransactionIdentifier.Hash,
	)
	require.Len(t, deps.mempool.submissions(), 1)
	require.Equal(
		t,
		mustDecodeHex(t, com.SignedTransaction),
		deps.mempool.submissions()[0].txBytes,
	)
}

// requestPayloads runs /construction/payloads and returns the unsigned
// transaction, for tests that need a well-formed body to work from.
func requestPayloads(
	t *testing.T,
	h http.Handler,
	addr string,
) string {
	t.Helper()
	rec := postJSON(t, h, "/construction/payloads", payloadsRequest(
		payloadOps(t, addr),
		map[string]any{"fee": "170000"},
		nil,
	))
	resp := decodeResponse[ConstructionPayloadsResponse](t, rec)
	return resp.UnsignedTransaction
}

// signPayload signs a hex-encoded signing payload with priv.
func signPayload(
	t *testing.T,
	priv []byte,
	payloadHex string,
) []byte {
	t.Helper()
	payload, err := hex.DecodeString(payloadHex)
	require.NoError(t, err)
	return ed25519Sign(priv, payload)
}

// TestConstructionParseNativeAssetOutputs covers multi-asset outputs in
// the parse path: each asset in an output becomes its own operation
// with a sub-coin identifier, alongside the output's ADA operation.
func TestConstructionParseNativeAssetOutputs(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x2b), nil,
	)
	policy := lcommon.Blake2b224(testKeyHash(0xce))
	name := cbor.NewByteString([]byte("tok"))
	assets := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policy: {name: big.NewInt(12)},
		},
	)
	output := testOutput(t, addr, 1_000_000)
	output.OutputAmount.Assets = &assets
	bodyCbor := testTxBody(
		t,
		[]shelley.ShelleyTransactionInput{
			shelley.NewShelleyTransactionInput(
				hexString(testHash(0xd3)), 0,
			),
		},
		[]babbage.BabbageTransactionOutput{output},
		170_000,
	)
	txCbor := testSignedTx(t, bodyCbor, nil)

	rec := postJSON(t, h, "/construction/parse", parseRequest(
		hexString(txCbor), true,
	))

	resp := decodeResponse[ConstructionParseResponse](t, rec)
	require.Len(t, resp.Operations, 3)
	require.Equal(t, OpOutput, resp.Operations[1].Type)
	require.Equal(t, "1000000", resp.Operations[1].Amount.Value)
	assetOp := resp.Operations[2]
	require.Equal(t, OpOutput, assetOp.Type)
	require.Equal(t, "12", assetOp.Amount.Value)
	require.Equal(
		t,
		hexString([]byte("tok")),
		assetOp.Amount.Currency.Symbol,
	)
	require.Equal(
		t,
		hexString(policy[:]),
		assetOp.Amount.Currency.Metadata["policyId"],
	)
	require.Equal(
		t,
		resp.Operations[1].CoinChange.CoinIdentifier.Identifier+
			":"+hexString(policy[:])+":"+
			hexString([]byte("tok")),
		assetOp.CoinChange.CoinIdentifier.Identifier,
	)
	require.Equal(t, int64(2), assetOp.OperationIdentifier.Index)
}

// TestConstructionMetadataConversionFailure covers protocol parameters
// that cannot be converted for the response: the endpoint must report a
// server error rather than emitting a zero suggested fee.
func TestConstructionMetadataConversionFailure(t *testing.T) {
	deps := newTestDeps()
	// A nil A0 makes the utxorpc conversion fail.
	pparams := testPParams(44, 155381)
	pparams.A0 = nil
	deps.ledger.pparams = pparams
	h := newTestHandler(t, deps)

	rec := postJSON(
		t, h, "/construction/metadata", metadataRequest(),
	)

	requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
}
