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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utxorpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/big"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpcCardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

// byronLedgerStub implements only what ReadParams reaches. The
// interface is embedded rather than implemented in full so that a method
// ReadParams should not be calling panics instead of returning a zero value.
//
// GetCurrentPParams returns nil, which is what LedgerState genuinely reports
// during a Byron prefix: Byron carries no protocol-parameter CBOR, so there is
// nothing to return and nothing Shelley-shaped may be substituted.
type byronLedgerStub struct {
	UtxorpcLedgerState

	tip ochainsync.Tip
	// pparamsCalls proves ReadParams consults the ledger rather than
	// answering from a cached or fabricated value.
	pparamsCalls int
	// tipCalls proves the Byron path short-circuits before the tip lookup;
	// there is no useful ledger tip to report alongside an absent parameter
	// set, and the lookup is not free.
	tipCalls int
}

func (s *byronLedgerStub) GetCurrentPParams() lcommon.ProtocolParameters {
	s.pparamsCalls++
	return nil
}

func (s *byronLedgerStub) GetCurrentPParamsForReporting() lcommon.ProtocolParameters {
	s.pparamsCalls++
	return nil
}

func (s *byronLedgerStub) Tip() ochainsync.Tip {
	s.tipCalls++
	return s.tip
}

func newByronQueryServer(
	t *testing.T,
	ls UtxorpcLedgerState,
) *queryServiceServer {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	return &queryServiceServer{utxorpc: u}
}

// TestReadParams_ByronEraFailedPrecondition locks in the defined Byron-era
// behavior for utxorpc.v1alpha.query.QueryService/ReadParams. A Byron prefix
// is an expected state during from-genesis synchronization, not a server
// fault, so the reply must carry a precondition code the caller can branch on
// rather than an untyped error that connect reports as CodeUnknown.
//
// v1beta ReadParams is served by rewriting the path onto this same alpha
// handler (see betaVersionedQueryHandler), so this covers both versions.
func TestReadParams_ByronEraFailedPrecondition(t *testing.T) {
	stub := &byronLedgerStub{
		tip: ochainsync.Tip{
			Point: ocommon.NewPoint(42, []byte{0xab, 0xcd}),
		},
	}
	srv := newByronQueryServer(t, stub)

	out, err := srv.ReadParams(
		context.Background(),
		connect.NewRequest(&query.ReadParamsRequest{}),
	)

	require.Error(t, err)
	require.Nil(t, out, "no partial response alongside an error")
	assert.Equal(
		t,
		connect.CodeFailedPrecondition,
		connect.CodeOf(err),
		"Byron-era unavailability is a precondition, not an unknown error",
	)
	assert.ErrorIs(t, err, ErrByronProtocolParams)
	assert.Equal(t, 1, stub.pparamsCalls)
	assert.Zero(
		t,
		stub.tipCalls,
		"Byron path should short-circuit before the tip lookup",
	)
}

// TestReadParams_ShelleyStateUnaffected keeps the Byron guard from
// over-triggering. The nil check sits ahead of every other step in the
// handler, so a state that does hold protocol parameters must still take the
// normal path and answer with them.
func TestReadParams_ShelleyStateUnaffected(t *testing.T) {
	stub := &shelleyLedgerStub{
		byronLedgerStub: byronLedgerStub{
			tip: ochainsync.Tip{
				Point: ocommon.NewPoint(42, []byte{0xab, 0xcd}),
			},
		},
		pparams: testShelleyPParams(),
	}
	srv := newByronQueryServer(t, stub)

	out, err := srv.ReadParams(
		context.Background(),
		connect.NewRequest(&query.ReadParamsRequest{}),
	)

	require.NoError(t, err)
	require.NotNil(t, out)
	require.NotNil(t, out.Msg.GetValues())
	require.NotNil(t, out.Msg.GetValues().GetCardano())
	assert.Equal(
		t,
		int64(44),
		out.Msg.GetValues().GetCardano().
			GetMinFeeCoefficient().GetInt(),
	)
	assert.Equal(t, uint64(42), out.Msg.GetLedgerTip().GetSlot())
}

// shelleyLedgerStub answers with real protocol parameters, overriding
// the Byron stub's nil.
type shelleyLedgerStub struct {
	byronLedgerStub

	pparams lcommon.ProtocolParameters
}

func (s *shelleyLedgerStub) GetCurrentPParams() lcommon.ProtocolParameters {
	s.pparamsCalls++
	return s.pparams
}

func (s *shelleyLedgerStub) GetCurrentPParamsForReporting() lcommon.ProtocolParameters {
	s.pparamsCalls++
	return s.pparams
}

func (s *shelleyLedgerStub) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	// Force the height fallback rather than carrying a block fixture; the
	// tip slot and hash are what this test checks.
	return models.Block{}, errors.New("no block")
}

func testShelleyPParams() *shelley.ShelleyProtocolParameters {
	rat := func(num, denom int64) *cbor.Rat {
		return &cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	return &shelley.ShelleyProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 rat(3, 10),
		Rho:                rat(3, 1000),
		Tau:                rat(2, 10),
	}
}

func TestCaip2FromNetworkMagic_KnownNetworks(t *testing.T) {
	mainnet, ok := ouroboros.NetworkByName("mainnet")
	require.True(t, ok, "expected mainnet network to exist")
	preprod, ok := ouroboros.NetworkByName("preprod")
	require.True(t, ok, "expected preprod network to exist")
	preview, ok := ouroboros.NetworkByName("preview")
	require.True(t, ok, "expected preview network to exist")

	tests := []struct {
		name  string
		magic uint32
		caip2 string
	}{
		{
			name:  "mainnet",
			magic: mainnet.NetworkMagic,
			caip2: "cardano:mainnet",
		},
		{
			name:  "preprod",
			magic: preprod.NetworkMagic,
			caip2: "cardano:preprod",
		},
		{
			name:  "preview",
			magic: preview.NetworkMagic,
			caip2: "cardano:preview",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := caip2FromNetworkMagic(tc.magic)
			require.Equal(t, tc.caip2, got)
		})
	}
}

func TestCaip2FromNetworkMagic_UnknownNetwork(t *testing.T) {
	const devnetMagic uint32 = 42
	got := caip2FromNetworkMagic(devnetMagic)
	require.Equal(t, "cardano:devnet", got)
}

func TestCaip2FromNetworkMagic_CustomNetwork(t *testing.T) {
	const customMagic uint32 = math.MaxUint32
	got := caip2FromNetworkMagic(customMagic)
	require.Equal(t, "cardano:4294967295", got)
}

func TestEffectiveSearchUtxosMaxItems(t *testing.T) {
	require.Equal(t, int32(100), effectiveSearchUtxosMaxItems(100, 10_000))
	require.Equal(t, int32(10_000), effectiveSearchUtxosMaxItems(0, 10_000))
}

func TestParseSearchUtxosStartToken_Valid(t *testing.T) {
	t.Parallel()
	txId := bytes.Repeat([]byte{0xab}, 32)
	cur, err := parseSearchUtxosStartToken(
		"1:2:3:abababababababababababababababababababababababababababababababab",
	)
	require.NoError(t, err)
	require.NotNil(t, cur)
	require.Equal(t, uint64(1), cur.Slot)
	require.Equal(t, uint32(2), cur.BlockIndex)
	require.Equal(t, uint32(3), cur.OutputIdx)
	require.Equal(t, txId, cur.TxId)
}

func TestParseSearchUtxosStartToken_NotFourParts(t *testing.T) {
	t.Parallel()
	_, err := parseSearchUtxosStartToken("1:2:3")
	require.Error(t, err)
}

func TestParseSearchUtxosStartToken_InvalidTxId(t *testing.T) {
	t.Parallel()
	_, err := parseSearchUtxosStartToken("10:20:30:42")
	require.Error(t, err)
}

func TestSearchUtxosMatchAllAddresses(t *testing.T) {
	t.Parallel()
	policy := []byte{0xaa, 0xbb}
	assetOnly := &query.UtxoPredicate{
		Match: &query.AnyUtxoPattern{
			UtxoPattern: &query.AnyUtxoPattern_Cardano{
				Cardano: &utxorpcCardano.TxOutputPattern{
					Asset: &utxorpcCardano.AssetPattern{
						PolicyId: policy,
					},
				},
			},
		},
	}
	addr := &utxorpcCardano.AddressPattern{ExactAddress: []byte{0x01}}
	asset := &utxorpcCardano.AssetPattern{PolicyId: policy}
	addrAndAsset := &query.UtxoPredicate{
		Match: &query.AnyUtxoPattern{
			UtxoPattern: &query.AnyUtxoPattern_Cardano{
				Cardano: &utxorpcCardano.TxOutputPattern{
					Address: addr,
					Asset:   asset,
				},
			},
		},
	}

	apAsset, assetP := extractSearchPredicatePatterns(assetOnly)
	require.Nil(t, apAsset)
	require.NotNil(t, assetP)
	require.True(t, searchUtxosMatchAllAddresses(assetOnly, apAsset, assetP))

	apBoth, assetBoth := extractSearchPredicatePatterns(addrAndAsset)
	require.NotNil(t, apBoth)
	require.False(
		t,
		searchUtxosMatchAllAddresses(addrAndAsset, apBoth, assetBoth),
	)

	require.True(t, searchUtxosMatchAllAddresses(nil, nil, nil))
}

func TestSearchUtxoAddressPatternsPreservesMatchIntent(t *testing.T) {
	payment := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	stake := bytes.Repeat([]byte{0xcd}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		payment,
		stake,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)

	got, err := searchUtxoAddressPatterns(&utxorpcCardano.AddressPattern{
		ExactAddress:   addrBytes,
		PaymentPart:    payment,
		DelegationPart: stake,
	})
	require.NoError(t, err)
	require.Equal(t, []models.UtxoAddressPattern{{
		ExactAddress:   addrBytes,
		PaymentPart:    payment,
		DelegationPart: stake,
	}}, got)
}

func TestSearchUtxoAddressPatternsRejectsInvalidCredentialLength(t *testing.T) {
	tests := []struct {
		name    string
		pattern *utxorpcCardano.AddressPattern
		message string
	}{
		{
			name: "exact address",
			pattern: &utxorpcCardano.AddressPattern{
				ExactAddress: []byte{0x01},
			},
			message: "failed to decode exact address",
		},
		{
			name: "payment part",
			pattern: &utxorpcCardano.AddressPattern{
				PaymentPart: []byte{0x01},
			},
			message: "invalid payment part length",
		},
		{
			name: "delegation part",
			pattern: &utxorpcCardano.AddressPattern{
				DelegationPart: []byte{0x01},
			},
			message: "invalid delegation part length",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := searchUtxoAddressPatterns(tc.pattern)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}
}

func TestExtractSearchPredicatePatterns_NilPredicate(t *testing.T) {
	addressPattern, assetPattern := extractSearchPredicatePatterns(nil)
	require.Nil(t, addressPattern)
	require.Nil(t, assetPattern)
}

func TestExtractSearchPredicatePatterns_WithCardanoMatch(t *testing.T) {
	address := &utxorpcCardano.AddressPattern{
		ExactAddress: []byte{0x01, 0x02},
	}
	asset := &utxorpcCardano.AssetPattern{
		PolicyId:  []byte{0xaa},
		AssetName: []byte{0xbb},
	}
	predicate := &query.UtxoPredicate{
		Match: &query.AnyUtxoPattern{
			UtxoPattern: &query.AnyUtxoPattern_Cardano{
				Cardano: &utxorpcCardano.TxOutputPattern{
					Address: address,
					Asset:   asset,
				},
			},
		},
	}

	addressPattern, assetPattern := extractSearchPredicatePatterns(predicate)
	require.Equal(t, address, addressPattern)
	require.Equal(t, asset, assetPattern)
}
