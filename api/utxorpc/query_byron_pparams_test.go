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

package utxorpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

// byronPParamsLedgerStub implements only what ReadParams reaches. The
// interface is embedded rather than implemented in full so that a method
// ReadParams should not be calling panics instead of returning a zero value.
//
// GetCurrentPParams returns nil, which is what LedgerState genuinely reports
// during a Byron prefix: Byron carries no protocol-parameter CBOR, so there is
// nothing to return and nothing Shelley-shaped may be substituted.
type byronPParamsLedgerStub struct {
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

func (s *byronPParamsLedgerStub) GetCurrentPParams() lcommon.ProtocolParameters {
	s.pparamsCalls++
	return nil
}

func (s *byronPParamsLedgerStub) Tip() ochainsync.Tip {
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
	stub := &byronPParamsLedgerStub{
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

// TestReadParams_ByronEraDoesNotSubstituteShelley guards the substitution
// prohibition directly: the error path must not manufacture a params message.
func TestReadParams_ByronEraDoesNotSubstituteShelley(t *testing.T) {
	srv := newByronQueryServer(t, &byronPParamsLedgerStub{})

	out, err := srv.ReadParams(
		context.Background(),
		connect.NewRequest(&query.ReadParamsRequest{}),
	)

	require.Error(t, err)
	require.Nil(t, out)
	// A caller that ignores the error must not find a usable Cardano params
	// block; the absence has to be unambiguous.
	if out != nil {
		assert.Nil(t, out.Msg.GetValues())
	}
}
