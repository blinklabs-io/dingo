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
	"context"
	"io"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

type evalTxLedgerStub struct {
	UtxorpcLedgerState

	redeemers map[lcommon.RedeemerKey]lcommon.ExUnits
}

func (s *evalTxLedgerStub) EvaluateTx(
	lcommon.Transaction,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	return 1, lcommon.ExUnits{Memory: 3, Steps: 3}, s.redeemers, nil
}

// The UTxO RPC v1alpha RedeemerPurpose enum has no value for a Dijkstra
// guarding redeemer; emitting Tag+1 for it would put an undefined enum value
// on the wire.
func TestEvalTxOmitsRedeemerPurposesOutsideSchema(t *testing.T) {
	t.Parallel()
	_, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	stub := &evalTxLedgerStub{
		redeemers: map[lcommon.RedeemerKey]lcommon.ExUnits{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Memory: 1,
				Steps:  1,
			},
			{Tag: lcommon.RedeemerTagGuarding, Index: 0}: {
				Memory: 2,
				Steps:  2,
			},
		},
	}
	srv := &submitServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: stub,
	})}

	out, err := srv.EvalTx(
		context.Background(),
		connect.NewRequest(&submit.EvalTxRequest{
			Tx: &submit.AnyChainTx{
				Type: &submit.AnyChainTx_Raw{Raw: txCbor},
			},
		}),
	)

	require.NoError(t, err)
	report := out.Msg.GetReport().GetCardano()
	require.Empty(t, report.GetErrors())
	require.Len(t, report.GetRedeemers(), 1)
	r := report.GetRedeemers()[0]
	require.Equal(t, cardano.RedeemerPurpose_REDEEMER_PURPOSE_SPEND, r.GetPurpose())
	require.Equal(t, uint64(1), r.GetExUnits().GetSteps())
}
