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
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

// With MaxEvalRequestsPerClient set, EvalTx is refused before transaction
// decoding once one remote host holds its per-client share of evaluation requests.
func TestEvalTxPerClientLimitIsResourceExhausted(t *testing.T) {
	t.Parallel()
	srv := &submitServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState:              &evalTxLedgerStub{},
		MaxEvalRequestsPerClient: 2,
	})}
	req := connect.NewRequest(&submit.EvalTxRequest{
		Tx: &submit.AnyChainTx{
			Type: &submit.AnyChainTx_Raw{Raw: []byte{0xff}},
		},
	})
	for range srv.utxorpc.config.MaxEvalRequestsPerClient {
		release, err := srv.utxorpc.admitEvalRequest(req.Peer())
		require.NoError(t, err)
		t.Cleanup(release)
	}
	_, err := srv.EvalTx(context.Background(), req)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}
