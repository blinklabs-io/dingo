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
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

type busySubmissionMempool struct {
	adds int
}

func (m *busySubmissionMempool) AddTransaction(
	context.Context,
	uint,
	[]byte,
) error {
	m.adds++
	return nil
}

func (m *busySubmissionMempool) TransactionsBounded(
	int,
	int64,
) ([]mempool.MempoolTransaction, int) {
	return nil, 0
}

func (m *busySubmissionMempool) AdmitSubmission(
	ctx context.Context,
) (context.Context, func(), error) {
	return ctx, nil, mempool.ErrSubmissionBusy
}

// A full submission gate answers ResourceExhausted before the request's
// transaction bytes are examined.
func TestSubmitTxBusyIsResourceExhausted(t *testing.T) {
	t.Parallel()
	pool := &busySubmissionMempool{}
	srv := &submitServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Mempool: pool,
	})}
	_, err := srv.SubmitTx(
		context.Background(),
		connect.NewRequest(&submit.SubmitTxRequest{
			Tx: &submit.AnyChainTx{
				Type: &submit.AnyChainTx_Raw{Raw: []byte{0xff}},
			},
		}),
	)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	require.ErrorIs(t, err, mempool.ErrSubmissionBusy)
	require.Zero(t, pool.adds)
}

// EvalTx is refused before transaction decoding once one remote host holds
// its per-client share of evaluation requests.
func TestEvalTxPerClientLimitIsResourceExhausted(t *testing.T) {
	t.Parallel()
	srv := &submitServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: &evalTxLedgerStub{},
	})}
	req := connect.NewRequest(&submit.EvalTxRequest{
		Tx: &submit.AnyChainTx{
			Type: &submit.AnyChainTx_Raw{Raw: []byte{0xff}},
		},
	})
	for range DefaultMaxEvalRequestsPerClient {
		release, err := srv.utxorpc.admitEvalRequest(req.Peer())
		require.NoError(t, err)
		t.Cleanup(release)
	}
	_, err := srv.EvalTx(context.Background(), req)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}
