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
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

type txLookupCountingLedger struct {
	UtxorpcLedgerState
	lookups atomic.Int64
}

func (l *txLookupCountingLedger) TransactionByHash(
	context.Context,
	[]byte,
) (*models.Transaction, error) {
	l.lookups.Add(1)
	return nil, nil
}

func TestReadTxTakesABulkSlot(t *testing.T) {
	t.Parallel()
	ls := &txLookupCountingLedger{}
	u := NewUtxorpc(UtxorpcConfig{
		Logger:                    discardLogger(),
		LedgerState:               ls,
		MaxConcurrentBulkRequests: 1,
	})
	srv := &queryServiceServer{utxorpc: u}
	req := connect.NewRequest(
		&query.ReadTxRequest{Hash: bytes.Repeat([]byte{0x01}, 32)},
	)

	release, err := u.acquireBulk(context.Background())
	require.NoError(t, err)
	_, err = srv.ReadTx(context.Background(), req)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	require.Zero(t, ls.lookups.Load(), "a refused read must not touch the ledger")

	release()
	_, err = srv.ReadTx(context.Background(), req)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	require.Equal(t, int64(1), ls.lookups.Load())

	// The handler returned its slot, so the budget is whole again.
	again, err := u.acquireBulk(context.Background())
	require.NoError(t, err)
	again()
}
