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

package mempool

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

type countingValidator struct {
	calls atomic.Int32
}

func (v *countingValidator) ValidateTx(gledger.Transaction) error {
	v.calls.Add(1)
	return nil
}

func (v *countingValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
	*utxoref.StateOverlay,
) error {
	v.calls.Add(1)
	return nil
}

func dupBatch(t *testing.T, ttls [2]uint64, aux [2]any) []byte {
	t.Helper()
	subs := make([]cbor.RawMessage, 0, 2)
	for i := range ttls {
		body, err := cbor.Encode(
			map[uint]any{0: []any{}, 1: []any{}, 3: ttls[i]},
		)
		require.NoError(t, err)
		sub, err := cbor.Encode(
			[]any{cbor.RawMessage(body), map[uint]any{}, aux[i]},
		)
		require.NoError(t, err)
		subs = append(subs, sub)
	}
	body, err := cbor.Encode(map[uint]any{
		0:  []any{},
		1:  []any{},
		2:  uint64(0),
		23: cbor.NewSetType(subs, true),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(body), map[uint]any{}, nil},
	)
	require.NoError(t, err)
	return txCbor
}

// TestAddTransactionRejectsDuplicateDijkstraSubTransactionBodyID asserts the
// rejection happens at decode, before the validator (and so script execution
// or ledger state) is reached, and that the pool stays empty.
func TestAddTransactionRejectsDuplicateDijkstraSubTransactionBodyID(
	t *testing.T,
) {
	t.Parallel()
	validator := &countingValidator{}
	pool := newTestMempoolWithValidator(t, validator)
	defer pool.Stop(context.Background())
	txType := uint(dijkstra.TxTypeDijkstra)

	err := pool.AddTransaction(
		txType,
		dupBatch(t, [2]uint64{10, 10}, [2]any{nil, map[uint]any{1: "m"}}),
	)
	require.ErrorContains(t, err, "duplicate Dijkstra sub-transaction body")
	require.Zero(t, validator.calls.Load())
	require.Empty(t, pool.Transactions())

	require.NoError(
		t,
		pool.AddTransaction(
			txType,
			dupBatch(t, [2]uint64{10, 11}, [2]any{nil, nil}),
		),
	)
	require.Equal(t, int32(1), validator.calls.Load())
	require.Len(t, pool.Transactions(), 1)
}
