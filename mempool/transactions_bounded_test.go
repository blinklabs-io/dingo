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
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMempool_TransactionsBounded(t *testing.T) {
	t.Parallel()
	// Mock txs are "tx-cbor-N": 9 bytes for N < 10.
	const txBytes = int64(len("tx-cbor-0"))

	tests := []struct {
		name      string
		maxItems  int
		maxBytes  int64
		wantItems int
	}{
		{"unbounded", 0, 0, 5},
		{"items at pool size", 5, 0, 5},
		{"items below pool size", 2, 0, 2},
		{"items above pool size", 6, 0, 5},
		{"bytes exactly fit three", 0, 3 * txBytes, 3},
		{"bytes one short of three", 0, 3*txBytes - 1, 2},
		{"items tighter than bytes", 1, 4 * txBytes, 1},
		{"bytes smaller than one tx keeps first", 0, 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestMempool(t)
			defer m.Stop(context.Background())
			addMockTransactions(t, m, 5)

			got, total := m.TransactionsBounded(tc.maxItems, tc.maxBytes)
			require.Len(t, got, tc.wantItems)
			require.Equal(t, 5, total, "total reports the whole pool")
			for i := range got {
				require.Equal(t, m.Transactions()[i].Hash, got[i].Hash)
			}
		})
	}
}

func TestMempool_TransactionsBounded_ReturnsCopies(t *testing.T) {
	t.Parallel()
	m := newTestMempool(t)
	defer m.Stop(context.Background())
	addMockTransactions(t, m, 2)

	got, _ := m.TransactionsBounded(1, 0)
	require.Len(t, got, 1)
	got[0].Cbor[0] = 'X'

	again, _ := m.TransactionsBounded(1, 0)
	require.NotEqual(t, byte('X'), again[0].Cbor[0])
}

// TestMempool_TransactionsBounded_RetainsOnlyResult requires the snapshot to
// allocate for the bounded result, not for the whole pool: a slice cut from a
// pool-sized backing array keeps every header reachable for as long as the
// caller holds the result.
func TestMempool_TransactionsBounded_RetainsOnlyResult(t *testing.T) {
	t.Parallel()
	for _, useDAG := range []bool{false, true} {
		t.Run(fmt.Sprintf("dag=%t", useDAG), func(t *testing.T) {
			t.Parallel()
			m := newBoundedTestMempool(t, useDAG)
			txs := addMockTransactions(t, m, 8)
			if useDAG {
				m.Lock()
				for _, tx := range txs {
					m.dag.add(appliedTx{hash: tx.Hash})
				}
				order, err := m.dag.topologicalOrder()
				m.Unlock()
				require.NoError(t, err)
				require.Len(t, order, len(txs),
					"the DAG path, not the admission-order fallback")
			}

			got, total := m.TransactionsBounded(2, 0)
			require.Len(t, got, 2)
			require.Equal(t, 8, total)
			require.LessOrEqual(t, cap(got), 2)

			got, _ = m.TransactionsBounded(0, 2*int64(len("tx-cbor-0")))
			require.Len(t, got, 2)
			require.LessOrEqual(t, cap(got), 2)
		})
	}
}

func newBoundedTestMempool(t *testing.T, useDAG bool) *Mempool {
	t.Helper()
	cfg := MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       newMockValidator(),
		MempoolCapacity: 1024 * 1024,
	}
	impl := ImplementationFIFO
	if useDAG {
		impl = ImplementationDAG
	}
	m, err := newMempool(cfg, impl)
	require.NoError(t, err)
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	if useDAG {
		require.NotNil(t, m.dag)
	}
	return m
}

// TestMempool_TransactionsBounded_DAGWalksOnlyPrefix orders the DAG opposite
// to admission and leaves the last ordered hash unresolvable. A bounded read
// never reaches that hash, so it answers in DAG order; a full read reaches it
// and falls back to admission order.
func TestMempool_TransactionsBounded_DAGWalksOnlyPrefix(t *testing.T) {
	t.Parallel()
	m := newBoundedTestMempool(t, true)
	txs := addMockTransactions(t, m, 6)
	m.Lock()
	for _, tx := range slices.Backward(txs) {
		m.dag.add(appliedTx{hash: tx.Hash})
	}
	delete(m.txByHash, txs[0].Hash)
	m.Unlock()

	got, total := m.TransactionsBounded(2, 0)
	require.Equal(t, 6, total)
	require.Len(t, got, 2)
	require.Equal(t, txs[5].Hash, got[0].Hash, "DAG order, not admission")
	require.Equal(t, txs[4].Hash, got[1].Hash)

	all, _ := m.TransactionsBounded(0, 0)
	require.Len(t, all, 6)
	require.Equal(t, txs[0].Hash, all[0].Hash,
		"a full read reaches the unresolvable hash and falls back")
}
