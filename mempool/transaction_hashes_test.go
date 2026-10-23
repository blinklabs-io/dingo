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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMempool_TransactionHashes(t *testing.T) {
	t.Parallel()
	for _, useDAG := range []bool{false, true} {
		t.Run(fmt.Sprintf("dag=%t", useDAG), func(t *testing.T) {
			t.Parallel()
			m := newBoundedTestMempool(t, useDAG)
			txs := addMockTransactions(t, m, 5)
			if useDAG {
				m.Lock()
				for _, tx := range txs {
					m.dag.add(appliedTx{hash: tx.Hash})
				}
				m.Unlock()
			}
			var want []string
			for _, tx := range m.Transactions() {
				want = append(want, tx.Hash)
			}
			require.Len(t, want, 5)

			all, total := m.TransactionHashes(0)
			require.Equal(t, want, all)
			require.Equal(t, 5, total)

			got, total := m.TransactionHashes(2)
			require.Equal(t, want[:2], got)
			require.Equal(t, 5, total, "total reports the whole pool")
			require.LessOrEqual(t, cap(got), 2)

			got, total = m.TransactionHashes(9)
			require.Equal(t, want, got)
			require.Equal(t, 5, total)
		})
	}
}

// TestMempool_TransactionHashes_FollowsDAGOrder orders the DAG opposite to
// admission: the hash listing must follow the order Transactions reports.
func TestMempool_TransactionHashes_FollowsDAGOrder(t *testing.T) {
	t.Parallel()
	m := newBoundedTestMempool(t, true)
	txs := addMockTransactions(t, m, 4)
	m.Lock()
	for _, tx := range slices.Backward(txs) {
		m.dag.add(appliedTx{hash: tx.Hash})
	}
	m.Unlock()

	got, _ := m.TransactionHashes(2)
	require.Equal(t, []string{txs[3].Hash, txs[2].Hash}, got)
}

// TestMempool_TransactionHashes_DoesNotCopyCbor counts allocations over a
// pool of large transactions. A listing that clones each transaction's CBOR
// allocates once per transaction; the hash listing allocates one result slice.
func TestMempool_TransactionHashes_DoesNotCopyCbor(t *testing.T) {
	// Not t.Parallel: testing.AllocsPerRun measures process-wide mallocs.
	const txCount = 64
	for _, useDAG := range []bool{false, true} {
		t.Run(fmt.Sprintf("dag=%t", useDAG), func(t *testing.T) {
			m := newBoundedTestMempool(t, useDAG)
			txs := addMockTransactionsOfSize(t, m, txCount, 4096)
			if useDAG {
				m.Lock()
				for _, tx := range txs {
					m.dag.add(appliedTx{hash: tx.Hash})
				}
				m.Unlock()
			}

			control := testing.AllocsPerRun(5, func() {
				_, _ = m.TransactionsBounded(0, 0)
			})
			require.GreaterOrEqual(t, control, float64(txCount),
				"control: the CBOR-copying snapshot allocates per transaction")

			allocs := testing.AllocsPerRun(5, func() {
				hashes, _ := m.TransactionHashes(0)
				if len(hashes) != txCount {
					panic("short hash listing")
				}
			})
			require.LessOrEqual(t, allocs, float64(2),
				"hash listing must not allocate per transaction")
		})
	}
}

// TestMempool_TransactionHashes_AgreesWithSnapshot corrupts the DAG index in
// each way the snapshot paths diagnose, and asserts that for every bound the
// hash listing names exactly the transactions TransactionsBounded returns, so
// the two DAG walks cannot drift apart on when they fall back.
func TestMempool_TransactionHashes_AgreesWithSnapshot(t *testing.T) {
	t.Parallel()
	const txCount = 6
	type corruption struct {
		name    string
		corrupt func(m *Mempool, order []string)
	}
	cases := []corruption{
		{name: "consistent", corrupt: func(*Mempool, []string) {}},
		{name: "order lost", corrupt: func(m *Mempool, _ []string) {
			m.dag.order = nil
		}},
	}
	for i := range txCount {
		cases = append(cases, corruption{
			name: fmt.Sprintf("ordered hash %d unresolvable", i),
			corrupt: func(m *Mempool, order []string) {
				delete(m.txByHash, order[i])
			},
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newBoundedTestMempool(t, true)
			txs := addMockTransactions(t, m, txCount)
			m.Lock()
			for _, tx := range slices.Backward(txs) {
				m.dag.add(appliedTx{hash: tx.Hash})
			}
			order := slices.Clone(m.dag.order)
			tc.corrupt(m, order)
			m.Unlock()

			for bound := range txCount + 2 {
				snapshot, snapshotTotal := m.TransactionsBounded(bound, 0)
				want := make([]string, len(snapshot))
				for i := range snapshot {
					want[i] = snapshot[i].Hash
				}
				got, total := m.TransactionHashes(bound)
				require.Equal(t, want, got, "bound %d", bound)
				require.Equal(t, snapshotTotal, total, "bound %d", bound)
			}
		})
	}
}
