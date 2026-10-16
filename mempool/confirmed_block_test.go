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
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// blockCarryingTx builds a Conway block whose only transaction is txBytes.
func blockCarryingTx(t *testing.T, txBytes []byte) models.Block {
	t.Helper()
	return blockCarryingTxs(t, 100, txBytes)
}

func blockCarryingTxs(
	t *testing.T,
	slot uint64,
	transactions ...[]byte,
) models.Block {
	t.Helper()
	bodies := make([]cbor.RawMessage, 0, len(transactions))
	witnesses := make([]cbor.RawMessage, 0, len(transactions))
	for _, txBytes := range transactions {
		var parts []cbor.RawMessage
		_, err := cbor.Decode(txBytes, &parts)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(parts), 2)
		bodies = append(bodies, parts[0])
		witnesses = append(witnesses, parts[1])
	}
	body, err := cbor.Encode(bodies)
	require.NoError(t, err)
	witnessBytes, err := cbor.Encode(witnesses)
	require.NoError(t, err)
	raw := testutil.BuildBlockBytesFromComponents(
		t,
		gledger.BlockTypeConway,
		slot,
		1,
		body,
		witnessBytes,
		[]byte{0xa0},
		[]byte{0x80},
	)
	return models.Block{Type: gledger.BlockTypeConway, Cbor: raw, Slot: slot}
}

func TestChainUpdateRestoresRolledBackTransactionsInLedgerOrder(t *testing.T) {
	t.Parallel()
	validator := newMockValidator()
	m, err := NewMempool(MempoolConfig{
		Logger:          slog.Default(),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validator,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, m.Stop(context.Background()))
		m.eventBus.Close()
	})
	first, firstHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(30, 1), 0, 1, 1,
	)
	require.NoError(t, err)
	second, secondHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(30, 2), 0, 2, 1,
	)
	require.NoError(t, err)
	third, thirdHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(30, 3), 0, 3, 1,
	)
	require.NoError(t, err)

	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{
			blockCarryingTxs(t, 101, third),
			blockCarryingTxs(t, 100, first, second),
		},
	}))
	wantOrder := []string{firstHash, secondHash, thirdHash}
	require.Equal(t, wantOrder, m.pendingRolledBackOrder)
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	transactions := m.Transactions()
	require.Len(t, transactions, len(wantOrder))
	for i, hash := range wantOrder {
		require.Equal(t, hash, transactions[i].Hash)
	}
}

func TestCoalescedRollbacksRestoreTransactionsInLedgerOrder(t *testing.T) {
	t.Parallel()
	validator := newMockValidator()
	m, err := NewMempool(MempoolConfig{
		Logger:          slog.Default(),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validator,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, m.Stop(context.Background()))
		m.eventBus.Close()
	})

	first, firstHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(31, 1), 0, 1, 1,
	)
	require.NoError(t, err)
	second, secondHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(31, 2), 0, 2, 1,
	)
	require.NoError(t, err)
	third, thirdHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(31, 3), 0, 3, 1,
	)
	require.NoError(t, err)
	fourth, fourthHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(31, 4), 0, 4, 1,
	)
	require.NoError(t, err)

	// The first event removes the newer blocks. The next rollback extends
	// farther back, so its transactions must precede those already queued.
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{
			blockCarryingTxs(t, 103, fourth),
			blockCarryingTxs(t, 102, third),
		},
	}))
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{
			blockCarryingTxs(t, 101, second),
			blockCarryingTxs(t, 100, first),
		},
	}))

	wantOrder := []string{firstHash, secondHash, thirdHash, fourthHash}
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	transactions := m.Transactions()
	require.Len(t, transactions, len(wantOrder))
	for i, hash := range wantOrder {
		require.Equal(t, hash, transactions[i].Hash)
	}
}

func newConfirmedBlockMempool(
	t *testing.T,
) (*Mempool, *mockValidator, *lockedBuffer, []byte, string) {
	t.Helper()
	logs := &lockedBuffer{}
	v := newMockValidator()
	m, err := NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       v,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, m.Stop(context.Background()))
		m.eventBus.Close()
	})
	txBytes := getTestTxBytes(t)
	require.NoError(
		t,
		m.AddTransaction(
			context.Background(),
			uint(conway.EraIdConway),
			txBytes,
		),
	)
	tx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway), txBytes,
	)
	require.NoError(t, err)
	return m, v, logs, txBytes, tx.Hash().String()
}

func chainUpdate(data any) event.Event {
	return event.NewEvent(chain.ChainUpdateEventType, data)
}

// A block that confirms a pending transaction must evict it as confirmed.
// Without that, the transaction is only removed when full-pool revalidation
// finds its inputs spent, and it is logged as an invalid transaction that was
// dropped.
func TestChainUpdateEvictsConfirmedTransactionWithoutDropWarning(
	t *testing.T,
) {
	t.Parallel()
	m, v, logs, txBytes, hash := newConfirmedBlockMempool(t)
	// The ledger has applied the block, so the inputs are gone.
	v.mu.Lock()
	v.failHashes[hash] = true
	v.mu.Unlock()

	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTx(t, txBytes),
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))

	_, ok := m.GetTransaction(hash)
	require.False(t, ok)
	out := logs.String()
	require.Contains(t, out, "confirmed transaction")
	require.NotContains(t, out, "failed re-validation")
}

// A block applied and then rolled back before the pool is rebuilt confirms
// nothing, so its transactions stay pending.
func TestChainUpdateKeepsTransactionOfARolledBackBlock(t *testing.T) {
	t.Parallel()
	m, _, _, txBytes, hash := newConfirmedBlockMempool(t)
	block := blockCarryingTx(t, txBytes)

	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))

	_, ok := m.GetTransaction(hash)
	require.True(t, ok)
	require.Len(t, m.Transactions(), 1)
}

func TestChainUpdateRestoresTransactionRolledBackAfterEviction(t *testing.T) {
	t.Parallel()
	m, v, _, txBytes, hash := newConfirmedBlockMempool(t)
	block := blockCarryingTx(t, txBytes)

	v.mu.Lock()
	v.failHashes[hash] = true
	v.mu.Unlock()
	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	_, ok := m.GetTransaction(hash)
	require.False(t, ok)

	v.mu.Lock()
	delete(v.failHashes, hash)
	v.mu.Unlock()
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))

	_, ok = m.GetTransaction(hash)
	require.True(t, ok)
}

func TestChainUpdateRejectsInvalidTransactionFromRolledBackBlock(t *testing.T) {
	t.Parallel()
	m, v, _, txBytes, hash := newConfirmedBlockMempool(t)
	block := blockCarryingTx(t, txBytes)

	v.mu.Lock()
	v.failHashes[hash] = true
	v.mu.Unlock()
	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))

	_, ok := m.GetTransaction(hash)
	require.False(t, ok)
}

func TestChainUpdateRetriesUnsettledRolledBackTransaction(t *testing.T) {
	t.Parallel()
	m, v, _, txBytes, hash := newConfirmedBlockMempool(t)
	block := blockCarryingTx(t, txBytes)

	m.RemoveTransaction(hash)
	v.mu.Lock()
	v.errorsByHash[hash] = ErrPendingStateMoved
	v.mu.Unlock()
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	require.ErrorIs(
		t,
		m.rebuildAfterChainUpdate(context.Background()),
		ErrPendingStateMoved,
	)
	require.Equal(t, []string{hash}, m.pendingRolledBackOrder)

	v.mu.Lock()
	delete(v.errorsByHash, hash)
	v.mu.Unlock()
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	_, ok := m.GetTransaction(hash)
	require.True(t, ok)
}
