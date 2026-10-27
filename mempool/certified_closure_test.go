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
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// closureValidator is the shared mockValidator with the ledger's optional
// certified-closure capability: the closure a certifying ranking block applies,
// keyed here by the block's slot.
type closureValidator struct {
	*mockValidator
	closureMu sync.Mutex
	closures  map[uint64][]lcommon.Transaction
	err       error
	calls     int
}

func (v *closureValidator) CertifiedClosureTransactions(
	_ context.Context,
	block gledger.Block,
) ([]lcommon.Transaction, error) {
	v.closureMu.Lock()
	defer v.closureMu.Unlock()
	v.calls++
	if v.err != nil {
		return nil, v.err
	}
	return v.closures[block.SlotNumber()], nil
}

var _ CertifiedClosureProvider = (*closureValidator)(nil)

func newCertifiedClosureMempool(
	t *testing.T,
) (*Mempool, *closureValidator, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	v := &closureValidator{
		mockValidator: newMockValidator(),
		closures:      make(map[uint64][]lcommon.Transaction),
	}
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
	return m, v, logs
}

// A certifying ranking block carries no transactions of its own: the ledger
// applies the certified endorser block its parent announced. Those closure
// transactions are confirmed by the block exactly as its own transactions
// would be, so the mempool must evict them as confirmed. Before, only
// block.Transactions() was considered, so a closure transaction stayed in the
// pool for as long as revalidation had not (yet) seen its inputs spent, and the
// forger could select it into a new endorser block.
//
// The validator still accepts the transaction here, which is the state of a
// pool whose revalidation is deferred or whose view has not caught up with the
// closure's spends: confirmation, not revalidation, must remove it.
func TestChainUpdateEvictsCertifiedClosureTransactions(t *testing.T) {
	t.Parallel()
	m, v, logs := newCertifiedClosureMempool(t)

	closureBytes, closureHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(40, 1), 0, 1, 1,
	)
	require.NoError(t, err)
	otherBytes, otherHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(40, 2), 0, 2, 1,
	)
	require.NoError(t, err)
	for _, txBytes := range [][]byte{closureBytes, otherBytes} {
		require.NoError(t, m.AddTransaction(
			context.Background(), uint(conway.EraIdConway), txBytes,
		))
	}
	closureTx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway), closureBytes,
	)
	require.NoError(t, err)

	const certifyingSlot = 200
	v.closures[certifyingSlot] = []lcommon.Transaction{closureTx}
	certifying := blockCarryingTxs(t, certifyingSlot)

	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: certifying}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))

	_, ok := m.GetTransaction(closureHash)
	require.False(t, ok, "certified closure transaction must leave the mempool")
	_, ok = m.GetTransaction(otherHash)
	require.True(t, ok, "unrelated transaction must stay")
	out := logs.String()
	require.Contains(t, out, "confirmed transaction")
	require.NotContains(t, out, "failed re-validation")
}

// Rolling back a certifying ranking block undoes its certified closure, so the
// closure transactions return to the pool for revalidation, symmetrically with
// the block's own transactions.
func TestChainUpdateRestoresCertifiedClosureOnRollback(t *testing.T) {
	t.Parallel()
	m, v, _ := newCertifiedClosureMempool(t)

	firstBytes, firstHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(41, 1), 0, 1, 1,
	)
	require.NoError(t, err)
	secondBytes, secondHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(41, 2), 0, 2, 1,
	)
	require.NoError(t, err)
	var closure []lcommon.Transaction
	for _, txBytes := range [][]byte{firstBytes, secondBytes} {
		require.NoError(t, m.AddTransaction(
			context.Background(), uint(conway.EraIdConway), txBytes,
		))
		tx, err := gledger.NewTransactionFromCbor(
			uint(conway.EraIdConway), txBytes,
		)
		require.NoError(t, err)
		closure = append(closure, tx)
	}
	const certifyingSlot = 210
	v.closures[certifyingSlot] = closure
	certifying := blockCarryingTxs(t, certifyingSlot)

	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: certifying}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	require.Empty(t, m.Transactions())

	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{certifying},
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	transactions := m.Transactions()
	require.Len(t, transactions, 2)
	require.Equal(t, firstHash, transactions[0].Hash)
	require.Equal(t, secondHash, transactions[1].Hash)
}

// A closure the ledger cannot resolve yet does not stop the block's own
// transactions from being confirmed; revalidation remains the backstop.
func TestChainUpdateConfirmsOwnTransactionsWhenClosureUnavailable(
	t *testing.T,
) {
	t.Parallel()
	m, v, _ := newCertifiedClosureMempool(t)
	txBytes, hash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(42, 1), 0, 1, 1,
	)
	require.NoError(t, err)
	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	))
	v.err = errors.New("endorser block not fetched")

	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 220, txBytes),
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	_, ok := m.GetTransaction(hash)
	require.False(t, ok)
}

// A certified closure carries Dijkstra block_transaction CBOR, with the
// validity flag last. Restoring it after a rollback re-admits it in the
// standalone form, with the flag third, so it decodes as the transaction it is.
func TestStandaloneTransactionCborReordersBlockTransaction(t *testing.T) {
	t.Parallel()
	body := []byte{0xa0}
	witnesses := []byte{0xa0}
	blockTx, err := cbor.Encode([]cbor.RawMessage{
		body, witnesses, {0xf6}, {0xf5},
	})
	require.NoError(t, err)
	want, err := cbor.Encode([]cbor.RawMessage{
		body, witnesses, {0xf5}, {0xf6},
	})
	require.NoError(t, err)
	require.Equal(t, want, standaloneTransactionCbor(blockTx))
	// The standalone form, and anything that is not a four-element array,
	// is returned unchanged.
	require.Equal(t, want, standaloneTransactionCbor(want))
	require.Equal(t, []byte{0x80}, standaloneTransactionCbor([]byte{0x80}))
}

// noteChainUpdate is noteChainUpdateContext for tests that drive chain
// updates directly.
func (m *Mempool) noteChainUpdate(evt event.Event) {
	m.noteChainUpdateContext(context.Background(), evt)
}
