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
	var parts []cbor.RawMessage
	_, err := cbor.Decode(txBytes, &parts)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(parts), 2)
	body, err := cbor.Encode([]cbor.RawMessage{parts[0]})
	require.NoError(t, err)
	witnesses, err := cbor.Encode([]cbor.RawMessage{parts[1]})
	require.NoError(t, err)
	raw := testutil.BuildBlockBytesFromComponents(
		t,
		gledger.BlockTypeConway,
		100,
		1,
		body,
		witnesses,
		[]byte{0xa0},
		[]byte{0x80},
	)
	return models.Block{Type: gledger.BlockTypeConway, Cbor: raw, Slot: 100}
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
}
