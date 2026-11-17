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

package indexer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// closureDeferralFixture indexes a registration block and a block spending its
// output, with the first block's certified closure unavailable until released.
type closureDeferralFixture struct {
	idx       *Indexer
	createKey utxoKey
	create    models.Block
	spend     models.Block
	available atomic.Bool
	fatal     atomic.Pointer[error]

	mu      sync.Mutex
	indexed []uint64 // block numbers whose transactions were resolved, in order
}

func newClosureDeferralFixture(
	t *testing.T,
	retryInterval time.Duration,
) *closureDeferralFixture {
	t.Helper()
	f := &closureDeferralFixture{}
	store := setupTestStore(t)
	f.create = validRegistrationHistoryBlock(t)
	f.spend = f.create
	f.spend.ID, f.spend.Number, f.spend.Slot =
		2, f.create.Number+1, f.create.Slot+1
	f.spend.Hash = []byte("deferral-spend-block")

	createHash := pad32("fb000001")
	createTx := buildTx(t, createHash,
		[]lcommon.TransactionInput{buildInput(t, pad32("fb000002"), 0)},
		[]lcommon.TransactionOutput{
			buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, simpleDatumCbor(t)),
		},
	)
	spendTx := buildTx(t, pad32("fb000003"),
		[]lcommon.TransactionInput{buildInput(t, createHash, 0)},
		[]lcommon.TransactionOutput{buildCNightOutput(t, "aa", "bb", 1)},
	)
	cfg := registrationRebuildConfig(store)
	cfg.FatalErrorFunc = func(err error) { f.fatal.Store(&err) }
	cfg.closureRetryInterval = retryInterval
	cfg.BlockTransactions = func(
		_ context.Context,
		block models.Block,
	) ([]lcommon.Transaction, error) {
		if block.Number == f.create.Number {
			if !f.available.Load() {
				return nil, ledger.ErrCertifiedClosureUnavailable
			}
			f.record(block.Number)
			return []lcommon.Transaction{createTx}, nil
		}
		f.record(block.Number)
		return []lcommon.Transaction{spendTx}, nil
	}
	idx, err := New(cfg)
	require.NoError(t, err)
	f.idx = idx
	f.createKey = utxoKey{TxHash: createHash, Index: 0}
	return f
}

func (f *closureDeferralFixture) record(n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.indexed = append(f.indexed, n)
}

func (f *closureDeferralFixture) order() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.indexed...)
}

func (f *closureDeferralFixture) tracked() bool {
	f.idx.mu.RLock()
	defer f.idx.mu.RUnlock()
	_, ok := f.idx.regUTxOs[f.createKey]
	return ok
}

func (f *closureDeferralFixture) send(
	action ledger.BlockAction,
	block models.Block,
) {
	f.idx.handleBlockEvent(event.Event{Data: ledger.BlockEvent{
		Action: action, Block: block,
	}})
}

// A block whose certified closure is unavailable must not stop the node, and
// nothing behind it may be indexed ahead of it.
func TestUnavailableClosureDefersBlockAndHoldsLaterEvents(t *testing.T) {
	t.Parallel()
	f := newClosureDeferralFixture(t, time.Hour)

	f.send(ledger.BlockActionApply, f.create)
	f.send(ledger.BlockActionApply, f.spend)
	require.Nil(t, f.fatal.Load(), "an unavailable closure is not fatal")
	require.Empty(t, f.order(), "the spend must wait behind the deferred block")

	f.available.Store(true)
	f.idx.retryDeferred()

	require.Equal(
		t, []uint64{f.create.Number, f.spend.Number}, f.order(),
		"deferred events are indexed in published order",
	)
	require.False(t, f.tracked(), "the spend of the registration is recorded")
	require.Nil(t, f.fatal.Load())
}

// A block undone while still deferred has nothing to undo, and its closure may
// never exist, so it must leave the queue.
func TestUndoDropsDeferredBlock(t *testing.T) {
	t.Parallel()
	f := newClosureDeferralFixture(t, time.Hour)

	f.send(ledger.BlockActionApply, f.create)
	f.send(ledger.BlockActionUndo, f.create)
	f.available.Store(true)
	f.idx.retryDeferred()

	require.Nil(t, f.fatal.Load())
	require.Empty(t, f.order(), "the undone block is never indexed")
	require.False(t, f.tracked())
	f.idx.eventMu.Lock()
	defer f.idx.eventMu.Unlock()
	require.Empty(t, f.idx.deferred)
}

// Only an unavailable closure waits; any other block error still stops the
// node rather than risk a gap in the index.
func TestOtherBlockErrorsStayFatal(t *testing.T) {
	t.Parallel()
	f := newClosureDeferralFixture(t, time.Hour)
	corrupt := errors.New("endorser transaction hash mismatch")
	f.idx.config.BlockTransactions = func(
		context.Context, models.Block,
	) ([]lcommon.Transaction, error) {
		return nil, corrupt
	}

	f.send(ledger.BlockActionApply, f.create)

	got := f.fatal.Load()
	require.NotNil(t, got)
	require.ErrorIs(t, *got, corrupt)
}

// With no later event to retry it, a deferred block is retried on a timer.
func TestDeferredBlockIsRetriedOnTimer(t *testing.T) {
	t.Parallel()
	f := newClosureDeferralFixture(t, 5*time.Millisecond)

	f.send(ledger.BlockActionApply, f.create)
	require.False(t, f.tracked())

	f.available.Store(true)
	testutil.WaitForCondition(
		t, f.tracked, 2*time.Second, "deferred block was not retried",
	)
	require.Nil(t, f.fatal.Load())
}
