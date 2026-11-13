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

package ouroboros

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A retry requested while an endorser block's validation is still running
// skips that block, since it is pending rather than retryable. If the running
// attempt then finds its basis block not yet the ledger tip, the request must
// still be honoured, or the block waits for the next ranking block and its
// vote window can close first.
func TestLeiosValidationRetryDuringPendingAttemptIsNotLost(t *testing.T) {
	t.Parallel()

	txRaw := testDijkstraTx(t, 0)
	blockRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint32(len(txRaw)),
		}},
	})
	require.NoError(t, err)
	point := ocommon.NewPoint(81, lcommon.Blake2b256Hash(blockRaw).Bytes())
	headerRaw := testDijkstraAnnouncementHeaderRawFor(
		t,
		point.Slot,
		testEbHash(point),
		uint64(len(blockRaw)),
	)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	retried := make(chan struct{}, 1)
	var calls atomic.Int32
	validationLedger := &fakeLeiosAnnouncementLedger{
		txValidationFunc: func(context.Context, gledger.BlockHeader) error {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
				return ledger.ErrLeiosValidationParentUnavailable
			}
			select {
			case retried <- struct{}{}:
			default:
			}
			return nil
		},
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: validationLedger,
	})
	defer func() { require.NoError(t, o.Close()) }()
	defer release()
	key := leiosBlockKey(point.Slot, point.Hash)
	data := &leiosEndorserBlockData{
		point:                    point,
		blockRaw:                 blockRaw,
		txCount:                  1,
		txsRaw:                   []cbor.RawMessage{txRaw},
		announcementHeaderRaw:    headerRaw,
		semanticValidationStatus: leiosEBValidationUnknown,
		slotVerified:             true,
		cacheKeys:                []string{key},
	}
	o.leiosMu.Lock()
	o.leiosEndorserBlocks[key] = data
	o.leiosMu.Unlock()

	o.scheduleLeiosEndorserBlockValidation(point, data)
	testutil.RequireReceive(
		t,
		firstStarted,
		time.Second,
		"first validation attempt start",
	)
	// The basis block becomes the ledger tip while the first attempt runs.
	o.RetryLeiosEndorserBlockValidations()
	release()

	testutil.RequireReceive(
		t,
		retried,
		5*time.Second,
		"validation retried after the pending attempt finished",
	)
	require.Eventually(t, func() bool {
		o.leiosMu.RLock()
		defer o.leiosMu.RUnlock()
		return o.leiosEndorserBlocks[key].semanticValidationStatus ==
			leiosEBValidationValid
	}, 5*time.Second, 10*time.Millisecond)
}

// concurrentLeiosValidationLedger serves endorser-block validation from
// concurrent attempts; fakeLeiosAnnouncementLedger counts calls without
// synchronization.
type concurrentLeiosValidationLedger struct {
	fakeLeiosAnnouncementLedger
	validate func(gledger.BlockHeader) error
}

func (l *concurrentLeiosValidationLedger) ValidateLeiosEndorserBlockTransactions(
	_ context.Context,
	header gledger.BlockHeader,
	_ []byte,
	_ [][]byte,
) error {
	return l.validate(header)
}

func testLeiosValidationEntry(
	t *testing.T,
	slot uint64,
	txIndex byte,
) (string, *leiosEndorserBlockData) {
	t.Helper()
	txRaw := testDijkstraTx(t, txIndex)
	blockRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint32(len(txRaw)),
		}},
	})
	require.NoError(t, err)
	point := ocommon.NewPoint(slot, lcommon.Blake2b256Hash(blockRaw).Bytes())
	key := leiosBlockKey(point.Slot, point.Hash)
	return key, &leiosEndorserBlockData{
		point:    point,
		blockRaw: blockRaw,
		txCount:  1,
		txsRaw:   []cbor.RawMessage{txRaw},
		announcementHeaderRaw: testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			testEbHash(point),
			uint64(len(blockRaw)),
		),
		semanticValidationStatus: leiosEBValidationUnknown,
		slotVerified:             true,
		cacheKeys:                []string{key},
	}
}

// An endorser block that arrives while every validation slot is busy must be
// validated once a slot frees, even when no retry was requested meanwhile.
// Otherwise it waits for the next ranking block and its vote window can close
// first.
func TestLeiosValidationSkippedForCapacityRunsWhenSlotFrees(t *testing.T) {
	t.Parallel()

	const busySlots = 2
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBusy := func() { releaseOnce.Do(func() { close(release) }) }
	validationLedger := &concurrentLeiosValidationLedger{
		validate: func(header gledger.BlockHeader) error {
			if header.SlotNumber() <= busySlots {
				<-release
			}
			return nil
		},
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: validationLedger,
	})
	defer func() { require.NoError(t, o.Close()) }()
	defer releaseBusy()

	for slot := uint64(1); slot <= busySlots; slot++ {
		key, data := testLeiosValidationEntry(t, slot, byte(slot))
		o.leiosMu.Lock()
		o.leiosEndorserBlocks[key] = data
		o.leiosMu.Unlock()
		o.scheduleLeiosEndorserBlockValidation(data.point, data)
	}
	require.Len(t, o.leiosValidationSlots, busySlots)

	key, data := testLeiosValidationEntry(t, busySlots+1, busySlots+1)
	o.leiosMu.Lock()
	o.leiosEndorserBlocks[key] = data
	o.leiosMu.Unlock()
	o.scheduleLeiosEndorserBlockValidation(data.point, data)

	releaseBusy()
	// A follow-up attempt is scheduled before the finishing attempt leaves
	// the wait group, so this returns only after it completes too.
	o.leiosValidationWG.Wait()
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	require.Equal(
		t,
		leiosEBValidationValid,
		o.leiosEndorserBlocks[key].semanticValidationStatus,
	)
}

// The ledger republishes each chain update as a ledger.block event, which can
// precede the ledger applying the block, so that event is not evidence the
// announcing block is the ledger tip and must not trigger endorser-block
// validation. The ledger's own at-tip notification, sent after the apply
// commits, does.
func TestLeiosValidationNotRetriedByLedgerBlockEventBeforeApply(t *testing.T) {
	t.Parallel()

	var applied atomic.Bool
	attempts := make(chan struct{}, 16)
	validationLedger := &fakeLeiosAnnouncementLedger{
		txValidationFunc: func(context.Context, gledger.BlockHeader) error {
			attempts <- struct{}{}
			if !applied.Load() {
				return ledger.ErrLeiosValidationParentUnavailable
			}
			return nil
		},
	}
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: validationLedger,
		EventBus:                bus,
	})
	defer func() { require.NoError(t, o.Close()) }()

	key, data := testLeiosValidationEntry(t, 81, 0)
	o.leiosMu.Lock()
	o.leiosEndorserBlocks[key] = data
	o.leiosMu.Unlock()

	bus.Publish(ledger.BlockEventType, event.NewEvent(
		ledger.BlockEventType,
		ledger.BlockEvent{Action: ledger.BlockActionApply, Point: data.point},
	))
	testutil.RequireNoReceive(
		t,
		attempts,
		300*time.Millisecond,
		"a ledger.block Apply event precedes the ledger apply",
	)

	// A chain update also precedes the apply: validation runs and finds the
	// basis block not yet the tip.
	bus.Publish(chain.ChainUpdateEventType, event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{Point: data.point},
	))
	testutil.RequireReceive(
		t,
		attempts,
		5*time.Second,
		"chain update retry before the ledger apply",
	)

	// The attempt reports itself before reading applied, so wait for it to
	// finish and record the basis block as unavailable. Otherwise it could
	// observe applied and validate without any retry.
	require.Eventually(t, func() bool {
		o.leiosMu.RLock()
		defer o.leiosMu.RUnlock()
		return o.leiosEndorserBlocks[key].semanticValidationStatus ==
			leiosEBValidationUnknown
	}, 5*time.Second, 10*time.Millisecond)

	applied.Store(true)
	o.RetryLeiosEndorserBlockValidations()
	testutil.RequireReceive(
		t,
		attempts,
		5*time.Second,
		"retry after the ledger apply",
	)
	require.Eventually(t, func() bool {
		o.leiosMu.RLock()
		defer o.leiosMu.RUnlock()
		return o.leiosEndorserBlocks[key].semanticValidationStatus ==
			leiosEBValidationValid
	}, 5*time.Second, 10*time.Millisecond)
}
