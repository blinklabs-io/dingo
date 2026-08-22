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

package ledger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type pastHorizonSlotTimeProvider struct {
	SlotTimeProvider
	rejectedSlot uint64
}

func (p pastHorizonSlotTimeProvider) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if slot == p.rejectedSlot {
		return time.Time{}, hardfork.ErrPastHorizon
	}
	return p.SlotTimeProvider.SlotToTime(slot)
}

func newFutureHeaderTestLedger(
	t *testing.T,
	systemStart time.Time,
	now time.Time,
) (*LedgerState, *[]time.Duration) {
	t.Helper()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	clock.nowFunc = func() time.Time { return now }
	waits := make([]time.Duration, 0, 1)
	clock.waitFunc = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	return &LedgerState{
		slotClock: clock,
		ctx:       t.Context(),
	}, &waits
}

func futureHeaderEvent(slot uint64, arrival time.Time) ChainsyncEvent {
	header := &envelopeTestHeader{
		slot: slot,
		era:  shelley.EraShelley,
	}
	return ChainsyncEvent{
		BlockHeader: header,
		ArrivalTime: arrival,
		Point:       ocommon.NewPoint(slot, []byte{byte(slot)}),
	}
}

func TestCheckChainsyncHeaderArrivalBoundaries(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	t.Run("current header is accepted immediately", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)

		require.NoError(t, ls.checkChainsyncHeaderArrival(
			futureHeaderEvent(100, arrival),
		))
		require.Empty(t, *waits)
	})

	t.Run("clock skew boundary waits until onset", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)

		require.NoError(t, ls.checkChainsyncHeaderArrival(
			futureHeaderEvent(102, arrival),
		))
		require.Equal(t, []time.Duration{2 * time.Second}, *waits)
	})

	t.Run("invalid arrival stays rejected after processing delay", func(t *testing.T) {
		arrival := systemStart.Add(100*time.Second - time.Nanosecond)
		processTime := systemStart.Add(103 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, processTime)

		err := ls.checkChainsyncHeaderArrival(
			futureHeaderEvent(102, arrival),
		)
		var futureErr *headerTooFarInFutureError
		require.ErrorAs(t, err, &futureErr)
		require.Empty(t, *waits)
	})

	t.Run("slot past the forecast horizon is rejected", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
		ls.slotClock.provider = pastHorizonSlotTimeProvider{
			SlotTimeProvider: ls.slotClock.provider,
			rejectedSlot:     10_000,
		}

		err := ls.checkChainsyncHeaderArrival(
			futureHeaderEvent(10_000, arrival),
		)
		var futureErr *headerTooFarInFutureError
		require.ErrorAs(t, err, &futureErr)
		require.ErrorIs(t, err, hardfork.ErrPastHorizon)
		require.Empty(t, *waits)
	})

	t.Run("processing delay does not change arrival judgment", func(t *testing.T) {
		arrival := systemStart.Add(101 * time.Second)
		processTime := systemStart.Add(103 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, processTime)

		require.NoError(t, ls.checkChainsyncHeaderArrival(
			futureHeaderEvent(102, arrival),
		))
		require.Empty(t, *waits)
	})

	t.Run("synthetic event without arrival remains compatible", func(t *testing.T) {
		now := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, now)

		require.NoError(t, ls.checkChainsyncHeaderArrival(
			futureHeaderEvent(10_000, time.Time{}),
		))
		require.Empty(t, *waits)
	})
}

func TestCheckChainsyncHeaderArrivalPropagatesCancellation(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.slotClock.waitFunc = func(context.Context, time.Duration) error {
		return context.Canceled
	}

	err := ls.checkChainsyncHeaderArrival(futureHeaderEvent(101, arrival))
	require.ErrorIs(t, err, context.Canceled)
}

func TestFutureHeaderRecyclesConnection(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ls.config.EventBus = bus
	_, recycleCh := bus.Subscribe(ConnectionRecycleRequestedEventType)

	var pending pendingPublishes
	err := ls.handleEventChainsyncBlockHeaderWithPending(
		futureHeaderEvent(103, arrival),
		&pending,
	)
	require.Error(t, err)
	var futureErr *headerTooFarInFutureError
	require.True(t, errors.As(err, &futureErr))
	pending.flush()

	recycleEvent := testutil.RequireReceive(
		t,
		recycleCh,
		2*time.Second,
		"far-future header should recycle its ChainSync connection",
	)
	recycle, ok := recycleEvent.Data.(ConnectionRecycleRequestedEvent)
	require.True(t, ok)
	require.Equal(t, "header_too_far_in_future", recycle.Reason)
}
