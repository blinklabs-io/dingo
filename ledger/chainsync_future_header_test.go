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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type pastHorizonSlotTimeProvider struct {
	SlotTimeProvider
	rejectedSlot uint64
}

type arrivalPastHorizonSlotTimeProvider struct {
	SlotTimeProvider
}

func (p arrivalPastHorizonSlotTimeProvider) TimeToSlot(
	time.Time,
) (uint64, error) {
	return 0, hardfork.ErrPastHorizon
}

type failingSlotTimeProvider struct {
	SlotTimeProvider
	rejectedSlot uint64
	err          error
}

func (p failingSlotTimeProvider) SlotToTime(slot uint64) (time.Time, error) {
	if slot == p.rejectedSlot {
		return time.Time{}, p.err
	}
	return p.SlotTimeProvider.SlotToTime(slot)
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
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
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

func TestAwaitChainsyncHeaderAdmissionBoundaries(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	t.Run("current header is accepted immediately", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)

		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
			futureHeaderEvent(100, arrival),
		)
		require.NoError(t, err)
		require.True(t, accepted)
		require.Empty(t, *waits)
	})

	t.Run("clock skew boundary waits until onset", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)

		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
			futureHeaderEvent(102, arrival),
		)
		require.NoError(t, err)
		require.True(t, accepted)
		require.Equal(t, []time.Duration{2 * time.Second}, *waits)
	})

	t.Run(
		"beyond skew is deliberately dropped despite processing delay",
		func(t *testing.T) {
			arrival := systemStart.Add(100*time.Second - time.Nanosecond)
			processTime := systemStart.Add(103 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, processTime)

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(102, arrival),
			)
			require.NoError(t, err)
			require.False(t, accepted)
			require.Empty(t, *waits)
		},
	)

	t.Run("slot past the forecast horizon is deferred", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
		ls.slotClock.provider = pastHorizonSlotTimeProvider{
			SlotTimeProvider: ls.slotClock.provider,
			rejectedSlot:     10_000,
		}

		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
			futureHeaderEvent(10_000, arrival),
		)
		require.NoError(t, err)
		require.True(t, accepted)
		require.Empty(t, *waits)
	})

	t.Run(
		"processing delay does not change arrival judgment",
		func(t *testing.T) {
			arrival := systemStart.Add(101 * time.Second)
			processTime := systemStart.Add(103 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, processTime)

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(102, arrival),
			)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, *waits)
		},
	)

	t.Run(
		"historical catch-up does not convert queued arrival time",
		func(t *testing.T) {
			arrival := systemStart.Add(1_000_000 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
			ls.slotClock.provider = arrivalPastHorizonSlotTimeProvider{
				SlotTimeProvider: ls.slotClock.provider,
			}

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(900, arrival),
			)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, *waits)
		},
	)

	t.Run(
		"synthetic event without arrival remains compatible",
		func(t *testing.T) {
			now := systemStart.Add(100 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, now)

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(10_000, time.Time{}),
			)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, *waits)
		},
	)
}

func TestAwaitChainsyncHeaderAdmissionPropagatesCancellation(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.slotClock.waitFunc = func(context.Context, time.Duration) error {
		return context.Canceled
	}

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		t.Context(),
		futureHeaderEvent(101, arrival),
	)
	require.False(t, accepted)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAwaitChainsyncHeaderAdmissionFailsClosedOnNilContext(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		nil,
		futureHeaderEvent(101, arrival),
	)
	require.False(t, accepted)
	require.EqualError(t, err, "chainsync header admission context is nil")
}

func TestAwaitChainsyncHeaderAdmissionFailsClosedOnConversionError(
	t *testing.T,
) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
	wantErr := errors.New("slot conversion unavailable")
	ls.slotClock.provider = failingSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		rejectedSlot:     101,
		err:              wantErr,
	}

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		t.Context(),
		futureHeaderEvent(101, arrival),
	)
	require.False(t, accepted)
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, *waits)
}

func TestFutureHeaderWaitDoesNotHoldChainsyncMutex(t *testing.T) {
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	waiting := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	ls.slotClock.waitFunc = func(context.Context, time.Duration) error {
		close(waiting)
		<-release
		return nil
	}
	go func() {
		e := futureHeaderEvent(101, arrival)
		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(), e)
		if err == nil && !accepted {
			err = errors.New("header was not accepted")
		}
		done <- err
	}()

	<-waiting
	mutexAvailable := ls.chainsyncMutex.TryLock()
	if mutexAvailable {
		ls.chainsyncMutex.Unlock()
	}
	close(release)
	require.NoError(t, <-done)
	require.True(t, mutexAvailable,
		"peer-local slot wait must not hold the node-wide chainsync mutex")
}

func TestAwaitChainsyncHeaderAdmissionUsesCrossEraSlotOnset(t *testing.T) {
	ls := crossEraLedger(t)
	provider := newSlotTimeConverterProvider(ls.timeConv())
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	boundary, err := provider.SlotToTime(200)
	require.NoError(t, err)
	arrival := boundary.Add(-defaultHeaderClockSkew)
	clock.nowFunc = func() time.Time { return arrival }
	var waited time.Duration
	clock.waitFunc = func(_ context.Context, delay time.Duration) error {
		waited = delay
		return nil
	}
	ls.slotClock = clock
	ls.ctx = t.Context()

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		t.Context(),
		futureHeaderEvent(200, arrival),
	)
	require.NoError(t, err)
	require.True(t, accepted)
	require.Equal(t, defaultHeaderClockSkew, waited)
}
