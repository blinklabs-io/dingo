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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type timestampFunc func(uint64) (time.Time, error)

func (f timestampFunc) SlotToTime(slot uint64) (time.Time, error) {
	return f(slot)
}

func TestBackfillTimestampFailurePreservesCheckpoint(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{true, false} {
		t.Run(map[bool]string{true: "failure", false: "recovery"}[fail], func(t *testing.T) {
			h := newBackfillHarness(t, 1, nil, nil,
				models.Block{Slot: 10, Number: 1})
			failure := errors.New("era lookup failed")
			h.idx.config.SlotTimer = timestampFunc(func(uint64) (time.Time, error) {
				if fail {
					return time.Time{}, failure
				}
				return time.Unix(123, 0), nil
			})
			err := h.idx.Backfill()
			if fail {
				require.ErrorIs(t, err, failure)
				require.Equal(t, uint64(1), h.idx.checkpointSlot)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint64(10), h.idx.checkpointSlot)
			}
		})
	}
}

func TestLiveTimestampFailurePreservesCheckpoint(t *testing.T) {
	t.Parallel()
	cbor, blockType := decodableBlockCbor(t)
	h := newBackfillHarness(t, 1, nil, nil)
	failure := errors.New("era lookup failed")
	var fatal error
	h.idx.config.FatalErrorFunc = func(err error) { fatal = err }
	h.idx.config.SlotTimer = timestampFunc(func(uint64) (time.Time, error) {
		return time.Time{}, failure
	})
	blk := models.Block{Slot: 10, Number: 1, Type: blockType, Cbor: cbor}
	h.idx.handleBlockEvent(event.Event{Data: ledger.BlockEvent{
		Action: ledger.BlockActionApply, Block: blk,
	}})
	require.ErrorIs(t, fatal, failure)
	require.Equal(t, uint64(1), h.idx.checkpointSlot)
	h.idx.config.SlotTimer = timestampFunc(func(uint64) (time.Time, error) {
		return time.Unix(123, 0), nil
	})
	fatal = nil
	h.idx.handleBlockEvent(event.Event{Data: ledger.BlockEvent{
		Action: ledger.BlockActionApply, Block: blk,
	}})
	require.NoError(t, fatal)
	require.Equal(t, uint64(1), h.idx.checkpointSlot, "queued blocks cannot cross a fatal indexing gap")
	restarted, err := New(h.idx.config)
	require.NoError(t, err)
	restarted.handleBlockEvent(event.Event{Data: ledger.BlockEvent{
		Action: ledger.BlockActionApply, Block: blk,
	}})
	require.Equal(t, uint64(10), restarted.checkpointSlot)
}

func TestBackfillDecodeFailurePreservesCheckpoint(t *testing.T) {
	t.Parallel()
	h := newBackfillHarness(t, 1, nil, nil,
		models.Block{Slot: 10, Number: 1})
	h.idx.config.blockDecoder = nil
	require.Error(t, h.idx.Backfill())
	require.Equal(t, uint64(1), h.idx.checkpointSlot)
}

func TestStopHonorsContextWhileHandlerRuns(t *testing.T) {
	t.Parallel()
	eb := event.NewEventBus(nil, nil)
	defer eb.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	idx := &Indexer{config: Config{EventBus: eb}}
	idx.subID = eb.SubscribeFunc(ledger.BlockEventType, func(event.Event) {
		close(entered)
		<-release
	})
	eb.Publish(ledger.BlockEventType, event.NewEvent(ledger.BlockEventType, nil))
	testutil.RequireReceive(t, entered, time.Second, "handler did not start")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan error, 1)
	go func() { done <- idx.Stop(ctx) }()
	require.ErrorIs(t, testutil.RequireReceive(t, done, time.Second, "Stop blocked"), context.Canceled)
}

func TestTimestampRejectsNegativeTime(t *testing.T) {
	t.Parallel()
	idx := &Indexer{config: Config{SlotTimer: timestampFunc(func(uint64) (time.Time, error) {
		return time.Unix(-1, 0), nil
	})}}
	_, err := idx.blockTimestamp(1)
	require.ErrorContains(t, err, "negative timestamp")
}

func TestTimestampRequiresSlotTimer(t *testing.T) {
	t.Parallel()
	h := newBackfillHarness(t, 1, nil, nil, models.Block{Slot: 10, Number: 1})
	h.idx.config.SlotTimer = nil
	require.ErrorContains(t, h.idx.Backfill(), "slot timer unavailable")
	require.Equal(t, uint64(1), h.idx.checkpointSlot)
}

func TestStopDrainsIdleSubscription(t *testing.T) {
	t.Parallel()
	eb := event.NewEventBus(nil, nil)
	defer eb.Close()
	idx := &Indexer{config: Config{EventBus: eb}}
	idx.Subscribe()
	require.NoError(t, idx.Stop(t.Context()))
}
