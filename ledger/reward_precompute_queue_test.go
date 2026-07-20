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
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestRewardPrecomputeCoalescesEpochTransitionBurst(t *testing.T) {
	ls := &LedgerState{}
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	enqueued := make(chan uint64)
	var (
		processedMu sync.Mutex
		processed   []uint64
	)
	precompute := func(evt event.EpochTransitionEvent) error {
		if evt.NewEpoch == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		processedMu.Lock()
		processed = append(processed, evt.NewEpoch)
		processedMu.Unlock()
		return nil
	}
	eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		func(evt event.Event) {
			ls.handleRewardPrecomputeEpochTransitionWith(evt, precompute)
			epochEvt := evt.Data.(event.EpochTransitionEvent)
			enqueued <- epochEvt.NewEpoch
		},
	)

	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		),
	)
	require.Equal(
		t,
		uint64(1),
		testutil.RequireReceive(
			t,
			enqueued,
			time.Second,
			"reward precompute callback did not enqueue first epoch",
		),
	)
	testutil.RequireReceive(
		t,
		firstStarted,
		time.Second,
		"first reward precompute did not start",
	)

	// Deliver a sequence longer than the EventBus default buffer's total capacity
	// while the first simulated calculation remains blocked. Waiting for each
	// callback isolates the behavior under test: callback delivery stays
	// independent of reward calculation, and the ledger retains only the newest
	// pending epoch.
	latestEpoch := uint64(event.DefaultSubscriberBuffer + 100)
	for epoch := uint64(2); epoch <= latestEpoch; epoch++ {
		eventBus.Publish(
			event.EpochTransitionEventType,
			event.NewEvent(
				event.EpochTransitionEventType,
				event.EpochTransitionEvent{
					NewEpoch:   epoch,
					EpochNonce: []byte{byte(epoch)},
				},
			),
		)
		require.Equal(
			t,
			epoch,
			testutil.RequireReceive(
				t,
				enqueued,
				time.Second,
				"reward precompute callback did not enqueue epoch",
			),
		)
	}
	close(releaseFirst)

	done := make(chan struct{})
	go func() {
		ls.rewardPrecomputeWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		time.Second,
		"coalesced reward precompute did not finish",
	)

	processedMu.Lock()
	defer processedMu.Unlock()
	require.Equal(t, []uint64{1, latestEpoch}, processed)
}

func TestRewardPrecomputeContinuesAfterPanic(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var processed []uint64
	precompute := func(evt event.EpochTransitionEvent) error {
		if evt.NewEpoch == 1 {
			close(firstStarted)
			<-releaseFirst
			panic("broken reward input")
		}
		processed = append(processed, evt.NewEpoch)
		return nil
	}

	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		precompute,
	)
	testutil.RequireReceive(
		t,
		firstStarted,
		time.Second,
		"panicking reward precompute did not start",
	)
	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 2, EpochNonce: []byte{2}},
		precompute,
	)
	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 3, EpochNonce: []byte{3}},
		precompute,
	)
	close(releaseFirst)

	done := make(chan struct{})
	go func() {
		ls.rewardPrecomputeWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		time.Second,
		"reward precompute worker stopped after panic",
	)
	require.Equal(t, []uint64{3}, processed)
}
