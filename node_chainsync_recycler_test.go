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

package dingo

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/chainsyncrecycler"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRecyclerComponentsTestNode builds the minimal node the recycler component
// provider reads, without starting anything.
func newRecyclerComponentsTestNode(t *testing.T) *Node {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	return &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		// The provider only hands the pointer to the recycler, so an empty
		// LedgerState is enough here -- no database fixture required.
		ledgerState: &ledger.LedgerState{},
		chainsyncState: chainsync.NewStateWithConfig(
			bus,
			nil,
			chainsync.Config{MaxClients: 1, StallTimeout: time.Minute},
		),
		eventBus: bus,
	}
}

func TestRecyclerComponentsProvidesLiveComponents(t *testing.T) {
	n := newRecyclerComponentsTestNode(t)
	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	provider := n.recyclerComponents()

	called := false
	ok := provider.WithLiveComponents(
		func(live chainsyncrecycler.LiveComponents) {
			called = true
			assert.NotNil(t, live.Ledger)
			assert.NotNil(t, live.ChainsyncState)
			assert.NotNil(t, live.ChainSelector)
			// The lifecycle lock must be held for the whole callback so a live
			// restore/truncate cannot swap the components mid-tick.
			assert.False(
				t,
				n.liveLifecycleMu.TryLock(),
				"liveLifecycleMu must be held while the callback runs",
			)
		},
	)
	assert.True(t, ok)
	assert.True(t, called)
	assert.True(
		t,
		n.liveLifecycleMu.TryLock(),
		"liveLifecycleMu must be released once the callback returns",
	)
	n.liveLifecycleMu.Unlock()
}

func TestRecyclerComponentsLeavesChainSelectorNilWhenUnset(t *testing.T) {
	n := newRecyclerComponentsTestNode(t)
	provider := n.recyclerComponents()

	ok := provider.WithLiveComponents(
		func(live chainsyncrecycler.LiveComponents) {
			// A typed-nil *ChainSelector stored in the interface would make this
			// non-nil and defeat every nil check in the recycler.
			assert.Nil(t, live.ChainSelector)
		},
	)
	assert.True(t, ok)
}

func TestRecyclerComponentsSkipsWhenLifecycleOpHoldsLock(t *testing.T) {
	n := newRecyclerComponentsTestNode(t)
	provider := n.recyclerComponents()

	n.liveLifecycleMu.Lock()
	t.Cleanup(n.liveLifecycleMu.Unlock)

	called := false
	ok := provider.WithLiveComponents(func(chainsyncrecycler.LiveComponents) {
		called = true
	})
	assert.False(t, ok, "a contended lifecycle lock must skip the tick")
	assert.False(t, called)
}

func TestRecyclerComponentsSkipsWhenStorageIsMidReinitialization(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(n *Node)
	}{
		{
			name:    "nil ledger state",
			prepare: func(n *Node) { n.ledgerState = nil },
		},
		{
			name:    "nil chainsync state",
			prepare: func(n *Node) { n.chainsyncState = nil },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := newRecyclerComponentsTestNode(t)
			tc.prepare(n)
			provider := n.recyclerComponents()

			called := false
			ok := provider.WithLiveComponents(
				func(chainsyncrecycler.LiveComponents) { called = true },
			)
			assert.False(t, ok)
			assert.False(t, called)
			assert.True(
				t,
				n.liveLifecycleMu.TryLock(),
				"a skipped tick must still release the lifecycle lock",
			)
			n.liveLifecycleMu.Unlock()
		})
	}
}

func TestRecyclerComponentsReleasesLockOnPanic(t *testing.T) {
	n := newRecyclerComponentsTestNode(t)
	provider := n.recyclerComponents()

	require.Panics(t, func() {
		provider.WithLiveComponents(func(chainsyncrecycler.LiveComponents) {
			panic("boom")
		})
	})
	assert.True(
		t,
		n.liveLifecycleMu.TryLock(),
		"a panicking tick must not leak the lifecycle lock",
	)
	n.liveLifecycleMu.Unlock()
}

// TestRecyclerComponentsIgnoresSnapshotMu pins the boundary between the two
// node locks: Snapshot no longer holds liveLifecycleMu, so a snapshot in
// progress must not stop the recycler from ticking.
func TestRecyclerComponentsIgnoresSnapshotMu(t *testing.T) {
	n := newRecyclerComponentsTestNode(t)
	provider := n.recyclerComponents()

	n.snapshotMu.Lock()
	t.Cleanup(n.snapshotMu.Unlock)

	called := false
	ok := provider.WithLiveComponents(func(chainsyncrecycler.LiveComponents) {
		called = true
	})
	assert.True(t, ok, "only liveLifecycleMu may skip a tick")
	assert.True(t, called)
}

// blockingComponents holds a tick inside WithLiveComponents until released, so
// shutdown ordering can be asserted without sleeping.
type blockingComponents struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingComponents) WithLiveComponents(
	func(chainsyncrecycler.LiveComponents),
) bool {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return false
}

func TestStopWaitsForChainsyncStallRecycler(t *testing.T) {
	phaseStarted := make(chan struct{}, 1)
	blocking := &blockingComponents{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	n := &Node{
		config: Config{
			logger: slog.New(nodeTestLogSignalHandler{
				message: "shutdown phase 1: stopping new work",
				seen:    phaseStarted,
			}),
			// No shutdown timeout is set: Config.cfg is nil here, so
			// configuredShutdownTimeout falls back to its 30s default and any
			// value set on the struct field would be inert. The test's own
			// RequireReceive bounds are what fail it if Stop never returns.
		},
	}
	n.chainsyncStallRecycler = chainsyncrecycler.New(chainsyncrecycler.Config{
		Components:   blocking,
		EventBus:     event.NewEventBus(nil, nil),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		StallTimeout: time.Minute,
		Interval:     time.Millisecond,
		Grace:        time.Second,
		Cooldown:     time.Minute,
	})
	require.NoError(t, n.chainsyncStallRecycler.Start(t.Context()))
	testutil.RequireReceive(
		t,
		blocking.entered,
		2*time.Second,
		"recycler tick should be in flight",
	)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- n.Stop()
	}()

	// Once phase 1 starts, Stop must still be waiting on the in-flight tick.
	testutil.RequireReceive(
		t,
		phaseStarted,
		2*time.Second,
		"shutdown phase 1 start",
	)
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before recycler exited: %v", err)
	default:
	}

	close(blocking.release)
	require.NoError(
		t,
		testutil.RequireReceive(t, stopDone, 2*time.Second, "node Stop"),
	)
}

func TestWaitChainsyncStallRecyclerIsSafeWithoutRecycler(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	// A node that failed before wiring the recycler must still shut down.
	n.waitChainsyncStallRecycler()
}
