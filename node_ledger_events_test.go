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
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

type fakeSyncLedger struct {
	mu          sync.Mutex
	atTip       bool
	tipSlot     uint64
	slotsBehind uint64
}

func (l *fakeSyncLedger) set(atTip bool, tipSlot, slotsBehind uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.atTip, l.tipSlot, l.slotsBehind = atTip, tipSlot, slotsBehind
}

func (l *fakeSyncLedger) IsAtTip() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.atTip
}

func (l *fakeSyncLedger) ChainTipSlot() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tipSlot
}

func (l *fakeSyncLedger) SlotsBehindHead() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.slotsBehind
}

func syncStateEvents(t *testing.T, bus *event.EventBus) <-chan event.Event {
	t.Helper()
	_, ch := bus.Subscribe(ledger.SyncStateEventType)
	return ch
}

func requireSyncState(
	t *testing.T,
	ch <-chan event.Event,
	want ledger.SyncStateEvent,
) {
	t.Helper()
	evt := testutil.RequireReceive(t, ch, 2*time.Second, "sync state event")
	require.Equal(t, want, evt.Data)
}

// TestSyncStateTrackerFollowsLedgerTipOnce checks the ledger latch moves the
// node to caught up once, and that after the selector reports falling behind
// the still-latched ledger cannot report it caught up again.
func TestSyncStateTrackerFollowsLedgerTipOnce(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ch := syncStateEvents(t, bus)
	ls := &fakeSyncLedger{}
	tr := newSyncStateTracker(bus, func() syncStateLedger { return ls })

	ls.set(false, 100, 900)
	tr.ledgerTipCommitted()
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "sync state event")

	ls.set(true, 1000, 2)
	tr.ledgerTipCommitted()
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateSyncing, Current: ledger.SyncStateCaughtUp,
		TipSlot: 1000, SlotsBehind: 2,
	})

	ls.set(true, 1010, 600)
	tr.genesisModeChanged(false)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateCaughtUp, Current: ledger.SyncStateSyncing,
		TipSlot: 1010, SlotsBehind: 600,
	})

	tr.ledgerTipCommitted()
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "sync state event")

	ls.set(true, 1700, 0)
	tr.genesisModeChanged(true)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateSyncing, Current: ledger.SyncStateCaughtUp,
		TipSlot: 1700,
	})
}

// TestSyncStateTrackerPublishesOnlyChanges checks that a Genesis exit after
// the ledger already reported caught up, and a repeated entry, publish
// nothing.
func TestSyncStateTrackerPublishesOnlyChanges(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ch := syncStateEvents(t, bus)
	ls := &fakeSyncLedger{}
	tr := newSyncStateTracker(bus, func() syncStateLedger { return ls })

	tr.genesisModeChanged(false)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "sync state event")

	ls.set(true, 50, 0)
	tr.ledgerTipCommitted()
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateSyncing, Current: ledger.SyncStateCaughtUp,
		TipSlot: 50,
	})
	tr.genesisModeChanged(true)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "sync state event")
}

// TestSyncStateTrackerWithoutLedger checks a transition is still reported
// while the ledger is being rebuilt, with the tip fields left zero.
func TestSyncStateTrackerWithoutLedger(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ch := syncStateEvents(t, bus)
	tr := newSyncStateTracker(bus, func() syncStateLedger { return nil })

	tr.ledgerTipCommitted()
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "sync state event")
	tr.genesisModeChanged(true)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateSyncing, Current: ledger.SyncStateCaughtUp,
	})
}

// TestSubscribeLedgerStateEventsFollowsGenesisMode checks the node wires the
// selector's Genesis mode events to sync state events.
func TestSubscribeLedgerStateEventsFollowsGenesisMode(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ch := syncStateEvents(t, bus)
	n := &Node{eventBus: bus, config: NewConfig()}
	n.subscribeLedgerStateEvents()

	bus.Publish(
		chainselection.GenesisModeExitedEventType,
		event.NewEvent(
			chainselection.GenesisModeExitedEventType,
			chainselection.GenesisModeExitedEvent{},
		),
	)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateSyncing, Current: ledger.SyncStateCaughtUp,
	})
	bus.Publish(
		chainselection.GenesisModeEnteredEventType,
		event.NewEvent(
			chainselection.GenesisModeEnteredEventType,
			chainselection.GenesisModeEnteredEvent{},
		),
	)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateCaughtUp, Current: ledger.SyncStateSyncing,
	})
}

type fakeParamsLedger struct {
	mu     sync.Mutex
	epoch  uint64
	params lcommon.ProtocolParameters
}

func (l *fakeParamsLedger) set(
	epoch uint64,
	params lcommon.ProtocolParameters,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.epoch, l.params = epoch, params
}

func (l *fakeParamsLedger) CurrentEpochAndPParams() (
	uint64,
	lcommon.ProtocolParameters,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch, l.params
}

func testShelleyPParams(maxTxSize uint) *shelley.ShelleyProtocolParameters {
	return &shelley.ShelleyProtocolParameters{
		MaxTxSize: maxTxSize,
		A0:        &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:       &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:       &cbor.Rat{Rat: big.NewRat(2, 10)},
	}
}

// TestProtocolParamsTrackerPublishesChanges checks the startup parameters are
// a baseline, an epoch with the same parameters publishes nothing, and an
// epoch with different ones publishes them with the epoch they apply from.
func TestProtocolParamsTrackerPublishesChanges(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ch := bus.Subscribe(ledger.ProtocolParamsEventType)
	ls := &fakeParamsLedger{}
	ls.set(10, testShelleyPParams(16384))
	tr := newProtocolParamsTracker(
		bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func() protocolParamsLedger { return ls },
	)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "baseline")

	// A separately built but equal parameter set is not a change.
	ls.set(11, testShelleyPParams(16384))
	tr.check(true)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "unchanged")

	changed := testShelleyPParams(32768)
	ls.set(12, changed)
	tr.check(true)
	evt := testutil.RequireReceive(t, ch, 2*time.Second, "changed")
	require.Equal(
		t,
		ledger.ProtocolParamsEvent{Epoch: 12, Params: changed},
		evt.Data,
	)

	ls.set(13, testShelleyPParams(32768))
	tr.check(true)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "unchanged again")
}

// TestProtocolParamsTrackerSkipsUncomparableParams checks a parameter set
// that cannot be compared neither publishes nor replaces the baseline.
func TestProtocolParamsTrackerSkipsUncomparableParams(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ch := bus.Subscribe(ledger.ProtocolParamsEventType)
	ls := &fakeParamsLedger{}
	ls.set(10, testShelleyPParams(16384))
	tr := newProtocolParamsTracker(
		bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func() protocolParamsLedger { return ls },
	)

	bad := testShelleyPParams(16384)
	bad.A0 = &cbor.Rat{}
	ls.set(11, bad)
	tr.check(true)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "uncomparable")

	ls.set(12, testShelleyPParams(16384))
	tr.check(true)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "baseline kept")
}

// TestProtocolParamsTrackerSeesParamsOutsideUtxorpc checks a change to a
// parameter the utxorpc form omits, here Shelley's MinUtxoValue, is still
// published.
func TestProtocolParamsTrackerSeesParamsOutsideUtxorpc(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ch := bus.Subscribe(ledger.ProtocolParamsEventType)
	ls := &fakeParamsLedger{}
	before := testShelleyPParams(16384)
	before.MinUtxoValue = 1000000
	ls.set(10, before)
	tr := newProtocolParamsTracker(
		bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func() protocolParamsLedger { return ls },
	)

	after := testShelleyPParams(16384)
	after.MinUtxoValue = 2000000
	ls.set(11, after)
	tr.check(true)
	evt := testutil.RequireReceive(t, ch, 2*time.Second, "MinUtxoValue change")
	require.Equal(
		t,
		ledger.ProtocolParamsEvent{Epoch: 11, Params: after},
		evt.Data,
	)
}

// TestSyncStateTrackerAppliesSelectorModeNotStaleCallback checks a Genesis
// exit callback that runs after the selector has already re-entered Genesis
// does not report the node caught up.
func TestSyncStateTrackerAppliesSelectorModeNotStaleCallback(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ch := syncStateEvents(t, bus)
	var mu sync.Mutex
	inGenesis := true
	tr := newSyncStateTracker(bus, func() syncStateLedger { return nil })
	tr.inGenesis = func() (bool, bool) {
		mu.Lock()
		defer mu.Unlock()
		return inGenesis, true
	}
	setGenesis := func(v bool) {
		mu.Lock()
		defer mu.Unlock()
		inGenesis = v
	}

	setGenesis(false)
	tr.genesisModeChanged(true)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateSyncing, Current: ledger.SyncStateCaughtUp,
	})

	// The selector leaves and re-enters Genesis; the second exit's callback
	// runs after the entry's, so the selector is back in Genesis by then.
	setGenesis(true)
	tr.genesisModeChanged(false)
	requireSyncState(t, ch, ledger.SyncStateEvent{
		Previous: ledger.SyncStateCaughtUp, Current: ledger.SyncStateSyncing,
	})
	tr.genesisModeChanged(true)
	testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "stale exit")
}
