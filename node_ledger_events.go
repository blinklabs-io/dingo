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
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"google.golang.org/protobuf/proto"
)

// syncStateLedger is the ledger state the sync state tracker reads.
type syncStateLedger interface {
	IsAtTip() bool
	ChainTipSlot() uint64
	SlotsBehindHead() uint64
}

// syncStateTracker publishes ledger.SyncStateEvent when the node moves
// between catching up to the chain tip and following it.
//
// Two sources drive it. The ledger latches IsAtTip the first time it applies
// blocks up to the tip, which covers a node without Genesis selection. The
// chain selector reports leaving and re-entering Genesis mode, the only
// signal that a caught-up node fell behind again. The ledger latch is
// consulted once: it never resets, so after a Genesis re-entry it would
// report caught up while the selector is still catching up.
type syncStateTracker struct {
	bus    *event.EventBus
	ledger func() syncStateLedger

	mu          sync.Mutex
	state       ledger.SyncState
	ledgerTipIn bool
}

func newSyncStateTracker(
	bus *event.EventBus,
	ledgerFn func() syncStateLedger,
) *syncStateTracker {
	return &syncStateTracker{
		bus:    bus,
		ledger: ledgerFn,
		state:  ledger.SyncStateSyncing,
	}
}

func (t *syncStateTracker) ledgerTipCommitted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ledgerTipIn {
		return
	}
	ls := t.ledger()
	if ls == nil || !ls.IsAtTip() {
		return
	}
	t.ledgerTipIn = true
	t.setLocked(ledger.SyncStateCaughtUp, ls)
}

func (t *syncStateTracker) genesisModeChanged(caughtUp bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	next := ledger.SyncStateSyncing
	if caughtUp {
		next = ledger.SyncStateCaughtUp
	}
	t.setLocked(next, t.ledger())
}

// setLocked publishes while holding t.mu: the sources arrive on separate
// subscriber goroutines, and publishing after unlocking could deliver two
// transitions in the opposite order to the one recorded.
func (t *syncStateTracker) setLocked(
	next ledger.SyncState,
	ls syncStateLedger,
) {
	if t.state == next {
		return
	}
	evt := ledger.SyncStateEvent{Previous: t.state, Current: next}
	if ls != nil {
		evt.TipSlot = ls.ChainTipSlot()
		evt.SlotsBehind = ls.SlotsBehindHead()
	}
	t.state = next
	t.bus.Publish(
		ledger.SyncStateEventType,
		event.NewEvent(ledger.SyncStateEventType, evt),
	)
}

// protocolParamsLedger is the ledger state the protocol parameter tracker
// reads.
type protocolParamsLedger interface {
	CurrentEpochAndPParams() (uint64, lcommon.ProtocolParameters)
}

// protocolParamsTracker publishes ledger.ProtocolParamsEvent when the
// parameters in force differ from the last ones it saw. Parameters change
// only at an epoch boundary, so it checks on each epoch transition. When the
// ledger crosses several boundaries before a check runs, the event reports
// the parameters in force at the check.
type protocolParamsTracker struct {
	bus    *event.EventBus
	logger *slog.Logger
	ledger func() protocolParamsLedger

	mu   sync.Mutex
	last *cardano.PParams
}

func newProtocolParamsTracker(
	bus *event.EventBus,
	logger *slog.Logger,
	ledgerFn func() protocolParamsLedger,
) *protocolParamsTracker {
	t := &protocolParamsTracker{bus: bus, logger: logger, ledger: ledgerFn}
	// The parameters in force at startup are the baseline, not a change.
	t.check(false)
	return t
}

func (t *protocolParamsTracker) check(publish bool) {
	ls := t.ledger()
	if ls == nil {
		return
	}
	epoch, params := ls.CurrentEpochAndPParams()
	if params == nil {
		return
	}
	// The utxorpc form is era-independent, so two parameter sets compare
	// field by field whatever era they belong to.
	compared, err := params.Utxorpc()
	if err != nil {
		t.logger.Warn("could not compare protocol parameters",
			"epoch", epoch, "error", err)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last != nil && proto.Equal(t.last, compared) {
		return
	}
	t.last = compared
	if !publish {
		return
	}
	t.bus.Publish(
		ledger.ProtocolParamsEventType,
		event.NewEvent(
			ledger.ProtocolParamsEventType,
			ledger.ProtocolParamsEvent{
				Epoch:  epoch,
				Params: params,
			},
		),
	)
}

// subscribeLedgerStateEvents wires the sync state and protocol parameter
// trackers. Both drive state with no resynchronization path, so their
// subscriptions block rather than detach.
func (n *Node) subscribeLedgerStateEvents() {
	syncTracker := newSyncStateTracker(n.eventBus, func() syncStateLedger {
		if ls := n.wallClockLedger.Load(); ls != nil {
			return ls
		}
		return nil
	})
	n.subscribeRequiredEvent(
		ledger.TipEventType,
		func(event.Event) { syncTracker.ledgerTipCommitted() },
	)
	n.subscribeRequiredEvent(
		chainselection.GenesisModeExitedEventType,
		func(event.Event) { syncTracker.genesisModeChanged(true) },
	)
	n.subscribeRequiredEvent(
		chainselection.GenesisModeEnteredEventType,
		func(event.Event) { syncTracker.genesisModeChanged(false) },
	)
	params := newProtocolParamsTracker(
		n.eventBus,
		n.config.logger,
		func() protocolParamsLedger {
			if ls := n.wallClockLedger.Load(); ls != nil {
				return ls
			}
			return nil
		},
	)
	n.subscribeRequiredEvent(
		event.EpochTransitionEventType,
		func(event.Event) { params.check(true) },
	)
}
