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

package chainsyncrecycler

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func testConnId(id uint) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: int(id),
		},
	}
}

func testTip(slot uint64, blockNumber uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte("hash")),
		BlockNumber: blockNumber,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// logSignalHandler signals on a per-message channel each time a matching
// record is logged, so tests can tell the tick-level panic recovery
// ("panic in stall checker tick, continuing") from the loop-level one
// ("panic in stall checker goroutine") instead of inferring it from the
// side effects, which are identical for both.
type logSignalHandler struct {
	signals map[string]chan struct{}
}

func newLogSignalHandler(messages ...string) logSignalHandler {
	signals := make(map[string]chan struct{}, len(messages))
	for _, message := range messages {
		signals[message] = make(chan struct{}, 1)
	}
	return logSignalHandler{signals: signals}
}

// signal returns the channel that fires when message is logged.
func (h logSignalHandler) signal(message string) chan struct{} {
	return h.signals[message]
}

func (h logSignalHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h logSignalHandler) Handle(_ context.Context, record slog.Record) error {
	ch, ok := h.signals[record.Message]
	if !ok {
		return nil
	}
	select {
	case ch <- struct{}{}:
	default:
	}
	return nil
}

func (h logSignalHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h logSignalHandler) WithGroup(string) slog.Handler {
	return h
}

// fakeLedger is a LedgerSource that reports fixed tips and records reconcile
// attempts, so plateau/backlog behavior can be exercised without a database.
type fakeLedger struct {
	mu                  sync.Mutex
	tip                 ochainsync.Tip
	atTip               bool
	securityParam       int
	primaryChainTipSlot uint64
	reconciled          bool
	reconcileErr        error
	reconcileReasons    []string
	reconcileConns      []ouroboros.ConnectionId
}

func (f *fakeLedger) Tip() ochainsync.Tip {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tip
}

func (f *fakeLedger) IsAtTip() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.atTip
}

func (f *fakeLedger) SecurityParam() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.securityParam
}

func (f *fakeLedger) PrimaryChainTipSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.primaryChainTipSlot
}

func (f *fakeLedger) setPrimaryChainTipSlot(slot uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.primaryChainTipSlot = slot
}

func (f *fakeLedger) ReconcileLivePrimaryChainLedgerDivergence(
	reason string,
	connId ouroboros.ConnectionId,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileReasons = append(f.reconcileReasons, reason)
	f.reconcileConns = append(f.reconcileConns, connId)
	return f.reconciled, f.reconcileErr
}

func (f *fakeLedger) reconcileCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reconcileReasons)
}

func (f *fakeLedger) lastReconcile() (string, ouroboros.ConnectionId) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reconcileReasons) == 0 {
		return "", ouroboros.ConnectionId{}
	}
	last := len(f.reconcileReasons) - 1
	return f.reconcileReasons[last], f.reconcileConns[last]
}

// fakeChainsyncState is a ChainsyncState returning a caller-controlled set of
// tracked clients.
type fakeChainsyncState struct {
	mu            sync.Mutex
	tracked       []chainsync.TrackedClient
	activeConn    *ouroboros.ConnectionId
	stalledChecks int
	rotationCalls int
}

func (f *fakeChainsyncState) CheckStalledClients() []ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stalledChecks++
	return nil
}

func (f *fakeChainsyncState) AdvanceHeaderSyncRotation() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rotationCalls++
}

func (f *fakeChainsyncState) GetTrackedClients() []chainsync.TrackedClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tracked
}

func (f *fakeChainsyncState) GetClientConnId() *ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.activeConn
}

func (f *fakeChainsyncState) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stalledChecks, f.rotationCalls
}

// fakeChainSelector is a ChainSelector with a fixed best peer and peer tips.
type fakeChainSelector struct {
	mu                sync.Mutex
	bestPeer          *ouroboros.ConnectionId
	peerTips          map[string]*chainselection.PeerChainTip
	localTip          ochainsync.Tip
	securityParam     uint64
	securityParamSets int
}

func (f *fakeChainSelector) SetLocalTip(tip ochainsync.Tip) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localTip = tip
}

func (f *fakeChainSelector) SetSecurityParam(k uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.securityParam = k
	f.securityParamSets++
}

func (f *fakeChainSelector) GetBestPeer() *ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bestPeer
}

func (f *fakeChainSelector) GetPeerTip(
	connId ouroboros.ConnectionId,
) *chainselection.PeerChainTip {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peerTips[connId.String()]
}

func (f *fakeChainSelector) observed() (ochainsync.Tip, uint64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.localTip, f.securityParam, f.securityParamSets
}

// publishedEvent records one publish call against the fake publisher.
type publishedEvent struct {
	eventType event.EventType
	evt       event.Event
	async     bool
}

type fakePublisher struct {
	mu     sync.Mutex
	events []publishedEvent
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{}
}

func (f *fakePublisher) Publish(eventType event.EventType, evt event.Event) {
	f.record(publishedEvent{eventType: eventType, evt: evt})
}

func (f *fakePublisher) PublishAsync(
	eventType event.EventType,
	evt event.Event,
) bool {
	f.record(publishedEvent{eventType: eventType, evt: evt, async: true})
	return true
}

func (f *fakePublisher) record(pe publishedEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, pe)
}

func (f *fakePublisher) all() []publishedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publishedEvent(nil), f.events...)
}

func (f *fakePublisher) byType(eventType event.EventType) []publishedEvent {
	var out []publishedEvent
	for _, pe := range f.all() {
		if pe.eventType == eventType {
			out = append(out, pe)
		}
	}
	return out
}

// fakeComponents is a ComponentProvider handing out a fixed component set. It
// mirrors the node adapter's contract: when available is false the callback is
// never invoked and the tick is skipped.
type fakeComponents struct {
	mu        sync.Mutex
	live      LiveComponents
	available bool
	calls     int
}

func newFakeComponents(live LiveComponents) *fakeComponents {
	return &fakeComponents{live: live, available: true}
}

func (f *fakeComponents) WithLiveComponents(fn func(LiveComponents)) bool {
	f.mu.Lock()
	available := f.available
	live := f.live
	f.calls++
	f.mu.Unlock()
	if !available {
		return false
	}
	fn(live)
	return true
}

func (f *fakeComponents) setAvailable(available bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.available = available
}

func (f *fakeComponents) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// stalledClient builds a tracked client in the stalled state.
func stalledClient(
	connId ouroboros.ConnectionId,
	observabilityOnly bool,
) chainsync.TrackedClient {
	return chainsync.TrackedClient{
		ConnId:            connId,
		Status:            chainsync.ClientStatusStalled,
		ObservabilityOnly: observabilityOnly,
		LastActivity:      time.Now(),
	}
}

// activeClient builds a tracked client that is not stalled.
func activeClient(
	connId ouroboros.ConnectionId,
	cursorSlot uint64,
) chainsync.TrackedClient {
	return chainsync.TrackedClient{
		ConnId:       connId,
		Cursor:       ocommon.NewPoint(cursorSlot, []byte("cursor")),
		Status:       chainsync.ClientStatusSyncing,
		LastActivity: time.Now(),
	}
}
