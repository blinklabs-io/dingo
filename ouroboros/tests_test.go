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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gconnection "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/blinklabs-io/ouroboros-mock/consensus"
	"github.com/blinklabs-io/ouroboros-mock/consensus/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsensusConformanceVectors replays the upstream consensus-
// conformance corpus (vectors + Replayer harness embedded in
// ouroboros-mock) against dingo's real chainsync ingestion path and
// chain selector. It lives in package ouroboros so it can drive the
// unexported chainsync handlers directly — no exported test hooks leak
// into production code.
func TestConsensusConformanceVectors(t *testing.T) {
	t.Parallel()

	vectors, err := consensus.CapturedVectors()
	if err != nil {
		t.Fatalf("CapturedVectors: %v", err)
	}
	const expectedScenarioCount = 7
	if len(vectors) != expectedScenarioCount {
		t.Fatalf(
			"consensus profile has %d scenarios, want %d; update the profile summary and tests with the shared corpus",
			len(vectors),
			expectedScenarioCount,
		)
	}
	expectedNames := map[string]bool{
		"intersect_origin_one_rollforward": true,
		"within_k_fork_v1":                 true,
		"fork_and_select_v1":               true,
		"slot_battle_v1":                   true,
		"exceeds_k_no_switch_v1":           true,
		"intersect_non_origin_v1":          true,
		"within_k_fork_winner_first_v1":    true,
	}
	profileCounts := map[string]int{
		"single-peer": 0,
		"fork-switch": 0,
		"no-switch":   0,
	}
	for _, cv := range vectors {
		if !expectedNames[cv.Name] {
			t.Fatalf("unexpected consensus scenario %q", cv.Name)
		}
		delete(expectedNames, cv.Name)
		switch {
		case len(cv.Vector.Capture.Peers) == 1:
			profileCounts["single-peer"]++
		case cv.Vector.Capture.ExpectedOutput.ExpectedRollback != nil:
			profileCounts["fork-switch"]++
		default:
			profileCounts["no-switch"]++
		}
	}
	if len(expectedNames) != 0 {
		t.Fatalf("consensus profile is missing scenarios: %v", expectedNames)
	}
	t.Logf(
		"Consensus conformance profile: total=%d single-peer=%d fork-switch=%d no-switch=%d",
		len(vectors),
		profileCounts["single-peer"],
		profileCounts["fork-switch"],
		profileCounts["no-switch"],
	)
	for _, cv := range vectors {
		t.Run(cv.Name, func(t *testing.T) {
			a := newReplayAdapter(t, cv.Vector.Capture)
			if err := consensus.RunConsensusVector(
				t, cv.Vector, a,
			); err != nil {
				t.Fatalf("%s: %v", cv.Vector.Title, err)
			}
			// Guard against a vacuous green: had the ingress-eligibility
			// gate dropped every header, the selector would have tracked
			// no peers and the run above would have failed with "no best
			// tip" — but assert ingestion explicitly so a future
			// regression is unambiguous rather than silently passing.
			if a.headersFed == 0 || a.tipEventsSeen == 0 {
				t.Fatalf(
					"vacuous replay: headersFed=%d tipEventsSeen=%d "+
						"(ingress eligibility likely dropped everything)",
					a.headersFed, a.tipEventsSeen,
				)
			}
		})
	}
}

// TestConsensusConformanceKGuardIsLive proves the k configuration is
// genuinely applied (not silently k=0): replaying a vector that carries
// security_param>0 with local_tip *cleared* must reject the far-ahead peer
// and fail the final_tip assertion. If this passed, the main conformance
// run would be a vacuous k=0 test in disguise.
func TestConsensusConformanceKGuardIsLive(t *testing.T) {
	t.Parallel()

	vectors, err := consensus.CapturedVectors()
	if err != nil {
		t.Fatalf("CapturedVectors: %v", err)
	}
	exercised := 0
	for _, cv := range vectors {
		if cv.Vector.Capture == nil ||
			cv.Vector.Capture.SecurityParam == 0 ||
			cv.Vector.Capture.LocalTip == nil {
			continue // only vectors whose pass depends on local_tip
		}
		exercised++
		// Same vector, but with local_tip removed: the implausibility
		// guard must now reject the peer leading by more than k.
		capNoLocal := *cv.Vector.Capture
		capNoLocal.LocalTip = nil
		v := cv.Vector
		v.Capture = &capNoLocal
		a := newReplayAdapter(t, &capNoLocal)
		err := consensus.RunConsensusVector(t, v, a)
		if err == nil {
			t.Fatalf(
				"%s: k=%d replay passed with local_tip cleared — "+
					"SecurityParam is not actually being applied",
				cv.Name, capNoLocal.SecurityParam,
			)
		}
		// A non-nil error on its own is not proof the k-guard fired: an
		// unrelated failure (e.g. a header that fails to decode) also
		// errors, but RunConsensusVector returns at that header — before it
		// reaches Stabilize and the chain selector — so headersFed and
		// tipEventsSeen stay at zero. Require that every header was ingested
		// and the selector actually evaluated the peer tips; the only
		// failure left once selection has run is the selector declining to
		// adopt the far-ahead peer, which is exactly the k-guard rejection
		// this test asserts. Without this gate an incidental ingestion
		// regression could masquerade as a live k-guard.
		if a.headersFed == 0 || a.tipEventsSeen == 0 {
			t.Fatalf(
				"%s: k=%d replay errored before reaching chain selection "+
					"(headersFed=%d tipEventsSeen=%d) — not evidence the "+
					"k-guard rejected the peer: %v",
				cv.Name, capNoLocal.SecurityParam,
				a.headersFed, a.tipEventsSeen, err,
			)
		}
		t.Logf(
			"%s: k=%d without local_tip rejected by the k-guard as "+
				"expected: %v",
			cv.Name, capNoLocal.SecurityParam, err,
		)
	}
	if exercised == 0 {
		t.Skip("no vector carries both security_param>0 and local_tip")
	}
}

func TestSelectedPeerTraceUsesPeerIdentityForEqualTips(t *testing.T) {
	t.Parallel()
	tip := format.Tip{Slot: 10, Hash: format.HexBytes{0xaa}, BlockNumber: 10}
	first := format.ServedMessage{
		Protocol:   format.ProtocolChainSync,
		MsgType:    format.ChainSyncMsgRollForward,
		Tip:        &tip,
		HeaderCbor: format.HexBytes{0x01},
	}
	second := first
	second.HeaderCbor = format.HexBytes{0x02}
	capture := &format.ConsensusCapture{Peers: []format.PeerInput{
		{PeerID: 1, Served: []format.ServedMessage{first}},
		{PeerID: 2, Served: []format.ServedMessage{second}},
	}}
	a := newReplayAdapter(t, capture)
	selected := a.connFor(2)
	other := a.connFor(1)
	chainTip := toGouroborosTip(tip)
	require.True(t, a.cs.UpdatePeerTip(selected, chainTip, nil))
	require.True(t, a.cs.UpdatePeerTip(other, chainTip, nil))
	require.Equal(t, selected, *a.cs.GetBestPeer())

	require.Equal(t, capture.Peers[1].Served, a.selectedPeerTrace())
}

const (
	tipEventBuffer    = 4096
	switchEventBuffer = 1024
	// switchBarrierTimeout bounds the wait for the barrier below. It is a
	// deadlock bound, not a settling delay: the barrier is already queued
	// behind the switches when the wait starts, so the normal cost is one
	// lane hand-off.
	switchBarrierTimeout = 30 * time.Second
)

// switchBarrier is a sentinel published through the chain-switch ordered lane
// so Stabilize can tell "no switch was decided" from "the switch has not been
// delivered yet".
//
// ChainSelector.publishSelection routes chain switches through
// EventBus.PublishOrdered, so EvaluateAndSwitch
// returns before the lane worker has handed them to subscribers. A lane is a
// FIFO drained by exactly one worker, so a sentinel enqueued after those
// switches is delivered after them: receiving it back is proof that every
// switch published earlier on this goroutine has already reached the
// subscription. Its Data type is not ChainSwitchEvent, so it is skipped rather
// than recorded as a decision.
type switchBarrier struct{}

// replayAdapter implements consensus.Replayer by driving dingo's real
// chainsync handlers and chain selector. The harness identifies peers by
// the vector's peer_id; the adapter synthesizes a stable ConnectionId per
// peer_id.
type replayAdapter struct {
	t          *testing.T
	o          *Ouroboros
	cs         *chainselection.ChainSelector
	bus        *event.EventBus
	conns      map[uint64]ouroboros.ConnectionId
	capture    *format.ConsensusCapture
	tipCh      <-chan event.Event
	switchCh   <-chan event.Event
	switches   []format.SwitchEvent
	downstream []format.ServedMessage

	headersFed    int
	tipEventsSeen int
}

func newReplayAdapter(
	t *testing.T, capture *format.ConsensusCapture,
) *replayAdapter {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	// SecurityParam (k) and LocalTip come from the vector, not from
	// adapter constants, so each scenario replays under the SUT
	// configuration it was forged for. k=0 / nil LocalTip — the
	// default for older vectors — reproduces the prior k-disabled
	// behaviour.
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		EventBus:                  bus,
		SecurityParam:             capture.SecurityParam,
		DisableEventSubscriptions: true,
	})
	if capture.LocalTip != nil {
		// Arms the implausibility-guard catch-up relaxation so a peer
		// leading by more than k is not rejected as a spoof — see the
		// LocalTip doc in the format package.
		cs.SetLocalTip(toGouroborosTip(*capture.LocalTip))
	}
	// Subscribe to the selector's input (peer tip updates) and output
	// (chain switches) with our own channels so Stabilize can drive the
	// selector synchronously, rather than racing the async SubscribeFunc
	// delivery the production node uses. Buffers are sized well beyond any
	// single vector's message count so no event is dropped before drained.
	_, tipCh := bus.SubscribeWithBuffer(
		chainselection.PeerTipUpdateEventType, tipEventBuffer,
	)
	_, switchCh := bus.SubscribeWithBuffer(
		chainselection.ChainSwitchEventType, switchEventBuffer,
	)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		// Open the ingress-eligibility gate: the captured peers are the
		// upstreams we want feeding selection. With ChainsyncState left
		// nil, reconcileChainsyncIngressAdmission honours this directly.
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.eventBus = bus
	return &replayAdapter{
		t:        t,
		o:        o,
		cs:       cs,
		bus:      bus,
		conns:    make(map[uint64]ouroboros.ConnectionId),
		capture:  capture,
		tipCh:    tipCh,
		switchCh: switchCh,
	}
}

func (a *replayAdapter) RollForward(
	peerID uint64, era uint, headerCbor []byte, tip format.Tip,
) error {
	hdr, err := gledger.NewBlockHeaderFromCbor(era, headerCbor)
	if err != nil {
		return fmt.Errorf("decode header (era %d): %w", era, err)
	}
	if err := a.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: a.connFor(peerID)},
		era, hdr, toGouroborosTip(tip),
	); err != nil {
		return err
	}
	a.headersFed++
	return nil
}

func (a *replayAdapter) RollBackward(
	peerID uint64, point format.Point, tip format.Tip,
) error {
	connId := a.connFor(peerID)
	rollbackPoint := toGouroborosPoint(point)
	rollbackTip := toGouroborosTip(tip)
	if err := a.o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: connId},
		rollbackPoint,
		rollbackTip,
	); err != nil {
		return err
	}
	a.cs.HandlePeerRollbackEvent(event.NewEvent(
		chainselection.PeerRollbackEventType,
		chainselection.PeerRollbackEvent{
			ConnectionId: connId,
			Point:        rollbackPoint,
			Tip:          rollbackTip,
		},
	))
	return nil
}

func (a *replayAdapter) Stabilize() {
	a.t.Helper()
	// Drain queued peer-tip updates into the selector, force a synchronous
	// evaluation, then collect any switch decisions it emitted. No sleeps
	// and no polling.
	//
	// chainselection.peer_tip_update is still published inline, on this
	// goroutine, by chainsyncClientRollForward, so every tip update is
	// already queued by the time Stabilize runs and a non-blocking drain
	// sees all of them. Chain switches are not: they go through an ordered
	// lane, so they need the barrier below.
	drainEvents(a.tipCh, func(evt event.Event) {
		a.tipEventsSeen++
		a.cs.HandlePeerTipUpdateEvent(evt)
	})
	a.cs.EvaluateAndSwitch()
	a.collectSwitchesThroughBarrier()
	a.downstream = a.observeDownstream(a.selectedPeerTrace())
}

// observeDownstream serves the selected peer's chain from Dingo's ChainSync
// server to a node-to-node client that intersects where the selected peer's
// trace starts, and returns what the server sent before its first AwaitReply.
// A trace that opens with a RollBackward to a block intersected above origin;
// the server cannot find that point without a block there, so a stand-in
// block with the point's slot and hash anchors the chain one block below the
// first header served. The stand-in is never served: the client intersects at
// it, and the first header must extend it.
//
// The replay has headers and no ledger, so the harness stands in for block
// fetch: each header the selected peer rolled forward is added to the server's
// chain as a block whose CBOR is a one-element array holding that header. A
// node-to-node RollForward carries only a block's first element, so this is
// everything a downstream peer could observe. The ledger tip is set to the
// tip the selector adopted, which is the tip the harness's final_tip assertion
// reads: a peer can advertise a tip beyond the last header it served.
func (a *replayAdapter) observeDownstream(
	selected []format.ServedMessage,
) []format.ServedMessage {
	a.t.Helper()
	if len(selected) == 0 {
		return nil
	}
	f := newChainsyncServerFixture(a.t, csmock.ModeNtN)
	ls := f.o.ledgerState
	intersect := ocommon.NewPointOrigin()
	if m := selected[0]; m.MsgType == format.ChainSyncMsgRollBackward &&
		len(m.Point.Hash) > 0 {
		intersect = toGouroborosPoint(*m.Point)
	}
	anchored := len(intersect.Hash) == 0
	for _, m := range selected {
		switch m.MsgType {
		case format.ChainSyncMsgRollForward:
			hdr, err := gledger.NewBlockHeaderFromCbor(*m.Era, m.HeaderCbor)
			require.NoError(a.t, err)
			if !anchored {
				require.Positive(
					a.t,
					hdr.BlockNumber(),
					"header extending a non-origin intersect has block number 0",
				)
				require.NoError(
					a.t,
					ls.Chain().AddBlock(a.t.Context(), &testBlock{
						BlockHeader: &testBlockHeader{
							hash:        gledger.NewBlake2b256(intersect.Hash),
							slotNumber:  intersect.Slot,
							blockNumber: hdr.BlockNumber() - 1,
						},
						blockType: int(
							gledger.BlockHeaderToBlockTypeMap[*m.Era],
						),
						cbor: []byte{0x80},
					}, nil),
				)
				anchored = true
			}
			blockCbor, err := cbor.Encode([]cbor.RawMessage{
				cbor.RawMessage(m.HeaderCbor),
			})
			require.NoError(a.t, err)
			require.NoError(a.t, ls.Chain().AddBlock(a.t.Context(), &testBlock{
				BlockHeader: hdr,
				blockType:   int(gledger.BlockHeaderToBlockTypeMap[*m.Era]),
				cbor:        blockCbor,
			}, nil))
		case format.ChainSyncMsgRollBackward:
			if !anchored {
				continue
			}
			require.NoError(
				a.t,
				ls.Chain().Rollback(a.t.Context(), toGouroborosPoint(*m.Point)),
			)
		}
	}
	bestTip, ok := a.BestTip()
	require.True(a.t, ok, "selector has no best tip")
	ls.SetTipForTesting(toGouroborosTip(bestTip))

	require.NoError(a.t, f.h.FindIntersect([]ocommon.Point{intersect}))
	require.True(
		a.t,
		f.observe(a.t).IsIntersectFound(),
		"expected IntersectFound",
	)
	var served []format.ServedMessage
	for {
		require.NoError(a.t, f.h.RequestNext())
		msg := f.observe(a.t)
		if msg.IsAwaitReply() {
			return served
		}
		tip, ok := msg.Tip()
		require.True(a.t, ok, "server message %d carries no tip", msg.Type())
		formatTip := fromGouroborosTip(tip)
		out := format.ServedMessage{
			Protocol: format.ProtocolChainSync,
			Tip:      &formatTip,
		}
		if header, _, ok := msg.RollForwardNtN(); ok {
			era := header.Era
			out.MsgType = format.ChainSyncMsgRollForward
			out.Era = &era
			out.HeaderCbor = header.HeaderCbor()
		} else {
			point, ok := msg.Point()
			require.True(a.t, msg.IsRollBackward() && ok,
				"unexpected server message type %d", msg.Type())
			formatPoint := fromGouroborosPoint(point)
			out.MsgType = format.ChainSyncMsgRollBackward
			out.Point = &formatPoint
		}
		served = append(served, out)
	}
}

// collectSwitchesThroughBarrier records every chain switch the selector has
// published so far, using a sentinel enqueued behind them as the drain barrier.
// See switchBarrier for why a non-blocking drain is not one.
func (a *replayAdapter) collectSwitchesThroughBarrier() {
	a.t.Helper()
	if !a.bus.PublishOrdered(
		chainselection.ChainSwitchEventType,
		event.NewEvent(chainselection.ChainSwitchEventType, switchBarrier{}),
	) {
		a.t.Fatal("event bus refused the chain-switch barrier")
	}
	for {
		evt := testutil.RequireReceive(
			a.t,
			a.switchCh,
			switchBarrierTimeout,
			"chain-switch barrier",
		)
		switch e := evt.Data.(type) {
		case switchBarrier:
			return
		case chainselection.ChainSwitchEvent:
			sw := format.SwitchEvent{
				PreviousTip: fromGouroborosTip(e.PreviousTip),
				NewTip:      fromGouroborosTip(e.NewTip),
			}
			if e.RollbackPoint != nil {
				point := fromGouroborosPoint(*e.RollbackPoint)
				sw.RollbackPoint = &point
			}
			a.switches = append(a.switches, sw)
		default:
			// Only the selector and the barrier above publish on this
			// lane, so anything else is a bug in one of them. Skipping it
			// would still terminate -- the barrier is behind it in the
			// same FIFO -- but it would drop a switch decision the
			// harness then reports as "never switched", which is a much
			// worse diagnosis than naming the payload.
			a.t.Fatalf(
				"unexpected %T on the chain_switch lane", evt.Data,
			)
		}
	}
}

func (a *replayAdapter) BestTip() (format.Tip, bool) {
	best := a.cs.GetBestPeer()
	if best == nil {
		return format.Tip{}, false
	}
	pt := a.cs.GetPeerTip(*best)
	if pt == nil {
		return format.Tip{}, false
	}
	return fromGouroborosTip(pt.Tip), true
}

func (a *replayAdapter) DrainSwitchEvents() []format.SwitchEvent {
	return a.switches
}

func (a *replayAdapter) DrainDownstreamChainSync() []format.ServedMessage {
	return a.downstream
}

func (a *replayAdapter) selectedPeerTrace() []format.ServedMessage {
	best := a.cs.GetBestPeer()
	if best == nil {
		return nil
	}
	for _, peer := range a.capture.Peers {
		if a.connFor(peer.PeerID) == *best {
			return cloneServedMessages(peer.Served)
		}
	}
	return nil
}

func cloneServedMessages(
	messages []format.ServedMessage,
) []format.ServedMessage {
	cloned := make([]format.ServedMessage, len(messages))
	for i, message := range messages {
		cloned[i] = message
		cloned[i].HeaderCbor = append(
			format.HexBytes(nil),
			message.HeaderCbor...)
		if message.Tip != nil {
			tip := *message.Tip
			tip.Hash = append(format.HexBytes(nil), message.Tip.Hash...)
			cloned[i].Tip = &tip
		}
		if message.Point != nil {
			point := *message.Point
			point.Hash = append(format.HexBytes(nil), message.Point.Hash...)
			cloned[i].Point = &point
		}
	}
	return cloned
}

func (a *replayAdapter) connFor(peerID uint64) ouroboros.ConnectionId {
	if id, ok := a.conns[peerID]; ok {
		return id
	}
	id := newTestConnId(
		"127.0.0.1:3001",
		fmt.Sprintf("10.0.0.%d:3001", peerID+1),
	)
	a.conns[peerID] = id
	return id
}

func drainEvents(ch <-chan event.Event, f func(event.Event)) {
	for {
		select {
		case evt := <-ch:
			f(evt)
		default:
			return
		}
	}
}

func toGouroborosPoint(p format.Point) ocommon.Point {
	return ocommon.Point{Slot: p.Slot, Hash: append([]byte(nil), p.Hash...)}
}

func fromGouroborosPoint(p ocommon.Point) format.Point {
	return format.Point{
		Slot: p.Slot,
		Hash: append(format.HexBytes(nil), p.Hash...),
	}
}

func toGouroborosTip(t format.Tip) ochainsync.Tip {
	return ochainsync.Tip{
		Point: toGouroborosPoint(
			format.Point{Slot: t.Slot, Hash: t.Hash},
		),
		BlockNumber: t.BlockNumber,
	}
}

func fromGouroborosTip(t ochainsync.Tip) format.Tip {
	p := fromGouroborosPoint(t.Point)
	return format.Tip{Slot: p.Slot, Hash: p.Hash, BlockNumber: t.BlockNumber}
}

// testDijkstraAnnouncementHeaderRawFor builds a ranking-block header
// announcing ebHash/ebSize at the given slot, mirroring
// testDijkstraAnnouncementHeaderRaw but for a caller-supplied endorser-block
// identity so tests can bind an announcement to a real, independently
// computed endorser-block hash.
func testDijkstraAnnouncementHeaderRawFor(
	t *testing.T,
	slot uint64,
	ebHash lcommon.Blake2b256,
	ebSize uint64,
) []byte {
	t.Helper()
	_, blockRaw := testDijkstraBlockRaw(t, int(slot))
	var components []cbor.RawMessage
	_, err := cbor.Decode(blockRaw, &components)
	require.NoError(t, err)
	require.Len(t, components, 2)

	var headerTop []cbor.RawMessage
	_, err = cbor.Decode(components[0], &headerTop)
	require.NoError(t, err)
	require.Len(t, headerTop, 2)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(headerTop[0], &headerBody)
	require.NoError(t, err)
	// A Dijkstra header body has exactly 12 fields; the last two are
	// leios_certified and eb_references_announcement, so replace them.
	require.Len(t, headerBody, 12)
	headerBody = append(
		headerBody[:10],
		mustCbor(t, false),
		mustCbor(t, []any{ebHash.Bytes(), ebSize}),
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err := cbor.Encode(headerTop)
	require.NoError(t, err)
	return headerRaw
}

// recordTestLeiosAnnouncement decodes headerRaw and records it as an
// announcement, mirroring the `record` helper in
// TestLeiosNotifyBlockAnnouncementIsConsumedAndDeduplicated.
func recordTestLeiosAnnouncement(
	t *testing.T,
	o *Ouroboros,
	headerRaw []byte,
) {
	t.Helper()
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(headerRaw)
	require.NoError(t, err)
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	require.True(t, ok)
	require.NoError(
		t,
		o.recordLeiosAnnouncement(
			headerRaw,
			ebHash,
			ebSize,
			header,
			"test",
			false,
		),
	)
}

// recordTestLeiosAnnouncementNoFail is recordTestLeiosAnnouncement's
// error-returning twin, for use from a worker goroutine: require's t.FailNow
// is documented as unsafe to call from any goroutine other than the one
// running the test function, so a concurrent caller must collect the error
// and assert on it back on the test goroutine instead.
func recordTestLeiosAnnouncementNoFail(o *Ouroboros, headerRaw []byte) error {
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(headerRaw)
	if err != nil {
		return err
	}
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	if !ok {
		return errors.New("header carries no leios announcement")
	}
	return o.recordLeiosAnnouncement(
		headerRaw,
		ebHash,
		ebSize,
		header,
		"test",
		false,
	)
}

// announceTestEndorserBlock records the announcement binding ebHash to slot.
func announceTestEndorserBlock(
	t *testing.T,
	o *Ouroboros,
	slot uint64,
	ebHash lcommon.Blake2b256,
	ebSize int,
) {
	t.Helper()
	recordTestLeiosAnnouncement(
		t,
		o,
		testDijkstraAnnouncementHeaderRawFor(t, slot, ebHash, uint64(ebSize)),
	)
}

func testEbHash(point ocommon.Point) lcommon.Blake2b256 {
	var ebHash lcommon.Blake2b256
	copy(ebHash[:], point.Hash)
	return ebHash
}

// TestStoreLeiosEndorserBlockAcceptsDifferentSlotOfSameHashWhileFirstIsLive
// is the wolf31o2 regression: the manifest is content-addressed, so the same
// hash can be a live, independently required occurrence at more than one
// slot at once (two elections producing an identical transaction-reference
// set), and both must be independently storable and verifiable through
// their own announcements. Rejecting the second occurrence just because a
// live announcement already exists for the hash at a different slot would
// drop that occurrence's offer/fetch and endorser data for whichever ranking
// block referenced it.
func TestStoreLeiosEndorserBlockAcceptsDifferentSlotOfSameHashWhileFirstIsLive(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 7, 1)
	second := ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash}
	txsRaw := []cbor.RawMessage{mustCbor(t, "tx0")}

	// This test covers announcement lock ordering; the injected validation
	// gate is tested separately.
	o := newOuroboros(OuroborosConfig{EnableLeios: false})
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	// A second, independent announcement of the same hash at a different
	// slot, while the first is still live (no expiry involved at all).
	announceTestEndorserBlock(
		t,
		o,
		second.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))
	require.NoError(t, o.storeLeiosEndorserBlock(
		second,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))

	firstData, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(
		t,
		firstData.slotVerified,
		"the first occurrence must bind to its own announcement",
	)

	secondData, ok := o.lookupLeiosEndorserBlock(second.Slot, second.Hash)
	require.True(t, ok)
	require.True(
		t,
		secondData.slotVerified,
		"the second occurrence must independently bind to its own announcement",
	)

	require.NotSame(
		t,
		firstData,
		secondData,
		"the two live occurrences must be tracked independently, not collapsed into one",
	)

	// Both occurrences must be independently available to the ledger
	// provider at once -- the concrete "offer/fetch and endorser data become
	// available" property wolf31o2's review asked for.
	_, ok = o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, ok, "the first occurrence must reach the ledger")
	_, ok = o.EndorserBlockTxsByHash(second.Hash, second.Slot)
	require.True(t, ok, "the second occurrence must reach the ledger too")
}

// TestStoreLeiosEndorserBlockAcceptsAnnouncedPointAndIsIdempotent covers the
// companion acceptance criteria: a store matching the announced point
// succeeds, and retransmitting the identical store (as every connection
// offering the block does) remains idempotent.
func TestStoreLeiosEndorserBlockAcceptsAnnouncedPointAndIsIdempotent(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 11)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	// Simulates a second connection re-offering the identical, correctly
	// bound endorser block: retransmission of a valid entry must not error.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
	require.True(t, data.slotVerified)
}

// TestStoreLeiosEndorserBlockCrossConnectionDifferentSlotsCoexistRegardlessOfOrder
// covers cross-connection arrival order for two live occurrences of the same
// hash: whichever connection's offer is stored first, a later offer for the
// same hash at a different, independently announced slot is accepted as its
// own occurrence rather than rejected, and neither disturbs the other.
// The test covers both arrival orders so either occurrence can arrive first.
func TestStoreLeiosEndorserBlockCrossConnectionDifferentSlotsCoexistRegardlessOfOrder(
	t *testing.T,
) {
	t.Parallel()

	for _, secondFirst := range []bool{false, true} {
		name := "first-then-second"
		if secondFirst {
			name = "second-then-first"
		}
		t.Run(name, func(t *testing.T) {
			point, blockRaw := testLeiosEndorserBlockRaw(t, 13)
			second := ocommon.Point{Slot: point.Slot + 5, Hash: point.Hash}

			o := newOuroboros(OuroborosConfig{EnableLeios: true})
			announceTestEndorserBlock(
				t,
				o,
				point.Slot,
				testEbHash(point),
				len(blockRaw),
			)
			announceTestEndorserBlock(
				t,
				o,
				second.Slot,
				testEbHash(point),
				len(blockRaw),
			)

			store := func(p ocommon.Point) {
				require.NoError(t, o.storeLeiosEndorserBlock(
					p,
					blockRaw,
					nil,
					leiosStorePeerOffered,
				))
			}
			if secondFirst {
				store(second)
				store(point)
			} else {
				store(point)
				store(second)
			}

			data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
			require.True(t, ok)
			require.Equal(
				t,
				point.Slot,
				data.point.Slot,
				"the first occurrence must be unharmed by the second's arrival",
			)
			require.True(t, data.slotVerified)
			secondData, ok := o.lookupLeiosEndorserBlock(
				second.Slot,
				second.Hash,
			)
			require.True(t, ok)
			require.True(t, secondData.slotVerified)
		})
	}
}

// TestPeerOfferedStoreWithheldUntilAnnouncementBindsIt is the reverse-order
// regression: the relay -- and dingo's own forge path -- queue the block offer
// before the ranking-block announcement, so an authentic manifest is routinely
// stored while no announcement exists yet. Nothing keyed on the peer-supplied
// slot may be published until an announcement corroborates it.
func TestPeerOfferedStoreWithheldUntilAnnouncementBindsIt(t *testing.T) {
	t.Parallel()

	txRaw := cbor.RawMessage{0x82, 0xa0, 0xa0}
	ref := lcommon.LeiosTransactionReference{
		TransactionHash: lcommon.Blake2b256Hash(txRaw),
		TransactionSize: uint16(len(txRaw)),
	}
	blockRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	})
	require.NoError(t, err)
	point := ocommon.NewPoint(41, lcommon.Blake2b256Hash(blockRaw).Bytes())

	ledger := &fakeLeiosAnnouncementLedger{}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: ledger,
	})
	defer func() { require.NoError(t, o.Close()) }()
	votes := &fakeLeiosVoteHandler{}
	o.leiosVotes = votes

	// Offer arrives first, before any announcement.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	require.Empty(
		t,
		votes.ebs,
		"an unverified peer-supplied slot must not drive vote emission",
	)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok, "the block is still cached so its txs can be fetched")
	require.False(t, data.slotVerified)

	// The matching announcement then binds it and releases publication.
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.slotVerified)
	require.Empty(t, votes.ebs)

	// The manifest references one transaction, while the first offer carried
	// only the manifest. Supplying the transaction completes the cache and
	// starts the semantic validation gate.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{txRaw},
		leiosStorePeerOffered,
	))
	require.Eventually(t, func() bool {
		votes.mu.Lock()
		defer votes.mu.Unlock()
		return len(votes.ebs) == 1
	}, time.Second, time.Millisecond)
	votes.mu.Lock()
	defer votes.mu.Unlock()
	require.Equal(t, point.Slot, votes.ebs[0].slot)
}

func TestPeerOfferedLedgerInvalidEndorserBlockIsNotVoted(t *testing.T) {
	t.Parallel()

	txRaw := cbor.RawMessage{0x82, 0xa0, 0xa0}
	ref := lcommon.LeiosTransactionReference{
		TransactionHash: lcommon.Blake2b256Hash(txRaw),
		TransactionSize: uint16(len(txRaw)),
	}
	blockRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	})
	require.NoError(t, err)
	point := ocommon.NewPoint(41, lcommon.Blake2b256Hash(blockRaw).Bytes())
	ledger := &fakeLeiosAnnouncementLedger{
		txValidationErr: errors.New(
			"ledger-invalid endorser-block transaction",
		),
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: ledger,
	})
	defer func() { require.NoError(t, o.Close()) }()
	votes := &fakeLeiosVoteHandler{}
	o.leiosVotes = votes
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{txRaw},
		leiosStorePeerOffered,
	))
	o.leiosValidationWG.Wait()
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.slotVerified)
	require.Equal(t, leiosEBValidationInvalid, data.semanticValidationStatus)
	require.Empty(
		t,
		votes.ebs,
		"a hash- and size-valid endorser block with a ledger-invalid transaction must not be voted on",
	)
}

// TestPeerOfferedStoreUnderFabricatedSlotStaysPermanentlyUnverified is the
// core attack in its store-first ordering: a peer offers an
// authentic, correctly-hashed manifest under a slot of its choosing before
// the genuine announcement arrives. The fabricated slot must never be voted
// on or reach the ledger. Unlike the pre-composite-key design, the genuine
// announcement for the real slot does not evict the fabricated entry -- they
// are now independent (slot, hash) occurrences -- so the fabricated entry
// simply sits cached but permanently unverified (until its own TTL prunes
// it), which is exactly as inert as if it had been evicted: nothing keyed on
// its slot is ever published.
func TestPeerOfferedStoreUnderFabricatedSlotStaysPermanentlyUnverified(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 41)
	fabricated := ocommon.Point{Slot: 42, Hash: point.Hash}

	o := newOuroboros(OuroborosConfig{EnableLeios: false})
	votes := &fakeLeiosVoteHandler{}
	o.leiosVotes = votes

	require.NoError(t, o.storeLeiosEndorserBlock(
		fabricated,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	require.Empty(
		t,
		votes.ebs,
		"a fabricated slot must not reach the vote handler",
	)

	// The genuine announcement is for a different slot; it has nothing to do
	// with the fabricated occurrence's own (slot, hash) key.
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	fabricatedData, ok := o.lookupLeiosEndorserBlock(
		fabricated.Slot,
		fabricated.Hash,
	)
	require.True(
		t,
		ok,
		"the fabricated entry is not evicted by an unrelated announcement",
	)
	require.False(
		t,
		fabricatedData.slotVerified,
		"the fabricated slot must never become verified",
	)
	require.Empty(t, votes.ebs)

	// The ledger must not be handed the fabricated slot either.
	_, provOk := o.EndorserBlockTxsByHash(fabricated.Hash, fabricated.Slot)
	require.False(t, provOk)
}

// TestEndorserBlockTxsByHashWithholdsUnverifiedSlotFromLedger guards the
// ledger-facing consumer directly: it keys the endorser blob it persists on
// this slot, so a complete-but-unbound entry must read as unavailable.
func TestEndorserBlockTxsByHashWithholdsUnverifiedSlotFromLedger(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 21, 1)
	txsRaw := []cbor.RawMessage{mustCbor(t, "tx0")}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(
		t,
		data.completeTxCache(),
		"the transaction set is whole; only the slot binding is missing",
	)
	_, provOk := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.False(
		t,
		provOk,
		"a complete but unverified entry must not reach the ledger",
	)

	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	_, provOk = o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, provOk)
}

// TestEndorserBlockTxsByHashAvailableAfterDBReload is the store -> drain ->
// clear-memory -> provider regression from review: a verified, persisted
// endorser block reloaded from the blob store after the in-memory cache is
// cleared must remain available to the ledger provider immediately, not
// only after some later event happens to re-verify it. loadLeiosEBFromDB
// must reconstruct the reload as already bound, since the blob store is
// only ever written from a verified entry in the first place.
func TestEndorserBlockTxsByHashAvailableAfterDBReload(t *testing.T) {
	t.Parallel()

	tx0, ref0 := testLeiosManifestTx(t, 0)
	blockRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref0},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(55, lcommon.Blake2b256Hash(blockRaw).Bytes())
	txsRaw := []cbor.RawMessage{tx0}

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStoreAuthoritative,
	))

	// Endorser-block persistence is asynchronous; drain the writer so the
	// blob store reflects the stored block before forcing a DB reload.
	o.StopLeiosPersistWriter()
	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(
		t,
		ok,
		"a reloaded, previously-verified entry must be immediately available",
	)
	require.Equal(t, txsRaw, gotTxs)
}

// TestBindLeiosEndorserBlockSlotDoesNotMutateSharedEntry guards the copy-on-
// write invariant this file otherwise depends on throughout: lookupLeiosEndorserBlock
// hands out a pointer after leiosMu is released, and readers use it without
// the lock, so bindLeiosEndorserBlockSlot must publish a distinct copy on
// verification rather than flipping slotVerified on the pointer a caller
// already holds. Concurrent unlocked reads of that already-held pointer must
// see it unmodified.
func TestBindLeiosEndorserBlockSlotDoesNotMutateSharedEntry(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 63)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	held, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.False(t, held.slotVerified)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = held.slotVerified
		}
	}()
	go func() {
		defer wg.Done()
		o.bindLeiosEndorserBlockSlot(point.Hash, point.Slot)
	}()
	wg.Wait()

	require.False(
		t,
		held.slotVerified,
		"the pointer a caller already held must never be mutated",
	)
	fresh, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(
		t,
		fresh.slotVerified,
		"a fresh lookup sees the published copy",
	)
}

// TestStoreAndAnnouncementRaceAlwaysEndsVerified is the coordinated store /
// announcement regression from review: whichever of a peer-offered store and
// its matching announcement runs first, the entry must end up verified.
// recordLeiosAnnouncement's reconciliation runs at most once per distinct
// announcement, so if it can run before the store inserts its entry (seeing
// nothing to reconcile) while the store's own announcement check ran before
// the announcement was recorded (seeing nothing to bind to), the entry is
// stuck unverified forever. Run across many independent hashes concurrently
// under -race to exercise both interleavings.
func TestStoreAndAnnouncementRaceAlwaysEndsVerified(t *testing.T) {
	t.Parallel()

	const n = 64
	o := newOuroboros(OuroborosConfig{EnableLeios: true})

	points := make([]ocommon.Point, n)
	blocks := make([][]byte, n)
	headers := make([][]byte, n)
	for i := range n {
		point, blockRaw := testLeiosEndorserBlockRaw(t, 100+i)
		points[i] = point
		blocks[i] = blockRaw
		headers[i] = testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			testEbHash(point),
			uint64(len(blockRaw)),
		)
	}

	// Collected here rather than asserted inside the goroutines below: require
	// (and t.FailNow, which it calls on failure) is documented as unsafe to
	// invoke from any goroutine other than the one running the test function.
	announceErrs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(2 * n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_ = o.storeLeiosEndorserBlock(
				points[i],
				blocks[i],
				nil,
				leiosStorePeerOffered,
			)
		}(i)
		go func(i int) {
			defer wg.Done()
			announceErrs[i] = recordTestLeiosAnnouncementNoFail(o, headers[i])
		}(i)
	}
	wg.Wait()

	for i := range n {
		require.NoError(t, announceErrs[i], "announcement %d", i)
	}
	for i := range n {
		data, ok := o.lookupLeiosEndorserBlock(points[i].Slot, points[i].Hash)
		require.True(t, ok)
		require.True(
			t,
			data.slotVerified,
			"entry %d must end up verified regardless of race order",
			i,
		)
	}
}

// TestLeiosAnnouncementBindsSlotIgnoresExpiredBinding is the idle-expiry
// regression from review: leiosAnnouncementSlots is only actively pruned as a
// side effect of a *new* announcement being accepted (pruneLeiosAnnouncements),
// so on an otherwise-idle node a binding can sit long past the acceptance
// window pruneLeiosAnnouncements itself enforces.
// leiosAnnouncementBindsSlotLocked must not treat a stale, long-expired
// binding as still live -- a peer-offered store for that same slot must be
// left merely unverified, the same as a hash with no binding at all.
func TestLeiosAnnouncementBindsSlotIgnoresExpiredBinding(t *testing.T) {
	t.Parallel()

	ledger := &fakeLeiosAnnouncementLedger{
		// SlotToTime always answers as if the binding's slot occurred long
		// enough ago to have aged out of leiosNotifyMaxAnnouncementAge.
		slotTime: time.Now().Add(-2 * leiosNotifyMaxAnnouncementAge),
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: ledger,
	})

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 200, 1)
	ebHash := testEbHash(point)
	announceTestEndorserBlock(t, o, point.Slot, ebHash, len(blockRaw))

	require.False(
		t,
		o.leiosAnnouncementBindsSlotLocked(point.Hash, point.Slot),
		"an expired binding must not verify a store for the same slot",
	)

	// A peer-offered store for that same, now-expired slot must be accepted
	// but left unverified rather than rejected -- the expired binding reads
	// as unknown, not as a live conflict.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.False(t, data.slotVerified)
}

// TestFetchEndorserBlockByPointRejectsStaleReloadedSlot is the P1 regression
// from the second review round: a hash persisted (and so already verified)
// under one slot must not be silently accepted as satisfying a later,
// authoritative request for the same hash at a different slot. The manifest
// is content-addressed, so the same hash can legitimately recur at a
// different slot; loadLeiosEBFromDB trusts a reload's persisted slot as
// verified for whatever occurrence wrote it, but FetchEndorserBlockByPoint
// must still compare that slot against the one it was actually asked about
// before treating the reload as already satisfying the request.
func TestFetchEndorserBlockByPointRejectsStaleReloadedSlot(t *testing.T) {
	t.Parallel()

	tx0, ref0 := testLeiosManifestTx(t, 0)
	blockRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref0},
	}.MarshalCBOR()
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(blockRaw).Bytes()
	staleSlot := uint64(41)
	authoritativeSlot := uint64(42)

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(
		ocommon.NewPoint(staleSlot, hash),
		blockRaw,
		[]cbor.RawMessage{tx0},
		leiosStoreAuthoritative,
	))

	// Endorser-block persistence is asynchronous; drain the writer so the
	// blob store reflects the stale-slot store, then clear the in-memory
	// cache so the next lookup must reload from the blob store.
	o.StopLeiosPersistWriter()
	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	// o.connManager is nil, so a genuine cache miss (or a correctly-rejected
	// stale reload) must return an error here, not silently succeed --
	// there is no way to actually fetch anything without a connection.
	err = o.FetchEndorserBlockByPoint(
		context.Background(),
		authoritativeSlot,
		hash,
	)
	require.Error(
		t,
		err,
		"must not silently accept a reload bound to a different slot",
	)
	// Without a connection to actually re-fetch and re-persist, the blob
	// store's single-slot-per-hash record is unchanged, so a fully
	// independent EndorserBlockTxsByHash query may still report the stale
	// slot -- there is nothing here that could have corrected it. The
	// contract this test guards is narrower: FetchEndorserBlockByPoint
	// itself must not have claimed the authoritative slot was satisfied.

	// Once a real fetch *does* succeed (simulated here directly), the
	// authoritative store must override the stale entry rather than being
	// rejected by it, and the blob's single record for this hash is
	// corrected going forward.
	require.NoError(t, o.storeLeiosEndorserBlock(
		ocommon.NewPoint(authoritativeSlot, hash),
		blockRaw,
		[]cbor.RawMessage{tx0},
		leiosStoreAuthoritative,
	))
	_, ok := o.EndorserBlockTxsByHash(hash, authoritativeSlot)
	require.True(t, ok)
}

// TestStoreLeiosEndorserBlockAuthoritativeAndAnnouncedOccurrencesCoexist
// covers a gap symmetric to the stale-cache-entry cases above: an
// authoritative occurrence of a hash at one slot, and a live announcement
// for the same hash at a different slot, are two independent, equally valid
// occurrences (the manifest is content-addressed) and neither blocks the
// other. A peer-offered store matching its own live announcement must
// succeed and coexist with the authoritative entry, not be rejected as if
// the authoritative source's slot were the hash's only valid one (issue
// review; wolf31o2 review).
func TestStoreLeiosEndorserBlockAuthoritativeAndAnnouncedOccurrencesCoexist(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 300)
	ebHash := testEbHash(point)
	announced := ocommon.Point{Slot: point.Slot - 50, Hash: point.Hash}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	// A live announcement binds this hash to an older, unrelated slot.
	announceTestEndorserBlock(t, o, announced.Slot, ebHash, len(blockRaw))

	// The ledger (or the local forge path) authoritatively establishes the
	// hash at a different slot; it is unaffected by the unrelated
	// announcement.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStoreAuthoritative,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
	require.True(t, data.slotVerified)

	// A peer-offered store matching its own live announcement must succeed
	// and become independently verified, coexisting with the authoritative
	// entry above rather than being rejected by it.
	require.NoError(t, o.storeLeiosEndorserBlock(
		announced,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	announcedData, ok := o.lookupLeiosEndorserBlock(
		announced.Slot,
		announced.Hash,
	)
	require.True(t, ok)
	require.True(t, announcedData.slotVerified)
}

// TestEndorserBlockTxHashesByHashWithholdsUnverifiedSlot is the second review
// round's comment 2 companion to
// TestEndorserBlockTxsByHashWithholdsUnverifiedSlotFromLedger:
// EndorserBlockTxHashesByHash feeds the forge loop's post-certificate mempool
// exclusion list, so a complete-but-unbound entry must read as unavailable
// there too, not just from the tx-body provider.
func TestEndorserBlockTxHashesByHashWithholdsUnverifiedSlot(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 22, 1)
	txsRaw := []cbor.RawMessage{mustCbor(t, "tx0")}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	_, ok = o.EndorserBlockTxHashesByHash(point.Hash, point.Slot)
	require.False(
		t,
		ok,
		"a complete but unverified entry must not reach the forge loop",
	)

	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	hashes, ok := o.EndorserBlockTxHashesByHash(point.Hash, point.Slot)
	require.True(t, ok)
	require.Len(t, hashes, 1)
}

// TestLeiosClosureCompleteLockedWithholdsUnverifiedEntry is the closure-wait
// half of the second review round's comment 2: a closure that is complete but
// not yet slot-verified must not report ready via
// leiosClosureCompleteLocked/waitForLeiosEndorserClosure, and a waiter
// registered on it must stay parked until bindLeiosEndorserBlockSlot
// corroborates the slot -- otherwise the node-to-client merge path (which
// waits on this same closure) could consume an unverified slot the same way
// EndorserBlockTxsByHash could before.
func TestLeiosClosureCompleteLockedWithholdsUnverifiedEntry(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 71)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{mustCbor(t, "tx0")},
		leiosStorePeerOffered,
	))
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.False(t, data.slotVerified)

	// The already-cached fast path must not report a complete-but-unverified
	// closure as ready.
	quickCtx, quickCancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer quickCancel()
	require.False(
		t,
		o.waitForLeiosEndorserClosure(quickCtx, point.Slot, point.Hash),
	)

	// A waiter registered while the entry is complete-but-unverified must
	// stay parked -- nothing signals it at store time -- until the slot is
	// corroborated.
	// The wait context is intentionally much longer than every assertion
	// below: a passing RequireReceive must be caused by the promotion's
	// explicit wakeup, not by this context happening to expire around the
	// same time.
	result := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		result <- o.waitForLeiosEndorserClosure(ctx, point.Slot, point.Hash)
	}()
	testutil.WaitForCondition(
		t,
		func() bool {
			o.leiosMu.RLock()
			defer o.leiosMu.RUnlock()
			return len(
				o.leiosClosureWaiters[leiosBlockKey(point.Slot, point.Hash)],
			) > 0
		},
		2*time.Second,
		"closure waiter to register",
	)
	testutil.RequireNoReceive(
		t,
		result,
		300*time.Millisecond,
		"a complete but unverified closure must not wake a waiter",
	)

	// bindLeiosEndorserBlockSlot corroborating the slot must wake the parked
	// waiter itself -- the store above never will, since the entry was
	// already complete before the binding arrived.
	o.bindLeiosEndorserBlockSlot(point.Hash, point.Slot)
	require.True(
		t,
		testutil.RequireReceive(
			t,
			result,
			500*time.Millisecond,
			"closure wait to resolve once the slot is verified",
		),
	)
}

// lockProbingVoteHandler's HandleEndorserBlock acquires leiosAnnouncementsMu
// itself before delegating, the way a real handler could legitimately need
// to (e.g. to cross-check announcement state). If bindLeiosEndorserBlockSlot's
// publish step still ran while recordLeiosAnnouncement held that same lock,
// this self-deadlocks instead of merely looking suspicious.
type lockProbingVoteHandler struct {
	*fakeLeiosVoteHandler
	o *Ouroboros
}

func (l *lockProbingVoteHandler) HandleEndorserBlock(
	slot uint64,
	ebHash lcommon.Blake2b256,
) {
	l.o.leiosAnnouncementsMu.Lock()
	l.o.leiosAnnouncementsMu.Unlock()
	l.fakeLeiosVoteHandler.HandleEndorserBlock(slot, ebHash)
}

// TestRecordLeiosAnnouncementPublishesAfterReleasingAnnouncementsLock verifies
// bindLeiosEndorserBlockSlot's promotion
// used to publish (vote emission, pipeline observation, persistence enqueue)
// while recordLeiosAnnouncement still held leiosAnnouncementsMu, a lock
// shared by every concurrent announcement. A vote handler that itself needs
// that lock would then deadlock. The goroutine here is bounded by a timeout
// so a regression shows up as a clean test failure rather than a hung test
// binary; recordTestLeiosAnnouncementNoFail (not recordTestLeiosAnnouncement)
// keeps require calls off that goroutine.
func TestRecordLeiosAnnouncementPublishesAfterReleasingAnnouncementsLock(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 250)
	o := newOuroboros(OuroborosConfig{EnableLeios: false})
	votes := &lockProbingVoteHandler{
		fakeLeiosVoteHandler: &fakeLeiosVoteHandler{},
		o:                    o,
	}
	o.leiosVotes = votes

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	headerRaw := testDijkstraAnnouncementHeaderRawFor(
		t,
		point.Slot,
		testEbHash(point),
		uint64(len(blockRaw)),
	)
	errCh := make(chan error, 1)
	go func() {
		errCh <- recordTestLeiosAnnouncementNoFail(o, headerRaw)
	}()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"recordLeiosAnnouncement deadlocked: publish must run after " +
				"releasing leiosAnnouncementsMu",
		)
	}
	require.Len(t, votes.ebs, 1)
}

// fixedBlockRequester returns a fixed manifest body for every BlockRequest,
// simulating a leios-fetch server's response to a MsgBlockOffer-driven fetch.
type fixedBlockRequester struct {
	blockRaw []byte
	calls    int
}

func (r *fixedBlockRequester) BlockRequest(
	_ context.Context,
	_ ocommon.Point,
) (protocol.Message, error) {
	r.calls++
	return leiosfetch.NewMsgBlock(cbor.RawMessage(r.blockRaw)), nil
}

// withLowerLeiosEndorserBlockCacheBudgets temporarily lowers the byte-budget
// vars so a test can exercise per-entry rejection or aggregate eviction
// without allocating hundreds of megabytes of test data. Restored via
// t.Cleanup so other tests keep the production defaults.
func withLowerLeiosEndorserBlockCacheBudgets(
	t *testing.T,
	maxEntryBytes, maxBytes int,
) {
	t.Helper()
	origEntry, origTotal := leiosEndorserBlockCacheMaxEntryBytes,
		leiosEndorserBlockCacheMaxBytes
	leiosEndorserBlockCacheMaxEntryBytes = maxEntryBytes
	leiosEndorserBlockCacheMaxBytes = maxBytes
	t.Cleanup(func() {
		leiosEndorserBlockCacheMaxEntryBytes = origEntry
		leiosEndorserBlockCacheMaxBytes = origTotal
	})
}

// Valid case: the fetched manifest's length matches what the offer declared,
// so the fetch is accepted and the bytes are returned unchanged.
func TestFetchAndValidateLeiosEbManifestAcceptsMatchingSize(t *testing.T) {
	t.Parallel()

	_, blockRaw := testLeiosEndorserBlockRaw(t, 1)
	client := &fixedBlockRequester{blockRaw: blockRaw}

	got, err := fetchAndValidateLeiosEbManifest(
		context.Background(),
		client,
		ocommon.Point{Slot: 1},
		uint64(len(blockRaw)),
	)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), got)
	require.Equal(t, 1, client.calls)
}

// Mismatched case: a peer that declares one size in its offer and serves a
// body of a different length is rejected rather than cached.
func TestFetchAndValidateLeiosEbManifestRejectsSizeMismatch(t *testing.T) {
	t.Parallel()

	_, blockRaw := testLeiosEndorserBlockRaw(t, 1)
	client := &fixedBlockRequester{blockRaw: blockRaw}

	got, err := fetchAndValidateLeiosEbManifest(
		context.Background(),
		client,
		ocommon.Point{Slot: 1},
		uint64(len(blockRaw))+1,
	)
	require.ErrorContains(t, err, "size mismatch")
	require.Nil(t, got)
}

// Oversized case: an entry whose retained bytes (manifest plus transaction
// bodies) exceed the per-entry budget is rejected rather than cached, and any
// previously cached (smaller) entry for the same hash is left untouched.
// Not t.Parallel: withLowerLeiosEndorserBlockCacheBudgets swaps the
// package-level cache budgets, which every concurrent Leios test observes.
func TestStoreLeiosEndorserBlockRejectsOversizedEntry(t *testing.T) {
	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<10, 1<<20) // 1 KiB / 1 MiB
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 7000, 1)
	oversizedTx := cbor.RawMessage(make([]byte, 2<<10)) // 2 KiB > 1 KiB cap

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{oversizedTx},
		leiosStoreAuthoritative,
	)
	require.ErrorContains(t, err, "exceeds max")

	_, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, ok)
}

// Eviction case: once the aggregate byte budget is exceeded, the cache evicts
// the oldest-inserted entries first -- the same policy already used for the
// entry-count cap -- until the remaining entries fit the budget.
func TestLeiosEndorserBlockCacheEvictsOldestFirstAtByteBudget(t *testing.T) {
	// Each entry retains one ~600-byte transaction body; five fit comfortably
	// under a 1500-byte aggregate budget, but not all five at once.
	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<20, 1500)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})

	const entries = 5
	points := make([]ocommon.Point, entries)
	for i := range entries {
		point, blockRaw := testLeiosEndorserBlockRaw(t, i+1)
		points[i] = point
		tx := cbor.RawMessage(make([]byte, 600))
		require.NoError(
			t,
			o.storeLeiosEndorserBlock(
				point,
				blockRaw,
				[]cbor.RawMessage{tx},
				leiosStoreAuthoritative,
			),
		)
	}

	o.leiosMu.RLock()
	totalBytes := 0
	for _, data := range o.leiosEndorserBlocks {
		totalBytes += data.approxBytes()
	}
	o.leiosMu.RUnlock()
	require.LessOrEqual(t, totalBytes, 1500)

	// The oldest entries were evicted; the most recently stored one survives.
	_, ok := o.lookupLeiosEndorserBlock(points[0].Slot, points[0].Hash)
	require.False(t, ok)
	_, ok = o.lookupLeiosEndorserBlock(
		points[entries-1].Slot,
		points[entries-1].Hash,
	)
	require.True(t, ok)
}

// Eviction correctness: eviction order must follow actual insertion order
// (seq), not the wall-clock insertedAt captured before leiosMu is acquired. A
// delayed goroutine can win the lock later while still carrying an earlier
// insertedAt than a goroutine that actually inserted first; sorting by seq
// instead keeps the truly-older entry as the eviction victim.
func TestLeiosEndorserBlockCacheEvictionOrdersBySeqNotInsertedAt(t *testing.T) {
	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<20, 300)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	tx := func() cbor.RawMessage { return cbor.RawMessage(make([]byte, 200)) }

	firstPoint, firstRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			firstPoint,
			firstRaw,
			[]cbor.RawMessage{tx()},
			leiosStoreAuthoritative,
		),
	)

	// Simulate the race directly: the entry inserted first (and so holding
	// the lower seq) is given a later wall-clock insertedAt than the entry
	// about to be inserted second.
	o.leiosMu.Lock()
	first := o.leiosEndorserBlocks[leiosBlockKey(firstPoint.Slot, firstPoint.Hash)]
	require.NotNil(t, first)
	first.insertedAt = time.Now().Add(time.Hour)
	o.leiosMu.Unlock()

	secondPoint, secondRaw := testLeiosEndorserBlockRaw(t, 2)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			secondPoint,
			secondRaw,
			[]cbor.RawMessage{tx()},
			leiosStoreAuthoritative,
		),
	)

	// Both entries together exceed the 300-byte aggregate budget, forcing one
	// eviction. Despite "first" appearing newest by insertedAt, it holds the
	// lower seq (it was actually inserted first) and must be the one evicted.
	_, firstStillCached := o.lookupLeiosEndorserBlock(
		firstPoint.Slot,
		firstPoint.Hash,
	)
	_, secondStillCached := o.lookupLeiosEndorserBlock(
		secondPoint.Slot,
		secondPoint.Hash,
	)
	require.False(t, firstStillCached)
	require.True(t, secondStillCached)
}

// Oversized case, partial-retention path: retainLeiosPartialTxs publishes
// merged partialTxs directly rather than through storeLeiosEndorserBlock, so
// it needs its own per-entry byte-budget check -- otherwise a peer dribbling
// enough small partial responses across repeated fetch attempts could grow an
// entry past the budget without ever going through that check.
func TestRetainLeiosPartialTxsRejectsMergeOverEntryByteBudget(t *testing.T) {
	withLowerLeiosEndorserBlockCacheBudgets(t, 1500, 1<<20) // 1.5 KiB / 1 MiB
	const txCount = 4
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 21, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	// A first partial, well under the budget, is retained normally.
	small := make([]cbor.RawMessage, txCount)
	small[0] = cbor.RawMessage(make([]byte, 500))
	o.retainLeiosPartialTxs(point.Slot, point.Hash, small, nil)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, 1, data.partialTxCount())

	// A second partial that would push the merged entry over the per-entry
	// byte budget is rejected -- the existing (smaller) partial survives.
	big := make([]cbor.RawMessage, txCount)
	big[1] = cbor.RawMessage(make([]byte, 5000))
	o.retainLeiosPartialTxs(point.Slot, point.Hash, big, nil)

	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, 1, data.partialTxCount())
	require.LessOrEqual(t, data.approxBytes(), 1500)
}

// leiosPersistedTestEntry builds a valid manifest and complete transaction set
// (txCount distinct, correctly hashed/sized transactions seeded from seedBase)
// suitable for validateLeiosEndorserBlockTxs, and returns the point it will be
// cached under.
func leiosPersistedTestEntry(
	t *testing.T,
	slot uint64,
	seedBase, txCount int,
) (ocommon.Point, []byte, []cbor.RawMessage) {
	t.Helper()
	refs := make([]lcommon.LeiosTransactionReference, txCount)
	txs := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		tx, ref := testLeiosManifestTx(t, byte(seedBase+i))
		txs[i] = tx
		refs[i] = ref
	}
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: refs,
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(slot, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	return point, manifestRaw, txs
}

// Oversized case, persisted-reload path: loadLeiosEBFromDB reloads a manifest
// and transaction set the blob store already holds -- e.g. a legacy or
// pre-cap-era persisted endorser block -- independently of
// storeLeiosEndorserBlock's admission check. It must apply the same per-entry
// byte budget rather than let a leios-fetch MsgBlockRequest for that point
// repopulate the in-memory cache past the limit on every cache miss: the
// reloaded entry is still served to the caller, but left uncached.
func TestLoadLeiosEBFromDBServesOversizedEntryUncached(t *testing.T) {
	point, manifestRaw, txs := leiosPersistedTestEntry(t, 33, 0, 20)

	withLowerLeiosEndorserBlockCacheBudgets(t, 200, 1<<20)
	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	require.NotNil(t, db)
	require.NoError(
		t,
		db.SetLeiosEB(point.Slot, point.Hash, manifestRaw, txs),
	)

	// The reload still succeeds and returns the complete set to the caller...
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.Greater(t, data.approxBytes(), 200)

	// ...but the oversized entry must not have been admitted into the
	// in-memory cache.
	o.leiosMu.RLock()
	_, cached := o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)]
	o.leiosMu.RUnlock()
	require.False(t, cached)
}

// Eviction case, persisted-reload path: two persisted endorser blocks that
// each individually fit the per-entry budget, but not both at once together,
// must still trigger aggregate-budget eviction of the oldest one when
// reloaded. This exercises loadLeiosEBFromDB's post-insert prune specifically
// -- removing that call would let both entries stay cached (over budget)
// without failing TestLoadLeiosEBFromDBServesOversizedEntryUncached above,
// since that test only reaches the per-entry early return.
func TestLoadLeiosEBFromDBPrunesAggregateBudgetAfterReload(t *testing.T) {
	point1, manifest1, txs1 := leiosPersistedTestEntry(t, 1, 0, 10)
	point2, manifest2, txs2 := leiosPersistedTestEntry(t, 2, 100, 10)

	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<20, 1500)
	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	require.NotNil(t, db)
	require.NoError(t, db.SetLeiosEB(point1.Slot, point1.Hash, manifest1, txs1))
	require.NoError(t, db.SetLeiosEB(point2.Slot, point2.Hash, manifest2, txs2))

	data1, ok := o.lookupLeiosEndorserBlock(point1.Slot, point1.Hash)
	require.True(t, ok)
	require.LessOrEqual(t, data1.approxBytes(), 1500)

	data2, ok := o.lookupLeiosEndorserBlock(point2.Slot, point2.Hash)
	require.True(t, ok)
	require.LessOrEqual(t, data2.approxBytes(), 1500)
	require.Greater(t, data1.approxBytes()+data2.approxBytes(), 1500)

	// Both entries together exceed the 1500-byte aggregate budget: the older
	// (point1) reload must have been evicted by the post-insert prune when
	// point2 was admitted, and total retained bytes stay within budget.
	o.leiosMu.RLock()
	_, point1Cached := o.leiosEndorserBlocks[leiosBlockKey(point1.Slot, point1.Hash)]
	_, point2Cached := o.leiosEndorserBlocks[leiosBlockKey(point2.Slot, point2.Hash)]
	totalBytes := 0
	for _, d := range o.leiosEndorserBlocks {
		totalBytes += d.approxBytes()
	}
	o.leiosMu.RUnlock()
	require.False(t, point1Cached)
	require.True(t, point2Cached)
	require.LessOrEqual(t, totalBytes, 1500)
}

type recordingLeiosPipelineHandler struct {
	observed int
}

func (h *recordingLeiosPipelineHandler) ObserveEndorserBlock(
	uint64,
	lcommon.Blake2b256,
) {
	h.observed++
}

// Investigation.
//
// A from-genesis musashi Leios sync stalls in the epoch-15 endorser-block
// region: a ranking block references an endorser block whose manifest is
// non-empty (eb_size 35,499 / 65,343 bytes) yet decodes to ZERO transaction
// references and is rejected with:
//
//	store manifest: decode leios endorser block:
//	leios endorser block must contain at least one transaction reference
//
// These tests establish, without a node, WHERE the zero-refs failure can come
// from. They prove:
//
//  1. The gouroboros manifest decoder correctly handles large reference maps
//     (multi-byte CBOR map headers), in both the array-wrapped (CIP-0164/dingo)
//     and bare-map (IOG prototype) wire shapes. A large, non-empty manifest
//     therefore NEVER decodes to zero refs. This disproves the "hand-rolled
//     header length mis-parse" hypothesis.
//
//  2. The exact "must contain at least one transaction reference" error is
//     produced ONLY by a manifest that is genuinely an empty references map
//     (0xa0 or [{}]). A real 35-65 KB manifest cannot produce it. So the bytes
//     handed to the decoder in the field are an empty/truncated/wrong manifest,
//     i.e. a FETCH/serving problem, not a decode bug on authentic bytes. This
//     matches the in-tree note (ledger/leios_apply.go leiosBackfiller) that the
//     prototype relay "returns empty manifests when hammered".
//
//  3. With the fix in storeLeiosEndorserBlock (verify the manifest hash before
//     decoding), a peer that serves an empty manifest for a valid endorser-block
//     point is diagnosed as a "point hash mismatch" (a fetch/serving error the
//     backfill can retry against other peers) rather than the misleading decode
// invariant failure that made look like a consensus/decode defect.

// bareRefMapFromArrayWrapped strips the single-element array wrapper produced by
// LeiosEndorserBlock.MarshalCBOR (0x81 || refMap) to yield the bare {hash=>size}
// references map, i.e. the IOG Leios prototype wire shape (CBOR major type 5).
func bareRefMapFromArrayWrapped(t *testing.T, arrayWrapped []byte) []byte {
	t.Helper()
	require.GreaterOrEqual(t, len(arrayWrapped), 2)
	// Definite one-element array header for a single map element.
	require.Equalf(
		t,
		byte(0x81),
		arrayWrapped[0],
		"expected single-element array wrapper, got 0x%x",
		arrayWrapped[0],
	)
	return arrayWrapped[1:]
}

// TestLeiosEndorserBlockLargeMapDecodesAllRefs proves the manifest decoder reads
// every reference of a large map. 1200 refs requires a 2-byte CBOR map-header
// length (0xb9 || uint16); 300 also needs the multi-byte branch. If the header
// parsing dropped or mis-counted refs for large maps (the "decode to zero"
// hypothesis), these would fail.
func TestLeiosEndorserBlockLargeMapDecodesAllRefs(t *testing.T) {
	t.Parallel()

	for _, refCount := range []int{24, 256, 300, 715, 1200, 1604} {
		_, arrayWrapped := testLeiosEndorserBlockRawWithRefs(t, 42, refCount)

		// Array-wrapped shape (CIP-0164 / dingo).
		decoded, err := lcommon.NewLeiosEndorserBlockFromCbor(arrayWrapped)
		require.NoErrorf(
			t,
			err,
			"array-wrapped decode failed for %d refs",
			refCount,
		)
		require.Lenf(
			t,
			decoded.TransactionReferences,
			refCount,
			"array-wrapped: expected %d refs", refCount,
		)

		// Bare-map shape (IOG Leios prototype).
		bareMap := bareRefMapFromArrayWrapped(t, arrayWrapped)
		decodedBare, err := lcommon.NewLeiosEndorserBlockFromCbor(bareMap)
		require.NoErrorf(t, err, "bare-map decode failed for %d refs", refCount)
		require.Lenf(
			t,
			decodedBare.TransactionReferences,
			refCount,
			"bare-map: expected %d refs", refCount,
		)
	}
}

// TestLeiosEndorserBlockZeroRefsErrorOnlyFromEmptyManifest proves the exact
// "must contain at least one transaction reference" error is emitted only for a
// genuinely empty references map, and never for a large non-empty manifest.
func TestLeiosEndorserBlockZeroRefsErrorOnlyFromEmptyManifest(t *testing.T) {
	t.Parallel()

	const zeroRefsMsg = "must contain at least one transaction reference"

	// Empty bare map: 0xa0.
	_, err := lcommon.NewLeiosEndorserBlockFromCbor([]byte{0xa0})
	require.ErrorContains(t, err, zeroRefsMsg)

	// Array-wrapped empty map: [ {} ] = 0x81 0xa0.
	_, err = lcommon.NewLeiosEndorserBlockFromCbor([]byte{0x81, 0xa0})
	require.ErrorContains(t, err, zeroRefsMsg)

	// A large, non-empty manifest of the size reported in (~1000 refs is
	// ~35 KB) does NOT produce the zero-refs error in either wire shape.
	_, arrayWrapped := testLeiosEndorserBlockRawWithRefs(t, 15, 1000)
	require.Greater(t, len(arrayWrapped), 30000, "manifest should be ~35 KB")

	_, err = lcommon.NewLeiosEndorserBlockFromCbor(arrayWrapped)
	require.NoError(t, err)

	_, err = lcommon.NewLeiosEndorserBlockFromCbor(
		bareRefMapFromArrayWrapped(t, arrayWrapped),
	)
	require.NoError(t, err)
}

// TestStoreLeiosEndorserBlockEmptyManifestIsHashMismatch reproduces the
// field scenario at the dingo store boundary: a peer returns an empty manifest
// (0xa0) in response to a by-point fetch for a valid, non-empty endorser block.
//
// With the hash-before-decode fix, storeLeiosEndorserBlock reports a "point hash
// mismatch" (correctly identifying a wrong/empty peer response, which the
// backfill treats as a retryable fetch failure) instead of the misleading
// "decode leios endorser block: ... must contain at least one transaction
// reference" that made the fetch problem look like a decode/consensus defect and
// wedged the ledger.
func TestStoreLeiosEndorserBlockEmptyManifestIsHashMismatch(t *testing.T) {
	t.Parallel()

	// The point identifies a real, non-empty endorser block (1000 refs).
	point, _ := testLeiosEndorserBlockRawWithRefs(t, 15, 1000)

	// The peer serves an empty manifest instead of the real bytes.
	emptyManifest := []byte{0xa0}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(
		point,
		emptyManifest,
		nil,
		leiosStoreAuthoritative,
	)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"leios endorser block cache: point hash mismatch",
	)
	// The wrong-bytes response is no longer misreported as a decode invariant
	// failure.
	require.NotContains(
		t,
		err.Error(),
		"must contain at least one transaction reference",
	)

	// Nothing was cached for the point.
	_, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, ok)
}

// TestStoreLeiosEndorserBlockGenuinelyEmptyEbStillRejected confirms the fix does
// not weaken the invariant: a manifest that genuinely IS an empty references map
// AND whose hash matches the requested point (a producer-side protocol
// violation, not a wrong peer response) is still rejected by the decode
// invariant.
func TestStoreLeiosEndorserBlockGenuinelyEmptyEbStillRejected(t *testing.T) {
	t.Parallel()

	emptyManifest := []byte{0xa0}
	hash := lcommon.Blake2b256Hash(emptyManifest)
	point := ocommon.NewPoint(15, hash.Bytes())

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	votes := &fakeLeiosVoteHandler{}
	pipeline := &recordingLeiosPipelineHandler{}
	o.leiosVotes = votes
	o.leiosPipeline = pipeline
	err := o.storeLeiosEndorserBlock(
		point,
		emptyManifest,
		nil,
		leiosStoreAuthoritative,
	)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"must contain at least one transaction reference",
	)
	_, cached := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, cached)
	require.Empty(t, votes.ebs)
	require.Zero(t, pipeline.observed)
}

// TestStoreLeiosEndorserBlockValidManifestStillStores confirms the reordered
// checks do not regress the happy path: a valid, hash-matching manifest is
// decoded and cached.
func TestStoreLeiosEndorserBlockValidManifestStillStores(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 15, 300)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, 300, data.txCount)
	require.Equal(t, []byte(cbor.RawMessage(blockRaw)), data.blockRaw)
}

func electionAnnouncement(
	t *testing.T,
	slot, blockNo uint64,
	issuer byte,
) []byte {
	t.Helper()
	raw := testDijkstraAnnouncementHeaderRawFor(
		t,
		slot,
		lcommon.Blake2b256{0xaa},
		1234,
	)
	var top, body []cbor.RawMessage
	_, err := cbor.Decode(raw, &top)
	require.NoError(t, err)
	if len(top) == 0 {
		t.Fatal("missing announcement header body")
		return nil
	}
	_, err = cbor.Decode(top[0], &body)
	require.NoError(t, err)
	if len(body) < 4 {
		t.Fatal("incomplete announcement header body")
		return nil
	}
	body[0] = mustCbor(t, blockNo)
	body[3] = mustCbor(t, bytes.Repeat([]byte{issuer}, 32))
	top[0] = mustCbor(t, body)
	return mustCbor(t, top)
}

func TestLeiosAnnouncementElectionBoundAcrossSources(t *testing.T) {
	// Header cryptography is supplied by the existing ledger fixture; this
	// exercises the real decode, time-window, pruning, recording and relay path.
	ledger := &fakeLeiosAnnouncementLedger{
		currentSlot: 11,
		slotTime:    time.Now().Add(-time.Minute),
	}
	o := newOuroboros(
		OuroborosConfig{EnableLeios: true, LeiosAnnouncementLedger: ledger},
	)
	o.leiosEBLog.registerConn("observer", nil, nil)
	first := electionAnnouncement(t, 10, 1, 1)
	require.NoError(t, o.acceptLeiosAnnouncement(first, "connection-a"))
	require.NoError(
		t,
		o.acceptLeiosAnnouncement(
			electionAnnouncement(t, 10, 2, 1),
			"connection-b",
		),
	)
	// Relaying an already-known header from a new source must not consume
	// another slot or fail after the election's two distinct headers are seen.
	require.NoError(t, o.acceptLeiosAnnouncement(first, "connection-c"))
	err := o.acceptLeiosAnnouncement(
		electionAnnouncement(t, 10, 3, 1),
		"connection-c",
	)
	require.ErrorContains(t, err, "third distinct")
	require.Len(t, o.leiosAnnouncements, 2)
	require.Len(
		t,
		o.leiosEBLog.items,
		2,
		"rejected announcement must not be relayed",
	)

	// Both parts of election identity matter: another issuer at the same
	// slot and the same issuer at another slot each get their own budget.
	for _, election := range []struct {
		slot   uint64
		issuer byte
	}{{10, 2}, {11, 1}} {
		require.NoError(
			t,
			o.acceptLeiosAnnouncement(
				electionAnnouncement(t, election.slot, 1, election.issuer),
				"connection-c",
			),
		)
		require.NoError(
			t,
			o.acceptLeiosAnnouncement(
				electionAnnouncement(t, election.slot, 2, election.issuer),
				"connection-c",
			),
		)
	}
	require.Len(t, o.leiosAnnouncements, 6)
}

type manualDeadlineContext struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	canceled atomic.Bool
	err      error
}

func (c *manualDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (c *manualDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *manualDeadlineContext) Err() error {
	if c.canceled.Load() {
		return c.err
	}
	return nil
}

func TestLeiosFetchRequestContextReusesEqualParentDeadline(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(-time.Second)
	parent := &manualDeadlineContext{
		Context:  context.Background(),
		deadline: deadline,
		done:     make(chan struct{}),
	}
	child, cancel := leiosFetchRequestContext(parent, deadline)
	defer cancel()

	// WithDeadline's strict parent-before-child check used to create an
	// independently expired timer here, even though the parent timer has not
	// delivered cancellation yet. The child must remain live until the parent
	// is cancelled.
	select {
	case <-child.Done():
		t.Fatal("child deadline fired before parent cancellation")
	default:
	}
	parent.err = context.DeadlineExceeded
	parent.canceled.Store(true)
	close(parent.done)
	select {
	case <-child.Done():
		require.ErrorIs(t, child.Err(), context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("child did not follow parent cancellation")
	}
}

func TestLeiosFetchRequestContextKeepsEarlierAttemptDeadline(t *testing.T) {
	t.Parallel()

	parent := &manualDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(time.Hour),
		done:     make(chan struct{}),
	}
	child, cancel := leiosFetchRequestContext(
		parent,
		time.Now().Add(-time.Second),
	)
	defer cancel()
	require.ErrorIs(t, child.Err(), context.DeadlineExceeded)
	require.NoError(t, parent.Err())
}

// assertCooldownWindow asserts the guard is in cooldown right up to, but not at,
// now+want.
func assertCooldownWindow(
	t *testing.T,
	g *leiosFetchGuard,
	now time.Time,
	want time.Duration,
) {
	t.Helper()
	require.Truef(
		t,
		g.inCooldown(now.Add(want-time.Nanosecond)),
		"expected in cooldown just before %s", want,
	)
	require.Falsef(
		t,
		g.inCooldown(now.Add(want)),
		"expected not in cooldown at %s", want,
	)
}

// TestLeiosFetchGuardCooldownEscalates verifies that consecutive backfill
// failures on the same connection escalate the cooldown exponentially from the
// base, so a connection that repeatedly returns wrong (hash-mismatching) or
// unservable/stalling responses is deprioritized for progressively longer.
func TestLeiosFetchGuardCooldownEscalates(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	base := leiosBackfillConnCooldown
	g := &leiosFetchGuard{}

	// 1st failure = base, then doubling on each consecutive failure.
	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, base)

	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 2*base)

	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 4*base)

	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 8*base)
}

// TestLeiosFetchGuardCooldownCaps verifies the escalating cooldown never exceeds
// leiosBackfillConnCooldownMax no matter how many consecutive failures occur.
func TestLeiosFetchGuardCooldownCaps(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	base := leiosBackfillConnCooldown
	g := &leiosFetchGuard{}

	for range 50 {
		g.markFetchFailed(now, base)
	}
	// At the cap: still cooling just before the max deadline, clear at/after it.
	assertCooldownWindow(t, g, now, leiosBackfillConnCooldownMax)
}

// TestLeiosFetchGuardCooldownResetsOnSuccess verifies a successful fetch clears
// the cooldown AND resets the escalation, so the next failure starts again at
// the base cooldown rather than the escalated one.
func TestLeiosFetchGuardCooldownResetsOnSuccess(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	base := leiosBackfillConnCooldown
	g := &leiosFetchGuard{}

	// Escalate a few times.
	g.markFetchFailed(now, base)
	g.markFetchFailed(now, base)
	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 4*base)

	// A success clears the cooldown immediately and resets escalation.
	g.markFetchOK()
	require.False(t, g.inCooldown(now), "success should clear the cooldown")

	// The next failure starts again at the base window, not the escalated one.
	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, base)
}

// The helpers in this file reproduce, byte for byte, how the Haskell Leios
// reference node encodes an endorser-block manifest and a MsgLeiosBlockTxs
// response. They follow ouroboros-consensus commit 1820edf5e (leios-prototype
// branch):
//
//   - LeiosDemoTypes: encodeLeiosEb, hashLeiosEb, hashLeiosTx, encodeLeiosTx,
//     encodeLeiosPoint (HASH = Blake2b_256)
//   - LeiosDemoOnlyTestFetch: encodeLeiosFetch (MsgLeiosBlockTxs),
//     encodeBitmaps, decodeBitmaps
//   - LeiosDemoLogic: msgLeiosBlockTxsRequest, which echoes the request point
//     and bitmaps and rejects zero bitmaps and non-ascending offsets
//
// They are written independently of gouroboros' encoders so that the test
// pins interoperability with the reference node rather than Dingo's agreement
// with itself.

// haskellCborUint is cborg's canonical (minimal-length) unsigned integer
// encoding, as used by encodeWord, encodeWord16, encodeWord32 and
// encodeWord64, for the given CBOR major type.
func haskellCborUint(major byte, v uint64) []byte {
	m := major << 5
	switch {
	case v < 24:
		return []byte{m | byte(v)}
	case v <= 0xff:
		return []byte{m | 24, byte(v)}
	case v <= 0xffff:
		b := []byte{m | 25, 0, 0}
		binary.BigEndian.PutUint16(b[1:], uint16(v))
		return b
	case v <= 0xffffffff:
		b := []byte{m | 26, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(b[1:], uint32(v))
		return b
	default:
		b := []byte{m | 27, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(b[1:], v)
		return b
	}
}

// haskellCborBytes is cborg's encodeBytes: a definite-length byte string.
func haskellCborBytes(b []byte) []byte {
	return append(haskellCborUint(2, uint64(len(b))), b...)
}

// haskellEncodeLeiosEb mirrors encodeLeiosEb: a definite-length map from tx
// hash (bytes) to tx byte size (word32), in EB order. Each tx hash is
// hashLeiosTx, Blake2b-256 over the complete serialized transaction (not the
// transaction body / tx id).
func haskellEncodeLeiosEb(txs []cbor.RawMessage) []byte {
	out := haskellCborUint(5, uint64(len(txs)))
	for _, tx := range txs {
		h := lcommon.Blake2b256Hash(tx)
		out = append(out, haskellCborBytes(h.Bytes())...)
		out = append(out, haskellCborUint(0, uint64(len(tx)))...)
	}
	return out
}

// haskellEncodeBlockTxs mirrors encodeLeiosFetch for
// MsgLeiosBlockTxs p bitmaps txs as served by msgLeiosBlockTxsRequest:
// listLen 4, word 3, encodeLeiosPoint (listLen 2, slot, hash bytes),
// encodeBitmaps (an indefinite-length map of word16 offset -> word64 bitmap,
// in request order), then listLen n of encodeLeiosTx (cbor-in-cbor: the
// stored tx bytes wrapped in a byte string). Transactions are emitted in
// bitmap order, where the most significant bit of each 64-bit bitmap is the
// first transaction of that window.
func haskellEncodeBlockTxs(
	point ocommon.Point,
	bitmaps [][2]uint64,
	txs []cbor.RawMessage,
) ([]byte, error) {
	out := []byte{0x84}
	out = append(out, haskellCborUint(0, 3)...)
	out = append(out, 0x82)
	out = append(out, haskellCborUint(0, point.Slot)...)
	out = append(out, haskellCborBytes(point.Hash)...)
	out = append(out, 0xbf)
	var served []cbor.RawMessage
	for _, bm := range bitmaps {
		out = append(out, haskellCborUint(0, bm[0])...)
		out = append(out, haskellCborUint(0, bm[1])...)
		for i := range 64 {
			if bm[1]&(uint64(1)<<(63-i)) == 0 {
				continue
			}
			idx := bm[0]*64 + uint64(i)
			if idx >= uint64(len(txs)) {
				return nil, fmt.Errorf(
					"bitmap offset %d requests tx %d of %d",
					bm[0], idx, len(txs),
				)
			}
			served = append(served, txs[idx])
		}
	}
	out = append(out, 0xff)
	out = append(out, haskellCborUint(4, uint64(len(served)))...)
	for _, tx := range served {
		out = append(out, haskellCborBytes(tx)...)
	}
	return out, nil
}

// haskellDecodeRequestBitmaps decodes the bitmaps of a Dingo
// MsgLeiosBlockTxsRequest the way the reference node does: decodeBitmaps
// requires an indefinite-length map, and msgLeiosBlockTxsRequest rejects a zero
// bitmap and offsets that are not strictly ascending. It returns the
// (offset, bitmap) windows in wire order.
func haskellDecodeRequestBitmaps(req protocol.Message) ([][2]uint64, error) {
	raw, err := cbor.Encode(req)
	if err != nil {
		return nil, err
	}
	var elems []cbor.RawMessage
	if _, err := cbor.Decode(raw, &elems); err != nil {
		return nil, err
	}
	if len(elems) != 3 {
		return nil, fmt.Errorf("request has %d elements, want 3", len(elems))
	}
	bm := []byte(elems[2])
	if len(bm) < 2 || bm[0] != 0xbf || bm[len(bm)-1] != 0xff {
		return nil, errors.New(
			"request bitmaps are not an indefinite-length map",
		)
	}
	body := bm[1 : len(bm)-1]
	var out [][2]uint64
	for len(body) > 0 {
		var offset, bitmap uint64
		n, err := cbor.Decode(body, &offset)
		if err != nil {
			return nil, err
		}
		body = body[n:]
		n, err = cbor.Decode(body, &bitmap)
		if err != nil {
			return nil, err
		}
		body = body[n:]
		if offset > 0xffff {
			return nil, fmt.Errorf("offset %d is not a word16", offset)
		}
		if bitmap == 0 {
			return nil, errors.New("a bitmap is zero")
		}
		if len(out) > 0 && offset <= out[len(out)-1][0] {
			return nil, errors.New("offsets not strictly ascending")
		}
		out = append(out, [2]uint64{offset, bitmap})
	}
	return out, nil
}

// haskellBlockTxsPeer answers BlockTxsRequest the way the Haskell reference
// node does, returning the response through gouroboros' wire decoder.
type haskellBlockTxsPeer struct {
	txs      []cbor.RawMessage
	requests int
}

func (p *haskellBlockTxsPeer) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	p.requests++
	windows, err := haskellDecodeRequestBitmaps(
		leiosfetch.NewMsgBlockTxsRequest(point, bitmaps),
	)
	if err != nil {
		return nil, fmt.Errorf("reference node rejects request: %w", err)
	}
	raw, err := haskellEncodeBlockTxs(point, windows, p.txs)
	if err != nil {
		return nil, err
	}
	return leiosfetch.NewMsgFromCbor(leiosfetch.MessageTypeBlockTxs, raw)
}

// TestLeiosFetchHaskellEncodedBlockTxsIsConsumed is an interoperability
// regression for the Haskell reference node's leios-fetch encoding.
// The endorser block has 70 transactions, so the fetch spans two 64-tx bitmap
// windows. Its manifest, point hash and MsgLeiosBlockTxs response are encoded
// the way the reference node encodes them. Dingo must send a request the
// reference node accepts, decode the response, and bind every returned
// transaction to the manifest. Manifest references hash the full transaction
// CBOR, so validating by transaction-body hash rejects every tx
// ("endorser tx 0 hash mismatch").
func TestLeiosFetchHaskellEncodedBlockTxsIsConsumed(t *testing.T) {
	t.Parallel()

	const txCount = 70
	txs := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		txs[i] = testDijkstraTx(t, byte(i))
	}
	manifestRaw := haskellEncodeLeiosEb(txs)
	// hashLeiosEb: the EB hash is Blake2b-256 of the encoded manifest.
	point := ocommon.NewPoint(
		3623,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)

	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, txCount)

	o := &Ouroboros{}
	peer := &haskellBlockTxsPeer{txs: txs}
	got, err := o.fetchLeiosEbTxsBatched(peer, point, txCount, manifestRaw)
	require.NoError(t, err)
	require.Len(t, got, txCount)
	require.NotZero(t, peer.requests)
	require.NoError(t, validateLeiosEndorserBlockTxs(manifestRaw, got))
}

// diffusingBlockTxsRequester simulates the relay near the live tip: it has only
// diffused the first `available` transactions of the endorser block, so it
// serves requested indices below that watermark and nothing above it. Raising
// `available` between fetch attempts models the relay finishing diffusion
// before it re-offers the block. Each served transaction's CBOR encodes its
// absolute index so callers can verify ordering, and every requested index is
// recorded so a test can prove a resumed fetch asks only for the missing tail.
type diffusingBlockTxsRequester struct {
	available int
	calls     int
	requested []int
}

func (r *diffusingBlockTxsRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	r.requested = append(r.requested, requested...)
	served := map[uint16]uint64{}
	txs := make([]cbor.RawMessage, 0, len(requested))
	for _, idx := range requested {
		if idx >= r.available {
			continue
		}
		served[uint16(idx/64)] |= 1 << uint(
			63-(idx%64),
		) // MSB-first, see leiosWindowNeededMask
		enc, err := cbor.Encode(idx)
		if err != nil {
			return nil, err
		}
		txs = append(txs, cbor.RawMessage(enc))
	}
	return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
}

// A fetch that runs out of diffused transactions must retain what it already
// holds against the cached endorser block instead of discarding it. Before
// this, the partial prefix was dropped on the floor and the next offer
// re-fetched the whole block from scratch.
func TestFetchLeiosEbTxsRetainsPartialTailOnIncompleteFetch(t *testing.T) {
	t.Parallel()

	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 7, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	requester := &diffusingBlockTxsRequester{available: diffused}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.Error(t, err)
	requireTxsInIndexOrder(t, txs, diffused)

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.False(t, data.completeTxCache())
	require.Equal(
		t,
		diffused,
		data.partialTxCount(),
		"partially fetched endorser block was discarded",
	)
	// What was retained is the diffused prefix itself, in index order, so the
	// next attempt resumes rather than re-fetching it.
	requireTxsInIndexOrder(t, leiosCollectTxs(data.partialTxs), diffused)
}

// A re-offer of the same endorser block must fetch only the still-missing
// transactions and complete the cached entry, rather than re-fetching the
// transactions dingo already holds.
func TestFetchLeiosEbTxsCompletesPartialTailOnReoffer(t *testing.T) {
	t.Parallel()

	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 11, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	first := &diffusingBlockTxsRequester{available: diffused}
	_, err := o.fetchLeiosEbTxsBatched(first, point, txCount, nil)
	require.Error(t, err)

	// The relay finished diffusing and re-offers the block.
	second := &diffusingBlockTxsRequester{available: txCount}
	txs, err := o.fetchLeiosEbTxsBatched(second, point, txCount, nil)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, txCount)

	require.NotEmpty(t, second.requested)
	require.Equal(
		t,
		diffused,
		slices.Min(second.requested),
		"re-offer re-fetched transactions already held",
	)
	require.Equal(t, txCount-1, slices.Max(second.requested))

	// Completing the block stores it through the unchanged path, so the
	// existing tip gate applies it exactly as a single-attempt fetch would.
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			txs,
			leiosStoreAuthoritative,
		),
	)
	gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, ok)
	requireTxsInIndexOrder(t, gotTxs, txCount)

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.Zero(
		t,
		data.partialTxCount(),
		"completed endorser block still retains partial-fetch state",
	)
}

// The relay offers each endorser block on every connection, so a manifest-only
// store routinely lands after another connection has fetched part of the
// transaction set. It must not drop the retained partial: doing so would send
// the next re-offer back to a from-scratch fetch. This mirrors the existing
// no-clobber invariant for a complete transaction set.
func TestStoreLeiosEndorserBlockManifestKeepsPartialTail(t *testing.T) {
	t.Parallel()

	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 13, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	requester := &diffusingBlockTxsRequester{available: diffused}
	_, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.Error(t, err)

	for range 3 {
		require.NoError(
			t,
			o.storeLeiosEndorserBlock(
				point,
				blockRaw,
				nil,
				leiosStoreAuthoritative,
			),
		)
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		require.True(t, ok)
		require.Equal(
			t,
			diffused,
			data.partialTxCount(),
			"redundant manifest store dropped the retained partial tail",
		)
	}
}

// Two connections can fetch overlapping parts of the same endorser block. The
// retained partial is a union, so neither attempt's progress is lost and the
// block completes once their combined coverage is whole.
func TestRetainLeiosPartialTxsUnionsAcrossAttempts(t *testing.T) {
	t.Parallel()

	const txCount = 100
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 17, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	head := make([]cbor.RawMessage, txCount)
	tail := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		enc, err := cbor.Encode(i)
		require.NoError(t, err)
		if i < 60 {
			head[i] = cbor.RawMessage(enc)
		}
		if i >= 40 {
			tail[i] = cbor.RawMessage(enc)
		}
	}
	o.retainLeiosPartialTxs(point.Slot, point.Hash, head, nil)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, 60, data.partialTxCount())

	o.retainLeiosPartialTxs(point.Slot, point.Hash, tail, nil)
	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, txCount, data.partialTxCount())

	// A fetch seeded from the union needs no further transactions from the
	// relay at all.
	requester := &diffusingBlockTxsRequester{available: 0}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, txCount)
	require.Zero(t, requester.calls)
}

// Retention validation can invalidate bodies that were already cached. Even
// when the current fetch contributes no replacement body, the sanitized union
// must replace the old cache entry so a later fetch cannot reuse the invalid
// body.
func TestRetainLeiosPartialTxsPublishesSanitizedHeldEntries(t *testing.T) {
	t.Parallel()

	_, ref1 := testLeiosManifestTx(t, 1)
	tx2, ref2 := testLeiosManifestTx(t, 2)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref1, ref2},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(23, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	// Seed index 0 with the body for index 1. The validation callback below
	// must clear it even though this attempt offers no replacement body.
	o.retainLeiosPartialTxs(
		point.Slot,
		point.Hash,
		[]cbor.RawMessage{tx2, nil},
		nil,
	)
	validate, err := leiosEndorserBlockTxValidator(manifestRaw, 2)
	require.NoError(t, err)
	o.retainLeiosPartialTxs(
		point.Slot,
		point.Hash,
		[]cbor.RawMessage{tx2, nil},
		validate,
	)

	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Empty(t, cached.partialTxs)
	require.Zero(t, cached.partialTxCount())
}

// Retention is scoped to endorser blocks dingo is actually tracking: a partial
// for an unknown hash is dropped rather than growing the cache.
func TestRetainLeiosPartialTxsIgnoresUnknownBlock(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	o.retainLeiosPartialTxs(99, []byte{0xde, 0xad}, []cbor.RawMessage{
		mustCbor(t, "tx0"),
	}, nil)
	_, ok := o.lookupLeiosEndorserBlock(99, []byte{0xde, 0xad})
	require.False(t, ok)
}

// An endorser block that never completes must not stay resident forever. The
// relay offers each block on every connection and every one of those offers
// re-stores the manifest, rebuilding the cache entry with a fresh insertedAt.
// Carrying the retained partial across that store must not also restart the
// block's ten-minute lifetime: the entry now holds transaction bodies rather
// than just a manifest, and a steady trickle of re-offers would otherwise keep
// refreshing it just before expiry, so it would never be pruned.
func TestStoreLeiosEndorserBlockPartialDoesNotRefreshCacheTTL(t *testing.T) {
	t.Parallel()

	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 19, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	requester := &diffusingBlockTxsRequester{available: diffused}
	_, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.Error(t, err)

	// Age the entry to just short of its TTL, the window in which a re-offer
	// would otherwise reset the clock before pruning can evict it.
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	aged := time.Now().Add(-leiosEndorserBlockCacheTTL + 2*time.Second)
	o.leiosMu.Lock()
	data.insertedAt = aged
	o.leiosMu.Unlock()

	for range 3 {
		require.NoError(
			t,
			o.storeLeiosEndorserBlock(
				point,
				blockRaw,
				nil,
				leiosStoreAuthoritative,
			),
		)
	}
	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(
		t,
		diffused,
		data.partialTxCount(),
		"redundant manifest store dropped the retained partial tail",
	)
	require.WithinDuration(
		t,
		aged,
		data.insertedAt,
		time.Second,
		"re-offer restarted the cache lifetime of an incomplete endorser block",
	)

	// Completing the block does refresh it: it is now a servable entry with
	// the same lifetime any freshly fetched endorser block gets.
	full := make([]cbor.RawMessage, txCount)
	for i := range full {
		enc, err := cbor.Encode(i)
		require.NoError(t, err)
		full[i] = cbor.RawMessage(enc)
	}
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			full,
			leiosStoreAuthoritative,
		),
	)
	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.Zero(t, data.partialTxCount())
	require.WithinDuration(t, time.Now(), data.insertedAt, time.Minute)
}

// waitForLeiosServeWaiter blocks until a serving wait has registered for the
// fixture's connection. The registry is written from the protocol goroutine
// and read here, so the predicate takes the lock.
func waitForLeiosServeWaiter(t *testing.T, f *chainsyncServerFixture) {
	t.Helper()
	testutil.WaitForCondition(
		t,
		func() bool {
			f.o.leiosServeWaitersMu.Lock()
			defer f.o.leiosServeWaitersMu.Unlock()
			return len(f.o.leiosServeWaiters[f.conn.Id()]) > 0
		},
		5*time.Second,
		"leios serve waiter to register for the connection",
	)
}

// TestLeiosServeWaitReleasedByRealPeerDisconnect is the regression
// test. It runs against the real NtC chainsync server connection the shared
// ouroboros-mock harness builds, and tears that connection down the way a peer
// actually does (Harness.Disconnect closes the driver end of the bearer)
// rather than by closing a channel the test made up.
//
// The configured closure-wait window is an hour, so nothing except the
// disconnect can end the wait inside the assertion budget.
//
// Against the first attempt at this fix -- which bound the wait to
// Protocol.DoneChan() -- this test fails by timing out: the serving callback
// runs inside gouroboros's recvLoop, recvLoop closes recvDoneChan only when it
// returns, and doneChan closes only after that, so DoneChan() cannot close
// while the callback it would release is still running.
func TestLeiosServeWaitReleasedByRealPeerDisconnect(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: time.Hour,
	})

	certRB := testDijkstraCertRBRaw(t, 80, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa1
	// No closure is ever stored for ebHash, so the serving path parks.
	block := models.Block{Cbor: certRB, Slot: 80, Hash: []byte{0x80}}

	type result struct {
		cbor []byte
		err  error
	}
	results := make(chan result, 1)
	go func() {
		cbor, err := f.o.serveLeiosCertRbWithWait(
			block,
			ebHash,
			block.Slot,
			f.conn.Id(),
			f.conn.ChainSync().Server,
		)
		results <- result{cbor: cbor, err: err}
	}()

	// Only disconnect once the wait is actually parked, so the disconnect
	// exercises the release path rather than the already-gone fast path.
	waitForLeiosServeWaiter(t, f)

	require.NoError(t, f.h.Disconnect())

	got := testutil.RequireReceive(
		t,
		results,
		10*time.Second,
		"CertRB closure wait to be released by the peer disconnect",
	)
	require.Error(t, got.err)
	require.ErrorIs(t, got.err, errLeiosClosureUnresolved)
	require.Nil(t, got.cbor)
	require.Contains(t, got.err.Error(), "cancelled")

	// The release must also clear the registry rather than leaking an entry
	// per closed connection.
	f.o.leiosServeWaitersMu.Lock()
	remaining := len(f.o.leiosServeWaiters)
	f.o.leiosServeWaitersMu.Unlock()
	require.Zero(t, remaining)
}

func TestLeiosServeWaitReleaseKeepsReplacementOwner(t *testing.T) {
	t.Parallel()
	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios: true, LeiosClosureWaitTimeout: time.Hour,
	})
	certRB := testDijkstraCertRBRaw(t, 81, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa2
	block := models.Block{Cbor: certRB, Slot: 81, Hash: []byte{0x81}}
	result := make(chan error, 1)
	t.Cleanup(func() { f.o.ReleaseLeiosServeWaiters(f.conn.Id()) })
	go func() {
		_, err := f.o.serveLeiosCertRbWithWait(
			block, ebHash, block.Slot, f.conn.Id(), f.conn.ChainSync().Server,
		)
		result <- err
	}()
	waitForLeiosServeWaiter(t, f)
	replacement := f.conn.ChainSync().Server
	replacementWait, cancelReplacement := f.o.registerLeiosServeWaiter(
		f.conn.Id(), replacement,
	)
	t.Cleanup(cancelReplacement)
	f.o.ReleaseLeiosServeWaitersOwner(f.conn.Id(), new(ochainsync.Server))
	select {
	case <-replacementWait:
		t.Fatal("old serving owner released replacement waiter")
	default:
	}
	f.o.ReleaseLeiosServeWaitersOwner(f.conn.Id(), replacement)
	err := testutil.RequireReceive(
		t,
		result,
		5*time.Second,
		"replacement serving owner released",
	)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	select {
	case <-replacementWait:
	default:
		t.Fatal("replacement owner did not release its waiter")
	}
}

func TestLeiosServeWaiterRejectsReplacedOwner(t *testing.T) {
	t.Parallel()
	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios: true,
	})
	live, cancelLive := f.o.registerLeiosServeWaiter(
		f.conn.Id(),
		f.conn.ChainSync().Server,
	)
	t.Cleanup(cancelLive)
	stale, cancelStale := f.o.registerLeiosServeWaiter(
		f.conn.Id(),
		new(ochainsync.Server),
	)
	t.Cleanup(cancelStale)
	select {
	case <-stale:
	default:
		t.Fatal("replaced serving owner registered a live waiter")
	}
	select {
	case <-live:
		t.Fatal("rejecting a stale owner released the current waiter")
	default:
	}
}

// TestLeiosServeWaitStillBoundedByTimeout keeps the timeout bound honest: a
// connection that stays up must still end the wait at the configured window,
// and report timeout rather than cancelled.
func TestLeiosServeWaitStillBoundedByTimeout(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: 50 * time.Millisecond,
	})

	certRB := testDijkstraCertRBRaw(t, 81, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa2
	block := models.Block{Cbor: certRB, Slot: 81, Hash: []byte{0x81}}

	got, err := f.o.serveLeiosCertRbWithWait(
		block,
		ebHash,
		block.Slot,
		f.conn.Id(),
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "timeout")
}

// TestLeiosServeWaiterNotRegisteredForClosedConnection covers the race the
// liveness re-check in registerLeiosServeWaiter closes: a connection already
// removed from the manager must not produce a wait that nothing will ever
// release, since its ConnClosedFunc may already have run.
func TestLeiosServeWaiterNotRegisteredForClosedConnection(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: time.Hour,
	})
	connId := f.conn.Id()

	// Drop the connection from the manager, then start a wait for it.
	require.NoError(t, f.h.Disconnect())
	testutil.WaitForCondition(
		t,
		func() bool {
			return f.o.connManager.GetConnectionById(connId) == nil
		},
		5*time.Second,
		"connection to be removed from the connection manager",
	)

	certRB := testDijkstraCertRBRaw(t, 82, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa3
	block := models.Block{Cbor: certRB, Slot: 82, Hash: []byte{0x82}}

	type result struct {
		cbor []byte
		err  error
	}
	results := make(chan result, 1)
	go func() {
		cbor, err := f.o.serveLeiosCertRbWithWait(
			block,
			ebHash,
			block.Slot,
			connId,
			nil,
		)
		results <- result{cbor: cbor, err: err}
	}()

	got := testutil.RequireReceive(
		t,
		results,
		10*time.Second,
		"closure wait to return immediately for an already-closed connection",
	)
	require.Error(t, got.err)
	require.ErrorIs(t, got.err, errLeiosClosureUnresolved)
	require.Nil(t, got.cbor)
	require.Contains(t, got.err.Error(), "cancelled")
}

// cappingBlockTxsRequester simulates a relay that serves at most maxPerResp
// transactions per BlockTxsRequest — a prefix of the requested ascending
// indices, mirroring the prototype relay's per-message size cap. Each returned
// transaction's CBOR encodes its absolute index so callers can verify ordering.
// includeBitmaps toggles whether the response echoes the served bitmaps (the
// prototype's 4-element form) or omits them (forcing the prefix fallback).
type cappingBlockTxsRequester struct {
	maxPerResp     int // <= 0 means no cap (serve all requested)
	serveNothing   bool
	includeBitmaps bool
	calls          int
}

func (r *cappingBlockTxsRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	n := len(requested)
	if r.serveNothing {
		n = 0
	} else if r.maxPerResp > 0 && n > r.maxPerResp {
		n = r.maxPerResp
	}
	served := map[uint16]uint64{}
	txs := make([]cbor.RawMessage, 0, n)
	for k, idx := range requested {
		if k >= n {
			break
		}
		served[uint16(idx/64)] |= 1 << uint(
			63-(idx%64),
		) // MSB-first, see leiosWindowNeededMask
		enc, err := cbor.Encode(idx)
		if err != nil {
			return nil, err
		}
		txs = append(txs, cbor.RawMessage(enc))
	}
	if r.includeBitmaps {
		return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
	}
	return leiosfetch.NewMsgBlockTxs(txs), nil
}

func requireTxsInIndexOrder(t *testing.T, txs []cbor.RawMessage, want int) {
	t.Helper()
	require.Len(t, txs, want)
	for i, raw := range txs {
		var idx int
		_, err := cbor.Decode(raw, &idx)
		require.NoError(t, err)
		require.Equalf(t, i, idx, "tx at position %d encodes index %d", i, idx)
	}
}

func TestFetchLeiosEbTxsBatchedReRequestsUntilComplete(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	// 639 txs across 10 windows; relay caps each response at 50, so most
	// windows need multiple rounds. Response echoes served bitmaps.
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{maxPerResp: 50, includeBitmaps: true},
		point,
		639,
		nil,
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 639)
}

func TestFetchLeiosEbTxsBatchedPrefixFallback(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	// Response omits bitmaps, so the fetch must assume a served prefix of the
	// requested ascending indices. Cap of 40 (< 64) forces re-requests.
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{maxPerResp: 40, includeBitmaps: false},
		point,
		116,
		nil,
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 116)
}

func TestFetchLeiosEbTxsBatchedFullResponse(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// No cap: every window served whole in one round.
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{maxPerResp: 0, includeBitmaps: true},
		point,
		200,
		nil,
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 200)
}

func TestFetchLeiosEbTxsBatchedNoProgressErrors(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// A relay that serves nothing must not loop forever; it returns an error
	// with whatever prefix was gathered (none here).
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{serveNothing: true, includeBitmaps: true},
		point,
		10,
		nil,
	)
	require.Error(t, err)
	require.Empty(t, txs)
}

func TestFetchLeiosEbTxsBatchedRejectsUnrepresentableWindowCount(
	t *testing.T,
) {
	t.Parallel()

	requester := &cappingBlockTxsRequester{}
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}

	txs, err := o.fetchLeiosEbTxsBatched(
		requester,
		point,
		leiosTxFetchWindowSize*leiosTxFetchMaxWindows+1,
		nil,
	)
	require.Error(t, err)
	require.Nil(t, txs)
	require.Zero(t, requester.calls)
	require.Contains(t, err.Error(), "requires 65537 bitmap windows")
}

func TestLeiosBitmapTxIndices(t *testing.T) {
	t.Parallel()

	// MSB-first: window 0 offsets 0,1 are bits 63,62; window 2 offset 3 is
	// bit 60 -> indices 0,1,131 ascending.
	got := leiosBitmapTxIndices(
		map[uint16]uint64{0: (1 << 63) | (1 << 62), 2: 1 << 60},
	)
	require.Equal(t, []int{0, 1, 131}, got)
	require.Nil(t, leiosBitmapTxIndices(nil))
}

func TestLeiosWindowNeededMask(t *testing.T) {
	t.Parallel()

	result := make([]cbor.RawMessage, 70)
	result[0] = cbor.RawMessage{0x00} // present
	result[2] = cbor.RawMessage{0x00} // present
	// MSB-first: offset o is bit 63-o. Offsets 0 and 2 are present (bits 63
	// and 61 clear); offset 1 is needed (bit 62 set), capped at txCount 70.
	mask := leiosWindowNeededMask(result, 0, 70)
	require.Equal(t, uint64(0), mask&(1<<63))    // offset 0 present
	require.NotEqual(t, uint64(0), mask&(1<<62)) // offset 1 needed
	require.Equal(t, uint64(0), mask&(1<<61))    // offset 2 present
	// window 1: only indices 64..69 exist (offsets 0..5), all needed -> the
	// top 6 bits (63..58) set.
	require.Equal(
		t,
		uint64(0b111111)<<58,
		leiosWindowNeededMask(result, 1, 70),
	)
}

// TestLeiosBitmapMSBFirstWireConvention pins the bitmap bit ordering to
// MSB-first, matching the IOG Leios relay: the transaction at window offset 0
// is the most-significant bit (bit 63). Encoding it LSB-first round-tripped
// fine against a dingo peer but made the relay serve only the high-index
// transactions of a partial window -- and nothing at all for a final window of
// <=32 txs -- so from-genesis catch-up stalled mid-epoch. This
// guards the request encode, the decode, and the server serve/validate paths
// against silently reverting to LSB (which a self-consistent mock would miss).
func TestLeiosBitmapMSBFirstWireConvention(t *testing.T) {
	t.Parallel()

	// 131 txs: windows 0,1 full; final window 2 holds just offsets 0,1,2
	// (indices 128,129,130) -- the small-final-window the LSB bug never served.
	const txCount = 131
	result := make([]cbor.RawMessage, txCount)
	mask := leiosWindowNeededMask(result, 2, txCount)
	// Offsets 0,1,2 must be the TOP bits 63,62,61 (what the relay reads), not
	// the bottom bits an LSB encoding would set.
	require.Equal(t, uint64(0b111)<<61, mask)
	// Decoding the same window yields the absolute indices in ascending order.
	require.Equal(
		t,
		[]int{128, 129, 130},
		leiosBitmapTxIndices(map[uint16]uint64{2: mask}),
	)
	// Server side: the bitmap is in range and selects exactly those txs.
	txs := make([]cbor.RawMessage, txCount)
	for i := range txs {
		txs[i] = cbor.RawMessage{byte(i)}
	}
	require.NoError(
		t,
		validateLeiosTxBitmap(txCount, map[uint16]uint64{2: mask}),
	)
	require.Equal(
		t,
		[]cbor.RawMessage{txs[128], txs[129], txs[130]},
		leiosTxsFromBitmap(txs, map[uint16]uint64{2: mask}),
	)
}

func TestLeiosNeededBitmap(t *testing.T) {
	t.Parallel()

	// 600 txs spans 10 windows (0..9); none fetched yet.
	result := make([]cbor.RawMessage, 600)
	// A batch is capped at maxWindows lowest-indexed windows.
	bm := leiosNeededBitmap(result, 600, leiosTxFetchWindowsPerRequest)
	require.Len(t, bm, leiosTxFetchWindowsPerRequest)
	for w := range uint16(leiosTxFetchWindowsPerRequest) {
		require.Contains(t, bm, w, "lowest windows selected first")
	}
	require.NotContains(t, bm, uint16(8))
	// Mark window 0 fully present; the batch then starts at window 1 and still
	// reaches one window past the previous cap.
	for i := range leiosTxFetchWindowSize {
		result[i] = cbor.RawMessage{0x00}
	}
	bm = leiosNeededBitmap(result, 600, leiosTxFetchWindowsPerRequest)
	require.NotContains(t, bm, uint16(0), "fully fetched window is skipped")
	require.Contains(t, bm, uint16(8))
	// Fewer remaining windows than the cap returns just those windows.
	require.Len(t, leiosNeededBitmap(result, 600, 100), 9)
}

// buildBitmapResponseTxs serves every index named by bitmaps: it decodes them
// in ascending order, sets the matching bit of the served response bitmap for
// each (MSB-first, see leiosWindowNeededMask), and encodes each transaction's
// absolute index as its CBOR body (see requireTxsInIndexOrder). It is the
// single source of truth for "serve everything requested, in full", shared by
// every fake relay below that needs that baseline behavior.
func buildBitmapResponseTxs(
	bitmaps map[uint16]uint64,
) (map[uint16]uint64, []cbor.RawMessage, error) {
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	served := map[uint16]uint64{}
	txs := make([]cbor.RawMessage, 0, len(requested))
	for _, idx := range requested {
		served[uint16(idx/64)] |= 1 << uint(
			63-(idx%64),
		) // MSB-first, see leiosWindowNeededMask
		enc, err := cbor.Encode(idx)
		if err != nil {
			return nil, nil, err
		}
		txs = append(txs, cbor.RawMessage(enc))
	}
	return served, txs, nil
}

// servingBlockTxsRequester serves every requested transaction in a single
// response (no per-message cap), echoing the served bitmap. It records the
// largest number of windows asked for in one request so a test can assert the
// fetch batches windows rather than requesting them one at a time.
type servingBlockTxsRequester struct {
	calls          int
	maxWindowsSeen int
}

func (r *servingBlockTxsRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	if len(bitmaps) > r.maxWindowsSeen {
		r.maxWindowsSeen = len(bitmaps)
	}
	served, txs, err := buildBitmapResponseTxs(bitmaps)
	if err != nil {
		return nil, err
	}
	return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
}

// oversizedBitmapRequester serves a legitimate-looking response for a small
// endorser block but echoes a response bitmap that also references a window
// far beyond txCount, simulating a relay (malicious or buggy) that declares a
// tiny transaction count yet returns a disproportionately large bitmap
type oversizedBitmapRequester struct {
	// extraWindow, when non-zero, is set to extraMask in the response bitmap
	// in addition to the legitimately served windows. extraMask has no effect
	// when extraWindow is zero.
	extraWindow uint16
	extraMask   uint64
}

func (r *oversizedBitmapRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	served, txs, err := buildBitmapResponseTxs(bitmaps)
	if err != nil {
		return nil, err
	}
	if r.extraWindow != 0 {
		served[r.extraWindow] = r.extraMask
	}
	return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
}

// TestFetchLeiosEbTxsBatchedRejectsOversizedResponseBitmap simulates a relay
// for a 1-transaction endorser block that echoes a legitimate response for
// that one transaction but also sets an unrelated, far-out-of-range bitmap
// window (1000, all 64 bits). A response bitmap that claims transactions the
// block cannot possibly have must be rejected outright (with an error
// mentioning "leios-fetch response bitmap"), not silently expanded into a
// huge index list.
func TestFetchLeiosEbTxsBatchedRejectsOversizedResponseBitmap(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// txCount 1 fits entirely in window 0; the relay also sets every bit of
	// window 1000, referencing indices far beyond the single requested
	// transaction.
	requester := &oversizedBitmapRequester{
		extraWindow: 1000,
		extraMask:   math.MaxUint64,
	}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, 1, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "leios-fetch response bitmap")
	require.Empty(t, txs)
}

// TestFetchLeiosEbTxsBatchedRejectsResponseBitmapPastBoundary simulates a
// relay for a 64-transaction endorser block (which fills window 0 exactly,
// indices 0..63) that also sets a single bit of window 1 — offset 0 of that
// window, i.e. index 64, the very first index past the last one this block
// can have. Even this smallest possible one-past-the-end violation must be
// rejected, proving the bound check is exact rather than merely "roughly
// close enough".
func TestFetchLeiosEbTxsBatchedRejectsResponseBitmapPastBoundary(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// txCount 64 exactly fills window 0 (indices 0..63); window 1 has no valid
	// indices at all, so setting just its offset-0 bit (bit 63, MSB-first —
	// see leiosWindowNeededMask — i.e. index 64) must be rejected as one past
	// the exact boundary.
	requester := &oversizedBitmapRequester{
		extraWindow: 1,
		extraMask:   1 << 63,
	}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, 64, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "leios-fetch response bitmap")
	require.Empty(t, txs)
}

// TestFetchLeiosEbTxsBatchedAcceptsExactBoundaryResponseBitmap simulates a
// relay for a 64-transaction endorser block that echoes a response bitmap
// covering exactly window 0 (indices 0..63) and nothing more — the largest
// bitmap that is still entirely valid for this block. This must be accepted
// and the fetch must complete normally, proving the new bound check does not
// reject legitimate, exactly-sized replies.
func TestFetchLeiosEbTxsBatchedAcceptsExactBoundaryResponseBitmap(
	t *testing.T,
) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// txCount 64 exactly fills window 0; a response bitmap covering only
	// window 0 (the exact boundary, no extra window) must be accepted.
	requester := &oversizedBitmapRequester{}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, 64, nil)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 64)
}

// TestFetchLeiosEbTxsBatchedPrefixFallback above already covers the empty
// (bitmaps omitted) response case: it falls back to the prefix assumption
// rather than being rejected as malformed.

func TestFetchLeiosEbTxsBatchedBatchesWindowsPerRequest(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01}}
	// 600 txs = 10 windows. With up-to-8-windows-per-request and a relay that
	// serves the whole request, this completes in 2 rounds (8 + 2 windows),
	// not 10 — proving requests batch multiple windows.
	requester := &servingBlockTxsRequester{}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, 600, nil)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 600)
	require.Equal(t, 2, requester.calls)
	require.Equal(t, leiosTxFetchWindowsPerRequest, requester.maxWindowsSeen)
}

// muxerServer is the subset of a gouroboros protocol server
// (*blockfetch.Server, *leiosfetch.Server, ...) that muxerServerPeer needs to
// start and stop. Every protocol package's Server embeds *protocol.Protocol,
// which defines both methods and is promoted onto the Server, so any of them
// satisfies this interface unchanged.
type muxerServer interface {
	Start()
	Stop()
}

// muxerServerPeer drives a real Dingo server-side protocol implementation
// (blockfetch, leios-fetch, ...) over a real net.Pipe/muxer pair, so
// assertions are about what Dingo actually puts on the wire rather than
// about what a callback returns directly. Each protocol still builds its own
// Config/Server -- gouroboros gives each protocol package distinct concrete
// types with no shared constructor -- but the net.Pipe/muxer plumbing and the
// send/readResponse wire mechanics below are identical across protocols, so
// this type is shared; each protocol-specific test file supplies only its
// own NewConfig/NewServer call (see newLeiosFetchServerPeer,
// newBlockfetchServerPeer).
type muxerServerPeer struct {
	peerConn net.Conn
	errChan  chan error
	muxer    *muxer.Muxer
	// pending holds segment payload bytes read but not yet returned by
	// readMessage, and pendingProtocolId the protocol those bytes belong to.
	pending           []byte
	pendingProtocolId uint16
}

// newMuxerServerPeer creates the net.Pipe pair and muxer, and returns the
// protocol.ProtocolOptions every protocol-specific *Server constructor needs
// (blockfetch.NewServer, leiosfetch.NewServer, ...) alongside the peer side.
// Build the protocol's Config/Server from opts, then call peer.start with it.
func newMuxerServerPeer(
	t *testing.T,
) (opts protocol.ProtocolOptions, peer *muxerServerPeer) {
	t.Helper()
	serverConn, peerConn := net.Pipe()
	m := muxer.New(serverConn)
	errChan := make(chan error, 4)
	opts = protocol.ProtocolOptions{
		ConnectionId: gconnection.ConnectionId{
			LocalAddr:  serverConn.LocalAddr(),
			RemoteAddr: serverConn.RemoteAddr(),
		},
		ErrorChan: errChan,
		Muxer:     m,
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	peer = &muxerServerPeer{peerConn: peerConn, errChan: errChan, muxer: m}
	t.Cleanup(func() {
		m.Stop()
		_ = serverConn.Close()
		_ = peerConn.Close()
	})
	return opts, peer
}

// start starts the caller's protocol server, then the muxer -- gouroboros
// requires the server to register itself with the muxer before the muxer
// starts dispatching -- and arranges for the server to stop during
// t.Cleanup. t.Cleanup runs LIFO, and this is registered after
// newMuxerServerPeer's own cleanup, so server.Stop still runs before the
// muxer/connection teardown that call registered, preserving the original
// stop order.
func (p *muxerServerPeer) start(t *testing.T, server muxerServer) {
	t.Helper()
	server.Start()
	p.muxer.Start()
	t.Cleanup(server.Stop)
}

// send writes msg to the server as a request segment for the given protocol.
func (p *muxerServerPeer) send(
	t *testing.T,
	protocolId uint16,
	msg protocol.Message,
) {
	t.Helper()
	data, err := cbor.Encode(msg)
	require.NoError(t, err)
	segment := muxer.NewSegment(protocolId, data, false)
	require.NotNil(t, segment)
	buf := &bytes.Buffer{}
	require.NoError(
		t,
		binary.Write(buf, binary.BigEndian, segment.SegmentHeader),
	)
	_, err = buf.Write(segment.Payload)
	require.NoError(t, err)
	_, err = p.peerConn.Write(buf.Bytes())
	require.NoError(t, err)
}

// readResponse reads one response segment, bounded by timeout so a request
// the server leaves pending fails the test instead of hanging it.
func (p *muxerServerPeer) readResponse(
	t *testing.T,
	timeout time.Duration,
) *muxer.Segment {
	t.Helper()
	require.NoError(t, p.peerConn.SetReadDeadline(time.Now().Add(timeout)))
	header := muxer.SegmentHeader{}
	require.NoError(t, binary.Read(p.peerConn, binary.BigEndian, &header))
	payload := make([]byte, header.PayloadLength)
	_, err := io.ReadFull(p.peerConn, payload)
	require.NoError(t, err)
	return &muxer.Segment{SegmentHeader: header, Payload: payload}
}

// readMessage returns the protocol ID and encoded bytes of the next single
// protocol message, bounded by timeout.
//
// A muxer segment boundary is not a message boundary. gouroboros' protocol
// send loop drains everything already queued into one payload buffer and
// emits it as a single segment (up to maxMessagesPerSegment messages), and
// splits a payload larger than muxer.SegmentMaxPayloadLength across several
// segments. So whenever the server queues a second message before the send
// loop has decided the boundary for the first, both messages arrive in one
// segment; comparing a whole segment payload against one encoded message is
// therefore racy. readMessage reassembles the byte stream and hands back
// exactly one CBOR message per call, which is what the protocol actually
// guarantees.
//
// Do not mix readMessage and readResponse on the same peer: readMessage
// buffers whatever a segment carried past the message it returns, and
// readResponse would read the connection past that buffer.
func (p *muxerServerPeer) readMessage(
	t *testing.T,
	timeout time.Duration,
) (uint16, []byte) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if len(p.pending) > 0 {
			var raw cbor.RawMessage
			n, err := cbor.Decode(p.pending, &raw)
			switch {
			case err == nil:
				// Cap the returned slice so a later append for a
				// continuation segment cannot write into it.
				msg := p.pending[:n:n]
				p.pending = p.pending[n:]
				return p.pendingProtocolId, msg
			case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
				// Message split across segments; read the rest below.
			default:
				require.NoError(t, err, "decoding buffered segment payload")
			}
		}
		remaining := time.Until(deadline)
		require.Positive(
			t,
			remaining,
			"timed out waiting for a complete protocol message",
		)
		segment := p.readResponse(t, remaining)
		if len(p.pending) == 0 {
			p.pendingProtocolId = segment.GetProtocolId()
		} else {
			require.Equal(
				t,
				p.pendingProtocolId,
				segment.GetProtocolId(),
				"segment for a different protocol split a buffered message",
			)
		}
		p.pending = append(p.pending, segment.Payload...)
	}
}

// encodeSegment returns the wire bytes of one raw segment, so a test can
// present exactly the framing gouroboros' send loop is allowed to produce.
func encodeSegment(t *testing.T, protocolId uint16, payload []byte) []byte {
	t.Helper()
	segment := muxer.NewSegment(protocolId, payload, true)
	require.NotNil(t, segment)
	buf := &bytes.Buffer{}
	require.NoError(
		t,
		binary.Write(buf, binary.BigEndian, segment.SegmentHeader),
	)
	_, err := buf.Write(segment.Payload)
	require.NoError(t, err)
	return buf.Bytes()
}

// TestMuxerServerPeerReadMessage pins the framing readMessage exists for: a
// segment boundary is not a message boundary, so a segment may carry several
// messages and a message may span several segments. Asserting on whole
// segment payloads made
// TestBlockfetchServerRequestRangeRejectsInvalidEnd flaky whenever the
// blockfetch send loop batched StartBatch and the first block body together.
func TestMuxerServerPeerReadMessage(t *testing.T) {
	const protocolId = uint16(3)
	first, err := cbor.Encode([]any{uint(2)})
	require.NoError(t, err)
	second, err := cbor.Encode(
		[]any{uint(4), cbor.NewByteString(bytes.Repeat([]byte{0xab}, 64))},
	)
	require.NoError(t, err)
	third, err := cbor.Encode([]any{uint(5)})
	require.NoError(t, err)

	for _, test := range []struct {
		name     string
		segments [][]byte
	}{
		{
			name:     "one message per segment",
			segments: [][]byte{first, second, third},
		},
		{
			name: "all messages batched into one segment",
			segments: [][]byte{
				slices.Concat(first, second, third),
			},
		},
		{
			name: "message split across segments",
			segments: [][]byte{
				slices.Concat(first, second[:10]),
				slices.Concat(second[10:], third),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverConn, peerConn := net.Pipe()
			t.Cleanup(func() {
				_ = serverConn.Close()
				_ = peerConn.Close()
			})
			peer := &muxerServerPeer{peerConn: peerConn}
			// Encode on the test goroutine: only the blocking writes
			// belong in the writer, which cannot call require.
			wire := make([][]byte, 0, len(test.segments))
			for _, payload := range test.segments {
				wire = append(
					wire,
					encodeSegment(t, protocolId, payload),
				)
			}
			written := make(chan error, 1)
			go func() {
				for _, segment := range wire {
					if _, err := serverConn.Write(segment); err != nil {
						written <- err
						return
					}
				}
				written <- nil
			}()
			for _, want := range [][]byte{first, second, third} {
				gotProtocolId, got := peer.readMessage(t, 5*time.Second)
				require.Equal(t, protocolId, gotProtocolId)
				require.Equal(t, want, got)
			}
			select {
			case err := <-written:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("segment writer did not finish")
			}
		})
	}
}

type listenerWithAddress struct {
	addr net.Addr
}

func (l *listenerWithAddress) Accept() (net.Conn, error) { return nil, net.ErrClosed }

func (l *listenerWithAddress) Close() error { return nil }

func (l *listenerWithAddress) Addr() net.Addr { return l.addr }

// TestIsTrustedNtCListener is the review regression:
// ConfigureListeners used to grant every UseNtC listener gouroboros' relaxed
// mux/query timeouts and 2GiB reassembly buffer unconditionally, on the
// premise that "NtC is a trusted local channel" -- true for a Unix socket,
// but not for internal/node/node.go's other UseNtC listener,
// cfg.PrivateBindAddr:cfg.PrivatePort, an ordinary operator-configurable TCP
// address with no code-enforced loopback restriction. An operator who
// widens PrivateBindAddr beyond loopback (or reaches it via a Unix socket
// from any local user/process) would give any client that completes an NtC
// handshake an unbounded mux segment-read timeout, an unbounded
// LocalStateQuery timeout, and a 2GiB-per-connection reassembly buffer --
// gouroboros' own anti-DoS defaults exist specifically to bound this for an
// untrusted remote peer.
func TestIsTrustedNtCListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		l    connmanager.ListenerConfig
		want bool
	}{
		{
			name: "unix socket is always trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "unix",
				ListenAddress: "/tmp/dingo.socket",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to IPv4 loopback is trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "127.0.0.1:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to IPv6 loopback is trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "[::1]:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to localhost hostname resolves as loopback",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "localhost:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to a wildcard address is not trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "0.0.0.0:3002",
				UseNtC:        true,
			},
			want: false,
		},
		{
			name: "tcp bound to a routable address is not trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "10.0.0.5:3002",
				UseNtC:        true,
			},
			want: false,
		},
		{
			name: "supplied non-loopback listener overrides loopback ListenAddress",
			l: connmanager.ListenerConfig{
				Listener: &listenerWithAddress{
					addr: &net.TCPAddr{
						IP:   net.ParseIP("192.0.2.10"),
						Port: 3002,
					},
				},
				ListenNetwork: "tcp",
				ListenAddress: "127.0.0.1:3002",
				UseNtC:        true,
			},
			want: false,
		},
		{
			name: "supplied loopback listener overrides routable ListenAddress",
			l: connmanager.ListenerConfig{
				Listener: &listenerWithAddress{
					addr: &net.TCPAddr{
						IP:   net.ParseIP("127.0.0.1"),
						Port: 3002,
					},
				},
				ListenNetwork: "tcp",
				ListenAddress: "192.0.2.10:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "unparseable tcp address is not trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "not-a-valid-address",
				UseNtC:        true,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isTrustedNtCListener(tt.l))
		})
	}
}

// TestConfigureListeners_UntrustedNtCListenerSkipsRelaxedTimeout covers the
// actual fix, not just the trust decision in isolation: a UseNtC listener
// ConfigureListeners cannot verify is local-only must not have
// WithMuxerSegmentReadTimeout(0) among its ConnectionOpts at all, so
// gouroboros' own 120s default mux segment-read timeout stays in force for
// it. There is no exported way to inspect a built ouroboros.ConnectionOptionFunc
// slice's effect directly, so this counts the length of ConnectionOpts a
// trusted vs. an untrusted NtC listener receive: the untrusted listener must
// end up with exactly one fewer option (the omitted WithMuxerSegmentReadTimeout).
func TestConfigureListeners_UntrustedNtCListenerSkipsRelaxedTimeout(
	t *testing.T,
) {
	t.Parallel()

	o := &Ouroboros{
		config: OuroborosConfig{},
	}

	trustedListener := connmanager.ListenerConfig{
		ListenNetwork: "unix",
		ListenAddress: "/tmp/dingo-test.socket",
		UseNtC:        true,
	}
	untrustedListener := connmanager.ListenerConfig{
		ListenNetwork: "tcp",
		ListenAddress: "0.0.0.0:3002",
		UseNtC:        true,
	}

	configured := o.ConfigureListeners(
		context.Background(),
		[]connmanager.ListenerConfig{trustedListener, untrustedListener},
	)
	assert.Len(t, configured, 2)

	assert.Equal(
		t,
		len(configured[0].ConnectionOpts),
		len(configured[1].ConnectionOpts)+1,
		"an untrusted NtC listener must receive exactly one fewer "+
			"ConnectionOpts entry than a trusted one -- the omitted "+
			"WithMuxerSegmentReadTimeout(0)",
	)
}

func TestConfigureListenersClassifiesSuppliedListenerByBoundAddress(
	t *testing.T,
) {
	t.Parallel()

	o := &Ouroboros{config: OuroborosConfig{}}
	configured := o.ConfigureListeners(
		context.Background(),
		[]connmanager.ListenerConfig{{
			Listener: &listenerWithAddress{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("192.0.2.10"),
					Port: 3002,
				},
			},
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:3002",
			UseNtC:        true,
		}},
	)

	assert.Len(t, configured, 1)
	assert.False(t, configured[0].TrustedLocal)
}

// TestConfigureListeners_NormalizesTCPListenAddressToNumeric is the
// review regression for a TOCTOU in
// isTrustedNtCListener: it resolved l.ListenAddress to classify the
// listener, but connmanager's startListener later binds the same
// listener's ListenAddress by calling net.Listen on the original,
// unresolved string -- a second, independent DNS lookup. If a hostname
// (or "localhost") resolved differently between the two lookups, a
// listener classified trusted from the first answer could bind to a
// different, non-loopback address on the second, handing that listener
// the relaxed timeouts and 2GiB reassembly buffer meant only for a
// verified-local one.
//
// ConfigureListeners now resolves a TCP NtC listener's address once and
// rewrites ListenAddress to the resulting numeric form before
// classifying it, so classification and the later bind are guaranteed to
// use the exact same literal address -- there is no second lookup left
// to disagree with the first. This proves that rewrite actually happens:
// a "localhost:0" input must come back as a numeric loopback address,
// not the original hostname string.
func TestConfigureListeners_NormalizesTCPListenAddressToNumeric(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{
		config: OuroborosConfig{},
	}

	configured := o.ConfigureListeners(
		context.Background(),
		[]connmanager.ListenerConfig{
			{
				ListenNetwork: "tcp",
				ListenAddress: "localhost:0",
				UseNtC:        true,
			},
		},
	)
	assert.Len(t, configured, 1)
	assert.NotEqual(
		t,
		"localhost:0",
		configured[0].ListenAddress,
		"ConfigureListeners must rewrite a hostname ListenAddress to its "+
			"resolved numeric form, not leave it for a second, "+
			"independent resolution at bind time",
	)
	host, _, err := net.SplitHostPort(configured[0].ListenAddress)
	assert.NoError(t, err)
	assert.NotNil(
		t,
		net.ParseIP(host),
		"the rewritten ListenAddress %q must have a literal IP host",
		configured[0].ListenAddress,
	)
}
