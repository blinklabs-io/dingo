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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/consensus"
	"github.com/blinklabs-io/ouroboros-mock/consensus/format"
)

// TestConsensusConformanceVectors replays the upstream consensus-
// conformance corpus (vectors + Replayer harness embedded in
// ouroboros-mock) against dingo's real chainsync ingestion path and
// chain selector. It lives in package ouroboros so it can drive the
// unexported chainsync handlers directly — no exported test hooks leak
// into production code.
func TestConsensusConformanceVectors(t *testing.T) {
	vectors, err := consensus.CapturedVectors()
	if err != nil {
		t.Fatalf("CapturedVectors: %v", err)
	}
	if len(vectors) == 0 {
		t.Skip("no captured vectors embedded")
	}
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

const (
	tipEventBuffer    = 4096
	switchEventBuffer = 1024
)

// replayAdapter implements consensus.Replayer by driving dingo's real
// chainsync handlers and chain selector. The harness identifies peers by
// the vector's peer_id; the adapter synthesizes a stable ConnectionId per
// peer_id.
type replayAdapter struct {
	o        *Ouroboros
	cs       *chainselection.ChainSelector
	bus      *event.EventBus
	conns    map[uint64]ouroboros.ConnectionId
	tipCh    <-chan event.Event
	switchCh <-chan event.Event
	switches []format.SwitchEvent

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
	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		// Open the ingress-eligibility gate: the captured peers are the
		// upstreams we want feeding selection. With ChainsyncState left
		// nil, reconcileChainsyncIngressAdmission honours this directly.
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.EventBus = bus
	return &replayAdapter{
		o:        o,
		cs:       cs,
		bus:      bus,
		conns:    make(map[uint64]ouroboros.ConnectionId),
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
	rollbackPoint := ocommon.Point{
		Slot: point.Slot,
		Hash: append([]byte(nil), point.Hash...),
	}
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
	// Drain queued peer-tip updates into the selector, force a synchronous
	// evaluation, then collect any switch decisions it emitted. All
	// synchronous: no sleeps, no races.
	drainEvents(a.tipCh, func(evt event.Event) {
		a.tipEventsSeen++
		a.cs.HandlePeerTipUpdateEvent(evt)
	})
	a.cs.EvaluateAndSwitch()
	drainEvents(a.switchCh, func(evt event.Event) {
		e, ok := evt.Data.(chainselection.ChainSwitchEvent)
		if !ok {
			return
		}
		a.switches = append(a.switches, format.SwitchEvent{
			PreviousTip: fromGouroborosTip(e.PreviousTip),
			NewTip:      fromGouroborosTip(e.NewTip),
		})
	})
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

func toGouroborosTip(t format.Tip) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: t.Slot,
			Hash: append([]byte(nil), t.Hash...),
		},
		BlockNumber: t.BlockNumber,
	}
}

func fromGouroborosTip(t ochainsync.Tip) format.Tip {
	return format.Tip{
		Slot:        t.Point.Slot,
		Hash:        append(format.HexBytes(nil), t.Point.Hash...),
		BlockNumber: t.BlockNumber,
	}
}
