// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
)

// behindPeerFixture is a ledger state on a linked chain long enough that a
// rollback well inside the chain still exceeds the (small) security parameter,
// which is the shape a lagging peer's FindIntersect produces on a real network.
type behindPeerFixture struct {
	ls        *LedgerState
	connId    ouroboros.ConnectionId
	blocks    []chain.RawBlock
	resyncCh  chan event.ChainsyncResyncEvent
	securityP int
}

const (
	behindPeerChainLength   = 20
	behindPeerSecurityParam = 4
)

// tipOf returns the chainsync tip for the block at depth blocks below the
// chain tip.
func (f *behindPeerFixture) tipAtDepth(depth int) ochainsync.Tip {
	blk := f.blocks[len(f.blocks)-1-depth]
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(blk.Slot, blk.Hash),
		BlockNumber: blk.BlockNumber,
	}
}

func (f *behindPeerFixture) pointAtDepth(depth int) ocommon.Point {
	return f.tipAtDepth(depth).Point
}

func newBehindPeerFixture(t *testing.T) *behindPeerFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(
			testSecurityParamLedger{securityParam: behindPeerSecurityParam},
		),
	)

	blocks := make([]chain.RawBlock, 0, behindPeerChainLength)
	var prevHash []byte
	for idx := range behindPeerChainLength {
		hash := testHashBytes(fmt.Sprintf("behind-peer-block-%d", idx))
		blocks = append(blocks, chain.RawBlock{
			Slot:        uint64(idx+1) * 10,
			Hash:        hash,
			BlockNumber: uint64(idx + 1),
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(blocks))

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	tipBlock := blocks[len(blocks)-1]
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(tipBlock.Slot, tipBlock.Hash),
		BlockNumber: tipBlock.BlockNumber,
	}
	nonce := []byte("nonce-behind-peer-tip")
	require.NoError(
		t,
		db.SetBlockNonce(tip.Point.Hash, tip.Point.Slot, nonce, false, nil),
	)
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.currentTipBlockNonce = append([]byte(nil), nonce...)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	ls.config.EventBus = bus
	resyncCh := make(chan event.ChainsyncResyncEvent, 8)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	return &behindPeerFixture{
		ls: ls,
		connId: ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 6000,
			},
			RemoteAddr: &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 3001,
			},
		},
		blocks:    blocks,
		resyncCh:  resyncCh,
		securityP: behindPeerSecurityParam,
	}
}

// requireNoResyncEvent fails if any chainsync re-sync was published. Every
// re-sync reason reachable from the over-K branch closes the connection, and
// the over-K reason additionally denies the peer for two minutes, which is the
// eviction this fix exists to prevent.
func (f *behindPeerFixture) requireNoResyncEvent(t *testing.T) {
	t.Helper()
	select {
	case e := <-f.resyncCh:
		t.Fatalf(
			"peer merely behind on our own chain must not trigger a "+
				"chainsync re-sync, got reason %q",
			e.Reason,
		)
	case <-time.After(200 * time.Millisecond):
	}
}

// A peer whose advertised tip is an ancestor of our tip holds a strict prefix
// of our chain: it is behind, not forked. Its FindIntersect answer is only as
// deep as our intersect ladder's granularity, so the resulting "fork depth" can
// exceed K with nothing having diverged. We must keep such a peer: no rollback,
// no re-sync, no denial, and no unrecoverable-divergence escalation telling the
// operator to re-bootstrap a perfectly canonical database.
func TestHandleEventChainsyncRollbackKeepsPeerBehindOnOurChain(t *testing.T) {
	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	// The rollback point is 12 blocks back — past K=4 — and the peer's own
	// advertised tip is 6 blocks back, both on our primary chain.
	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Point:        f.pointAtDepth(12),
			Tip:          f.tipAtDepth(6),
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(
		t,
		localTip,
		f.ls.chain.Tip(),
		"a peer behind us must not move our chain tip",
	)
	assert.Equal(t, SyncingChainsyncState, f.ls.chainsyncState)
	f.requireNoResyncEvent(t)
	assert.Zero(
		t,
		promtestutil.ToFloat64(f.ls.metrics.unrecoverableRollbacks),
		"a peer that is merely behind must not count as unrecoverable divergence",
	)
	assert.Empty(
		t,
		f.ls.unrecoverableRollbacks,
		"a peer that is merely behind must not be tracked as un-crossable",
	)
}

// The same behind peer reconnecting and offering the same intersect must not
// escalate either: the per-connection rollback-loop detector fires on the
// second identical rollback, and its un-crossable branch reports the
// operator-facing "local chain has diverged ... re-bootstrap from a Mithril
// snapshot" error, which would destroy a healthy database.
func TestHandleEventChainsyncRollbackBehindPeerRepeatDoesNotEscalate(
	t *testing.T,
) {
	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	evt := ChainsyncEvent{
		ConnectionId: f.connId,
		Point:        f.pointAtDepth(12),
		Tip:          f.tipAtDepth(6),
	}
	for attempt := range rollbackLoopThreshold + 2 {
		require.NoErrorf(
			t,
			f.ls.handleEventChainsyncRollback(evt, nil),
			"attempt %d",
			attempt,
		)
	}

	assert.Equal(t, localTip, f.ls.chain.Tip())
	f.requireNoResyncEvent(t)
	assert.Zero(
		t,
		promtestutil.ToFloat64(f.ls.metrics.unrecoverableRollbacks),
	)
}

// Control: a peer whose advertised tip is not on our chain and which asks us to
// roll back further than K is a genuine deep fork. It must still be rejected
// and evicted — the fix must not weaken the security-parameter gate.
func TestHandleEventChainsyncRollbackStillRejectsDeepForkPeer(t *testing.T) {
	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Point:        f.pointAtDepth(12),
			// A tip on a competing chain: our chain has no such block.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					f.pointAtDepth(6).Slot,
					testHashBytes("competing-fork-tip"),
				),
				BlockNumber: f.tipAtDepth(6).BlockNumber,
			},
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, localTip, f.ls.chain.Tip())
	select {
	case e := <-f.resyncCh:
		assert.Equal(
			t,
			event.ChainsyncResyncReasonRollbackExceedsK,
			e.Reason,
		)
		assert.Equal(t, f.connId, e.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal("expected over-K rejection for a genuinely divergent peer")
	}
}

// Control: an unknown (zero) peer tip carries no evidence that the peer is
// behind rather than forked, so it must fail safe to the existing rejection.
func TestHandleEventChainsyncRollbackRejectsUnknownPeerTipOverK(t *testing.T) {
	f := newBehindPeerFixture(t)

	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Point:        f.pointAtDepth(12),
		},
		nil,
	)
	require.NoError(t, err)

	select {
	case e := <-f.resyncCh:
		assert.Equal(
			t,
			event.ChainsyncResyncReasonRollbackExceedsK,
			e.Reason,
		)
	case <-time.After(time.Second):
		t.Fatal("expected over-K rejection for an unknown peer tip")
	}
}

// Control: a peer whose advertised tip is ahead of ours is offering a
// competing longer chain, not a prefix of ours, even when the rollback point
// itself is on our chain. It must keep the over-K rejection.
func TestHandleEventChainsyncRollbackRejectsPeerTipAheadOfUs(t *testing.T) {
	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Point:        f.pointAtDepth(12),
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					localTip.Point.Slot+10,
					testHashBytes("longer-competing-chain-tip"),
				),
				BlockNumber: localTip.BlockNumber + 1,
			},
		},
		nil,
	)
	require.NoError(t, err)

	select {
	case e := <-f.resyncCh:
		assert.Equal(
			t,
			event.ChainsyncResyncReasonRollbackExceedsK,
			e.Reason,
		)
	case <-time.After(time.Second):
		t.Fatal("expected over-K rejection for a peer ahead of us")
	}
}

// The behind-peer classification is observable: an operator seeing repeated
// over-K rejections needs to tell "our upstreams are lagging us" from "someone
// is offering a competing chain", and the counter is the signal that
// distinguishes them.
func TestHandleEventChainsyncRollbackBehindPeerCounter(t *testing.T) {
	f := newBehindPeerFixture(t)

	require.NoError(t, f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Point:        f.pointAtDepth(12),
			Tip:          f.tipAtDepth(6),
		},
		nil,
	))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(f.ls.metrics.chainsyncBehindPeers),
	)

	// A genuinely divergent peer must not be counted as behind.
	divergent := newBehindPeerFixture(t)
	require.NoError(t, divergent.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: divergent.connId,
			Point:        divergent.pointAtDepth(12),
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					divergent.pointAtDepth(6).Slot,
					testHashBytes("counter-competing-fork-tip"),
				),
				BlockNumber: divergent.tipAtDepth(6).BlockNumber,
			},
		},
		nil,
	))
	assert.Zero(
		t,
		promtestutil.ToFloat64(divergent.ls.metrics.chainsyncBehindPeers),
	)
}

// Control: a peer advertising exactly our own tip is not behind us. Asking for
// a rollback past K from there is intersect drift, not a lagging peer, and must
// keep the existing rejection.
func TestHandleEventChainsyncRollbackRejectsPeerTipEqualToOurs(t *testing.T) {
	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Point:        f.pointAtDepth(12),
			Tip:          f.tipAtDepth(0),
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, localTip, f.ls.chain.Tip())
	select {
	case e := <-f.resyncCh:
		assert.Equal(
			t,
			event.ChainsyncResyncReasonRollbackExceedsK,
			e.Reason,
		)
	case <-time.After(time.Second):
		t.Fatal("expected over-K rejection for a peer at our own tip")
	}
}
