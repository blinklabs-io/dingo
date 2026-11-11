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
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// reintersectFixture runs dingo's resync handler against a real chainsync
// client whose peer streams blocks as fast as it is asked.
type reintersectFixture struct {
	o         *Ouroboros
	cm        *connmanager.ConnectionManager
	state     *dchainsync.State
	connId    ouroboros.ConnectionId
	intersect ocommon.Point

	findIntersects atomic.Int32
	rollForwards   atomic.Int32
	rollBackwards  atomic.Int32
	// await makes the peer answer every later RequestNext with AwaitReply and
	// never send the block, as a peer at its tip does.
	await atomic.Bool
	// refuseIntersect makes the peer answer every later FindIntersect with
	// IntersectNotFound.
	refuseIntersect atomic.Bool

	mu           sync.Mutex
	offered      [][]ocommon.Point
	needRollback bool
}

func newReintersectFixture(
	t *testing.T,
	timeout time.Duration,
) *reintersectFixture {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{EventBus: bus, Logger: logger},
	)
	t.Cleanup(func() {
		_ = cm.Stop(t.Context())
	})

	ls, _ := newTestLedgerStateWithChain(t, 3)
	chainTip := ls.PrimaryChainTip()
	f := &reintersectFixture{
		cm:        cm,
		intersect: chainTip.Point,
	}
	tip := chainTip

	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() { _ = serverPipe.Close(); _ = clientPipe.Close() })
	serverConfig := ochainsync.NewConfig(
		ochainsync.WithFindIntersectFunc(func(
			_ ochainsync.CallbackContext,
			points []ocommon.Point,
		) (ocommon.Point, ochainsync.Tip, error) {
			n := f.findIntersects.Add(1)
			f.mu.Lock()
			f.offered = append(f.offered, points)
			f.needRollback = true
			f.mu.Unlock()
			if n > 1 && f.refuseIntersect.Load() {
				return ocommon.Point{}, tip, ochainsync.ErrIntersectNotFound
			}
			return f.intersect, tip, nil
		}),
		ochainsync.WithRequestNextFunc(func(cb ochainsync.CallbackContext) error {
			if f.await.Load() {
				return cb.Server.AwaitReply()
			}
			f.mu.Lock()
			rollback := f.needRollback
			f.needRollback = false
			f.mu.Unlock()
			if rollback {
				return cb.Server.RollBackward(f.intersect, tip)
			}
			return cb.Server.RollForward(1, []byte{0x80}, tip)
		}),
	)
	serverReady := make(chan error, 1)
	var serverConn *ouroboros.Connection
	go func() {
		var err error
		serverConn, err = ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithDelayProtocolStart(true),
			ouroboros.WithLogger(logger),
			ouroboros.WithChainSyncConfig(serverConfig),
		)
		serverReady <- err
	}()
	f.o = newOuroboros(OuroborosConfig{EventBus: bus, Logger: logger})
	clientConfig := ochainsync.NewConfig(
		ochainsync.WithPipelineLimit(4),
		ochainsync.WithIntersectFoundFunc(f.o.chainsyncClientIntersectFound),
		ochainsync.WithRollForwardRawFunc(func(
			ochainsync.CallbackContext,
			uint,
			[]byte,
			ochainsync.Tip,
		) error {
			f.rollForwards.Add(1)
			return nil
		}),
		ochainsync.WithRollBackwardFunc(func(
			ochainsync.CallbackContext,
			ocommon.Point,
			ochainsync.Tip,
		) error {
			f.rollBackwards.Add(1)
			return nil
		}),
	)
	client, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithNetworkMagic(42),
		ouroboros.WithDelayProtocolStart(true),
		ouroboros.WithLogger(logger),
		ouroboros.WithChainSyncConfig(clientConfig),
	)
	require.NoError(t, err)
	require.NoError(t, testutil.RequireReceive(
		t, serverReady, 5*time.Second, "server handshake",
	))
	t.Cleanup(func() { _ = client.Close(); _ = serverConn.Close() })
	require.True(t, cm.AddConnection(client, false, "127.0.0.1:3001"))

	f.connId = client.Id()
	f.state = dchainsync.NewState(bus, nil)
	require.True(t, f.state.AddClientConnId(f.connId))
	f.o.eventBus = bus
	f.o.connManager = cm
	f.o.chainsyncState = f.state
	f.o.ledgerState = ls
	f.o.chainsyncReintersectTimeout = timeout
	f.o.SubscribeChainsyncResync(t.Context())

	serverConn.ChainSync().Server.Start()
	client.ChainSync().Client.Start()
	require.NoError(t, client.ChainSync().Client.Sync([]ocommon.Point{f.intersect}))
	testutil.WaitForCondition(
		t,
		func() bool { return f.rollForwards.Load() > 0 },
		5*time.Second,
		"peer never started streaming",
	)
	return f
}

func (f *reintersectFixture) publish(reason string) {
	f.o.eventBus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: f.connId,
				Reason:       reason,
			},
		),
	)
}

func (f *reintersectFixture) connected() bool {
	return f.cm.GetConnectionById(f.connId) != nil
}

// A local resync reason re-intersects the live client on its own connection:
// the peer sees one more FindIntersect, answers it, and keeps streaming.
func TestChainsyncResyncReintersectsInPlace(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return f.findIntersects.Load() == 2 },
		5*time.Second,
		"the resync must send one more FindIntersect",
	)
	forwardsAtIntersect := f.rollForwards.Load()
	testutil.WaitForCondition(
		t,
		func() bool {
			return f.rollBackwards.Load() >= 1 &&
				f.rollForwards.Load() > forwardsAtIntersect
		},
		5*time.Second,
		"the re-intersected sync must roll back to the intersection and stream on",
	)
	require.True(t, f.connected(), "an in-place resync must keep the connection")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.offered[1])
	require.Equal(t, f.intersect, f.offered[1][0],
		"the re-intersect must offer the ledger's own points")
}

// Evidence a peer delivered at or below the new intersection survives the
// re-intersect, and anything above it is dropped.
func TestChainsyncResyncReintersectKeepsHistoryBelowIntersection(
	t *testing.T,
) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	below := newTestBlockHeader(f.intersect.Slot-1, 2, 0xa1)
	at := newTestBlockHeader(f.intersect.Slot, 3, 0xa2)
	above := newTestBlockHeader(f.intersect.Slot+5, 4, 0xa3)
	for _, hdr := range []gledger.BlockHeader{below, at, above} {
		f.state.RecordObservedHeader(dchainsync.ObservedHeader{
			ConnectionId: f.connId,
			BlockHeader:  hdr,
			Point: ocommon.NewPoint(
				hdr.SlotNumber(), hdr.Hash().Bytes(),
			),
		})
	}

	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.WaitForCondition(
		t,
		func() bool { return f.findIntersects.Load() == 2 },
		5*time.Second,
		"the resync must re-intersect",
	)
	testutil.WaitForCondition(
		t,
		func() bool {
			_, _, ok := f.state.LookupObservedHeader(
				f.connId, above.Hash().Bytes(),
			)
			return !ok
		},
		5*time.Second,
		"history above the intersection must be dropped",
	)
	for name, hdr := range map[string][]byte{
		"below": below.Hash().Bytes(),
		"at":    at.Hash().Bytes(),
	} {
		_, _, ok := f.state.LookupObservedHeader(f.connId, hdr)
		require.True(t, ok, "history %s the intersection must survive", name)
	}
	require.True(t, f.connected())
}

// A re-intersect that cannot finish inside the watchdog falls back to the
// close: at its tip a peer answers the drained request only with the next
// block.
func TestChainsyncResyncReintersectWatchdogFallsBackToClose(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 300*time.Millisecond)
	f.await.Store(true)

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"a re-intersect that never drains must close the connection",
	)
	require.EqualValues(t, 1, f.findIntersects.Load(),
		"no FindIntersect may be sent while a reply is outstanding")
}

// A peer that does not know any offered point is closed rather than left in
// Idle with no sync running.
func TestChainsyncResyncReintersectNotFoundFallsBackToClose(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	f.refuseIntersect.Store(true)

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"IntersectNotFound must close the connection",
	)
	require.EqualValues(t, 2, f.findIntersects.Load())
}

// A resync that blames the peer closes it even though a client is running.
func TestChainsyncResyncPeerFaultReasonStillCloses(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)

	f.publish(event.ChainsyncResyncReasonRollbackExceedsK)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"a peer-fault resync must close the connection",
	)
	require.EqualValues(t, 1, f.findIntersects.Load(),
		"a peer-fault resync must not re-intersect")
}

// The re-intersected sync resumes as soon as Reintersect returns, so the
// history trim and the withheld-header marker must be settled before the first
// reply of the new cursor, not after Reintersect returns: a header of the new
// cursor delivered in between would be withheld or trimmed.
func TestChainsyncResyncReintersectSettlesStateBeforeSyncResumes(
	t *testing.T,
) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	cfg := ochainsync.NewConfig(f.o.chainsyncClientConnOpts()...)
	require.NotNil(t, cfg.IntersectFoundFunc,
		"production clients must settle a re-intersect on IntersectFound")
	above := newTestBlockHeader(f.intersect.Slot+5, 4, 0xa3)
	f.state.RecordObservedHeader(dchainsync.ObservedHeader{
		ConnectionId: f.connId,
		BlockHeader:  above,
		Point: ocommon.NewPoint(
			above.SlotNumber(), above.Hash().Bytes(),
		),
	})
	f.o.futureHeaderResyncMu.Lock()
	f.o.futureHeaderResyncs[f.connId] = &scheduledChainsyncResync{
		fired:  true,
		reason: event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery,
		cancel: func() {},
	}
	f.o.futureHeaderResyncMu.Unlock()
	returned := make(chan struct{})
	release := make(chan struct{})
	f.o.chainsyncReintersectReturnedHook = func() {
		close(returned)
		<-release
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	f.publish(event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery)
	testutil.RequireReceive(
		t, returned, 5*time.Second, "Reintersect must return",
	)
	forwardsAtReturn := f.rollForwards.Load()
	testutil.WaitForCondition(
		t,
		func() bool {
			return f.rollBackwards.Load() >= 1 &&
				f.rollForwards.Load() > forwardsAtReturn
		},
		5*time.Second,
		"the re-intersected sync must stream on",
	)
	require.False(t, f.o.futureHeaderResyncPending(f.connId),
		"the new cursor's headers must not be withheld")
	_, _, ok := f.state.LookupObservedHeader(f.connId, above.Hash().Bytes())
	require.False(t, ok,
		"history above the intersection must be gone before the new cursor")
	close(release)
}
