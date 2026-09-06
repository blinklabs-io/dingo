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
	"context"
	"log/slog"
	"runtime"
	"testing"
	"time"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newOutboundStartTestOuroboros builds an Ouroboros wired with every
// dependency hasDependencies() requires, so HandleOutboundConnEvent actually
// reaches the chainsync start rather than dropping the event as unwired.
func newOutboundStartTestOuroboros(
	t *testing.T,
	logger *slog.Logger,
	bus *event.EventBus,
) (*Ouroboros, *connmanager.ConnectionManager, ouroboros.ConnectionId) {
	t.Helper()

	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: bus,
			Logger:   logger,
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})

	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		ouroboros_mock.ConversationKeepAlive,
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(
			keepalive.NewConfig(
				keepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
				keepalive.WithPeriod(30*time.Second),
				keepalive.WithTimeout(15*time.Second),
			),
		),
	)
	require.NoError(t, err)
	connManager.AddConnection(oConn, false, "127.0.0.1:1234")

	m, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       txsubmissionTestValidator{},
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, m.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, m.Stop(context.Background()))
	})

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.eventBus = bus
	o.connManager = connManager
	o.chainsyncState = dchainsync.NewState(bus, nil)
	o.mempool = &mempool.FIFO{Mempool: m}
	o.peerGov = peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	return o, connManager, oConn.Id()
}

// TestOutboundChainsyncStartFailureClosesConnection is the regression test for
// an outbound peer left half-connected.
//
// When chainsync fails to start, HandleOutboundConnEvent rolls back the
// registration and returns before starting txsubmission. It used to leave the
// TCP connection open, so peer governance still counted the peer as connected,
// nothing retried, and the peer was effectively lost for the lifetime of the
// connection. That matters for transient causes -- an intersect-point or
// rollback-anchor lookup hitting a storage fault -- which are recoverable on a
// reconnect but were never retried.
//
// The failure is induced here by closing the ledger's database, which makes
// the rollback-anchor lookup return a storage error, which fails the chainsync
// client start.
func TestOutboundChainsyncStartFailureClosesConnection(t *testing.T) {
	logBuf := &lockedBuffer{}
	logger := slog.New(
		slog.NewJSONHandler(
			logBuf,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	baselineGoroutines := runtime.NumGoroutine()

	o, connManager, connId := newOutboundStartTestOuroboros(t, logger, bus)

	// A ledger whose database is closed makes the rollback-anchor lookup
	// return a storage error rather than "no anchor".
	ls, db := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls
	o.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(2, ledgerTipHashAbsentFromChain),
		BlockNumber: 2,
	})
	require.NoError(t, dbtest.CloseDatabase(db))
	_, _, anchorErr := ls.RollbackWindowIntersectAnchor()
	require.Error(t, anchorErr, "fixture must produce an anchor lookup error")

	o.HandleOutboundConnEvent(event.NewEvent(
		peergov.OutboundConnectionEventType,
		peergov.OutboundConnectionEvent{ConnectionId: connId},
	))

	// The connection must not be left open: peer governance has to observe
	// the failure to apply backoff and reconnect.
	require.Eventually(
		t,
		func() bool { return connManager.GetConnectionById(connId) == nil },
		2*time.Second,
		20*time.Millisecond,
		"outbound connection was left open after chainsync start failure",
	)

	// The tracked chainsync client registration must have been rolled back.
	require.False(
		t,
		o.chainsyncState.HasClientConnId(connId),
		"chainsync client registration must be rolled back",
	)
	require.Zero(t, o.chainsyncState.ClientConnCount())

	require.Contains(
		t,
		logBuf.String(),
		"failed to start chainsync client, closing outbound connection",
	)

	// The torn-down connection must not strand goroutines. A small margin
	// absorbs the test harness's own workers (mempool, event bus).
	require.Eventually(
		t,
		func() bool { return runtime.NumGoroutine() <= baselineGoroutines+8 },
		5*time.Second,
		50*time.Millisecond,
		"goroutines leaked after the failed chainsync start (baseline %d, now %d)",
		baselineGoroutines,
		runtime.NumGoroutine(),
	)
}
