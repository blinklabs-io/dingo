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

package ledger_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	dingoouroboros "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type remoteAddrConn struct {
	net.Conn
	localAddr  net.Addr
	remoteAddr net.Addr
}

func (c remoteAddrConn) LocalAddr() net.Addr  { return c.localAddr }
func (c remoteAddrConn) RemoteAddr() net.Addr { return c.remoteAddr }

func newPeerConnection(t *testing.T, remote string) *ouroboros.Connection {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", remote)
	require.NoError(t, err)
	conn, err := ouroboros.New(
		ouroboros.WithConnection(remoteAddrConn{
			Conn: ouroboros_mock.NewConnection(
				ouroboros_mock.ProtocolRoleClient,
				ouroboros_mock.ConversationKeepAlive,
			),
			localAddr:  localAddr,
			remoteAddr: remoteAddr,
		}),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(keepalive.NewConfig(
			keepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
			keepalive.WithPeriod(30*time.Second),
			keepalive.WithTimeout(15*time.Second),
		)),
	)
	require.NoError(t, err)
	return conn
}

// One peer repeatedly supplies a block whose apply-time header check fails
// while another peer is honest. Each failure runs through the ledger's
// deferred verdict and recovery, the ouroboros resync handler, and the peer
// governor: only the supplying peer is closed and denied, from any source
// port, the rejected block is dropped from the primary chain, and the node
// continues on the honest block.
func TestDeferredHeaderFailureDeniesOnlySupplyingPeerAndFollowsHonestChain(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	fixture := ledger.NewDeferredHeaderRecoveryFixture(t, bus)

	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{EventBus: bus, Logger: logger},
	)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()
		_ = connManager.Stop(stopCtx)
	})
	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	harnessMempool, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       fixture.Ledger,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	o, err := dingoouroboros.NewOuroboros(dingoouroboros.OuroborosConfig{
		Logger:         logger,
		EventBus:       bus,
		LedgerState:    fixture.Ledger,
		NetworkMagic:   ouroboros_mock.MockNetworkMagic,
		Mempool:        &mempool.FIFO{Mempool: harnessMempool},
		ChainsyncState: chainsync.NewState(bus, fixture.Ledger),
		ConnManager:    connManager,
		PeerGov:        peerGov,
	})
	require.NoError(t, err)
	o.SubscribeChainsyncResync(t.Context())

	honestConn := newPeerConnection(t, "10.0.0.2:3001")
	require.True(t, connManager.AddConnection(
		honestConn, false, "10.0.0.2:3001",
	))
	honest := honestConn.Id()

	for _, remote := range []string{
		"10.0.0.1:3001",
		"10.0.0.1:51001",
		"10.0.0.1:51002",
	} {
		badConn := newPeerConnection(t, remote)
		require.True(t, connManager.AddConnection(badConn, false, remote))
		bad := badConn.Id()

		require.True(
			t,
			fixture.SupplyStateInvalidDeferredBlock(bad),
			"recovery must run for %s", remote,
		)

		assert.Equal(t, fixture.RewindPoint(), fixture.PrimaryTip())
		require.Eventually(
			t,
			func() bool { return connManager.GetConnectionById(bad) == nil },
			2*time.Second,
			20*time.Millisecond,
			"the supplying connection %s must be closed", remote,
		)
		require.True(
			t,
			peerGov.IsDenied(remote),
			"the supplying peer %s must be denied", remote,
		)
		require.NotNil(t, connManager.GetConnectionById(honest))
		require.False(
			t,
			peerGov.IsDenied(honest.RemoteAddr.String()),
			"the honest peer must stay eligible",
		)
	}

	honestTip := fixture.AppendHonestBlock()
	assert.Equal(t, honestTip, fixture.PrimaryTip())
	assert.NotEqual(t, fixture.RejectedPoint(), fixture.PrimaryTip())
	assert.False(t, peerGov.IsDenied(honest.RemoteAddr.String()))
}
