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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// TestLocalstatequeryServerAcquire_PointAheadOfTip_GracefulFailure is the
// blinklabs-io/dingo#4156 regression. Before this fix, localstatequeryServerAcquire
// unconditionally accepted any AcquireSpecificPoint, deferring the actual
// tip-validity check to the first Query -- and a rejection surfacing there
// has no graceful wire-level reply (unlike a rejection at Acquire time), so
// it propagated as a fatal protocol error and gouroboros tore the whole
// LocalStateQuery connection down instead of just failing one Acquire.
//
// Acquiring a point ahead of this node's own current tip is not a client
// bug: it is the ordinary result of two independently-syncing nodes (this
// node and whatever reference the caller derived the point from) not
// advancing in perfect lockstep, which happens on close to every block at
// real cadence. It must fail gracefully -- an error errors.Is-matching
// gouroboros' own olocalstatequery.ErrAcquireFailurePointNotOnChain, which
// its server (handleAcquire/handleReAcquire) translates into a wire-level
// AcquireFailure reply -- not with an arbitrary error gouroboros has no
// graceful handling for.
func TestLocalstatequeryServerAcquire_PointAheadOfTip_GracefulFailure(t *testing.T) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, db := newTestLedgerStateWithChain(t, 2)
	o.ledgerState = ls

	tipHash := bytes.Repeat([]byte{1}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	aheadHash := bytes.Repeat([]byte{2}, 32)
	connID := ouroboros.ConnectionId{}
	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(2, aheadHash),
		},
		false,
	)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, olocalstatequery.ErrAcquireFailurePointNotOnChain),
		"expected a gracefully-mapped AcquireFailurePointNotOnChain, got: %v",
		err,
	)

	o.localstatequeryAcquireMutex.Lock()
	_, recorded := o.localstatequeryAcquiredPoints[connID]
	o.localstatequeryAcquireMutex.Unlock()
	require.False(
		t, recorded,
		"a rejected Acquire must not record the unvalidated point",
	)
}

// TestLocalstatequeryServerAcquire_PointOnChain_Succeeds is the companion
// positive case: a specific point genuinely matching this node's chain at
// that slot must still be accepted and recorded, unchanged from before the
// #4156 fix.
func TestLocalstatequeryServerAcquire_PointOnChain_Succeeds(t *testing.T) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, db := newTestLedgerStateWithChain(t, 2)
	o.ledgerState = ls

	tipHash := bytes.Repeat([]byte{2}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(2, tipHash),
	}, nil))

	connID := ouroboros.ConnectionId{}
	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(2, tipHash),
		},
		false,
	)
	require.NoError(t, err)

	o.localstatequeryAcquireMutex.Lock()
	recorded, ok := o.localstatequeryAcquiredPoints[connID]
	o.localstatequeryAcquireMutex.Unlock()
	require.True(t, ok)
	require.Equal(t, uint64(2), recorded.Slot)
}

// TestLocalstatequeryServerAcquire_VolatileTip_ClearsPoint covers the
// AcquireVolatileTip branch, unchanged by the #4156 fix: it must clear any
// previously-recorded pinned point rather than being validated as a
// specific point.
func TestLocalstatequeryServerAcquire_VolatileTip_ClearsPoint(t *testing.T) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, _ := newTestLedgerStateWithChain(t, 1)
	o.ledgerState = ls

	connID := ouroboros.ConnectionId{}
	o.localstatequeryAcquiredPoints[connID] = ledger.QueryPoint{Slot: 1}

	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireVolatileTip{},
		false,
	)
	require.NoError(t, err)

	o.localstatequeryAcquireMutex.Lock()
	_, recorded := o.localstatequeryAcquiredPoints[connID]
	o.localstatequeryAcquireMutex.Unlock()
	require.False(t, recorded)
}

// TestLocalstatequeryProtocol_PointAheadOfTip_ConnectionSurvivesAndStaysUsable
// is the blinklabs-io/dingo#4156 regression at the actual protocol level a
// bot reviewer asked for: the three tests above drive
// localstatequeryServerAcquire's callback directly, which proves the
// callback's return value is right, but not that gouroboros' server
// actually turns that into a graceful wire reply rather than tearing the
// connection down -- that translation lives entirely in gouroboros'
// server.go, outside this callback. This test wires a real dingo
// *Ouroboros server (the same localstatequeryServerConnOpts production
// code every real NtC connection uses) to a real gouroboros client over an
// actual connection (net.Pipe -- an in-memory net.Conn pair, no sockets
// needed), the same way connmanager/listener.go wires a live NtC listener.
//
// It Acquires a point ahead of this node's tip (reproducing the exact race
// blinklabs-io/dingo#4156 was filed for) and asserts two things a direct
// callback test cannot: the client's Acquire call itself returns
// ErrAcquireFailurePointNotOnChain (not a connection-closed/EOF error), and
// -- the part that actually matters, since #4156's bug was the whole
// connection dying -- a second, valid Acquire on the very same connection
// immediately afterward still succeeds.
func TestLocalstatequeryProtocol_PointAheadOfTip_ConnectionSurvivesAndStaysUsable(
	t *testing.T,
) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, db := newTestLedgerStateWithChain(t, 2)
	o.ledgerState = ls

	tipHash := bytes.Repeat([]byte{1}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	rawServer, rawClient := net.Pipe()

	type serverResult struct {
		conn *ouroboros.Connection
		err  error
	}
	serverDone := make(chan serverResult, 1)
	go func() {
		conn, err := ouroboros.New(
			ouroboros.WithConnection(rawServer),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithServer(true),
			ouroboros.WithLocalStateQueryConfig(
				olocalstatequery.NewConfig(
					o.localstatequeryServerConnOpts()...,
				),
			),
		)
		serverDone <- serverResult{conn: conn, err: err}
	}()

	cliConn, err := ouroboros.New(
		ouroboros.WithConnection(rawClient),
		ouroboros.WithNetworkMagic(42),
		ouroboros.WithNodeToNode(false),
	)
	require.NoError(t, err)
	defer cliConn.Close() //nolint:errcheck

	var srvRes serverResult
	select {
	case srvRes = <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server side of ouroboros.New never completed the handshake")
	}
	require.NoError(t, srvRes.err)
	defer srvRes.conn.Close() //nolint:errcheck

	client := cliConn.LocalStateQuery().Client
	require.NotNil(t, client)

	aheadHash := bytes.Repeat([]byte{2}, 32)
	aheadPoint := ocommon.NewPoint(2, aheadHash)
	acquireErr := client.Acquire(&aheadPoint)
	require.Error(
		t,
		acquireErr,
		"an ahead-of-tip Acquire must fail",
	)
	require.True(
		t,
		errors.Is(acquireErr, olocalstatequery.ErrAcquireFailurePointNotOnChain),
		"expected a graceful AcquireFailurePointNotOnChain reply over the "+
			"wire, got: %v",
		acquireErr,
	)

	// The actual #4156 bug: before the fix, the ahead-of-tip Acquire above
	// didn't just fail -- it took the whole connection down with it, so any
	// subsequent call would see a connection-closed error instead of a
	// normal protocol response. Prove the connection is still alive and
	// usable by Acquiring a genuinely valid point right after.
	require.NoError(
		t,
		client.AcquireVolatileTip(),
		"the connection must remain usable after a graceful AcquireFailure",
	)
	require.NoError(t, client.Release())
}
