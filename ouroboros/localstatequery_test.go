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
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func TestReleaseLocalStateQueryAcquiredPointOwnerKeepsReplacement(
	t *testing.T,
) {
	t.Parallel()

	connID := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	oldOwner := olocalstatequery.NewServer(protocol.ProtocolOptions{}, nil)
	replacement := olocalstatequery.NewServer(protocol.ProtocolOptions{}, nil)
	o := &Ouroboros{
		localstatequeryAcquiredPoints: map[ouroboros.ConnectionId]ledger.QueryPoint{
			connID: {Slot: 100, Hash: []byte{0xAB}},
		},
		localstatequeryOwners: map[ouroboros.ConnectionId]*olocalstatequery.Server{
			connID: replacement,
		},
	}

	o.ReleaseLocalStateQueryAcquiredPointOwner(connID, oldOwner)
	require.True(t, o.HasLocalStateQueryAcquiredPointForTesting(connID))

	o.ReleaseLocalStateQueryAcquiredPointOwner(connID, replacement)
	require.False(t, o.HasLocalStateQueryAcquiredPointForTesting(connID))
}

// TestLocalstatequeryServerAcquire_PointAheadOfTip_GracefulFailure is the
// ahead-of-tip Acquire regression. Before the fix, localstatequeryServerAcquire
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
func TestLocalstatequeryServerAcquire_PointAheadOfTip_GracefulFailure(
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
// fix.
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
	// VerifyPointQueryable now also exercises queryHardFork's
	// HardForkCurrentEraQuery case, which needs an epoch record covering
	// the acquired slot to resolve an era from --
	// newTestLedgerStateWithChain seeds blocks only.
	require.NoError(t, db.SetEpoch(
		0, 0, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	// GetAccountState needs a network_state row at or before the acquired
	// slot; genesis sync writes this slot-0 baseline on a real node.
	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 0, nil))

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

// TestLocalstatequeryServerAcquire_UnexpectedError_MappedToPointTooOld
// covers a regression: an error VerifyPointQueryable returns matching neither
// ledger.ErrPointNotOnChain nor ledger.ErrHistoricalStateUnavailable --
// e.g. a real database error inside one of its own reads, not the point
// genuinely being unqueryable -- was previously returned bare. gouroboros'
// handleAcquire treats any non-sentinel error as fatal and tears the whole
// connection down, reintroducing the exact connection-killing failure mode
// this whole mechanism exists to avoid, just triggered a different way.
// Closing the database out from under a genuinely on-chain point forces
// VerifyPointQueryable's own reads to fail with a raw "database closed"
// style error, matching neither sentinel -- exactly the shape this handler
// must map to a graceful AcquireFailurePointTooOld instead of propagating
// bare.
func TestLocalstatequeryServerAcquire_UnexpectedError_MappedToPointTooOld(
	t *testing.T,
) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
		config: OuroborosConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls, db := newTestLedgerStateWithChain(t, 2)
	o.ledgerState = ls

	tipHash := bytes.Repeat([]byte{2}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(2, tipHash),
	}, nil))
	require.NoError(t, dbtest.CloseDatabase(db))

	connID := ouroboros.ConnectionId{}
	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(2, tipHash),
		},
		false,
	)
	require.Error(t, err)
	require.False(
		t,
		errors.Is(err, olocalstatequery.ErrAcquireFailurePointNotOnChain),
		"a closed-database error must not be misclassified as "+
			"point-not-on-chain: got %v",
		err,
	)
	require.True(
		t,
		errors.Is(err, olocalstatequery.ErrAcquireFailurePointTooOld),
		"expected an unexpected internal error to map to the graceful "+
			"AcquireFailurePointTooOld a well-behaved client already knows "+
			"how to handle, got: %v",
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

// TestLocalstatequeryServerAcquire_PastRetentionFloor_MappedToPointTooOld
// covers a gap the other tests in this file leave open: reverting
// localstatequeryServerAcquire's call from VerifyPointQueryable back to the
// narrower VerifyPointOnChain leaves every other test in this file green --
// UnexpectedError_MappedToPointTooOld still passes because a closed database
// fails VerifyPointOnChain too, and no other test Acquires a point that is
// genuinely still on-chain but past a query-type-specific retention floor.
// This Acquires slot 1 of a real two-block chain (on-chain, so
// VerifyPointOnChain alone would accept it)
// after marking the durable consumed-UTxO prune floor
// (database.ConsumedUtxoPruneFloorSyncKey) at slot 2 --
// checkUtxoRetentionWindow (ledger/queries.go), reached only through
// VerifyPointQueryable, rejects it with ErrHistoricalStateUnavailable, which
// this handler maps to the graceful AcquireFailurePointTooOld. Calling
// VerifyPointOnChain instead of VerifyPointQueryable at this call site makes
// this test fail with a nil error.
func TestLocalstatequeryServerAcquire_PastRetentionFloor_MappedToPointTooOld(
	t *testing.T,
) {
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
	require.NoError(t, db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey, "2", nil,
	))

	pointHash := bytes.Repeat([]byte{1}, 32)
	connID := ouroboros.ConnectionId{}
	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(1, pointHash),
		},
		false,
	)
	require.Error(t, err)
	require.False(
		t,
		errors.Is(err, olocalstatequery.ErrAcquireFailurePointNotOnChain),
		"a point past a retention floor is still genuinely on-chain, not "+
			"absent from it: got %v",
		err,
	)
	require.True(
		t,
		errors.Is(err, olocalstatequery.ErrAcquireFailurePointTooOld),
		"expected a retention-floor rejection to map to the graceful "+
			"AcquireFailurePointTooOld, got: %v",
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

// TestLocalstatequeryServerAcquire_VolatileTip_ClearsPoint covers the
// AcquireVolatileTip branch, unchanged by the ahead-of-tip fix: it must clear
// any previously-recorded pinned point rather than being validated as a
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
// is the ahead-of-tip regression at the actual protocol level a
// the three tests above drive
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
// the fix addresses) and asserts two things a direct
// callback test cannot: the client's Acquire call itself returns
// ErrAcquireFailurePointNotOnChain (not a connection-closed/EOF error), and
// -- the part that actually matters, since the bug was the whole
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
					o.localstatequeryServerConnOpts(false)...,
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
		errors.Is(
			acquireErr,
			olocalstatequery.ErrAcquireFailurePointNotOnChain,
		),
		"expected a graceful AcquireFailurePointNotOnChain reply over the "+
			"wire, got: %v",
		acquireErr,
	)

	// The actual bug: before the fix, the ahead-of-tip Acquire above
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

// newSnapshotTestOuroboros builds an Ouroboros over an on-disk ledger, which
// is what lets a test commit a write while a session still holds its
// snapshot. The tip starts at slot 1.
func newSnapshotTestOuroboros(
	t *testing.T,
	cfg OuroborosConfig,
) (*Ouroboros, *database.Database, ocommon.Point) {
	t.Helper()
	ls, db := newTestLedgerStateWithChainAt(t, 2, t.TempDir())
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	o := newOuroboros(cfg)
	o.ledgerState = ls
	// Registered after the ledger's own cleanup, so it runs first and the
	// snapshots are released before the database closes.
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	tip := ocommon.NewPoint(1, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip, BlockNumber: 1}, nil))
	return o, db, tip
}

func moveTestTip(
	t *testing.T,
	db *database.Database,
	slot uint64,
) ocommon.Point {
	t.Helper()
	tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: tip, BlockNumber: slot}, nil,
	))
	return tip
}

func queryChainPoint(
	t *testing.T,
	o *Ouroboros,
	ctx olocalstatequery.CallbackContext,
) (any, error) {
	t.Helper()
	return o.localstatequeryServerQuery(
		ctx,
		olocalstatequery.QueryWrapper{Query: &olocalstatequery.ChainPointQuery{}},
	)
}

func acquireSnapshotTestSession(
	t *testing.T,
	o *Ouroboros,
	ctx olocalstatequery.CallbackContext,
) *localstatequerySession {
	t.Helper()
	require.NoError(t, o.localstatequeryServerAcquire(
		ctx, olocalstatequery.AcquireVolatileTip{}, false,
	))
	o.localstatequeryAcquireMutex.Lock()
	defer o.localstatequeryAcquireMutex.Unlock()
	session := o.localstatequerySessions[ctx.ConnectionId]
	require.NotNil(t, session, "Acquire must record a session")
	return session
}

// TestLocalstatequeryQueryAnswersFromAcquiredSnapshot proves a session keeps
// answering from the state it acquired while the chain advances, and that a
// re-Acquire moves it to the new state and closes the old snapshot.
func TestLocalstatequeryQueryAnswersFromAcquiredSnapshot(t *testing.T) {
	t.Parallel()

	o, db, acquiredTip := newSnapshotTestOuroboros(t, OuroborosConfig{})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}
	first := acquireSnapshotTestSession(t, o, ctx)

	newTip := moveTestTip(t, db, 2)

	got, err := queryChainPoint(t, o, ctx)
	require.NoError(t, err)
	require.Equal(t, acquiredTip, got, "a session must not see the new tip")

	require.NoError(t, o.localstatequeryServerAcquire(
		ctx, olocalstatequery.AcquireVolatileTip{}, true,
	))
	got, err = queryChainPoint(t, o, ctx)
	require.NoError(t, err)
	require.Equal(t, newTip, got, "a re-Acquire must observe the new tip")

	_, err = first.view.Query(&olocalstatequery.ChainPointQuery{}, 0)
	require.ErrorIs(
		t, err, ledger.ErrQueryViewClosed,
		"a re-Acquire must close the snapshot it replaces",
	)
}

// TestLocalstatequeryFailedReAcquireClosesPreviousSnapshot proves a rejected
// re-Acquire leaves the session registered with its snapshot closed.
func TestLocalstatequeryFailedReAcquireClosesPreviousSnapshot(t *testing.T) {
	t.Parallel()

	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}
	session := acquireSnapshotTestSession(t, o, ctx)

	err := o.localstatequeryServerAcquire(
		ctx,
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(2, bytes.Repeat([]byte{9}, 32)),
		},
		true,
	)
	require.ErrorIs(t, err, olocalstatequery.ErrAcquireFailurePointNotOnChain)

	// The previous snapshot was closed to free its admission slot for the
	// replacement, so the session stays registered but no longer answers.
	_, err = queryChainPoint(t, o, ctx)
	require.ErrorIs(t, err, ledger.ErrQueryViewClosed)
	o.localstatequeryAcquireMutex.Lock()
	require.Same(t, session, o.localstatequerySessions[ctx.ConnectionId])
	o.localstatequeryAcquireMutex.Unlock()
}

// TestLocalstatequeryReleaseClosesSnapshot proves Release closes the session's
// snapshot and forgets the connection.
func TestLocalstatequeryReleaseClosesSnapshot(t *testing.T) {
	t.Parallel()

	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{})
	server := olocalstatequery.NewServer(protocol.ProtocolOptions{}, nil)
	ctx := olocalstatequery.CallbackContext{
		ConnectionId: ouroboros.ConnectionId{},
		Server:       server,
	}
	session := acquireSnapshotTestSession(t, o, ctx)

	require.NoError(t, o.localstatequeryServerRelease(ctx))

	require.False(t, o.HasLocalStateQueryAcquiredPointForTesting(ctx.ConnectionId))
	_, err := session.view.Query(&olocalstatequery.ChainPointQuery{}, 0)
	require.ErrorIs(t, err, ledger.ErrQueryViewClosed)
}

// TestLocalstatequeryDisconnectClosesSnapshot proves a client that vanishes
// without releasing does not leak its snapshot, and that a stale close from a
// replaced connection owner leaves the live session alone.
func TestLocalstatequeryDisconnectClosesSnapshot(t *testing.T) {
	t.Parallel()

	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{})
	owner := olocalstatequery.NewServer(protocol.ProtocolOptions{}, nil)
	stale := olocalstatequery.NewServer(protocol.ProtocolOptions{}, nil)
	ctx := olocalstatequery.CallbackContext{
		ConnectionId: ouroboros.ConnectionId{},
		Server:       owner,
	}
	session := acquireSnapshotTestSession(t, o, ctx)

	o.ReleaseLocalStateQueryAcquiredPointOwner(ctx.ConnectionId, stale)
	_, err := session.view.Query(&olocalstatequery.ChainPointQuery{}, 0)
	require.NoError(t, err, "a stale owner must not close the live session")

	o.ReleaseLocalStateQueryAcquiredPointOwner(ctx.ConnectionId, owner)
	require.False(t, o.HasLocalStateQueryAcquiredPointForTesting(ctx.ConnectionId))
	_, err = session.view.Query(&olocalstatequery.ChainPointQuery{}, 0)
	require.ErrorIs(t, err, ledger.ErrQueryViewClosed)
}

// TestLocalstatequerySnapshotExpires proves a snapshot held past the
// configured lifetime is closed and logged, and that the session then fails
// its queries rather than silently answering from live state.
func TestLocalstatequerySnapshotExpires(t *testing.T) {
	t.Parallel()

	logs := &lockedBuffer{}
	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{
		Logger:                         slog.New(slog.NewJSONHandler(logs, nil)),
		LocalStateQueryViewMaxLifetime: 20 * time.Millisecond,
	})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}
	session := acquireSnapshotTestSession(t, o, ctx)

	testutil.WaitForCondition(t, func() bool {
		_, err := session.view.Query(&olocalstatequery.ChainPointQuery{}, 0)
		return errors.Is(err, ledger.ErrQueryViewClosed)
	}, testutil.AsyncWait, "the snapshot was never closed")
	testutil.WaitForCondition(t, func() bool {
		return strings.Contains(logs.String(), "ledger snapshot expired")
	}, testutil.AsyncWait, "the expiry was never logged")

	_, err := queryChainPoint(t, o, ctx)
	require.ErrorIs(t, err, ledger.ErrQueryViewClosed)
}

// TestLocalstatequeryReleaseCancelsExpiry proves a released session is not
// expired afterwards: the lifetime timer must die with the session.
func TestLocalstatequeryReleaseCancelsExpiry(t *testing.T) {
	t.Parallel()

	logs := &lockedBuffer{}
	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{
		Logger:                         slog.New(slog.NewJSONHandler(logs, nil)),
		LocalStateQueryViewMaxLifetime: 20 * time.Millisecond,
	})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}
	session := acquireSnapshotTestSession(t, o, ctx)
	require.NoError(t, o.localstatequeryServerRelease(ctx))

	// Stop reports false when the timer already fired; it must not have.
	require.False(t, session.expiry.Stop(), "Release must have stopped the timer")
	require.NotContains(t, logs.String(), "expired")
}

// TestLocalstatequeryQueryRejectsUnsupportedQuery proves a query the node does
// not implement is an error on the session, not a panic, and leaves the
// session usable.
func TestLocalstatequeryQueryRejectsUnsupportedQuery(t *testing.T) {
	t.Parallel()

	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}
	acquireSnapshotTestSession(t, o, ctx)

	for name, query := range map[string]any{
		"nil":     nil,
		"foreign": struct{}{},
	} {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := o.localstatequeryServerQuery(
					ctx, olocalstatequery.QueryWrapper{Query: query},
				)
				require.ErrorContains(t, err, "unsupported query type")
			})
		})
	}
	_, err := queryChainPoint(t, o, ctx)
	require.NoError(t, err)
}

// TestOuroborosCloseClosesLocalStateQuerySnapshots proves shutting the
// Ouroboros down releases every connection's snapshot.
func TestOuroborosCloseClosesLocalStateQuerySnapshots(t *testing.T) {
	t.Parallel()

	o, _, _ := newSnapshotTestOuroboros(t, OuroborosConfig{})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}
	session := acquireSnapshotTestSession(t, o, ctx)

	require.NoError(t, o.Close())

	require.False(t, o.HasLocalStateQueryAcquiredPointForTesting(ctx.ConnectionId))
	_, err := session.view.Query(&olocalstatequery.ChainPointQuery{}, 0)
	require.ErrorIs(t, err, ledger.ErrQueryViewClosed)
}

// TestLocalstatequeryWithoutLedgerStateFailsWithoutPanic covers the
// unavailable-dependency case for every callback.
func TestLocalstatequeryWithoutLedgerStateFailsWithoutPanic(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{})
	ctx := olocalstatequery.CallbackContext{ConnectionId: ouroboros.ConnectionId{}}

	require.NotPanics(t, func() {
		err := o.localstatequeryServerAcquire(
			ctx, olocalstatequery.AcquireVolatileTip{}, false,
		)
		require.ErrorIs(t, err, errLocalStateQueryLedgerUnavailable)

		_, err = queryChainPoint(t, o, ctx)
		require.ErrorIs(t, err, errLocalStateQueryLedgerUnavailable)

		require.NoError(t, o.localstatequeryServerRelease(ctx))
	})
}

// TestLocalstatequeryProtocol_AcquireQueryRelease drives the whole session
// over a real connection: acquire, query while the chain advances, release,
// and a second acquire that sees the advance.
func TestLocalstatequeryProtocol_AcquireQueryRelease(t *testing.T) {
	t.Parallel()

	o, db, acquiredTip := newSnapshotTestOuroboros(t, OuroborosConfig{})
	client := newLocalStateQueryTestClient(t, o)

	require.NoError(t, client.AcquireVolatileTip())
	newTip := moveTestTip(t, db, 2)

	point, err := client.GetChainPoint()
	require.NoError(t, err)
	require.Equal(t, acquiredTip.Slot, point.Slot)
	require.Equal(t, acquiredTip.Hash, point.Hash)
	blockNo, err := client.GetChainBlockNo()
	require.NoError(t, err)
	require.Equal(t, int64(1), blockNo)
	require.NoError(t, client.Release())

	require.NoError(t, client.AcquireVolatileTip())
	point, err = client.GetChainPoint()
	require.NoError(t, err)
	require.Equal(t, newTip.Slot, point.Slot)
	require.NoError(t, client.Release())
}

// newLocalStateQueryTestClient wires o's LocalStateQuery server to a real
// gouroboros client over an in-memory connection and returns the client.
func newLocalStateQueryTestClient(
	t *testing.T,
	o *Ouroboros,
) *olocalstatequery.Client {
	t.Helper()
	conn := newNtCTestClientConn(
		t,
		ouroboros.WithLocalStateQueryConfig(
			olocalstatequery.NewConfig(
				o.localstatequeryServerConnOpts(false)...,
			),
		),
	)
	client := conn.LocalStateQuery().Client
	require.NotNil(t, client)
	return client
}

// newNtCTestClientConn runs a node-to-client server configured by serverOpts
// against a real gouroboros client over an in-memory connection, the way a
// live NtC listener does, and returns the client side.
func newNtCTestClientConn(
	t *testing.T,
	serverOpts ...ouroboros.ConnectionOptionFunc,
) *ouroboros.Connection {
	t.Helper()
	rawServer, rawClient := net.Pipe()
	type serverResult struct {
		conn *ouroboros.Connection
		err  error
	}
	serverDone := make(chan serverResult, 1)
	go func() {
		conn, err := ouroboros.New(append([]ouroboros.ConnectionOptionFunc{
			ouroboros.WithConnection(rawServer),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithServer(true),
		}, serverOpts...)...)
		serverDone <- serverResult{conn: conn, err: err}
	}()
	cliConn, err := ouroboros.New(
		ouroboros.WithConnection(rawClient),
		ouroboros.WithNetworkMagic(42),
		ouroboros.WithNodeToNode(false),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cliConn.Close() })
	srv := testutil.RequireReceive(
		t, serverDone, testutil.AsyncWait, "server handshake",
	)
	require.NoError(t, srv.err)
	t.Cleanup(func() { _ = srv.conn.Close() })
	return cliConn
}
