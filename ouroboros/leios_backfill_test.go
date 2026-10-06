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
	"net"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// fakeConnAddr is a comparable net.Addr so a connection.ConnectionId can be used
// as a map key in these unit tests without a live connection.
type fakeConnAddr string

func (a fakeConnAddr) Network() string { return "test" }
func (a fakeConnAddr) String() string  { return string(a) }

func namedConnId(name string) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{RemoteAddr: fakeConnAddr(name)}
}

// TestLeiosFetchGuardRecentlySucceeded verifies the positive-affinity signal
// that FetchEndorserBlockByPoint uses to prefer connections that recently served
// an endorser block, complementing the per-connection failure cooldown.
func TestLeiosFetchGuardRecentlySucceeded(t *testing.T) {
	t.Parallel()
	g := &leiosFetchGuard{}
	now := time.Now()

	// A guard that has never succeeded is not "recently successful".
	if g.recentlySucceeded(now, time.Minute) {
		t.Fatal("fresh guard should not report a recent success")
	}

	// After a success it is preferred within the affinity window...
	g.markFetchOK()
	if !g.recentlySucceeded(time.Now(), time.Minute) {
		t.Fatal("guard should report a recent success right after markFetchOK")
	}
	// ...and no longer once the window has elapsed.
	if g.recentlySucceeded(time.Now().Add(2*time.Minute), time.Minute) {
		t.Fatal("guard should not report a recent success past the window")
	}

	// A later failure must drop the positive-affinity preference so a
	// now-flaky connection is not still treated as proven.
	g.markFetchFailed(time.Now(), leiosBackfillConnCooldown)
	if g.recentlySucceeded(time.Now(), time.Minute) {
		t.Fatal(
			"a failure after a success should clear the affinity preference",
		)
	}
}

// TestLeiosBackfillConnOrderAffinity verifies the backfill connection ordering:
// healthy connections that recently succeeded come first, then other healthy
// connections, then connections cooling down from a recent failure.
func TestLeiosBackfillConnOrderAffinity(t *testing.T) {
	t.Parallel()
	proven := namedConnId("proven")
	fresh := namedConnId("fresh")
	cooled := namedConnId("cooled")
	connIds := []ouroboros.ConnectionId{fresh, cooled, proven}

	now := time.Now()
	provenGuard := &leiosFetchGuard{}
	freshGuard := &leiosFetchGuard{}
	cooledGuard := &leiosFetchGuard{}
	guards := map[ouroboros.ConnectionId]*leiosFetchGuard{
		proven: provenGuard,
		fresh:  freshGuard,
		cooled: cooledGuard,
	}
	provenGuard.markFetchOK()
	cooledGuard.markFetchFailed(now, leiosBackfillConnCooldown)
	guardFor := func(id ouroboros.ConnectionId) *leiosFetchGuard {
		return guards[id]
	}

	order := leiosBackfillConnOrder(
		connIds,
		0,
		time.Now(),
		leiosBackfillAffinityWindow,
		guardFor,
	)
	require.Equal(
		t,
		[]ouroboros.ConnectionId{proven, fresh, cooled},
		order,
		"proven connection first, then fresh, then cooled",
	)
}

// TestLeiosBackfillConnOrderPreservesRotation verifies that within a partition
// the round-robin start offset is preserved, so concurrent backfills still
// spread across the available connections instead of all hammering one.
func TestLeiosBackfillConnOrderPreservesRotation(t *testing.T) {
	t.Parallel()
	a := namedConnId("a")
	b := namedConnId("b")
	c := namedConnId("c")
	connIds := []ouroboros.ConnectionId{a, b, c}
	// All connections are fresh (never tried), so ordering is pure rotation.
	guards := map[ouroboros.ConnectionId]*leiosFetchGuard{a: {}, b: {}, c: {}}
	guardFor := func(id ouroboros.ConnectionId) *leiosFetchGuard {
		return guards[id]
	}

	require.Equal(
		t,
		[]ouroboros.ConnectionId{b, c, a},
		leiosBackfillConnOrder(
			connIds,
			1,
			time.Now(),
			leiosBackfillAffinityWindow,
			guardFor,
		),
		"start=1 rotates the fresh partition",
	)
	require.Equal(
		t,
		[]ouroboros.ConnectionId{c, a, b},
		leiosBackfillConnOrder(
			connIds,
			2,
			time.Now(),
			leiosBackfillAffinityWindow,
			guardFor,
		),
		"start=2 rotates the fresh partition",
	)
}

func TestLeiosBackfillPrefersActivePeerThatSuppliedPoint(t *testing.T) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "first", "active", "third")
	first, active, third := peers[0], peers[1], peers[2]
	state := chainsync.NewState(nil, nil)
	state.SetClientConnId(active.Id())
	o := newOuroboros(OuroborosConfig{ConnManager: cm, ChainsyncState: state})
	point := ocommon.Point{Slot: 200, Hash: []byte{0x04, 0x05}}
	o.recordLeiosBackfillSource(point, active.Id(), active.LeiosNotify().Client)

	got := o.leiosBackfillConnCandidatesForPoint(
		[]ouroboros.ConnectionId{first.Id(), third.Id(), active.Id()},
		0,
		point,
		time.Now(),
	)
	require.Equal(
		t,
		[]ouroboros.ConnectionId{
			active.Id(), first.Id(), third.Id(), active.Id(),
		},
		leiosBackfillCandidateIds(got),
	)
	require.Same(t, active, got[0].conn)
}

func TestLeiosBackfillKeepsFallbackWhenActivePeerDidNotSupplyPoint(
	t *testing.T,
) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "proven", "active", "fresh")
	proven, active, fresh := peers[0], peers[1], peers[2]
	state := chainsync.NewState(nil, nil)
	state.SetClientConnId(active.Id())
	o := newOuroboros(OuroborosConfig{ConnManager: cm, ChainsyncState: state})
	order := []ouroboros.ConnectionId{proven.Id(), fresh.Id(), active.Id()}
	got := o.leiosBackfillConnCandidatesForPoint(
		order, 0, ocommon.Point{Slot: 200, Hash: []byte{0x04}}, time.Now(),
	)
	require.Equal(t, order, leiosBackfillCandidateIds(got))
	for _, candidate := range got {
		require.Nil(t, candidate.conn)
	}
}

func TestLeiosBackfillPointSelectorUsesActiveAnnouncingPeer(t *testing.T) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "first", "active", "third")
	first, active, third := peers[0], peers[1], peers[2]
	state := chainsync.NewState(nil, nil)
	state.SetClientConnId(active.Id())
	o := newOuroboros(OuroborosConfig{
		ConnManager:    cm,
		ChainsyncState: state,
	})
	point := ocommon.Point{Slot: 200, Hash: []byte{0x04, 0x05}}
	for _, conn := range []*ouroboros.Connection{first, active, third} {
		require.NoError(t, o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{
				ConnectionId: conn.Id(),
				Client:       conn.LeiosNotify().Client,
			},
			oleiosnotify.NewMsgBlockTxsOffer(point),
		))
	}

	got := o.leiosBackfillConnCandidatesForPoint(
		[]ouroboros.ConnectionId{first.Id(), third.Id(), active.Id()},
		0,
		point,
		time.Now(),
	)
	require.Equal(t, active.Id(), got[0].connId)
	require.Same(t, active, got[0].conn)
}

func TestLeiosBackfillPointSelectorKeepsFallbackWithoutActiveAnnouncement(
	t *testing.T,
) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "first", "active")
	first, active := peers[0], peers[1]
	state := chainsync.NewState(nil, nil)
	state.SetClientConnId(active.Id())
	o := newOuroboros(OuroborosConfig{
		ConnManager:    cm,
		ChainsyncState: state,
	})
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	// A successful by-point store is not an announcement and must not create
	// source provenance for the active peer.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		raw,
		nil,
		leiosStoreBackfill,
	))

	fallback := []ouroboros.ConnectionId{first.Id(), active.Id()}
	got := o.leiosBackfillConnCandidatesForPoint(fallback, 0, point, time.Now())
	require.Equal(t, fallback, leiosBackfillCandidateIds(got))
	for _, candidate := range got {
		require.Nil(t, candidate.conn)
	}
}

func TestLeiosBackfillPreferenceKeepsAnnouncingConnectionLifetime(
	t *testing.T,
) {
	t.Parallel()

	local := fakeConnAddr("same-local")
	remote := fakeConnAddr("same-remote")
	first := newLeiosFetchConversationWithAddrs(t, local, remote)
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(first, false, "active"))
	state := chainsync.NewState(nil, nil)
	state.SetClientConnId(first.Id())
	o := newOuroboros(OuroborosConfig{ConnManager: cm, ChainsyncState: state})
	point := ocommon.Point{Slot: 200, Hash: []byte{0x04, 0x05}}
	o.recordLeiosBackfillSource(point, first.Id(), first.LeiosNotify().Client)

	candidates := o.leiosBackfillConnCandidatesForPoint(
		[]ouroboros.ConnectionId{first.Id()},
		0,
		point,
		time.Now(),
	)
	require.Len(t, candidates, 2)
	require.Same(t, first, candidates[0].conn)
	require.Nil(t, candidates[1].conn)
	require.Same(t, first, o.leiosBackfillCandidateConn(candidates[0], first))

	replacement := newLeiosFetchConversationWithAddrs(t, local, remote)
	require.Equal(t, first.Id(), replacement.Id())
	require.True(t, cm.AddConnection(replacement, false, "active"))
	require.Same(t, replacement, cm.GetConnectionById(first.Id()))
	// The promoted attempt remains bound to the announcing connection. The
	// replacement is available only at the unchanged fallback position.
	require.Same(t, first, candidates[0].conn)
	require.Nil(t, candidates[1].conn)
	require.Nil(t, o.leiosBackfillCandidateConn(candidates[0], first))
	require.Same(t, replacement,
		o.leiosBackfillCandidateConn(candidates[1], first))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, cm.Stop(ctx))
}

type leiosFixedAddrConn struct {
	net.Conn
	local  net.Addr
	remote net.Addr
}

func (c leiosFixedAddrConn) LocalAddr() net.Addr  { return c.local }
func (c leiosFixedAddrConn) RemoteAddr() net.Addr { return c.remote }

func newLeiosFetchConversationWithAddrs(
	t *testing.T,
	local net.Addr,
	remote net.Addr,
) *ouroboros.Connection {
	t.Helper()
	raw := leiosFixedAddrConn{
		Conn: ouroboros_mock.NewConnection(
			ouroboros_mock.ProtocolRoleClient,
			leiosFetchHandshake(),
		),
		local:  local,
		remote: remote,
	}
	conn, err := ouroboros.New(
		ouroboros.WithConnection(raw),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func leiosBackfillCandidateIds(
	candidates []leiosBackfillConnCandidate,
) []ouroboros.ConnectionId {
	ret := make([]ouroboros.ConnectionId, 0, len(candidates))
	for _, candidate := range candidates {
		ret = append(ret, candidate.connId)
	}
	return ret
}

func newLeiosBackfillSelectorPeers(
	t *testing.T,
	names ...string,
) (*connmanager.ConnectionManager, []*ouroboros.Connection) {
	t.Helper()
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	peers := make([]*ouroboros.Connection, 0, len(names))
	for _, name := range names {
		conn, _ := newLeiosFetchConversation(t, leiosFetchHandshake())
		t.Cleanup(func() { _ = conn.Close() })
		require.NotNil(t, conn.LeiosNotify())
		require.NotNil(t, conn.LeiosNotify().Client)
		require.True(t, cm.AddConnection(conn, false, name))
		peers = append(peers, conn)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	return cm, peers
}

// TestFetchEndorserBlockOnConnSkipsBusyConnection verifies that lock
// acquisition is not outside the per-attempt budget. A tip-driven fetch may
// hold the same guard, in which case backfill must return immediately so its
// caller can try another peer. Contention is not a peer failure and therefore
// must not change the connection's cooldown state.
func TestFetchEndorserBlockOnConnSkipsBusyConnection(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	connId := namedConnId("busy")
	point := ocommon.Point{Slot: 100, Hash: []byte{0x03}}
	// Keep the failure path safe: if a regression blocks until the test releases
	// the guard, the awakened fetch can finish from this empty cached block
	// without dereferencing the nil client below.
	o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)] = &leiosEndorserBlockData{
		point:      point,
		txCount:    0,
		insertedAt: time.Now(),
	}
	g := o.leiosFetchGuardFor(connId)
	g.mu.Lock()
	defer g.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- o.fetchEndorserBlockOnConn(
			context.Background(),
			connId,
			nil,
			point,
			time.Now().Add(leiosBackfillPerAttemptTimeout),
		)
	}()
	err := testutil.RequireReceive(
		t,
		errCh,
		time.Second,
		"busy leios-fetch connection should be skipped",
	)
	require.ErrorIs(t, err, errLeiosBackfillConnBusy)
	require.Zero(t, g.consecutiveFailures.Load())
	require.False(t, g.inCooldown(time.Now()))
}

// TestFetchLeiosEbTxsBatchedUntilPastDeadline verifies that a per-attempt
// deadline already in the past makes the fetch return immediately without
// issuing a single request, so FetchEndorserBlockByPoint can fail over to
// another connection instead of parking on a slow-but-alive relay.
func TestFetchLeiosEbTxsBatchedUntilPastDeadline(t *testing.T) {
	t.Parallel()
	o := &Ouroboros{}
	o.config.LeiosTxFetchTailBudget = time.Minute // would otherwise keep retrying
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	requester := &cappingBlockTxsRequester{maxPerResp: 50, includeBitmaps: true}

	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		context.Background(),
		requester,
		point,
		200,
		nil,
		time.Now().Add(-time.Second),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline")
	require.Empty(t, txs)
	require.Zero(
		t,
		requester.calls,
		"no request should be issued past the deadline",
	)
}

// TestLeiosFetchResponseTimeoutFitsBackfillAttempt ensures a single request
// that receives no response cannot add more than one per-connection attempt
// budget before the fetch loop gets another chance to fail over.
func TestLeiosFetchResponseTimeoutFitsBackfillAttempt(t *testing.T) {
	t.Parallel()
	require.LessOrEqual(
		t,
		leiosFetchResponseTimeout,
		leiosBackfillPerAttemptTimeout,
	)
}

// dribbleBlockTxsRequester simulates a slow-but-alive relay that always serves a
// little (one transaction per request) but takes real time per round, so the
// leios-fetch protocol per-message timeout never fires yet the fetch never
// promptly completes. The per-round latency exercises the wall-clock per-attempt
// deadline (it is relay-latency simulation, not goroutine synchronization).
type dribbleBlockTxsRequester struct {
	perRound time.Duration
	calls    int
}

func (r *dribbleBlockTxsRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	time.Sleep(r.perRound)
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	if len(requested) == 0 {
		return leiosfetch.NewMsgBlockTxsFull(
			point,
			map[uint16]uint64{},
			nil,
		), nil
	}
	idx := requested[0]
	served := map[uint16]uint64{uint16(idx / 64): 1 << uint(63-(idx%64))}
	enc, err := cbor.Encode(idx)
	if err != nil {
		return nil, err
	}
	return leiosfetch.NewMsgBlockTxsFull(
		point,
		served,
		[]cbor.RawMessage{cbor.RawMessage(enc)},
	), nil
}

// TestFetchLeiosEbTxsBatchedUntilAbandonsSlowRelay verifies that a relay which
// keeps making progress (so the no-progress and tail-stall guards never fire)
// but is too slow to finish is abandoned at the per-attempt deadline, returning
// the contiguous prefix fetched so far. This is the case: without the
// deadline the fetch would run until every transaction was served, parking the
// whole backfill on one peer.
func TestFetchLeiosEbTxsBatchedUntilAbandonsSlowRelay(t *testing.T) {
	t.Parallel()
	o := &Ouroboros{}
	o.config.LeiosTxFetchTailBudget = time.Minute
	point := ocommon.Point{Slot: 100, Hash: []byte{0x0a}}
	const txCount = 100
	requester := &dribbleBlockTxsRequester{perRound: 20 * time.Millisecond}

	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		context.Background(),
		requester,
		point,
		txCount,
		nil,
		time.Now().Add(60*time.Millisecond),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline")
	require.NotEmpty(
		t,
		txs,
		"the prefix fetched before the deadline is returned",
	)
	require.Less(
		t,
		len(txs),
		txCount,
		"the fetch is abandoned before completing",
	)
}

// TestFetchLeiosEbTxsBatchedNoDeadlineStillCompletes verifies the zero-deadline
// wrapper is unchanged: with no per-attempt deadline the fetch completes fully.
func TestFetchLeiosEbTxsBatchedNoDeadlineStillCompletes(t *testing.T) {
	t.Parallel()
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x0b}}
	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		context.Background(),
		&cappingBlockTxsRequester{maxPerResp: 50, includeBitmaps: true},
		point,
		200,
		nil,
		time.Time{}, // zero deadline: no per-attempt bound
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 200)
}

// TestLeiosFetchGuardCooldown verifies the per-connection backfill cooldown that
// FetchEndorserBlockByPoint uses to prefer healthy leios-fetch connections over
// ones that recently failed or timed out.
func TestLeiosFetchGuardCooldown(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	g := &leiosFetchGuard{}

	// A fresh guard is never in cooldown.
	if g.inCooldown(now) {
		t.Fatal("fresh guard should not be in cooldown")
	}

	// After a failed fetch, the connection cools down for the configured window.
	g.markFetchFailed(now, leiosBackfillConnCooldown)
	if !g.inCooldown(now) {
		t.Fatal("guard should be in cooldown immediately after a failed fetch")
	}
	// Still cooling down just before the deadline.
	if !g.inCooldown(now.Add(leiosBackfillConnCooldown - time.Nanosecond)) {
		t.Fatal("guard should still be in cooldown before the deadline")
	}
	// No longer cooling down at/after the deadline.
	if g.inCooldown(now.Add(leiosBackfillConnCooldown)) {
		t.Fatal("guard should not be in cooldown at the deadline")
	}
	if g.inCooldown(now.Add(leiosBackfillConnCooldown + time.Second)) {
		t.Fatal("guard should not be in cooldown after the deadline")
	}

	// A successful fetch clears the cooldown immediately, even mid-window.
	g.markFetchFailed(now, leiosBackfillConnCooldown)
	g.markFetchOK()
	if g.inCooldown(now) {
		t.Fatal("markFetchOK should clear the cooldown")
	}
}
