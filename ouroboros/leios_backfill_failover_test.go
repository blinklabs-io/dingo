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
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
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
		t.Fatal("a failure after a success should clear the affinity preference")
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
		leiosBackfillConnOrder(connIds, 1, time.Now(), leiosBackfillAffinityWindow, guardFor),
		"start=1 rotates the fresh partition",
	)
	require.Equal(
		t,
		[]ouroboros.ConnectionId{c, a, b},
		leiosBackfillConnOrder(connIds, 2, time.Now(), leiosBackfillAffinityWindow, guardFor),
		"start=2 rotates the fresh partition",
	)
}

// TestFetchEndorserBlockOnConnSkipsBusyConnection verifies that lock
// acquisition is not outside the per-attempt budget. A tip-driven fetch may
// hold the same guard, in which case backfill must return immediately so its
// caller can try another peer. Contention is not a peer failure and therefore
// must not change the connection's cooldown state.
func TestFetchEndorserBlockOnConnSkipsBusyConnection(t *testing.T) {
	t.Parallel()
	o := NewOuroboros(OuroborosConfig{})
	connId := namedConnId("busy")
	point := ocommon.Point{Slot: 100, Hash: []byte{0x03}}
	// Keep the failure path safe: if a regression blocks until the test releases
	// the guard, the awakened fetch can finish from this empty cached block
	// without dereferencing the nil client below.
	o.leiosEndorserBlocks[leiosBlockKey(point.Hash)] = &leiosEndorserBlockData{
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
			connId,
			nil,
			point,
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
		requester,
		point,
		200,
		time.Now().Add(-time.Second),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline")
	require.Empty(t, txs)
	require.Zero(t, requester.calls, "no request should be issued past the deadline")
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
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	time.Sleep(r.perRound)
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	if len(requested) == 0 {
		return leiosfetch.NewMsgBlockTxsFull(point, map[uint16]uint64{}, nil), nil
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
// the contiguous prefix fetched so far. This is the #2819 case: without the
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
		requester,
		point,
		txCount,
		time.Now().Add(60*time.Millisecond),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline")
	require.NotEmpty(t, txs, "the prefix fetched before the deadline is returned")
	require.Less(t, len(txs), txCount, "the fetch is abandoned before completing")
}

// TestFetchLeiosEbTxsBatchedNoDeadlineStillCompletes verifies the zero-deadline
// wrapper is unchanged: with no per-attempt deadline the fetch completes fully.
func TestFetchLeiosEbTxsBatchedNoDeadlineStillCompletes(t *testing.T) {
	t.Parallel()
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x0b}}
	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		&cappingBlockTxsRequester{maxPerResp: 50, includeBitmaps: true},
		point,
		200,
		time.Time{}, // zero deadline: no per-attempt bound
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 200)
}
