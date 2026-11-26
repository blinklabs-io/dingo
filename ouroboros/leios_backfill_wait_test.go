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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// leiosWaitTestHold is how long the competing fetch holds the connection
// guard. It only has to be long enough to observe that the caller waited.
const leiosWaitTestHold = 300 * time.Millisecond

// cacheCompleteLeiosEndorserBlock seeds point as a complete, slot-verified
// endorser block with no transactions, so a fetch that obtains the guard
// finishes from the cache without touching the network.
func cacheCompleteLeiosEndorserBlock(o *Ouroboros, point ocommon.Point) {
	o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)] = &leiosEndorserBlockData{
		point:        point,
		blockRaw:     []byte{0x80},
		txCount:      0,
		slotVerified: true,
		insertedAt:   time.Now(),
	}
}

// holdLeiosFetchGuard takes connId's fetch guard, as a background prefetch of
// another endorser block would, and releases it after hold. The returned
// channel closes once the guard is released.
func holdLeiosFetchGuard(
	t *testing.T,
	o *Ouroboros,
	connId ouroboros.ConnectionId,
	hold time.Duration,
) <-chan struct{} {
	t.Helper()
	g := o.leiosFetchGuardFor(connId)
	g.mu.Lock()
	released := make(chan struct{})
	go func() {
		time.Sleep(hold)
		g.mu.Unlock()
		close(released)
	}()
	return released
}

// TestFetchEndorserBlockRequiredWaitsForBusyConnection is the single
// leios-fetch connection case. Another fetch holds the connection's guard; a
// required fetch (the certified closure block application is blocked on) must
// wait for it and complete, instead of failing busy at once.
func TestFetchEndorserBlockRequiredWaitsForBusyConnection(t *testing.T) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "only")
	o := newOuroboros(OuroborosConfig{ConnManager: cm})
	point := ocommon.NewPoint(300, []byte{3, 0})
	cacheCompleteLeiosEndorserBlock(o, point)
	released := holdLeiosFetchGuard(t, o, peers[0].Id(), leiosWaitTestHold)

	ctx, cancel := context.WithTimeout(
		ledger.WithRequiredEndorserBlockFetch(t.Context()),
		30*time.Second,
	)
	defer cancel()
	start := time.Now()
	err := o.fetchEndorserBlockWithCandidates(
		ctx,
		point.Slot,
		point.Hash,
		[]ouroboros.ConnectionId{peers[0].Id()},
		cm.GetConnectionById,
		o.fetchEndorserBlockOnConn,
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), leiosWaitTestHold,
		"the required fetch must have waited for the guard")
	testutil.RequireReceive(t, released, time.Second, "holder released")
	g := o.leiosFetchGuardFor(peers[0].Id())
	require.True(t, g.mu.TryLock(), "the waiter must release the guard")
	g.mu.Unlock()
	require.Zero(t, g.consecutiveFailures.Load(),
		"waiting for a busy connection is not a peer failure")
}

// TestFetchEndorserBlockBackgroundSkipsBusyConnection keeps the background
// prefetch behaviour: without the required marker a busy connection is
// skipped at once so the prefetch does not queue ahead of a required fetch.
func TestFetchEndorserBlockBackgroundSkipsBusyConnection(t *testing.T) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "only")
	o := newOuroboros(OuroborosConfig{ConnManager: cm})
	point := ocommon.NewPoint(301, []byte{3, 1})
	cacheCompleteLeiosEndorserBlock(o, point)
	released := holdLeiosFetchGuard(t, o, peers[0].Id(), 2*time.Second)
	defer func() { <-released }()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	start := time.Now()
	err := o.fetchEndorserBlockWithCandidates(
		ctx,
		point.Slot,
		point.Hash,
		[]ouroboros.ConnectionId{peers[0].Id()},
		cm.GetConnectionById,
		o.fetchEndorserBlockOnConn,
	)
	require.ErrorIs(t, err, errLeiosBackfillConnBusy)
	require.Less(t, time.Since(start), time.Second,
		"a background fetch must not wait for a busy connection")
}

// TestFetchEndorserBlockRequiredWaitCancels verifies that a required fetch
// waiting for a busy connection returns promptly when its context is
// cancelled, does not cool the connection down, and does not leave the guard
// held once the holder releases it.
func TestFetchEndorserBlockRequiredWaitCancels(t *testing.T) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "only")
	o := newOuroboros(OuroborosConfig{ConnManager: cm})
	point := ocommon.NewPoint(302, []byte{3, 2})
	cacheCompleteLeiosEndorserBlock(o, point)
	released := holdLeiosFetchGuard(t, o, peers[0].Id(), 2*time.Second)

	ctx, cancel := context.WithCancel(
		ledger.WithRequiredEndorserBlockFetch(t.Context()),
	)
	errCh := make(chan error, 1)
	go func() {
		errCh <- o.fetchEndorserBlockWithCandidates(
			ctx,
			point.Slot,
			point.Hash,
			[]ouroboros.ConnectionId{peers[0].Id()},
			cm.GetConnectionById,
			o.fetchEndorserBlockOnConn,
		)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	err := testutil.RequireReceive(
		t,
		errCh,
		500*time.Millisecond,
		"a cancelled wait must return promptly",
	)
	require.ErrorIs(t, err, context.Canceled)
	g := o.leiosFetchGuardFor(peers[0].Id())
	require.Zero(t, g.consecutiveFailures.Load())
	require.False(t, g.inCooldown(time.Now()))

	testutil.RequireReceive(t, released, 5*time.Second, "holder released")
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		if !g.mu.TryLock() {
			return false
		}
		g.mu.Unlock()
		return true
	}, 2*time.Second, 10*time.Millisecond,
		"an abandoned wait must not leave the guard held")
}

// TestLeiosFetchGuardLockUntilDeadline verifies the wait is bounded by the
// attempt deadline as well as ctx: a guard still held when the deadline
// passes yields a busy error without the guard being taken.
func TestLeiosFetchGuardLockUntilDeadline(t *testing.T) {
	t.Parallel()
	g := &leiosFetchGuard{}
	g.mu.Lock()
	start := time.Now()
	err := g.lockUntil(t.Context(), time.Now().Add(100*time.Millisecond))
	require.ErrorIs(t, err, errLeiosBackfillConnBusy)
	require.Less(t, time.Since(start), time.Second)
	g.mu.Unlock()
	// Give the abandoned waiter time to take the guard, so the check below
	// does not just barge in ahead of it.
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		if !g.mu.TryLock() {
			return false
		}
		g.mu.Unlock()
		return true
	}, 2*time.Second, 10*time.Millisecond,
		"an expired wait must not leave the guard held")
}
