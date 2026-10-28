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

package chainselection

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

const (
	bandK     = 4
	bandLocal = 1_000
)

func bandChain() loneFarChain {
	return loneFarChain{localBlock: bandLocal, k: bandK}
}

// newBandSelector returns a selector with a local tip at bandLocal whose only
// reference peer delivered block reference. applied, when non-nil, backs the
// live applied tip the way the node wires it.
func newBandSelector(
	t *testing.T,
	reference uint64,
	applied *atomic.Uint64,
) *ChainSelector {
	t.Helper()
	c := bandChain()
	cfg := ChainSelectorConfig{
		SecurityParam:             bandK,
		DisableEventSubscriptions: true,
	}
	if applied != nil {
		cfg.AppliedTip = func() (ochainsync.Tip, bool) {
			return c.tip(applied.Load()), true
		}
	}
	cs := NewChainSelector(cfg)
	cs.SetLocalTip(c.tip(bandLocal))
	require.True(t, cs.UpdatePeerTip(
		newTestConnectionId(1), c.tip(reference), nil,
	))
	return cs
}

func bandUpdate(block uint64) PeerTipUpdateEvent {
	c := bandChain()
	return PeerTipUpdateEvent{
		ConnectionId:     newTestConnectionId(2),
		AdmissionID:      1,
		Tip:              c.tip(4_687_076),
		ObservedTip:      c.tip(block),
		ObservedPrevHash: c.prevHash(block),
	}
}

func awaitBand(
	cs *ChainSelector,
	ctx context.Context,
	conn ouroboros.ConnectionId,
	block uint64,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- cs.AwaitCatchUpBand(ctx, conn, bandChain().tip(block))
	}()
	return done
}

// The hold must agree with the plausibility bound the admission path applies,
// or a released header would still be rejected (or pended) and the wait would
// only have delayed the disconnect.
func TestCatchUpBandHoldMatchesPlausibilityBound(t *testing.T) {
	t.Parallel()

	conn := newTestConnectionId(2)
	for reference := uint64(bandLocal - 2); reference <= bandLocal+3*bandK; reference++ {
		for observed := uint64(bandLocal + 1); observed <= bandLocal+4*bandK; observed++ {
			t.Run(fmt.Sprintf("ref=%d/observed=%d", reference, observed), func(t *testing.T) {
				t.Parallel()

				admitted := newBandSelector(t, reference, nil).
					PreparePeerTipAdmissionDisposition(bandUpdate(observed)) ==
					PeerTipUpdateAccepted
				inBand := reference > bandLocal && reference <= bandLocal+bandK
				cs := newBandSelector(t, reference, nil)
				cs.mutex.RLock()
				defer cs.mutex.RUnlock()
				observedTip := bandChain().tip(observed)
				localTip := bandChain().tip(bandLocal)
				require.Equal(t, inBand && !admitted,
					cs.catchUpBandHoldLocked(conn, observedTip, localTip, false),
					"a fresh connection is held only from inside the band")
				require.Equal(t, !admitted,
					cs.catchUpBandHoldLocked(conn, observedTip, localTip, true),
					"a held connection waits until the admission bound passes it")
			})
		}
	}
}

// The wait keeps a connection whose header overshoots the band parked while the
// applied tip lags, releases it once the header is admissible, and does so
// without holding the selector lock.
func TestAwaitCatchUpBandParksUntilHeaderAdmissible(t *testing.T) {
	t.Parallel()

	var applied atomic.Uint64
	applied.Store(bandLocal)
	reference := uint64(bandLocal + 2)
	cs := newBandSelector(t, reference, &applied)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn := newTestConnectionId(2)
	const block = bandLocal + 2*bandK + 4 // past reference+K

	done := awaitBand(cs, ctx, conn, block)
	testutil.RequireNoReceive(t, done, 350*time.Millisecond,
		"a header past reference+K must wait for the applied tip")

	// A selector call that takes the write lock must not queue behind the wait.
	setDone := make(chan struct{})
	go func() {
		cs.SetLocalTip(bandChain().tip(bandLocal))
		close(setDone)
	}()
	testutil.RequireReceive(t, setDone, 5*time.Second,
		"the wait must not hold the selector lock")

	// The reference is now stale, but the header is still past localTip+2K.
	applied.Store(bandLocal + 3)
	testutil.RequireNoReceive(t, done, 350*time.Millisecond,
		"a held connection must stay held after its reference goes stale")

	applied.Store(block - 2*bandK)
	require.NoError(t, testutil.RequireReceive(t, done, 10*time.Second,
		"the wait must end once the header is admissible"))
	require.Equal(t, PeerTipUpdateAccepted,
		cs.PreparePeerTipAdmissionDisposition(bandUpdate(block)),
		"the released header must meet the ordinary bound")
}

func TestAwaitCatchUpBandReturnsAtOnceWhenNotHeld(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		reference uint64
		block     uint64
		slot      *uint64
	}{
		{"within reference+K", bandLocal + 2, bandLocal + 2 + bandK, nil},
		{"stale reference", bandLocal - 2, bandLocal + 2*bandK + 4, nil},
		{"reference past catch-up floor", bandLocal + bandK + 1,
			bandLocal + 3*bandK + 4, nil},
		{"height no chain could reach", bandLocal + 2, bandLocal + 100,
			new(uint64(bandLocal*20 + 10))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cs := newBandSelector(t, tc.reference, nil)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tip := bandChain().tip(tc.block)
			if tc.slot != nil {
				tip.Point.Slot = *tc.slot
			}
			require.NoError(t, cs.AwaitCatchUpBand(
				ctx, newTestConnectionId(2), tip,
			))
		})
	}
}

func TestAwaitCatchUpBandStopsOnCancel(t *testing.T) {
	t.Parallel()

	cs := newBandSelector(t, bandLocal+2, nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := awaitBand(cs, ctx, newTestConnectionId(2), bandLocal+2*bandK+4)
	testutil.RequireNoReceive(t, done, 250*time.Millisecond,
		"the connection must be parked before it is cancelled")
	cancel()
	require.ErrorIs(t, testutil.RequireReceive(t, done, 5*time.Second,
		"cancel must release the wait"), context.Canceled)
}

// A fabricated chain far past the band is never adopted: waiting records
// nothing, a connection that waits cannot corroborate another, and when the
// wait is bounded the header meets the unchanged admission verdict.
func TestAwaitCatchUpBandRecordsNothingAndLeavesVerdictUnchanged(t *testing.T) {
	t.Parallel()

	cs := newBandSelector(t, bandLocal+2, nil)
	conn := newTestConnectionId(2)
	const block = bandLocal + 2*bandK + 36
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, cs.AwaitCatchUpBand(ctx, conn, bandChain().tip(block)),
		context.DeadlineExceeded)

	cs.mutex.RLock()
	_, claimed := cs.farTipClaims[conn]
	_, pendingClaimed := cs.pendingFarTipClaims[conn]
	cs.mutex.RUnlock()
	require.False(t, claimed || pendingClaimed,
		"a waiting connection must leave no far-tip claim")
	require.Nil(t, cs.GetPeerTip(conn))

	// A second connection delivering the same far frontier is not corroborated
	// by the connection that waited.
	other := bandUpdate(block)
	other.ConnectionId = newTestConnectionId(3)
	require.NotEqual(t, PeerTipUpdateAccepted,
		cs.PreparePeerTipAdmissionDisposition(other),
		"a connection that only waited must not vouch for another")
	require.Nil(t, cs.GetPeerTip(other.ConnectionId))
	require.NotEqual(t, PeerTipUpdateAccepted,
		cs.PreparePeerTipAdmissionDisposition(bandUpdate(block)),
		"a lone far frontier is still not adopted once the wait ends")
	require.Nil(t, cs.GetPeerTip(conn))
}

// Every frontier the admission path rejects outright, rather than holding as a
// pending far claim, is one the wait parks, both for a new connection bounded
// by another peer's frontier and for a known connection bounded by its own. A
// rejection the wait does not cover closes the connection as before.
func TestCatchUpBandHoldCoversEveryRejection(t *testing.T) {
	t.Parallel()

	conn := newTestConnectionId(2)
	knownPeer := func(t *testing.T, frontier uint64) *ChainSelector {
		t.Helper()
		cs := NewChainSelector(ChainSelectorConfig{
			SecurityParam:             bandK,
			DisableEventSubscriptions: true,
		})
		cs.SetLocalTip(bandChain().tip(bandLocal))
		require.True(t, cs.UpdatePeerTip(conn, bandChain().tip(frontier), nil))
		return cs
	}
	setups := map[string]func(*testing.T, uint64) *ChainSelector{
		"new connection": func(t *testing.T, reference uint64) *ChainSelector {
			return newBandSelector(t, reference, nil)
		},
		"known connection": knownPeer,
	}
	for name, setup := range setups {
		for reference := uint64(bandLocal - 2); reference <= bandLocal+3*bandK; reference++ {
			for observed := reference + 1; observed <= bandLocal+4*bandK; observed++ {
				t.Run(fmt.Sprintf("%s/ref=%d/observed=%d", name, reference, observed), func(t *testing.T) {
					t.Parallel()

					update := bandUpdate(observed)
					update.ConnectionId = conn
					rejected := setup(t, reference).
						PreparePeerTipAdmissionDisposition(update) ==
						PeerTipUpdateRejected
					cs := setup(t, reference)
					cs.mutex.RLock()
					defer cs.mutex.RUnlock()
					held := cs.catchUpBandHoldLocked(
						conn,
						bandChain().tip(observed),
						bandChain().tip(bandLocal),
						false,
					)
					if rejected {
						require.True(t, held,
							"a rejected frontier must be parked, not disconnected")
					}
				})
			}
		}
	}
}

// bandReferencePeer returns the connection newBandSelector registered as the
// reference.
func bandReferencePeer(t *testing.T, cs *ChainSelector) ouroboros.ConnectionId {
	t.Helper()
	peers := cs.GetAllPeerTips()
	require.Len(t, peers, 1)
	for connId := range peers {
		return connId
	}
	return ouroboros.ConnectionId{}
}

// A parked connection is released, not stranded, when the peer it is bounded
// against goes away.
func TestAwaitCatchUpBandReleasesWhenReferencePeerLeaves(t *testing.T) {
	t.Parallel()

	cs := newBandSelector(t, bandLocal+2, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	done := awaitBand(cs, ctx, newTestConnectionId(2), bandLocal+2*bandK+4)
	testutil.RequireNoReceive(t, done, 250*time.Millisecond,
		"the connection must be parked before its reference leaves")
	cs.RemovePeer(bandReferencePeer(t, cs))
	require.NoError(t, testutil.RequireReceive(t, done, 5*time.Second,
		"the wait must re-derive the reference and release"))
}

// A rollback on the reference connection while another connection is parked
// moves the reference below the local tip. The parked connection stays parked
// until its header is admissible from the new reference, and is then accepted.
func TestAwaitCatchUpBandHeldAcrossReferenceRollback(t *testing.T) {
	t.Parallel()

	var applied atomic.Uint64
	applied.Store(bandLocal)
	cs := newBandSelector(t, bandLocal+2, &applied)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const block = bandLocal + 2*bandK + 4
	done := awaitBand(cs, ctx, newTestConnectionId(2), block)
	testutil.RequireNoReceive(t, done, 250*time.Millisecond,
		"the connection must be parked before the rollback")

	rollback := bandChain().tip(bandLocal - 2)
	cs.HandlePeerRollbackEvent(event.Event{Data: PeerRollbackEvent{
		ConnectionId: bandReferencePeer(t, cs),
		Point:        rollback.Point,
		Tip:          rollback,
	}})
	testutil.RequireNoReceive(t, done, 350*time.Millisecond,
		"a header past localTip+2K must stay parked after the rollback")

	applied.Store(block - 2*bandK)
	require.NoError(t, testutil.RequireReceive(t, done, 10*time.Second,
		"the wait must end once the header is admissible"))
	require.Equal(t, PeerTipUpdateAccepted,
		cs.PreparePeerTipAdmissionDisposition(bandUpdate(block)),
		"the released header must meet the ordinary bound")
}

// A local rollback while a connection is parked moves its reference past the
// band. The connection stays parked until the header is within the catch-up
// bound that then applies, and is then accepted.
func TestAwaitCatchUpBandHeldAcrossLocalRollback(t *testing.T) {
	t.Parallel()

	var applied atomic.Uint64
	applied.Store(bandLocal)
	cs := newBandSelector(t, bandLocal+2, &applied)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const block = bandLocal + 2*bandK + 4
	done := awaitBand(cs, ctx, newTestConnectionId(2), block)
	testutil.RequireNoReceive(t, done, 250*time.Millisecond,
		"the connection must be parked before the local rollback")

	applied.Store(bandLocal - bandK - 1)
	testutil.RequireNoReceive(t, done, 350*time.Millisecond,
		"a header past the catch-up bound must stay parked after a local rollback")

	applied.Store(block - 2*bandK)
	require.NoError(t, testutil.RequireReceive(t, done, 10*time.Second,
		"the wait must end once the header is admissible"))
	require.Equal(t, PeerTipUpdateAccepted,
		cs.PreparePeerTipAdmissionDisposition(bandUpdate(block)),
		"the released header must meet the ordinary bound")
}
