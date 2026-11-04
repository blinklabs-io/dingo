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
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func missingCorroborationFixture(
	t *testing.T,
) (*eagernessDeliveryFixture, ouroboros.ConnectionId, ouroboros.ConnectionId) {
	t.Helper()
	f := newEagernessDeliveryFixture(t, 1000)
	f.selector.securityParam = 100
	f.selector.config.SecurityParam = 100
	candidate, blinkA, blinkB := corrConn(201), corrConn(202), corrConn(203)
	f.selector.config.PeerIdentity = func(conn ouroboros.ConnectionId) string {
		if conn == candidate {
			return "iog"
		}
		return "blink"
	}
	repairAdmit(t, f, candidate, 11)
	repairAdmit(t, f, blinkA, 32)
	repairAdmit(t, f, blinkB, 75)
	f.selector.SetLocalTip(repairTip("c", 10))
	for _, number := range []uint64{43, 44} {
		f.selector.RecordDeliveredPeerTip(repairUpdate(f, blinkA, number))
	}
	for _, number := range []uint64{95, 96, 97} {
		f.selector.RecordDeliveredPeerTip(repairUpdate(f, blinkB, number))
	}
	require.False(
		t,
		f.selector.ShouldApplyIngress(candidate),
		"ahead witnesses lack the exact candidate point",
	)
	require.False(
		t,
		f.selector.ShouldApplyIngress(blinkA),
		"a second relay under the same operator cannot corroborate",
	)
	require.False(t, f.selector.ShouldApplyIngress(blinkB))
	return f, candidate, blinkA
}

func TestVerifiedCorroborationRepairsExactWitnessOverlap(t *testing.T) {
	t.Parallel()
	f, candidate, witness := missingCorroborationFixture(t)
	u := repairUpdate(f, witness, 44)
	anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
	require.True(t, needed)
	require.Equal(
		t,
		repairTip("c", 10),
		anchor,
		"proof must reach below the admission head to the applied anchor",
	)
	before := f.selector.GetAllPeerTips()[witness]
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		allowed, err := f.selector.AwaitIngressCorroboration(ctx, candidate)
		done <- allowed && err == nil
	}()
	require.True(
		t,
		f.selector.RecordVerifiedEagernessPath(u, anchor, repairPath(10, 44), 1,
			func() bool { return true }, func() bool { return true }),
	)
	select {
	case allowed := <-done:
		require.True(
			t,
			allowed,
			"strict exact ancestry must release the held candidate",
		)
	case <-ctx.Done():
		t.Fatal("strict exact ancestry must release the held candidate")
	}
	require.True(t, f.selector.ShouldApplyIngress(candidate))
	after := f.selector.GetAllPeerTips()[witness]
	require.Equal(
		t,
		before.ObservedTip,
		after.ObservedTip,
		"a witness proof cannot admit its endpoint",
	)
	require.Equal(
		t,
		before.observedSlots,
		after.observedSlots,
		"a witness proof cannot add density credit",
	)
	require.EqualValues(
		t,
		43,
		f.selector.deliveredPeerTips[witness].observedPointBaseBlock,
		"repair is a read-only witness view, not a rewrite of delivery history",
	)
	f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, 45))
	_, _, needed = f.selector.EagernessDeliveryRepair(
		repairUpdate(f, witness, 45),
	)
	require.False(
		t,
		needed,
		"a fixed certificate covering the missing range need not recertify each linked endpoint",
	)
	require.EqualValues(
		t,
		44,
		f.selector.eagernessDeliveries[witness].proofHead,
	)
	f.generation.Store(2)
	f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, 46))
	require.False(
		t,
		f.selector.ShouldApplyIngress(candidate),
		"ordinary continuity cannot refresh strict ancestor certification",
	)
	_, _, needed = f.selector.EagernessDeliveryRepair(
		repairUpdate(f, witness, 46),
	)
	require.True(
		t,
		needed,
		"an expired certificate must be reauthenticated despite fresh continuity",
	)
}

func TestVerifiedCorroborationRetainsTrustBoundaries(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"withdrawal", "generation", "same identity", "unknown identity", "rollback", "disconnect", "conflicting history", "candidate height", "uncertified continuity", "missing canonical anchor"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, candidate, witness := missingCorroborationFixture(t)
			u := repairUpdate(f, witness, 44)
			anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
			require.True(t, needed)
			var withdrawn atomic.Bool
			path := repairPath(10, 44)
			if name == "missing canonical anchor" {
				path[22].Point.Hash[0] ^= 0xff
				require.False(
					t,
					f.selector.RecordVerifiedEagernessPath(
						u,
						anchor,
						path,
						1,
						func() bool { return true },
						func() bool { return true },
					),
					"a canonical prefix cannot replace the peer's different admitted fork point",
				)
				return
			}
			require.True(
				t,
				f.selector.RecordVerifiedEagernessPath(
					u,
					anchor,
					path,
					1,
					func() bool { return true },
					func() bool { return !withdrawn.Load() },
				),
			)
			require.True(t, f.selector.ShouldApplyIngress(candidate))
			switch name {
			case "withdrawal":
				withdrawn.Store(true)
			case "generation":
				f.generation.Store(2)
			case "same identity":
				f.selector.config.PeerIdentity = func(ouroboros.ConnectionId) string { return "one" }
			case "unknown identity":
				f.selector.config.PeerIdentity = func(ouroboros.ConnectionId) string { return "" }
			case "rollback":
				f.selector.RecordDeliveredPeerRollback(
					PeerRollbackEvent{
						ConnectionId: witness,
						Point:        repairTip("c", 10).Point,
					},
				)
			case "disconnect":
				f.selector.RemovePeer(witness)
			case "conflicting history":
				f.selector.deliveredPeerTips[witness].observedPoints[0].Hash[0] ^= 0xff
			case "candidate height":
				f.selector.peerTips[candidate].ObservedTip.BlockNumber++
			case "uncertified continuity":
				entry := f.selector.eagernessDeliveries[witness]
				entry.proofGeneration = 0
				f.selector.eagernessDeliveries[witness] = entry
			}
			require.False(
				t,
				f.selector.ShouldApplyIngress(candidate),
				"invalid ancestry must not supply independent testimony",
			)
		})
	}
}

func TestVerifiedCorroborationExcludesUncertifiedAdmissionPrefix(t *testing.T) {
	t.Parallel()
	f := newEagernessDeliveryFixture(t, 1000)
	candidate, witness := corrConn(211), corrConn(212)
	for number := uint64(1); number <= 10; number++ {
		repairAdmit(t, f, witness, number)
	}
	repairAdmit(t, f, candidate, 5)
	f.selector.SetLocalTip(repairTip("c", 15))
	for number := uint64(13); number <= 16; number++ {
		f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, number))
	}
	u := repairUpdate(f, witness, 16)
	anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
	require.True(t, needed)
	require.EqualValues(t, 10, anchor.BlockNumber)
	require.True(
		t,
		f.selector.RecordVerifiedEagernessPath(u, anchor, repairPath(10, 16), 1,
			func() bool { return true }, func() bool { return true }),
	)
	require.EqualValues(
		t,
		1,
		f.selector.eagernessDeliveries[witness].fragment.baseBlock,
	)
	require.False(
		t,
		f.selector.ShouldApplyIngress(candidate),
		"the older admitted prefix joined for LoE was not part of strict verification",
	)
}

func TestVerifiedCorroborationTrimsCertificateWithRollingWindow(t *testing.T) {
	t.Parallel()
	f := newEagernessDeliveryFixture(t, 1)
	candidate, witness := corrConn(213), corrConn(214)
	repairAdmit(t, f, candidate, 11)
	repairAdmit(t, f, witness, 10)
	f.selector.SetLocalTip(repairTip("c", 10))
	f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, 15))
	f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, 16))
	u := repairUpdate(f, witness, 16)
	anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
	require.True(t, needed)
	require.True(
		t,
		f.selector.RecordVerifiedEagernessPath(u, anchor, repairPath(10, 16), 1,
			func() bool { return true }, func() bool { return true }),
	)
	require.True(t, f.selector.ShouldApplyIngress(candidate))
	for number := uint64(17); number <= 18; number++ {
		f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, number))
		repairAdmit(t, f, witness, number)
	}
	require.EqualValues(
		t,
		12,
		f.selector.eagernessDeliveries[witness].proofBase,
	)
	require.EqualValues(
		t,
		16,
		f.selector.eagernessDeliveries[witness].proofHead,
	)
	require.False(
		t,
		f.selector.ShouldApplyIngress(candidate),
		"cropping must remove the exact old witness point",
	)
	for number := uint64(19); number <= 23; number++ {
		f.selector.RecordDeliveredPeerTip(repairUpdate(f, witness, number))
		repairAdmit(t, f, witness, number)
	}
	require.Zero(t, f.selector.eagernessDeliveries[witness].proofGeneration,
		"a certificate with no retained points cannot survive rolling eviction")
}

func TestVerifiedCorroborationRetriesAfterSameHeaderStaging(t *testing.T) {
	t.Parallel()
	f, candidate, witness := missingCorroborationFixture(t)
	u := repairUpdate(f, witness, 44)
	_, _, needed := f.selector.EagernessDeliveryRepair(u)
	require.True(t, needed)
	// A first proof can defer while ordinary verification/staging succeeds.
	// The callback worker must still recover the older gap at this same head.
	repairAdmit(t, f, witness, 44)
	anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
	require.True(
		t,
		needed,
		"staging the held header must not suppress its missing witness repair",
	)
	require.Equal(t, repairTip("c", 10), anchor)
	require.True(
		t,
		f.selector.RecordVerifiedEagernessPath(u, anchor, repairPath(10, 44), 1,
			func() bool { return true }, func() bool { return true }),
		"a strict path ending at the admission head can still recover older testimony",
	)
	require.True(t, f.selector.ShouldApplyIngress(candidate))
}
