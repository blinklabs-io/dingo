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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newDynamoHoldFixture wires a lone Genesis peer, which no witness can
// corroborate, as the ChainSync Jumping dynamo.
func newDynamoHoldFixture(
	t *testing.T,
	budget time.Duration,
) (*Ouroboros, ouroboros.ConnectionId, func() <-chan error) {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         400,
		GenesisWindowSlots:    1_000_000,
		MinCorroboratingPeers: 1,
	})
	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	conn := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip:     cs.PreparePeerTipAdmissionDisposition,
		ChainsyncResolvePeerTip:     resolvePeerTipLikeNode(cs),
		ChainsyncApplyEligible:      cs.ShouldApplyIngress,
		ChainsyncAwaitCorroboration: cs.AwaitIngressCorroboration,
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.corroborationHoldTimeout = 10 * time.Millisecond
	o.dynamoCorroborationHoldBudget = budget
	o.chainsyncScheduleAt = func(time.Time, func()) func() { return func() {} }
	o.csj = &chainsyncJumping{
		dynamo:    conn,
		hasDynamo: true,
		jumpers:   make(map[ouroboros.ConnectionId]*csjJumper),
		streamers: make(map[ouroboros.ConnectionId]struct{}),
		objectors: make(map[ouroboros.ConnectionId]struct{}),
	}
	header := newLinkedTestHeaders(1)[0]
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(10_000_000, []byte("advertised-tip")),
		BlockNumber: 500_000,
	}
	deliver := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			done <- o.chainsyncClientRollForward(
				ochainsync.CallbackContext{ConnectionId: conn},
				0,
				header,
				tip,
			)
		}()
		return done
	}
	return o, conn, deliver
}

// A dynamo header that outlasts one corroboration hold is held again, not
// withheld with the dynamo's cursor moved past it, and stops being held once
// the connection is no longer the dynamo.
func TestChainsyncDynamoHeaderHeldAcrossHoldExpiries(t *testing.T) {
	t.Parallel()
	o, conn, deliver := newDynamoHoldFixture(t, time.Minute)
	done := deliver()
	select {
	case err := <-done:
		t.Fatalf("dynamo header withheld after one hold expiry: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	o.csj.mu.Lock()
	o.csj.hasDynamo = false
	o.csj.mu.Unlock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("former dynamo still held after rotation")
	}
	o.futureHeaderResyncMu.Lock()
	resync := o.futureHeaderResyncs[conn]
	o.futureHeaderResyncMu.Unlock()
	require.Nil(t, resync, "a former dynamo keeps the single-hold behaviour")
}

// A dynamo header still uncorroborated when the hold budget runs out is
// re-delivered from the local chain by a resync rather than withheld.
func TestChainsyncDynamoHoldBudgetReintersects(t *testing.T) {
	t.Parallel()
	o, conn, deliver := newDynamoHoldFixture(t, 50*time.Millisecond)
	select {
	case err := <-deliver():
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dynamo hold did not end at its budget")
	}
	o.futureHeaderResyncMu.Lock()
	resync := o.futureHeaderResyncs[conn]
	o.futureHeaderResyncMu.Unlock()
	require.NotNil(t, resync, "an expired dynamo hold must re-intersect")
	require.Equal(
		t,
		event.ChainsyncResyncReasonGenesisCorroborationHoldExpired,
		resync.reason,
	)
}
