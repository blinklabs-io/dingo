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

	"github.com/blinklabs-io/dingo/event"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// chainsyncMetricValue returns the value of the named metric, selecting the
// series whose reason label equals reason when reason is non-empty. It
// returns 0 when the series is absent.
func chainsyncMetricValue(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
	reason string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if reason != "" {
				matched := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "reason" && lp.GetValue() == reason {
						matched = true
					}
				}
				if !matched {
					continue
				}
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

// A header parked for deferred producer-authorization recovery is counted per
// header, and the gauge reports the connections currently parked.
func TestDeferredHeaderParkMetrics(t *testing.T) {
	t.Parallel()

	var localTip, forkParent, forkHash, forkChild gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0], forkChild[0] = 0x01, 0x03, 0x04, 0x05
	f := newDeferredHeaderFixture(t, localTip)
	reg := prometheus.NewRegistry()
	f.o.registerer = newTrackingRegisterer(reg)
	f.o.initChainsyncMetrics()
	const (
		parked      = "dingo_chainsync_deferred_headers_total"
		connections = "dingo_chainsync_deferred_header_connections"
	)
	require.Zero(t, chainsyncMetricValue(t, reg, parked, ""))
	require.Zero(t, chainsyncMetricValue(t, reg, connections, ""))

	f.rollForward(t, nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 10,
		slotNumber:  86400,
	}})
	require.Equal(t, 1.0, chainsyncMetricValue(t, reg, parked, ""))
	require.Equal(t, 1.0, chainsyncMetricValue(t, reg, connections, ""))

	// A second header parked for the same connection counts again but does
	// not park another connection.
	childPoint := ocommon.NewPoint(86420, forkChild.Bytes())
	f.o.markDeferredHeaderRecovery(
		f.conn,
		f.o.ledgerState.Tip(),
		childPoint,
		ochainsync.Tip{Point: childPoint, BlockNumber: 11},
		true,
	)
	require.Equal(t, 2.0, chainsyncMetricValue(t, reg, parked, ""))
	require.Equal(t, 1.0, chainsyncMetricValue(t, reg, connections, ""))

	f.o.clearDeferredHeaderRecovery(f.conn)
	require.Equal(t, 2.0, chainsyncMetricValue(t, reg, parked, ""))
	require.Zero(t, chainsyncMetricValue(t, reg, connections, ""))
}

// A deferred header whose block the chain already holds is resolved at once
// rather than parked, so it must not be counted.
func TestDeferredHeaderHeldByChainIsNotCountedAsParked(t *testing.T) {
	t.Parallel()

	var parentHash, heldHash gledger.Blake2b256
	parentHash[0], heldHash[0] = 0x01, 0x02
	f := newDeferredHeaderFixture(t, heldHash)
	reg := prometheus.NewRegistry()
	f.o.registerer = newTrackingRegisterer(reg)
	f.o.initChainsyncMetrics()

	f.rollForward(t, nonByronTestHeader{&testBlockHeader{
		hash:        heldHash,
		prevHash:    parentHash,
		blockNumber: 10,
		slotNumber:  86400,
	}})
	require.Zero(t, chainsyncMetricValue(
		t, reg, "dingo_chainsync_deferred_headers_total", "",
	))
	require.Zero(t, chainsyncMetricValue(
		t, reg, "dingo_chainsync_deferred_header_connections", "",
	))
}

// Every resync event the subscriber handles is counted under its reason when
// that is a fixed one, and under "other" when the publisher formatted an
// error or connection ID into it.
func TestChainsyncResyncsCountedByReason(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{EventBus: bus, PromRegistry: reg})
	o.eventBus = bus
	o.SubscribeChainsyncResync(t.Context())

	const resyncs = "dingo_chainsync_resyncs_total"
	publish := func(reason string) {
		bus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{Reason: reason},
			),
		)
	}
	publish(event.ChainsyncResyncReasonHeaderValidationRecovery)
	publish(event.ChainsyncResyncReasonHeaderValidationRecovery)
	publish(event.ChainsyncResyncReasonRollbackLoop)
	publish("blockfetch start failed: connection 10.0.0.1:3001 closed")
	publish("blockfetch start failed: connection 10.0.0.2:3001 closed")

	require.Eventually(t, func() bool {
		return chainsyncMetricValue(
			t, reg, resyncs,
			event.ChainsyncResyncReasonHeaderValidationRecovery,
		) == 2 &&
			chainsyncMetricValue(
				t, reg, resyncs, event.ChainsyncResyncReasonRollbackLoop,
			) == 1 &&
			chainsyncMetricValue(
				t, reg, resyncs, chainsyncResyncOtherReason,
			) == 2
	}, 5*time.Second, 10*time.Millisecond)
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == resyncs {
			require.Len(t, mf.GetMetric(), 3,
				"formatted reasons must not add series")
		}
	}
}
