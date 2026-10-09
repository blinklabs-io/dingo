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

package dingo

import (
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/chainsyncrecycler"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncDecisionCountersPreMaterializeLabels(t *testing.T) {
	t.Parallel()
	_, registry := newMetricsTestNode(t)

	assert.Equal(t, map[string]float64{
		string(chainselection.PeerTipRejectedObservedFrontier): 0,
		string(chainselection.PeerTipRejectedAdvertisedTip):    0,
	}, counterValues(
		t, registry, "dingo_chainselection_peer_tip_rejections_total",
	))
	assert.Equal(t, map[string]float64{
		string(chainsyncrecycler.PlateauOutcomeReconciled):         0,
		string(chainsyncrecycler.PlateauOutcomeBacklogNotRecycled): 0,
		string(chainsyncrecycler.PlateauOutcomeResync):             0,
	}, counterValues(
		t, registry, "dingo_chainsync_plateau_decisions_total",
	))
}

// The composed selector config is what the binary runs, so the rejection
// counter is verified from it: a hook defined in chainselection but not set
// here would count nothing at runtime.
func TestBuildChainSelectorConfigWiresPeerTipRejectionCounter(t *testing.T) {
	t.Parallel()
	n, registry := newMetricsTestNode(t)
	cfg := n.buildChainSelectorConfig(2160, false, 0)
	cfg.DisableEventSubscriptions = true
	cfg.ConnectionLive = func(ouroboros.ConnectionId) bool { return true }
	selector := chainselection.NewChainSelector(cfg)
	selector.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})
	require.True(t, selector.UpdatePeerTip(
		newNodeTestConnId(3401),
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100500, Hash: []byte("ok")},
			BlockNumber: 50500,
		},
		nil,
	))

	require.False(t, selector.UpdatePeerTip(
		newNodeTestConnId(3402),
		ochainsync.Tip{
			Point: ocommon.Point{
				Slot: math.MaxUint64,
				Hash: []byte("spoof"),
			},
			BlockNumber: math.MaxUint64,
		},
		nil,
	))

	assert.Equal(t, float64(1), counterValues(
		t, registry, "dingo_chainselection_peer_tip_rejections_total",
	)[string(chainselection.PeerTipRejectedObservedFrontier)])
}

func TestBuildChainsyncRecyclerConfigWiresPlateauDecisionCounter(
	t *testing.T,
) {
	t.Parallel()
	n, registry := newMetricsTestNode(t)
	cfg := n.buildChainsyncRecyclerConfig(chainsync.DefaultConfig())
	require.NotNil(t, cfg.OnPlateauDecision)

	cfg.OnPlateauDecision(chainsyncrecycler.PlateauOutcomeBacklogNotRecycled)

	values := counterValues(
		t, registry, "dingo_chainsync_plateau_decisions_total",
	)
	assert.Equal(t, float64(1), values[string(
		chainsyncrecycler.PlateauOutcomeBacklogNotRecycled,
	)])
	assert.Equal(t, float64(0), values[string(
		chainsyncrecycler.PlateauOutcomeResync,
	)])
}
