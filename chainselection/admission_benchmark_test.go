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
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const admissionCostPeerCount = 10

func admissionCostFixture(
	historySize int,
) (*ChainSelector, PeerTipUpdateEvent) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             2160,
		DisableEventSubscriptions: true,
	})
	var measured PeerTipUpdateEvent
	for i := 0; i < admissionCostPeerCount; i++ {
		connID := newTestConnectionId(100 + i)
		point := ocommon.Point{Slot: uint64(10_000 + i), Hash: []byte("tip")}
		tip := ochainsync.Tip{Point: point, BlockNumber: uint64(10_000 + i)}
		history := make([]ochainsync.Tip, historySize)
		for j := range history {
			history[j] = tip
		}
		cs.peerTips[connID] = &PeerChainTip{
			ConnectionId:       connID,
			Tip:                tip,
			ObservedTip:        tip,
			observedTipSet:     true,
			observedTipHistory: history,
			nowFn:              cs.nowFn,
		}
		update := PeerTipUpdateEvent{
			ConnectionId: connID,
			AdmissionID:  1,
			Tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: tip.Point.Slot + 1,
					Hash: []byte("next"),
				},
				BlockNumber: tip.BlockNumber + 1,
			},
			ObservedTip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: tip.Point.Slot + 1,
					Hash: []byte("next"),
				},
				BlockNumber: tip.BlockNumber + 1,
			},
		}
		if i == admissionCostPeerCount-1 {
			measured = update
			continue
		}
		if !cs.PreparePeerTipAdmission(update) {
			panic("failed to stage admission cost fixture")
		}
	}
	return cs, measured
}

func measurePreparePeerTipAdmissionAllocs(historySize int) float64 {
	cs, update := admissionCostFixture(historySize)
	admissionID := uint64(1)
	return testing.AllocsPerRun(20, func() {
		admissionID++
		update.AdmissionID = admissionID
		if !cs.PreparePeerTipAdmission(update) {
			panic("failed to prepare measured admission")
		}
		cs.mutex.Lock()
		delete(cs.pendingPeerTips, update.ConnectionId)
		delete(cs.pendingPeerTipFrontiers, update.ConnectionId)
		cs.mutex.Unlock()
	})
}

// Not t.Parallel: testing.AllocsPerRun is a process-wide measurement.
func TestPreparePeerTipAdmissionCostDoesNotScaleWithCommittedHistory(
	t *testing.T,
) {
	withoutHistory := measurePreparePeerTipAdmissionAllocs(0)
	withHistory := measurePreparePeerTipAdmissionAllocs(2161)
	require.LessOrEqual(t, withHistory, withoutHistory+20,
		"staging must not clone the committed k+1 header history")
}

func BenchmarkPreparePeerTipAdmission(b *testing.B) {
	cs, update := admissionCostFixture(2161)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		update.AdmissionID = uint64(i + 2)
		if !cs.PreparePeerTipAdmission(update) {
			b.Fatal("failed to prepare measured admission")
		}
		cs.mutex.Lock()
		delete(cs.pendingPeerTips, update.ConnectionId)
		delete(cs.pendingPeerTipFrontiers, update.ConnectionId)
		cs.mutex.Unlock()
	}
}
