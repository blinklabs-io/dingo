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

package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetrics_RecordForgedBlock(t *testing.T) {
	m := NewMetrics()

	ev := &BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      100,
		BlockHash: "hash100",
	}
	m.RecordEvent(ev)

	snap := m.Snapshot()
	require.Equal(t, 1, snap.TotalBlocksForged)
	require.Equal(t, 1, snap.BlocksByNode["p1"])
	require.Equal(t, uint64(100), snap.MaxSlotByNode["p1"])
	require.Empty(t, snap.Equivocations)
	require.Empty(t, snap.SlotRegressions)
}

func TestMetrics_BlocksByNode_MultipleNodes(t *testing.T) {
	m := NewMetrics()

	for i := 0; i < 3; i++ {
		m.RecordEvent(&BlockEvent{
			Type:      EventForgedBlock,
			NodeID:    "p1",
			Slot:      uint64(100 + i),
			BlockHash: "hash",
		})
	}
	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p2",
		Slot:      200,
		BlockHash: "hash200",
	})

	snap := m.Snapshot()
	require.Equal(t, 4, snap.TotalBlocksForged)
	require.Equal(t, 3, snap.BlocksByNode["p1"])
	require.Equal(t, 1, snap.BlocksByNode["p2"])
}

func TestMetrics_Equivocation_SameNodeSameSlotDifferentHash(t *testing.T) {
	m := NewMetrics()

	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      42,
		BlockHash: "hashA",
	})
	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      42,
		BlockHash: "hashB",
	})

	snap := m.Snapshot()
	require.Len(t, snap.Equivocations, 1)
	eq := snap.Equivocations[0]
	require.Equal(t, "p1", eq.NodeID)
	require.Equal(t, uint64(42), eq.Slot)
	require.Equal(t, "hashA", eq.HashA)
	require.Equal(t, "hashB", eq.HashB)
}

func TestMetrics_NoEquivocation_SameNodeSameSlotSameHash(t *testing.T) {
	m := NewMetrics()

	for i := 0; i < 3; i++ {
		m.RecordEvent(&BlockEvent{
			Type:      EventForgedBlock,
			NodeID:    "p1",
			Slot:      42,
			BlockHash: "sameHash",
		})
	}

	snap := m.Snapshot()
	require.Empty(t, snap.Equivocations)
}

func TestMetrics_NoEquivocation_DifferentNodesSameSlot(t *testing.T) {
	m := NewMetrics()

	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      42,
		BlockHash: "hashA",
	})
	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p2",
		Slot:      42,
		BlockHash: "hashB",
	})

	snap := m.Snapshot()
	require.Empty(
		t,
		snap.Equivocations,
		"different nodes at the same slot are not equivocation",
	)
}

func TestMetrics_SlotMonotonicity_NoRegression(t *testing.T) {
	m := NewMetrics()

	for _, slot := range []uint64{10, 20, 30, 40} {
		m.RecordEvent(&BlockEvent{
			Type:      EventForgedBlock,
			NodeID:    "p1",
			Slot:      slot,
			BlockHash: "h",
		})
	}

	snap := m.Snapshot()
	require.Empty(t, snap.SlotRegressions)
	require.Equal(t, uint64(40), snap.MaxSlotByNode["p1"])
}

func TestMetrics_SlotRegression_Detected(t *testing.T) {
	m := NewMetrics()

	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      100,
		BlockHash: "h1",
	})
	// This slot is lower than the previous — should trigger a regression.
	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      50,
		BlockHash: "h2",
	})

	snap := m.Snapshot()
	require.Len(t, snap.SlotRegressions, 1)
	reg := snap.SlotRegressions[0]
	require.Equal(t, "p1", reg.NodeID)
	require.Equal(t, uint64(100), reg.PrevSlot)
	require.Equal(t, uint64(50), reg.NewSlot)
}

func TestMetrics_ChainTipTracking(t *testing.T) {
	m := NewMetrics()

	m.RecordEvent(&BlockEvent{
		Type:   EventChainExtended,
		NodeID: "p1",
		Slot:   200,
	})
	m.RecordEvent(&BlockEvent{
		Type:   EventChainExtended,
		NodeID: "p1",
		Slot:   150, // older slot; should not replace tip
	})
	snap := m.Snapshot()
	require.Equal(t, uint64(200), snap.ChainTipByNode["p1"])
	m.RecordEvent(&BlockEvent{
		Type:   EventChainExtended,
		NodeID: "p1",
		Slot:   250,
	})

	snap = m.Snapshot()
	require.Equal(t, uint64(250), snap.ChainTipByNode["p1"])
}

func TestMetrics_MempoolCount(t *testing.T) {
	m := NewMetrics()

	for i := 0; i < 7; i++ {
		m.RecordEvent(&BlockEvent{Type: EventMempoolAdd, NodeID: "p1"})
	}

	snap := m.Snapshot()
	require.Equal(t, 7, snap.MempoolTxCount)
}

func TestMetrics_TxSubmittedCounts(t *testing.T) {
	tests := []struct {
		name            string
		txType          string
		wantDelegations int
		wantGovernance  int
		wantPlutus      int
	}{
		{
			name:            "delegation",
			txType:          "delegation",
			wantDelegations: 1,
		},
		{
			name:           "governance",
			txType:         "governance",
			wantGovernance: 1,
		},
		{
			name:       "plutus",
			txType:     "plutus",
			wantPlutus: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := NewMetrics()

			m.RecordEvent(&BlockEvent{
				Type:   EventTxSubmitted,
				TxType: test.txType,
			})

			snap := m.Snapshot()
			require.Equal(t, test.wantDelegations, snap.DelegationsProcessed)
			require.Equal(t, test.wantGovernance, snap.GovernanceProcessed)
			require.Equal(t, test.wantPlutus, snap.PlutusProcessed)
		})
	}
}

func TestMetrics_NilEvent(t *testing.T) {
	m := NewMetrics()
	// Must not panic
	m.RecordEvent(nil)
	snap := m.Snapshot()
	require.Equal(t, 0, snap.TotalBlocksForged)
}

func TestMetrics_SnapshotIsIndependent(t *testing.T) {
	m := NewMetrics()

	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      1,
		BlockHash: "h1",
	})

	snap := m.Snapshot()

	// Mutate after snapshot — snap should be unaffected
	m.RecordEvent(&BlockEvent{
		Type:      EventForgedBlock,
		NodeID:    "p1",
		Slot:      2,
		BlockHash: "h2",
	})

	require.Equal(t, 1, snap.TotalBlocksForged)
}
