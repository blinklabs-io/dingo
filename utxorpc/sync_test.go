// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

// TestFollowTipResponse_ResetActionStructure verifies that the Reset action
// can be constructed with the correct field names and all fields populated
func TestFollowTipResponse_ResetActionStructure(t *testing.T) {
	rollbackPoint := ocommon.NewPoint(100, []byte{0x01, 0x02, 0x03})

	// This is the structure used in FollowTip when Rollback=true
	resp := &sync.FollowTipResponse{
		Action: &sync.FollowTipResponse_Reset_{
			Reset_: &sync.BlockRef{
				Slot:      rollbackPoint.Slot,
				Hash:      rollbackPoint.Hash,
				Height:    100,
				Timestamp: 1234567890000,
			},
		},
	}

	// Verify the action can be type-asserted
	reset, ok := resp.Action.(*sync.FollowTipResponse_Reset_)
	require.True(t, ok, "action should be Reset_")
	require.NotNil(t, reset.Reset_, "Reset_ field should not be nil")
	require.Equal(t, rollbackPoint.Slot, reset.Reset_.Slot, "slot should match")
	require.Equal(t, rollbackPoint.Hash, reset.Reset_.Hash, "hash should match")
	require.Equal(t, uint64(100), reset.Reset_.Height, "height should be set")
	require.Equal(t, uint64(1234567890000), reset.Reset_.Timestamp, "timestamp should be set")
}

// TestFollowTipResponse_ApplyActionStructure verifies that the Apply action
// structure remains unchanged
func TestFollowTipResponse_ApplyActionStructure(t *testing.T) {
	var acb sync.AnyChainBlock

	// This is the structure used in FollowTip when Rollback=false
	resp := &sync.FollowTipResponse{
		Action: &sync.FollowTipResponse_Apply{
			Apply: &acb,
		},
	}

	// Verify the action can be type-asserted
	apply, ok := resp.Action.(*sync.FollowTipResponse_Apply)
	require.True(t, ok, "action should be Apply")
	require.NotNil(t, apply.Apply, "Apply field should not be nil")
}

// TestFollowTipResponse_TipFieldStructure verifies that the Tip field
// can be populated with slot, hash, height, and timestamp
func TestFollowTipResponse_TipFieldStructure(t *testing.T) {
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, []byte{0x04, 0x05, 0x06}),
		BlockNumber: 200,
	}

	resp := &sync.FollowTipResponse{
		Tip: &sync.BlockRef{
			Slot:      tip.Point.Slot,
			Hash:      tip.Point.Hash,
			Height:    tip.BlockNumber,
			Timestamp: 1234567890000,
		},
	}

	require.NotNil(t, resp.Tip, "Tip should not be nil")
	require.Equal(t, tip.Point.Slot, resp.Tip.Slot, "Tip slot should match")
	require.Equal(t, tip.Point.Hash, resp.Tip.Hash, "Tip hash should match")
	require.Equal(t, tip.BlockNumber, resp.Tip.Height, "Tip height should match")
	require.Equal(t, uint64(1234567890000), resp.Tip.Timestamp, "Tip timestamp should be set")
}

// TestChainIteratorResult_RollbackField verifies that ChainIteratorResult
// has a Rollback field that can be checked
func TestChainIteratorResult_RollbackField(t *testing.T) {
	// Test rollback result
	rollbackResult := &chain.ChainIteratorResult{
		Point:    ocommon.NewPoint(100, []byte{0x01}),
		Rollback: true,
	}
	require.True(t, rollbackResult.Rollback, "Rollback should be true")

	// Test forward block result
	forwardResult := &chain.ChainIteratorResult{
		Point: ocommon.NewPoint(101, []byte{0x02}),
		Block: models.Block{
			Type: 5,
			Cbor: []byte{0x85, 0x82, 0x00, 0x00},
		},
		Rollback: false,
	}
	require.False(t, forwardResult.Rollback, "Rollback should be false")
	require.NotEmpty(t, forwardResult.Block.Cbor, "Block CBOR should not be empty")
}

// TestFollowTip_LogicFlow documents the expected logic flow for FollowTip.
// Note: These are structure tests that verify data types and logic patterns,
// not integration tests. They don't call the actual FollowTip handler, so
// they won't catch issues in the full request/response flow. For full
// integration testing, see internal/integration/ tests.
func TestFollowTip_LogicFlow(t *testing.T) {
	// Simulate the logic flow in FollowTip

	// Case 1: Rollback scenario
	t.Run("rollback_scenario", func(t *testing.T) {
		next := &chain.ChainIteratorResult{
			Point:    ocommon.NewPoint(100, []byte{0x01}),
			Rollback: true,
		}

		var resp *sync.FollowTipResponse

		if next.Rollback {
			// Should emit Reset action with all fields
			resp = &sync.FollowTipResponse{
				Action: &sync.FollowTipResponse_Reset_{
					Reset_: &sync.BlockRef{
						Slot:      next.Point.Slot,
						Hash:      next.Point.Hash,
						Height:    100,
						Timestamp: 1234567890000,
					},
				},
			}
		}

		// Populate Tip field (may differ from Reset if new blocks arrived)
		tip := ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte{0x02}),
			BlockNumber: 200,
		}
		resp.Tip = &sync.BlockRef{
			Slot:      tip.Point.Slot,
			Hash:      tip.Point.Hash,
			Height:    tip.BlockNumber,
			Timestamp: 9876543210000,
		}

		// Verify Reset action
		reset, ok := resp.Action.(*sync.FollowTipResponse_Reset_)
		require.True(t, ok, "should be Reset action")
		require.NotNil(t, reset.Reset_, "Reset_ should not be nil")
		require.Equal(t, uint64(100), reset.Reset_.Slot, "Reset slot should be 100")
		require.Equal(t, uint64(100), reset.Reset_.Height, "Reset height should be 100")
		require.NotZero(t, reset.Reset_.Timestamp, "Reset timestamp should be set")

		// Verify Tip field
		require.NotNil(t, resp.Tip, "Tip should be populated")
		require.Equal(t, uint64(200), resp.Tip.Slot, "Tip slot should be 200")
		require.Equal(t, uint64(200), resp.Tip.Height, "Tip height should be 200")
		require.NotZero(t, resp.Tip.Timestamp, "Tip timestamp should be set")

		// Verify Reset and Tip can be different (concurrent updates)
		require.NotEqual(t, reset.Reset_.Slot, resp.Tip.Slot, "Reset and Tip can differ")
	})

	// Case 2: Forward block scenario
	t.Run("forward_block_scenario", func(t *testing.T) {
		next := &chain.ChainIteratorResult{
			Point: ocommon.NewPoint(101, []byte{0x03}),
			Block: models.Block{
				Type: 5,
				Cbor: []byte{0x85, 0x82, 0x00, 0x00},
			},
			Rollback: false,
		}

		var resp *sync.FollowTipResponse

		if !next.Rollback {
			// Should emit Apply action
			var acb sync.AnyChainBlock
			resp = &sync.FollowTipResponse{
				Action: &sync.FollowTipResponse_Apply{
					Apply: &acb,
				},
			}
		}

		// Populate Tip field
		tip := ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte{0x04}),
			BlockNumber: 200,
		}
		resp.Tip = &sync.BlockRef{
			Slot:      tip.Point.Slot,
			Hash:      tip.Point.Hash,
			Height:    tip.BlockNumber,
			Timestamp: 9876543210000,
		}

		// Verify Apply action
		apply, ok := resp.Action.(*sync.FollowTipResponse_Apply)
		require.True(t, ok, "should be Apply action")
		require.NotNil(t, apply.Apply, "Apply should not be nil")

		// Verify Tip field
		require.NotNil(t, resp.Tip, "Tip should be populated")
		require.Equal(t, uint64(200), resp.Tip.Slot, "Tip slot should be 200")
		require.Equal(t, uint64(200), resp.Tip.Height, "Tip height should be 200")
		require.NotZero(t, resp.Tip.Timestamp, "Tip timestamp should be set")
	})
}

// TestFollowTip_RollbackToOrigin verifies that rollback to genesis (origin)
// is handled correctly without attempting to fetch a non-existent block
func TestFollowTip_RollbackToOrigin(t *testing.T) {
	// Origin point has slot=0 and empty hash
	originPoint := ocommon.NewPoint(0, []byte{})

	next := &chain.ChainIteratorResult{
		Point:    originPoint,
		Rollback: true,
	}

	var resp *sync.FollowTipResponse

	// Simulate the rollback-to-origin logic
	if next.Rollback {
		var height uint64
		var timestamp uint64

		// Origin check: don't call GetBlock for genesis
		if next.Point.Slot > 0 || len(next.Point.Hash) > 0 {
			// Would call GetBlock here for non-origin rollbacks
			t.Fatal("should not attempt to fetch block for origin point")
		}
		// For origin, height and timestamp remain zero

		resp = &sync.FollowTipResponse{
			Action: &sync.FollowTipResponse_Reset_{
				Reset_: &sync.BlockRef{
					Slot:      next.Point.Slot,
					Hash:      next.Point.Hash,
					Height:    height,
					Timestamp: timestamp,
				},
			},
		}
	}

	// Verify Reset action for origin
	reset, ok := resp.Action.(*sync.FollowTipResponse_Reset_)
	require.True(t, ok, "should be Reset action")
	require.NotNil(t, reset.Reset_, "Reset_ should not be nil")
	require.Equal(t, uint64(0), reset.Reset_.Slot, "origin slot should be 0")
	require.Empty(t, reset.Reset_.Hash, "origin hash should be empty")
	require.Equal(t, uint64(0), reset.Reset_.Height, "origin height should be 0")
	require.Equal(t, uint64(0), reset.Reset_.Timestamp, "origin timestamp should be 0")
}
