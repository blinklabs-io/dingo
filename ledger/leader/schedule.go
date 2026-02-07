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

// Package leader provides Ouroboros Praos leader election functionality
// for block production. It determines which slots a stake pool is eligible
// to produce blocks based on the stake distribution snapshot.
package leader

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Schedule represents the leader schedule for a stake pool in an epoch.
// It contains the slots where the pool is eligible to produce blocks.
type Schedule struct {
	Epoch       uint64              // Epoch this schedule is for
	PoolId      lcommon.PoolKeyHash // Pool key hash
	PoolStake   uint64              // Pool's stake from Go snapshot
	TotalStake  uint64              // Total active stake from Go snapshot
	EpochNonce  []byte              // Epoch nonce for VRF
	LeaderSlots []uint64            // Slots where pool is leader

	mu sync.RWMutex
}

// NewSchedule creates a new empty schedule for an epoch.
func NewSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
	poolStake uint64,
	totalStake uint64,
	epochNonce []byte,
) *Schedule {
	return &Schedule{
		Epoch:       epoch,
		PoolId:      poolId,
		PoolStake:   poolStake,
		TotalStake:  totalStake,
		EpochNonce:  epochNonce,
		LeaderSlots: make([]uint64, 0),
	}
}

// AddLeaderSlot adds a slot where this pool is the leader.
func (s *Schedule) AddLeaderSlot(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LeaderSlots = append(s.LeaderSlots, slot)
}

// IsLeaderForSlot returns true if the pool is leader for the given slot.
func (s *Schedule) IsLeaderForSlot(slot uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return slices.Contains(s.LeaderSlots, slot)
}

// SlotCount returns the number of slots where this pool is leader.
func (s *Schedule) SlotCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.LeaderSlots)
}

// StakeRatio returns the pool's stake as a fraction of total stake.
func (s *Schedule) StakeRatio() float64 {
	if s.TotalStake == 0 {
		return 0
	}
	return float64(s.PoolStake) / float64(s.TotalStake)
}

// Calculator computes leader schedules using VRF and stake distribution.
type Calculator struct {
	// ActiveSlotCoeff (f) determines block production rate.
	// For mainnet, f = 0.05 (5% of slots have blocks on average)
	ActiveSlotCoeff float64

	// SlotsPerEpoch is the number of slots in an epoch.
	SlotsPerEpoch uint64
}

// NewCalculator creates a calculator with the given protocol parameters.
func NewCalculator(activeSlotCoeff float64, slotsPerEpoch uint64) *Calculator {
	return &Calculator{
		ActiveSlotCoeff: activeSlotCoeff,
		SlotsPerEpoch:   slotsPerEpoch,
	}
}

// CalculateSchedule computes which slots a pool leads in the given epoch.
// This uses the certified VRF to determine leader eligibility per slot.
//
// The leader check for each slot uses:
// - Pool's relative stake (sigma = poolStake / totalStake)
// - Active slot coefficient (f)
// - VRF output for the slot
//
// A pool is leader if: VRF_output < threshold(sigma)
func (c *Calculator) CalculateSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
	poolVrfSkey []byte,
	poolStake uint64,
	totalStake uint64,
	epochNonce []byte,
) (*Schedule, error) {
	if totalStake == 0 {
		return nil, errors.New("total stake cannot be zero")
	}

	schedule := NewSchedule(epoch, poolId, poolStake, totalStake, epochNonce)

	// Calculate epoch slot range
	epochStartSlot := epoch * c.SlotsPerEpoch
	epochEndSlot := epochStartSlot + c.SlotsPerEpoch

	// Check each slot in the epoch
	for slot := epochStartSlot; slot < epochEndSlot; slot++ {
		isLeader, err := c.isSlotLeader(
			slot,
			poolVrfSkey,
			poolStake,
			totalStake,
			epochNonce,
		)
		if err != nil {
			return nil, fmt.Errorf("check slot %d: %w", slot, err)
		}
		if isLeader {
			schedule.AddLeaderSlot(slot)
		}
	}

	return schedule, nil
}

// isSlotLeader determines if the pool is leader for a specific slot.
// This is a placeholder - the actual implementation requires:
// 1. VRF computation with the pool's VRF secret key
// 2. Comparison against the leadership threshold
//
// TODO: Integrate with gouroboros consensus.IsSlotLeader() when available
func (c *Calculator) isSlotLeader(
	slot uint64,
	poolVrfSkey []byte,
	poolStake uint64,
	totalStake uint64,
	epochNonce []byte,
) (bool, error) {
	// The actual leader check requires:
	// 1. Compute VRF proof for the slot seed (epochNonce || slot)
	// 2. Convert VRF output to a value in [0, 1)
	// 3. Compute threshold for the stake ratio
	// 4. Pool is leader if VRF output < threshold

	// For now, return false - actual VRF implementation needed
	// This will be integrated with gouroboros/consensus when ready
	_ = slot
	_ = poolVrfSkey
	_ = poolStake
	_ = totalStake
	_ = epochNonce

	return false, nil
}

// Threshold calculates the leadership threshold for a given stake ratio.
// threshold(sigma) = 1 - (1-f)^sigma
// where f is the active slot coefficient and sigma is the relative stake.
func (c *Calculator) Threshold(stakeRatio float64) float64 {
	if stakeRatio <= 0 {
		return 0
	}
	if stakeRatio >= 1 {
		return c.ActiveSlotCoeff
	}
	// threshold(sigma) = 1 - (1-f)^sigma
	return 1 - math.Pow(1-c.ActiveSlotCoeff, stakeRatio)
}
