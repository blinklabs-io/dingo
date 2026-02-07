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

package leader

import (
	"math"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testVRFSeed is a deterministic 32-byte VRF seed for testing.
var testVRFSeed = []byte("test_vrf_seed_for_leader_sched!!")

// testEpochNonce is a deterministic 32-byte epoch nonce for testing.
var testEpochNonce = func() []byte {
	nonce := make([]byte, 32)
	for i := range nonce {
		nonce[i] = byte(i)
	}
	return nonce
}()

func TestNewSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("pool1234567890123456"))

	schedule := NewSchedule(
		10,     // epoch
		poolId, // pool ID
		1000,   // pool stake
		10000,  // total stake
		[]byte("nonce"),
	)

	assert.NotNil(t, schedule)
	assert.Equal(t, uint64(10), schedule.Epoch)
	assert.Equal(t, poolId, schedule.PoolId)
	assert.Equal(t, uint64(1000), schedule.PoolStake)
	assert.Equal(t, uint64(10000), schedule.TotalStake)
	assert.Equal(t, []byte("nonce"), schedule.EpochNonce)
	assert.Empty(t, schedule.LeaderSlots)
}

func TestScheduleAddLeaderSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	schedule := NewSchedule(10, poolId, 1000, 10000, nil)

	schedule.AddLeaderSlot(100)
	schedule.AddLeaderSlot(200)
	schedule.AddLeaderSlot(300)

	assert.Len(t, schedule.LeaderSlots, 3)
	assert.Contains(t, schedule.LeaderSlots, uint64(100))
	assert.Contains(t, schedule.LeaderSlots, uint64(200))
	assert.Contains(t, schedule.LeaderSlots, uint64(300))
}

func TestScheduleIsLeaderForSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	schedule := NewSchedule(10, poolId, 1000, 10000, nil)

	schedule.AddLeaderSlot(100)
	schedule.AddLeaderSlot(200)

	assert.True(t, schedule.IsLeaderForSlot(100))
	assert.True(t, schedule.IsLeaderForSlot(200))
	assert.False(t, schedule.IsLeaderForSlot(150))
	assert.False(t, schedule.IsLeaderForSlot(0))
}

func TestScheduleSlotCount(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	schedule := NewSchedule(10, poolId, 1000, 10000, nil)

	assert.Equal(t, 0, schedule.SlotCount())

	schedule.AddLeaderSlot(100)
	assert.Equal(t, 1, schedule.SlotCount())

	schedule.AddLeaderSlot(200)
	schedule.AddLeaderSlot(300)
	assert.Equal(t, 3, schedule.SlotCount())
}

func TestScheduleStakeRatio(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}

	tests := []struct {
		name       string
		poolStake  uint64
		totalStake uint64
		expected   float64
	}{
		{
			name:       "10% stake",
			poolStake:  1000,
			totalStake: 10000,
			expected:   0.1,
		},
		{
			name:       "50% stake",
			poolStake:  5000,
			totalStake: 10000,
			expected:   0.5,
		},
		{
			name:       "100% stake",
			poolStake:  10000,
			totalStake: 10000,
			expected:   1.0,
		},
		{
			name:       "zero total stake",
			poolStake:  1000,
			totalStake: 0,
			expected:   0,
		},
		{
			name:       "zero pool stake",
			poolStake:  0,
			totalStake: 10000,
			expected:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := NewSchedule(10, poolId, tt.poolStake, tt.totalStake, nil)
			assert.InDelta(t, tt.expected, schedule.StakeRatio(), 0.0001)
		})
	}
}

func TestNewCalculator(t *testing.T) {
	calc := NewCalculator(0.05, 432000)

	assert.Equal(t, 0.05, calc.ActiveSlotCoeff)
	assert.Equal(t, uint64(432000), calc.SlotsPerEpoch)
}

func TestCalculatorThreshold(t *testing.T) {
	// Mainnet parameters: f = 0.05
	calc := NewCalculator(0.05, 432000)

	tests := []struct {
		name       string
		stakeRatio float64
		expected   float64
	}{
		{
			name:       "zero stake",
			stakeRatio: 0,
			expected:   0,
		},
		{
			name:       "small stake (1%)",
			stakeRatio: 0.01,
			// threshold = 1 - (1-0.05)^0.01 = 1 - 0.95^0.01
			expected: 1 - math.Pow(0.95, 0.01),
		},
		{
			name:       "10% stake",
			stakeRatio: 0.1,
			// threshold = 1 - (1-0.05)^0.1 = 1 - 0.95^0.1
			expected: 1 - math.Pow(0.95, 0.1),
		},
		{
			name:       "100% stake",
			stakeRatio: 1.0,
			// threshold = 1 - (1-0.05)^1 = 1 - 0.95 = 0.05
			expected: 0.05,
		},
		{
			name:       "greater than 100%",
			stakeRatio: 1.5,
			// Capped at f (active slot coefficient)
			expected: 0.05,
		},
		{
			name:       "negative stake",
			stakeRatio: -0.1,
			expected:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calc.Threshold(tt.stakeRatio)
			assert.InDelta(t, tt.expected, result, 0.0001)
		})
	}
}

func TestCalculateScheduleZeroTotalStake(t *testing.T) {
	calc := NewCalculator(0.05, 432000)
	poolId := lcommon.PoolKeyHash{}

	_, err := calc.CalculateSchedule(
		10,            // epoch
		poolId,        // pool ID
		testVRFSeed,   // VRF key
		1000,          // pool stake
		0,             // zero total stake
		testEpochNonce, // epoch nonce
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "total stake cannot be zero")
}

func TestCalculateSchedulePoolWithStakeGetsSlots(t *testing.T) {
	// Use a small epoch (20 slots) with high f=0.9 and 100% stake to ensure
	// we reliably get leader slots. VRF Prove is expensive (~0.2s per call),
	// so we keep the slot count small.
	calc := NewCalculator(0.9, 20)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule, err := calc.CalculateSchedule(
		5,               // epoch
		poolId,          // pool ID
		testVRFSeed,     // VRF key (32-byte seed)
		1_000_000,       // pool stake = 100% of total
		1_000_000,       // total stake
		testEpochNonce,  // epoch nonce (32 bytes)
	)

	require.NoError(t, err)
	require.NotNil(t, schedule)

	assert.Equal(t, uint64(5), schedule.Epoch)
	assert.Equal(t, poolId, schedule.PoolId)
	assert.Equal(t, uint64(1_000_000), schedule.PoolStake)
	assert.Equal(t, uint64(1_000_000), schedule.TotalStake)
	assert.Equal(t, testEpochNonce, schedule.EpochNonce)

	// With 100% stake and f=0.9, we expect ~18 leader slots in 20 slots.
	slotCount := schedule.SlotCount()
	assert.Greater(t, slotCount, 0,
		"pool with 100%% stake should be elected for at least some slots")
}

func TestCalculateScheduleZeroPoolStakeGetsNoSlots(t *testing.T) {
	calc := NewCalculator(0.9, 20)
	poolId := lcommon.PoolKeyHash{}

	schedule, err := calc.CalculateSchedule(
		5,              // epoch
		poolId,         // pool ID
		testVRFSeed,    // VRF key
		0,              // zero pool stake
		1_000_000,      // total stake
		testEpochNonce, // epoch nonce
	)

	require.NoError(t, err)
	require.NotNil(t, schedule)

	// Zero stake should never produce leader slots
	assert.Equal(t, 0, schedule.SlotCount(),
		"pool with zero stake should never be elected leader")
}

func TestCalculateScheduleFullStakeApproxRate(t *testing.T) {
	// With f=0.9 and 100% stake, a pool should be leader for ~90% of slots.
	// Use 20 slots to keep VRF computation fast while having enough samples.
	const slotsPerEpoch = 20
	calc := NewCalculator(0.9, slotsPerEpoch)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule, err := calc.CalculateSchedule(
		3,               // epoch
		poolId,          // pool ID
		testVRFSeed,     // VRF key
		10_000_000,      // pool stake = 100% of total
		10_000_000,      // total stake
		testEpochNonce,  // epoch nonce
	)

	require.NoError(t, err)
	require.NotNil(t, schedule)

	slotCount := schedule.SlotCount()
	// Expected ~18 slots (90% of 20). With 20 Bernoulli trials at p=0.9,
	// getting <=14 is astronomically unlikely (binomial CDF < 0.01%).
	assert.Greater(t, slotCount, 14,
		"pool with 100%% stake and f=0.9 should get >70%% leader slots")
	assert.LessOrEqual(t, slotCount, slotsPerEpoch,
		"pool should not exceed total slots in epoch")

	t.Logf(
		"leader slots: %d / %d (%.1f%%)",
		slotCount,
		slotsPerEpoch,
		float64(slotCount)/float64(slotsPerEpoch)*100,
	)
}

func TestCalculateScheduleIsDeterministic(t *testing.T) {
	calc := NewCalculator(0.9, 10)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule1, err := calc.CalculateSchedule(
		7, poolId, testVRFSeed, 500_000, 1_000_000, testEpochNonce,
	)
	require.NoError(t, err)

	schedule2, err := calc.CalculateSchedule(
		7, poolId, testVRFSeed, 500_000, 1_000_000, testEpochNonce,
	)
	require.NoError(t, err)

	// Same inputs must produce identical leader slot lists
	assert.Equal(t, schedule1.LeaderSlots, schedule2.LeaderSlots,
		"leader election should be deterministic")
}

func TestCalculateScheduleInvalidVRFKey(t *testing.T) {
	calc := NewCalculator(0.9, 10)
	poolId := lcommon.PoolKeyHash{}

	t.Run("nil key", func(t *testing.T) {
		// A nil VRF key should cause an error from the VRF signer creation
		_, err := calc.CalculateSchedule(
			5, poolId, nil, 1000, 10000, testEpochNonce,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create VRF signer")
	})

	t.Run("too short key", func(t *testing.T) {
		// A 16-byte key is too short (must be 32 bytes)
		shortKey := make([]byte, 16)
		_, err := calc.CalculateSchedule(
			5, poolId, shortKey, 1000, 10000, testEpochNonce,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create VRF signer")
		assert.Contains(t, err.Error(), "seed must be 32 bytes")
	})
}

func TestScheduleConcurrentAccess(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	schedule := NewSchedule(10, poolId, 1000, 10000, nil)

	// Concurrent writes
	done := make(chan bool)
	for i := range 10 {
		go func(slot int) {
			schedule.AddLeaderSlot(uint64(slot * 100))
			done <- true
		}(i)
	}

	// Wait for all writes
	for range 10 {
		<-done
	}

	// Concurrent reads
	for range 10 {
		go func() {
			_ = schedule.SlotCount()
			_ = schedule.IsLeaderForSlot(100)
			_ = schedule.StakeRatio()
			done <- true
		}()
	}

	// Wait for all reads
	for range 10 {
		<-done
	}

	assert.Equal(t, 10, schedule.SlotCount())
}
