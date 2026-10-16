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
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// coeffTestVRFSeed is a 32-byte VRF seed for active-slot-coefficient tests.
var coeffTestVRFSeed = []byte("coeff_test_vrf_seed_32_bytes_ok!")

// coeffTestNonce is a 32-byte epoch nonce for active-slot-coefficient tests.
var coeffTestNonce = func() []byte {
	nonce := make([]byte, 32)
	for i := range nonce {
		nonce[i] = byte(i + 7)
	}
	return nonce
}()

// coeffTestPoolID is a deterministic pool key hash.
var coeffTestPoolID = func() lcommon.PoolKeyHash {
	var id lcommon.PoolKeyHash
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}()

func TestCalculateScheduleFloatFallbackPreservesConversion(t *testing.T) {
	calc := NewCalculator(0.05)
	got, err := calc.activeSlotCoeffRat()
	require.NoError(t, err)
	require.Equal(t, new(big.Rat).SetFloat64(0.05), got)
	// Binary64 conversion is not rounded into the node domain.
	_, err = NewCalculator(1e-20).CalculateSchedule(
		10, EpochSlotRange{StartSlot: 100, SlotCount: 4},
		coeffTestPoolID, coeffTestVRFSeed, 1, 3, coeffTestNonce,
		consensus.ConsensusModeCPraos,
	)
	require.ErrorContains(t, err, "PositiveUnitInterval")
}

// TestCalculateScheduleRejectsOutOfRangeRationalActiveSlotCoeff keeps the
// (0, 1] validation applied to the exact rational path too, so a malformed
// genesis cannot silently produce a degenerate eligibility decision.
func TestCalculateScheduleRejectsOutOfRangeRationalActiveSlotCoeff(
	t *testing.T,
) {
	for name, coeff := range map[string]*big.Rat{
		"zero":     new(big.Rat),
		"negative": big.NewRat(-1, 20),
		"above one": new(big.Rat).SetFrac(
			big.NewInt(21), big.NewInt(20),
		),
	} {
		t.Run(name, func(t *testing.T) {
			calc := NewCalculator(0.05)
			calc.ActiveSlotCoeffRat = coeff
			_, err := calc.CalculateSchedule(
				10,
				EpochSlotRange{StartSlot: 100, SlotCount: 2},
				coeffTestPoolID,
				coeffTestVRFSeed,
				1,
				100,
				coeffTestNonce,
				consensus.ConsensusModeCPraos,
			)
			require.Error(t, err)
		})
	}
}

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

func testEpochSlotRange(epoch, slotsPerEpoch uint64) EpochSlotRange {
	return EpochSlotRange{
		StartSlot: epoch * slotsPerEpoch,
		SlotCount: slotsPerEpoch,
	}
}

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
			schedule := NewSchedule(
				10,
				poolId,
				tt.poolStake,
				tt.totalStake,
				nil,
			)
			assert.InDelta(t, tt.expected, schedule.StakeRatio(), 0.0001)
		})
	}
}

func TestNewCalculator(t *testing.T) {
	calc := NewCalculator(0.05)

	assert.Equal(t, 0.05, calc.ActiveSlotCoeff)
}

func TestCalculateScheduleInvalidActiveSlotCoeff(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}

	tests := []struct {
		name            string
		activeSlotCoeff float64
	}{
		{"NaN", math.NaN()},
		{"positive Inf", math.Inf(1)},
		{"negative Inf", math.Inf(-1)},
		{"zero", 0},
		{"negative", -0.05},
		{"greater than 1", 1.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewCalculator(tt.activeSlotCoeff)
			_, err := calc.CalculateSchedule(
				5, testEpochSlotRange(5, 20),
				poolId, testVRFSeed, 1000, 10000, testEpochNonce,
				consensus.ConsensusModeCPraos,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "active slot coefficient")
		})
	}
}

func TestCalculateScheduleZeroTotalStake(t *testing.T) {
	calc := NewCalculator(0.05)
	poolId := lcommon.PoolKeyHash{}

	_, err := calc.CalculateSchedule(
		10, // epoch
		testEpochSlotRange(10, 432000),
		poolId,         // pool ID
		testVRFSeed,    // VRF key
		1000,           // pool stake
		0,              // zero total stake
		testEpochNonce, // epoch nonce
		consensus.ConsensusModeCPraos,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "total stake cannot be zero")
}

func TestCalculateSchedulePoolWithStakeGetsSlots(t *testing.T) {
	// Use a small epoch (20 slots) with high f=0.9 and 100% stake to ensure
	// we reliably get leader slots. VRF Prove is expensive (~0.2s per call),
	// so we keep the slot count small.
	calc := NewCalculator(0.9)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule, err := calc.CalculateSchedule(
		5, // epoch
		testEpochSlotRange(5, 20),
		poolId,         // pool ID
		testVRFSeed,    // VRF key (32-byte seed)
		1_000_000,      // pool stake = 100% of total
		1_000_000,      // total stake
		testEpochNonce, // epoch nonce (32 bytes)
		consensus.ConsensusModeCPraos,
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
	calc := NewCalculator(0.9)
	poolId := lcommon.PoolKeyHash{}

	schedule, err := calc.CalculateSchedule(
		5, // epoch
		testEpochSlotRange(5, 20),
		poolId,         // pool ID
		testVRFSeed,    // VRF key
		0,              // zero pool stake
		1_000_000,      // total stake
		testEpochNonce, // epoch nonce
		consensus.ConsensusModeCPraos,
	)

	require.NoError(t, err)
	require.NotNil(t, schedule)

	// Zero stake should never produce leader slots
	assert.Equal(t, 0, schedule.SlotCount(),
		"pool with zero stake should never be elected leader")
}

func TestCalculateScheduleFullStakeUnitCoefficient(t *testing.T) {
	calc := NewCalculator(1)
	schedule, err := calc.CalculateSchedule(
		3, EpochSlotRange{StartSlot: 60, SlotCount: 4},
		coeffTestPoolID, testVRFSeed, 10_000_000, 10_000_000,
		testEpochNonce, consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{60, 61, 62, 63}, schedule.LeaderSlotsSnapshot())
}

func TestCalculateScheduleUsesExplicitEpochSlotRange(t *testing.T) {
	const (
		epoch              = uint64(299)
		preprodEpochStart  = uint64(127_526_400)
		naiveEpochStart    = epoch * 432_000
		testEpochSlotCount = uint64(4)
	)

	calc := NewCalculator(1.0)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule, err := calc.CalculateSchedule(
		epoch,
		EpochSlotRange{
			StartSlot: preprodEpochStart,
			SlotCount: testEpochSlotCount,
		},
		poolId,
		testVRFSeed,
		1_000_000,
		1_000_000,
		testEpochNonce,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	slots := schedule.LeaderSlotsSnapshot()
	require.NotEmpty(t, slots)
	for _, slot := range slots {
		assert.GreaterOrEqual(t, slot, preprodEpochStart)
		assert.Less(t, slot, preprodEpochStart+testEpochSlotCount)
		assert.Less(t, slot, naiveEpochStart)
	}
}

func TestCalculateScheduleIsDeterministic(t *testing.T) {
	calc := NewCalculator(0.9)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule1, err := calc.CalculateSchedule(
		7, testEpochSlotRange(7, 10),
		poolId, testVRFSeed, 500_000, 1_000_000, testEpochNonce,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)

	schedule2, err := calc.CalculateSchedule(
		7, testEpochSlotRange(7, 10),
		poolId, testVRFSeed, 500_000, 1_000_000, testEpochNonce,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)

	// Same inputs must produce identical leader slot lists
	assert.Equal(t, schedule1.LeaderSlots, schedule2.LeaderSlots,
		"leader election should be deterministic")
}

func TestCalculateScheduleInvalidVRFKey(t *testing.T) {
	calc := NewCalculator(0.9)
	poolId := lcommon.PoolKeyHash{}

	t.Run("nil key", func(t *testing.T) {
		// A nil VRF key should cause an error from the VRF signer creation
		_, err := calc.CalculateSchedule(
			5, testEpochSlotRange(5, 10),
			poolId, nil, 1000, 10000, testEpochNonce,
			consensus.ConsensusModeCPraos,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create VRF signer")
	})

	t.Run("too short key", func(t *testing.T) {
		// A 16-byte key is too short (must be 32 bytes)
		shortKey := make([]byte, 16)
		_, err := calc.CalculateSchedule(
			5, testEpochSlotRange(5, 10),
			poolId, shortKey, 1000, 10000, testEpochNonce,
			consensus.ConsensusModeCPraos,
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

// TestCalculateScheduleProducesSortedSlots guards the invariant that
// IsLeaderForSlot's binary search depends on: CalculateSchedule must emit
// leader slots in ascending order.
func TestCalculateScheduleProducesSortedSlots(t *testing.T) {
	calc := NewCalculator(0.9)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule, err := calc.CalculateSchedule(
		7,
		testEpochSlotRange(7, 30),
		poolId,
		testVRFSeed,
		1_000_000,
		1_000_000,
		testEpochNonce,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	slots := schedule.LeaderSlotsSnapshot()
	require.NotEmpty(t, slots, "high-stake pool should win some slots")
	assert.True(t, slices.IsSorted(slots),
		"CalculateSchedule must produce ascending-sorted leader slots, got %v",
		slots)
}

// TestScheduleIsLeaderForSlotBinarySearch cross-checks the binary-search
// lookup against a linear scan over a wide, sparse slot set so a regression
// in either ordering or search would be caught.
func TestScheduleIsLeaderForSlotBinarySearch(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	schedule := NewSchedule(10, poolId, 1000, 10000, nil)

	want := []uint64{1, 5, 9, 42, 100, 1_000, 50_000, 1_000_000}
	for _, slot := range want {
		schedule.AddLeaderSlot(slot)
	}

	for probe := range uint64(60) {
		assert.Equal(t,
			slices.Contains(want, probe),
			schedule.IsLeaderForSlot(probe),
			"binary search disagrees with linear scan for slot %d", probe)
	}
	// Spot-check the large boundary values too.
	assert.True(t, schedule.IsLeaderForSlot(1_000_000))
	assert.False(t, schedule.IsLeaderForSlot(1_000_001))
}

// BenchmarkIsLeaderForSlot exercises lookups against a large schedule,
// approximating a high-stake pool with many leader slots per epoch.
func BenchmarkIsLeaderForSlot(b *testing.B) {
	poolId := lcommon.PoolKeyHash{}
	schedule := NewSchedule(10, poolId, 1000, 10000, nil)
	const n = 20_000
	for i := range uint64(n) {
		schedule.AddLeaderSlot(i * 3) // ascending, sparse
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Miss on odd offset, exercising the not-found path too.
		_ = schedule.IsLeaderForSlot(uint64(i%n)*3 + uint64(i&1))
	}
}
