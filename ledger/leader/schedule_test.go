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

	"github.com/blinklabs-io/dingo/consensus/leaderthreshold"
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

// TestFloat64ActiveSlotCoeffRoundTripOverstatesGenesisThreshold pins the
// direction of the precision loss that a float64 round trip of the Shelley
// genesis active slot coefficient introduces.
//
// A Shelley genesis "activeSlotsCoeff": 0.05 decodes to the EXACT rational
// 1/20 (gouroboros' cbor.Rat uses big.Rat.SetString). Routing that value
// through float64 (num/denom, then big.Rat.SetFloat64) yields
// 3602879701896397/2^56, which is strictly GREATER than 1/20 because 0.05 is
// not representable in binary64 and the nearest double rounds up.
//
// A strictly larger f produces a strictly larger leadership threshold, whose
// acceptance region strictly CONTAINS the exact-genesis one: such a node can
// only ever claim MORE leader slots than the reference, never fewer. That is
// the same one-sided signature reported in, so the direction is
// worth pinning even though the magnitude here (~5.6e-17 relative) is far too
// small to account for the three phantom slots per epoch reported there.
func TestFloat64ActiveSlotCoeffRoundTripOverstatesGenesisThreshold(
	t *testing.T,
) {
	genesisCoeff := big.NewRat(1, 20)

	// The exact float64 round trip the leader schedule used to perform:
	// LedgerState.ActiveSlotCoeff() divides the genesis numerator and
	// denominator as float64, then the calculator called SetFloat64 on it.
	roundTripped := new(big.Rat).SetFloat64(
		float64(genesisCoeff.Num().Int64()) /
			float64(genesisCoeff.Denom().Int64()),
	)
	require.NotNil(t, roundTripped)
	require.Equal(t, 1, roundTripped.Cmp(genesisCoeff),
		"float64 round trip of 1/20 must be strictly greater than 1/20")

	const poolStake = uint64(59_000_000)
	const totalStake = uint64(1_000_000_000)

	exactThreshold, err := consensus.CertifiedNatThresholdWithMode(
		poolStake, totalStake, genesisCoeff, consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	roundTripThreshold, err := consensus.CertifiedNatThresholdWithMode(
		poolStake, totalStake, roundTripped, consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)

	require.Equal(t, 1, roundTripThreshold.Cmp(exactThreshold),
		"the float64-round-tripped coefficient must yield a strictly larger "+
			"threshold, i.e. a strict superset of eligible slots")
}

// TestCalculateScheduleUsesExactRationalActiveSlotCoeff proves the schedule
// calculator derives its leadership threshold from the exact genesis rational
// when one is supplied, rather than from a float64 approximation.
//
// f = 1/3 is used because it is not representable in binary64 at all, so the
// exact and approximated thresholds differ by a wide, unambiguous margin.
func TestCalculateScheduleUsesExactRationalActiveSlotCoeff(t *testing.T) {
	exactCoeff := big.NewRat(1, 3)
	const poolStake = uint64(59_000_000)
	const totalStake = uint64(1_000_000_000)

	calc := NewCalculator(1.0 / 3.0)
	calc.ActiveSlotCoeffRat = exactCoeff

	schedule, err := calc.CalculateSchedule(
		10,
		EpochSlotRange{StartSlot: 100, SlotCount: 4},
		coeffTestPoolID,
		coeffTestVRFSeed,
		poolStake,
		totalStake,
		coeffTestNonce,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	wantThreshold, err := leaderthreshold.Threshold(
		poolStake, totalStake, exactCoeff, consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule.Threshold)
	require.Equal(t, 0, schedule.Threshold.Cmp(wantThreshold),
		"schedule threshold must be derived from the exact genesis rational")

	// Guard against the assertion above passing by accident: the float64
	// approximation of 1/3 must produce a different threshold.
	approxThreshold, err := consensus.CertifiedNatThresholdWithMode(
		poolStake,
		totalStake,
		new(big.Rat).SetFloat64(1.0/3.0),
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.NotEqual(t, 0, approxThreshold.Cmp(wantThreshold),
		"test is only meaningful when exact and approximated f differ")
}

// TestCalculateScheduleFallsBackToFloatActiveSlotCoeff keeps the float64-only
// construction working for callers that have no exact rational available.
func TestCalculateScheduleFallsBackToFloatActiveSlotCoeff(t *testing.T) {
	const poolStake = uint64(59_000_000)
	const totalStake = uint64(1_000_000_000)

	calc := NewCalculator(0.05)
	schedule, err := calc.CalculateSchedule(
		10,
		EpochSlotRange{StartSlot: 100, SlotCount: 4},
		coeffTestPoolID,
		coeffTestVRFSeed,
		poolStake,
		totalStake,
		coeffTestNonce,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule.Threshold)

	wantThreshold, err := leaderthreshold.Threshold(
		poolStake,
		totalStake,
		new(big.Rat).SetFloat64(0.05),
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.Equal(t, 0, schedule.Threshold.Cmp(wantThreshold))
}

// TestCalculateScheduleRejectsOutOfRangeRationalActiveSlotCoeff keeps the
// (0, 1] validation applied to the exact rational path too, so a malformed
// genesis cannot silently produce a degenerate threshold.
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

func TestCalculatorThreshold(t *testing.T) {
	// Mainnet parameters: f = 0.05
	calc := NewCalculator(0.05)

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

func TestCalculatorThresholdInvalidInputs(t *testing.T) {
	tests := []struct {
		name            string
		activeSlotCoeff float64
		stakeRatio      float64
		expected        float64
	}{
		{
			name:            "NaN active slot coeff",
			activeSlotCoeff: math.NaN(),
			stakeRatio:      0.5,
			expected:        0,
		},
		{
			name:            "Inf active slot coeff",
			activeSlotCoeff: math.Inf(1),
			stakeRatio:      0.5,
			expected:        0,
		},
		{
			name:            "negative Inf active slot coeff",
			activeSlotCoeff: math.Inf(-1),
			stakeRatio:      0.5,
			expected:        0,
		},
		{
			name:            "zero active slot coeff",
			activeSlotCoeff: 0,
			stakeRatio:      0.5,
			expected:        0,
		},
		{
			name:            "negative active slot coeff",
			activeSlotCoeff: -0.05,
			stakeRatio:      0.5,
			expected:        0,
		},
		{
			name:            "active slot coeff > 1",
			activeSlotCoeff: 1.5,
			stakeRatio:      0.5,
			expected:        0,
		},
		{
			name:            "NaN stake ratio",
			activeSlotCoeff: 0.05,
			stakeRatio:      math.NaN(),
			expected:        0,
		},
		{
			name:            "Inf stake ratio",
			activeSlotCoeff: 0.05,
			stakeRatio:      math.Inf(1),
			expected:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewCalculator(tt.activeSlotCoeff)
			result := calc.Threshold(tt.stakeRatio)
			assert.Equal(t, tt.expected, result)
		})
	}
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

func TestCalculateScheduleFullStakeApproxRate(t *testing.T) {
	// With f=0.9 and 100% stake, a pool should be leader for ~90% of slots.
	// Use 20 slots to keep VRF computation fast while having enough samples.
	const slotsPerEpoch = 20
	calc := NewCalculator(0.9)
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	schedule, err := calc.CalculateSchedule(
		3, // epoch
		testEpochSlotRange(3, slotsPerEpoch),
		poolId,         // pool ID
		testVRFSeed,    // VRF key
		10_000_000,     // pool stake = 100% of total
		10_000_000,     // total stake
		testEpochNonce, // epoch nonce
		consensus.ConsensusModeCPraos,
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
