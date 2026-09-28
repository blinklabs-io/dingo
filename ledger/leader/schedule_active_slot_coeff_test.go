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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/consensus/leaderthreshold"
	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
// the same one-sided signature reported in dingo #2798, so the direction is
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
