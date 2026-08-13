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

package ledger

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/stretchr/testify/require"
)

func TestLeaderThresholdMarginSignsTheDecision(t *testing.T) {
	threshold := big.NewInt(1000)

	tests := []struct {
		name        string
		leaderValue *big.Int
		want        float64
	}{
		{"well below threshold", big.NewInt(500), 0.5},
		{"just below threshold", big.NewInt(999), 0.001},
		{"exactly at threshold", big.NewInt(1000), 0},
		{"just over threshold", big.NewInt(1001), -0.001},
		{"far over threshold", big.NewInt(2000), -1},
		{"zero leader value", big.NewInt(0), 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := leaderThresholdMargin(tc.leaderValue, threshold)
			require.InDelta(t, tc.want, got, 1e-12)
		})
	}
}

// The margin has to stay meaningful at the magnitudes it actually sees: a
// 256-bit threshold against a 256-bit leader value. A float64 conversion done
// before the division would lose the distinction entirely.
func TestLeaderThresholdMarginAtRealMagnitudes(t *testing.T) {
	// A threshold near the real one for a 1.35% sigma pool at f=0.05:
	// 2^256 * 6.9e-4.
	twoTo256 := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	threshold := new(big.Int).Div(
		new(big.Int).Mul(twoTo256, big.NewInt(69236783929)),
		big.NewInt(100000000000000),
	)

	// A leader value 0.12% under the threshold -- the size of the stake
	// discrepancy reported from preview. It must read as a small positive
	// margin, not as zero.
	overshoot := new(big.Int).Div(threshold, big.NewInt(1000))
	justUnder := new(big.Int).Sub(threshold, overshoot)
	got := leaderThresholdMargin(justUnder, threshold)
	require.InDelta(t, 0.001, got, 1e-9)
	require.Greater(t, got, 0.0, "a value under the threshold is eligible")

	// And 0.12% over reads as the mirror-image negative.
	justOver := new(big.Int).Add(threshold, overshoot)
	got = leaderThresholdMargin(justOver, threshold)
	require.InDelta(t, -0.001, got, 1e-9)
	require.Less(t, got, 0.0, "a value over the threshold is ineligible")
}

func TestLeaderThresholdMarginDegenerateInputs(t *testing.T) {
	require.Equal(t, 0.0, leaderThresholdMargin(nil, big.NewInt(10)))
	require.Equal(t, 0.0, leaderThresholdMargin(big.NewInt(10), nil))
	require.Equal(t, 0.0, leaderThresholdMargin(big.NewInt(1), big.NewInt(0)))
	require.Equal(t, 0.0, leaderThresholdMargin(big.NewInt(1), big.NewInt(-5)))
}

// The margin is only trustworthy if it is derived the same way the decision
// is. This pins leaderValueForMode against the gouroboros comparison it
// mirrors: for a range of outputs, the sign of the margin must agree with
// IsVRFOutputBelowThresholdWithMode in both consensus modes.
func TestLeaderValueForModeAgreesWithTheDecision(t *testing.T) {
	modes := []consensus.ConsensusMode{
		consensus.ConsensusModeCPraos,
		consensus.ConsensusModeTPraos,
	}
	// A threshold high enough that some outputs land either side of it.
	bits := map[consensus.ConsensusMode]int64{
		consensus.ConsensusModeCPraos: 256,
		consensus.ConsensusModeTPraos: 512,
	}

	for _, mode := range modes {
		upper := new(big.Int).Exp(big.NewInt(2), big.NewInt(bits[mode]), nil)
		threshold := new(big.Int).Div(upper, big.NewInt(2))

		for i := range 64 {
			output := make([]byte, 64)
			output[0] = byte(i * 4)
			output[63] = byte(i)

			below, err := consensus.IsVRFOutputBelowThresholdWithMode(
				output, threshold, mode,
			)
			require.NoError(t, err)

			margin := leaderThresholdMargin(
				leaderValueForMode(output, mode), threshold,
			)
			require.Equal(t, below, margin > 0,
				"mode=%v output=%d: margin sign (%v) disagrees with the "+
					"eligibility decision (%v)", mode, i, margin, below,
			)
		}
	}
}

func TestLeaderValueForModeEmptyOutput(t *testing.T) {
	require.Nil(t, leaderValueForMode(nil, consensus.ConsensusModeCPraos))
	require.Nil(
		t,
		leaderValueForMode([]byte{}, consensus.ConsensusModeTPraos),
	)
}
