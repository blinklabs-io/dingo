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

	"github.com/blinklabs-io/gouroboros/consensus"
)

// leaderThresholdMarginPrecision is enough mantissa for a ratio of two
// 512-bit integers to survive the conversion to float64 without the
// division itself being the source of error.
const leaderThresholdMarginPrecision = 128

// leaderThresholdMargin reports how much room a block's VRF leader value had
// against the stake-derived threshold, as a fraction of the threshold:
//
//	margin = (threshold - leaderValue) / threshold
//
// Positive means eligible, and the magnitude is the headroom: 0.5 cleared the
// bar with half the threshold to spare. Negative means rejected, and the
// magnitude is how far over it landed.
//
// The sign is the eligibility decision, so this is a strictly richer signal
// than the boolean. What it buys is the *distribution*: dingo derives its
// leadership stake by independent reimplementation rather than taking it from
// the reference node, so its relative stake error is never provably zero, and
// a threshold is a knife edge -- an error of eps flips a decision with
// probability about eps per block, which is a permanent wedge after roughly
// 1/eps blocks. Recording the margin for every decision, not just failures,
// turns that eps into something an operator's node measures instead of
// something we argue about after a node has already wedged. A stake error
// shows up as decisions clustering near zero; a derivation bug shows up as
// margins that are not marginal at all.
//
// Returns 0 for a non-positive threshold, which is the degenerate
// zero-stake case the callers already skip.
func leaderThresholdMargin(leaderValue, threshold *big.Int) float64 {
	if leaderValue == nil || threshold == nil || threshold.Sign() <= 0 {
		return 0
	}
	diff := new(big.Int).Sub(threshold, leaderValue)
	margin := new(big.Float).SetPrec(leaderThresholdMarginPrecision).Quo(
		new(big.Float).SetPrec(leaderThresholdMarginPrecision).SetInt(diff),
		new(big.Float).SetPrec(leaderThresholdMarginPrecision).SetInt(threshold),
	)
	result, _ := margin.Float64()
	return result
}

// leaderValueForMode derives the value that is compared against the
// threshold, mirroring consensus.IsVRFOutputBelowThresholdWithMode: CPraos
// hashes the VRF output with the "L" domain separator, TPraos compares the
// raw output. Kept in step with that function -- a divergence here would
// misreport the margin for decisions that were themselves correct.
func leaderValueForMode(
	vrfOutput []byte,
	mode consensus.ConsensusMode,
) *big.Int {
	if len(vrfOutput) == 0 {
		return nil
	}
	if mode == consensus.ConsensusModeTPraos {
		return consensus.VRFOutputToInt(vrfOutput)
	}
	return consensus.VRFOutputToInt(consensus.VrfLeaderValue(vrfOutput))
}
