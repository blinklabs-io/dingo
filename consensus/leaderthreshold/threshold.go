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

package leaderthreshold

import (
	"math/big"

	"github.com/blinklabs-io/gouroboros/consensus"
)

// Threshold returns the integer comparison threshold for the Praos leader
// rule. The ledger rule compares the real-valued certified-natural ratio
// strictly against 1-(1-f)^sigma. For an integer leader value v, that is
// equivalent to v < ceil(certNatMax*(1-(1-f)^sigma)).
//
// Gouroboros performs the exact-rational and bounded-precision calculation,
// including the strict-comparison ceiling and consensus-mode validation.
// Keep this wrapper as the Dingo-owned boundary so callers do not duplicate
// the protocol calculation. The returned ceiling is used unchanged; adding
// to it would admit the ceiling itself, which the real-valued rule rejects.
func Threshold(
	poolStake uint64,
	totalStake uint64,
	activeSlotCoeff *big.Rat,
	mode consensus.ConsensusMode,
) (*big.Int, error) {
	return consensus.CertifiedNatThresholdWithMode(
		poolStake,
		totalStake,
		activeSlotCoeff,
		mode,
	)
}
