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
// equivalent to v < ceil(certNatMax*(1-(1-f)^sigma)), not v < floor(...).
//
// The gouroboros version used by Dingo currently returns floor(...). Keep its
// rigorously calculated value as the base, and raise it by one only when exact
// rational arithmetic proves the real cutoff is not an integer. When the
// cutoff is integral, the returned floor is retained so the exact boundary is
// still rejected.
func Threshold(
	poolStake uint64,
	totalStake uint64,
	activeSlotCoeff *big.Rat,
	mode consensus.ConsensusMode,
) (*big.Int, error) {
	threshold, err := consensus.CertifiedNatThresholdWithMode(
		poolStake,
		totalStake,
		activeSlotCoeff,
		mode,
	)
	if err != nil {
		return nil, err
	}
	if cutoffIsInteger(poolStake, totalStake, activeSlotCoeff, mode) {
		return threshold, nil
	}
	return new(big.Int).Add(threshold, big.NewInt(1)), nil
}

// cutoffIsInteger proves whether the real cutoff is an integer for the
// rational protocol inputs Dingo has available. If sigma's reduced exponent
// does not make (1-f)^sigma rational, the cutoff is irrational and therefore
// cannot be an integer. The rational case is evaluated exactly.
func cutoffIsInteger(
	poolStake uint64,
	totalStake uint64,
	activeSlotCoeff *big.Rat,
	mode consensus.ConsensusMode,
) bool {
	if activeSlotCoeff == nil || activeSlotCoeff.Sign() <= 0 ||
		totalStake == 0 || poolStake == 0 {
		return true
	}
	if activeSlotCoeff.Cmp(big.NewRat(1, 1)) >= 0 {
		// f == 1 has cutoff certNatMax. f > 1 is rejected by the
		// underlying threshold helper before this result is used.
		return true
	}
	if poolStake > totalStake {
		poolStake = totalStake
	}

	sigma := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(poolStake),
		new(big.Int).SetUint64(totalStake),
	)
	rootDegree := sigma.Denom()
	if !rootDegree.IsUint64() {
		return false
	}
	rootDegreeUint := rootDegree.Uint64()
	oneMinusF := new(big.Rat).Sub(big.NewRat(1, 1), activeSlotCoeff)
	rootNumerator, ok := exactNthRoot(oneMinusF.Num(), rootDegreeUint)
	if !ok {
		return false
	}
	rootDenominator, ok := exactNthRoot(oneMinusF.Denom(), rootDegreeUint)
	if !ok {
		return false
	}

	power := new(big.Rat).SetFrac(
		new(big.Int).Exp(rootNumerator, sigma.Num(), nil),
		new(big.Int).Exp(rootDenominator, sigma.Num(), nil),
	)
	probability := new(big.Rat).Sub(big.NewRat(1, 1), power)
	upperBound := new(big.Int).Lsh(big.NewInt(1), 256)
	if mode == consensus.ConsensusModeTPraos {
		upperBound.Lsh(big.NewInt(1), 512)
	}
	cutoffNumerator := new(big.Int).Mul(upperBound, probability.Num())
	remainder := new(big.Int)
	new(big.Int).QuoRem(cutoffNumerator, probability.Denom(), remainder)
	return remainder.Sign() == 0
}

// exactNthRoot reports an integer nth root only when the input is a perfect
// power. Protocol stake denominators are at most 64 bits; avoiding a search
// for degrees larger than the input's bit length keeps adversarial inputs
// bounded while preserving the exact proof.
func exactNthRoot(value *big.Int, degree uint64) (*big.Int, bool) {
	if value.Sign() < 0 || degree == 0 {
		return nil, false
	}
	if value.Sign() == 0 || value.Cmp(big.NewInt(1)) == 0 || degree == 1 {
		return new(big.Int).Set(value), true
	}
	if degree > uint64(value.BitLen()) {
		return nil, false
	}

	low := big.NewInt(1)
	high := new(big.Int).Lsh(
		big.NewInt(1),
		uint((uint64(value.BitLen())+degree-1)/degree),
	)
	for low.Cmp(high) < 0 {
		mid := new(big.Int).Add(low, high)
		mid.Rsh(mid, 1)
		power := new(big.Int).Exp(mid, new(big.Int).SetUint64(degree), nil)
		if power.Cmp(value) < 0 {
			low.Add(mid, big.NewInt(1))
		} else {
			high.Set(mid)
		}
	}
	if new(big.Int).Exp(low, new(big.Int).SetUint64(degree), nil).
		Cmp(value) != 0 {
		return nil, false
	}
	return low, true
}
