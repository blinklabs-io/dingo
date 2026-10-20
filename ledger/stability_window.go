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
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// ShelleyStabilityWindow returns the Shelley-era stability window, 3k/f
// slots rounded up, from the Shelley genesis. It is the window the ledger
// uses from Shelley onwards, so callers that need it before a LedgerState
// exists, such as a snapshot restore sizing the history it keeps, agree with
// the ledger's own StabilityWindow.
func ShelleyStabilityWindow(sg *shelley.ShelleyGenesis) (uint64, error) {
	if sg == nil {
		return 0, errors.New("no Shelley genesis")
	}
	k := sg.SecurityParam
	if k < 0 {
		return 0, fmt.Errorf("invalid negative security parameter %d", k)
	}
	if k == 0 {
		return 0, errors.New("security parameter is zero")
	}
	activeSlotsCoeff := sg.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil {
		return 0, errors.New("ActiveSlotsCoeff.Rat is nil")
	}
	if activeSlotsCoeff.Num().Sign() <= 0 {
		return 0, fmt.Errorf(
			"ActiveSlotsCoeff must be positive, got %s",
			activeSlotsCoeff.String(),
		)
	}
	numerator := new(big.Int).SetUint64(uint64(k))
	numerator.Mul(numerator, big.NewInt(3))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	denominator := new(big.Int).Set(activeSlotsCoeff.Num())
	window, remainder := new(
		big.Int,
	).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() != 0 {
		window.Add(window, big.NewInt(1))
	}
	if window.Sign() <= 0 {
		return 0, fmt.Errorf(
			"stability window calculation produced non-positive result "+
				"for security parameter %d and active slots coefficient %s",
			k, activeSlotsCoeff.String(),
		)
	}
	if !window.IsUint64() {
		return 0, fmt.Errorf(
			"stability window calculation overflowed uint64 for security "+
				"parameter %d and active slots coefficient %s",
			k, activeSlotsCoeff.String(),
		)
	}
	return window.Uint64(), nil
}
