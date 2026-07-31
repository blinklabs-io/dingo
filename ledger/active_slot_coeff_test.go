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
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// newActiveSlotCoeffLedgerState builds a minimal LedgerState whose Shelley
// genesis carries the given activeSlotsCoeff JSON literal.
func newActiveSlotCoeffLedgerState(
	t *testing.T,
	coeffJSON string,
) *LedgerState {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": `+coeffJSON+`,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	return &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// TestActiveSlotCoeffRatIsExactGenesisRational pins that the leader-check
// coefficient accessor returns the genesis value exactly, and that the float64
// accessor does not.
//
// A Shelley genesis "activeSlotsCoeff": 0.05 decodes to exactly 1/20.
// ActiveSlotCoeff() divides the numerator and denominator as float64, and the
// nearest binary64 value to 0.05 is strictly GREATER than 1/20, so a threshold
// derived from it is strictly larger than the reference node's — a node using it
// can only over-claim leader slots, never miss any. That is the one-sided
// signature reported in dingo #2798, so the direction is pinned here even though
// the magnitude (~5.6e-17 relative) is far too small to account for the three
// phantom slots per epoch reported there.
func TestActiveSlotCoeffRatIsExactGenesisRational(t *testing.T) {
	ls := newActiveSlotCoeffLedgerState(t, "0.05")

	exact := ls.ActiveSlotCoeffRat()
	require.NotNil(t, exact)
	require.Equal(t, 0, exact.Cmp(big.NewRat(1, 20)),
		"ActiveSlotCoeffRat must return the genesis value exactly")

	approx := new(big.Rat).SetFloat64(ls.ActiveSlotCoeff())
	require.NotNil(t, approx)
	require.Equal(t, 1, approx.Cmp(exact),
		"the float64 accessor must be strictly greater than 1/20, which is "+
			"why the leader check must not use it")
}

// TestActiveSlotCoeffRatReturnsCopy proves callers cannot mutate shared genesis
// state through the returned pointer. big.Rat is mutable, and the leader
// schedule hands this value to the consensus package.
func TestActiveSlotCoeffRatReturnsCopy(t *testing.T) {
	ls := newActiveSlotCoeffLedgerState(t, "0.05")

	first := ls.ActiveSlotCoeffRat()
	require.NotNil(t, first)
	first.SetInt64(7)

	second := ls.ActiveSlotCoeffRat()
	require.NotNil(t, second)
	require.Equal(t, 0, second.Cmp(big.NewRat(1, 20)),
		"mutating a returned coefficient must not corrupt the genesis value")
}

// TestActiveSlotCoeffRatWithoutGenesis returns nil rather than a degenerate
// zero value, so callers can fall back explicitly.
func TestActiveSlotCoeffRatWithoutGenesis(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	require.Nil(t, ls.ActiveSlotCoeffRat())
}
