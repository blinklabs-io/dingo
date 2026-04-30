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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// shelleyGenesisCfgForNonceWindow loads a Shelley genesis with concrete
// k and f values so nonceStabilityWindow has real inputs to compute on.
// k=10, f=1/2 chosen so 3k/f=60 and 4k/f=80 — distinct, small integers
// that make divergent results obvious in test failures.
func shelleyGenesisCfgForNonceWindow(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(`{
			"systemStart": "2022-10-25T00:00:00Z",
			"securityParam": 10,
			"activeSlotsCoeff": 0.5
		}`),
	))
	return cfg
}

// TestNonceStabilityWindow_EraDispatch asserts the randomness-stabilisation
// window is era-aware:
//
//   - TPraos eras (Shelley, Allegra, Mary, Alonzo): 3k/f
//     (cardano-ledger's computeStabilityWindow)
//   - Praos eras (Babbage, Conway): 4k/f
//     (cardano-ledger's computeRandomnessStabilisationWindow)
//
// The bug in #2125 is that nonceStabilityWindow returns 4k/f universally,
// shifting the candidate-nonce freeze cutoff in pre-Babbage epochs and
// producing an epoch nonce that diverges from cardano-node. That makes
// every TPraos block VRF-fail at the next epoch boundary — observed at
// Shelley→Allegra in the internal/test/devnet/eras stack.
//
// Today this test fails for Shelley/Allegra/Mary/Alonzo. After the fix,
// all six era cases pass.
func TestNonceStabilityWindow_EraDispatch(t *testing.T) {
	cases := []struct {
		name         string
		eraId        uint
		expectedSlot uint64 // 3k/f=60 for TPraos, 4k/f=80 for Praos
	}{
		{name: "shelley", eraId: shelley.EraIdShelley, expectedSlot: 60},
		{name: "allegra", eraId: allegra.EraIdAllegra, expectedSlot: 60},
		{name: "mary", eraId: mary.EraIdMary, expectedSlot: 60},
		{name: "alonzo", eraId: alonzo.EraIdAlonzo, expectedSlot: 60},
		{name: "babbage", eraId: babbage.EraIdBabbage, expectedSlot: 80},
		{name: "conway", eraId: conway.EraIdConway, expectedSlot: 80},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				config: LedgerStateConfig{
					CardanoNodeConfig: shelleyGenesisCfgForNonceWindow(t),
				},
			}
			got := ls.nonceStabilityWindow(tc.eraId)
			require.Equalf(
				t,
				tc.expectedSlot,
				got,
				"era %s (id=%d): expected stability window %d (issue #2125)",
				tc.name, tc.eraId, tc.expectedSlot,
			)
		})
	}
}
