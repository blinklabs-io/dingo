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
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
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
// window is era-aware. The split is NOT TPraos-vs-Praos; Babbage uses the
// 3k/f window even though it runs Praos:
//
//   - Shelley, Allegra, Mary, Alonzo (TPraos): 3k/f
//   - Babbage (Praos, but 3k/f for backwards compatibility): 3k/f
//   - Conway, Dijkstra (Praos): 4k/f
//
// Each Praos era is enumerated explicitly in
// nonceStabilityWindowKMultiplier; the Dijkstra row here is the
// regression guard that the post-Conway Praos cohort still gets 4k/f
// after the Babbage→Conway split moved.
//
// Treating Babbage as 4k/f shifts its candidate-freeze cutoff and
// produces an eta0 for the Babbage→Conway transition that diverges from
// peers, so the first Conway block we forge VRF-fails on every relay
// and every Conway block our peers forge VRF-fails on us.
//
// Concretely with k=10, f=1/2: 3k/f = 60 slots and 4k/f = 80 slots.
// The Babbage row asserting 60 is the regression guard for the
// Babbage→Conway VRF wedge.
func TestNonceStabilityWindow_EraDispatch(t *testing.T) {
	cases := []struct {
		name         string
		eraId        uint
		expectedSlot uint64 // 3k/f=60 for TPraos+Babbage, 4k/f=80 for Conway+
	}{
		{name: "shelley", eraId: shelley.EraIdShelley, expectedSlot: 60},
		{name: "allegra", eraId: allegra.EraIdAllegra, expectedSlot: 60},
		{name: "mary", eraId: mary.EraIdMary, expectedSlot: 60},
		{name: "alonzo", eraId: alonzo.EraIdAlonzo, expectedSlot: 60},
		{name: "babbage", eraId: babbage.EraIdBabbage, expectedSlot: 60},
		{name: "conway", eraId: conway.EraIdConway, expectedSlot: 80},
		{name: "dijkstra", eraId: dijkstra.EraIdDijkstra, expectedSlot: 80},
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
