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
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
)

// TestSkipDijkstraTxValidationScope documents the accepted non-validating
// behaviour of the Musashi prototype profile and, more importantly, its
// boundary.
//
// The prototype's trust argument — endorser transactions are stored but never
// applied, so ranking-block transactions spending endorser-resident outputs are
// unresolvable and disagree on essentially every transaction — only exists in
// Dijkstra/Leios. Every earlier era therefore keeps its full per-transaction
// rule set even on a Musashi node. If this test starts failing because a
// pre-Dijkstra era began skipping validation, the prototype bypass has widened
// beyond its justification.
func TestSkipDijkstraTxValidationScope(t *testing.T) {
	preDijkstraEras := []struct {
		name string
		id   uint
	}{
		{"byron", byron.EraIdByron},
		{"shelley", shelley.EraIdShelley},
		{"allegra", allegra.EraIdAllegra},
		{"mary", mary.EraIdMary},
		{"alonzo", alonzo.EraIdAlonzo},
		{"babbage", babbage.EraIdBabbage},
		{"conway", conway.EraIdConway},
	}

	t.Run("prototype profile skips only Dijkstra", func(t *testing.T) {
		ls := &LedgerState{
			config: LedgerStateConfig{SkipDijkstraTxValidation: true},
		}
		assert.True(
			t,
			ls.skipDijkstraTxValidation(dijkstra.EraIdDijkstra),
			"Dijkstra transactions are the accepted non-validating scope",
		)
		for _, era := range preDijkstraEras {
			assert.False(
				t,
				ls.skipDijkstraTxValidation(era.id),
				"%s transactions must still be validated on the prototype profile",
				era.name,
			)
		}
	})

	t.Run("standard profile validates every era", func(t *testing.T) {
		ls := &LedgerState{
			config: LedgerStateConfig{SkipDijkstraTxValidation: false},
		}
		assert.False(
			t,
			ls.skipDijkstraTxValidation(dijkstra.EraIdDijkstra),
			"Dijkstra validation must be enforced off the prototype profile",
		)
		for _, era := range preDijkstraEras {
			assert.False(t, ls.skipDijkstraTxValidation(era.id), era.name)
		}
	})
}

// TestDijkstraTxValidationErrorsArePrototypeOnly pins the trust boundary: the
// Musashi prototype may skip and trust Dijkstra transaction validation, while
// a standard Leios profile must reject the same failure.
func TestDijkstraTxValidationErrorsArePrototypeOnly(t *testing.T) {
	for _, profile := range []struct {
		name      string
		skip      bool
		wantTrust bool
	}{
		{"musashi prototype profile", true, true},
		{"standard profile", false, false},
	} {
		t.Run(profile.name, func(t *testing.T) {
			ls := &LedgerState{
				config: LedgerStateConfig{
					SkipDijkstraTxValidation: profile.skip,
				},
			}
			assert.Equal(
				t,
				profile.wantTrust,
				ls.trustDijkstraTxValidationError(dijkstra.EraIdDijkstra),
				"Dijkstra validation trust must match the prototype bypass",
			)
			assert.False(
				t,
				ls.trustDijkstraTxValidationError(conway.EraIdConway),
				"Conway validation failures must still reject the block",
			)
			assert.False(
				t,
				ls.trustDijkstraTxValidationError(babbage.EraIdBabbage),
				"Babbage validation failures must still reject the block",
			)
		})
	}
}

func TestDijkstraEraGateUsesCurrentEra(t *testing.T) {
	assert.True(t, dijkstraEraGate(eras.DijkstraEraDesc))
	assert.False(t, dijkstraEraGate(eras.ConwayEraDesc))
}
