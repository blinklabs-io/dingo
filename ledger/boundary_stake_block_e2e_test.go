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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// A block at the boundary slot itself follows the rollover that opens the
// epoch, so a boundary read past it counts the balances, registrations and
// delegations the rollover leaves and nothing the block did. The block at the
// slot after it, and Dijkstra's SNAP after the enactment, are the controls.
func TestBoundaryStakeIgnoresTheBlockAtTheBoundarySlot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		dijkstra bool
	}{
		{"conway", false},
		{"dijkstra", true},
	} {
		for _, offset := range []uint64{0, 1} {
			name := tc.name + " block at boundary"
			if offset == 1 {
				name = tc.name + " block after boundary"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h := newBoundaryStakeHarness(t, "set")
				f := h.scenario.BoundaryStake(6)
				if tc.dijkstra {
					h.scenario.DijkstraBoundary(f)
				}
				at := f.Boundary + offset
				// Pointed holds 13 and 6 credited at the boundary: it
				// withdraws all of it.
				h.scenario.WithdrawAtSlot(f.Pointed, at, 19)
				fresh := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x7c}, 28))
				cred := []any{uint64(fresh.Tag), fresh.Key}
				h.scenario.CertsAtSlot(
					at, 1, 0x7d, map[int]uint64{0: 2_000_000},
					[]any{uint64(0), cred}, []any{uint64(2), cred, f.Pool},
				)
				h.scenario.CertsAtSlot(
					at, 2, 0x7e, nil,
					[]any{uint64(1), []any{uint64(f.Dereg.Tag), f.Dereg.Key}},
				)
				h.scenario.AfterBoundaryStake(f)
				tip, _, err := h.set.Tip()
				require.NoError(t, err)
				require.Greater(t, tip.Slot, f.Boundary)
				got := h.scenario.ReadBoundaryStake(f)
				if tc.dijkstra {
					// Dijkstra reaps the pool retiring into the epoch before
					// SNAP, so the snapshot holds no stake for it.
					require.Empty(t, got.Active)
					require.Zero(t, got.Stakes[string(f.Pool)])
					require.Zero(t, got.Delegators[string(f.Pool)])
					return
				}
				requireFixtureFigures(t, f, got)
			})
		}
	}
}
