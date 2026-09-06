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

package dingo

import (
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestForgeHeaderFrontierToleranceSlotsIsOperatorTunable covers the config
// plumbing for the header-frontier forge gate. The bound decides whether a
// block producer forges or skips, so an operator whose ledger pipeline is
// legitimately slow on their deployment must be able to reach it without
// rebuilding; the gate's own default lives in ledger/forging and is only a
// fallback for a zero value.
//
// Each hop is asserted separately, because a break in any one of them leaves
// the knob silently inert: the loaded config carries the value, the node
// Config snapshot copies it out of the loaded config, and the accessor
// reports it.
//
// IMPORTANT: this covers the NewConfigFromInternal path only. The binary does
// NOT take it -- internal/node.buildDingoConfig composes dingo.Config via
// dingo.NewConfig from an explicit With... list, and a field missing from that
// list is dropped no matter how green this test is. That is exactly how
// ForgeHeaderFrontierToleranceSlots shipped inert while every layer here
// asserted green. The runtime composition path is covered by
// TestBuildDingoConfigWiresForgeTolerances in internal/node; presence of a
// field at each layer is not wiring.
func TestForgeHeaderFrontierToleranceSlotsIsOperatorTunable(t *testing.T) {
	t.Run("explicit value survives every hop", func(t *testing.T) {
		loaded := &internalconfig.Config{
			ForgeHeaderFrontierToleranceSlots: 42,
		}
		c := &Config{cfg: loaded}
		// syncCompatFields is what the loaded-config constructor runs to
		// project the parsed config onto the fields the node reads.
		c.syncCompatFields()
		require.Equal(
			t,
			uint64(42),
			c.ForgeHeaderFrontierToleranceSlots(),
			"loaded config value must reach the accessor",
		)
		require.Equal(
			t,
			uint64(42),
			c.forgeHeaderFrontierToleranceSlots,
			"the node Config snapshot the forger reads must carry it",
		)
	})

	t.Run("option func sets it", func(t *testing.T) {
		c := NewConfig(WithForgeHeaderFrontierToleranceSlots(17))
		require.Equal(t, uint64(17), c.ForgeHeaderFrontierToleranceSlots())
		c.syncCompatFields()
		require.Equal(t, uint64(17), c.forgeHeaderFrontierToleranceSlots)
	})

	t.Run("defaults fill an unset value", func(t *testing.T) {
		loaded := internalconfig.Config{}
		loaded.ApplyDefaults()
		require.Equal(
			t,
			uint64(internalconfig.DefaultForgeHeaderFrontierToleranceSlots),
			loaded.ForgeHeaderFrontierToleranceSlots,
		)
		require.Equal(
			t,
			uint64(5),
			loaded.ForgeHeaderFrontierToleranceSlots,
			"the documented default must not drift silently",
		)
	})

	t.Run("an explicit value is not overwritten by defaults", func(t *testing.T) {
		loaded := internalconfig.Config{
			ForgeHeaderFrontierToleranceSlots: 9,
		}
		loaded.ApplyDefaults()
		require.Equal(
			t,
			uint64(9),
			loaded.ForgeHeaderFrontierToleranceSlots,
		)
	})
}
