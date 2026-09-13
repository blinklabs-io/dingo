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

// TestForgeStalenessBoundsAreOperatorTunable covers the config plumbing for
// the three opt-in forge staleness bounds.
//
// Each hop is asserted separately, because a break in any one of them leaves
// the knob silently inert: the loaded config carries the value, the node
// Config snapshot copies it out of the loaded config, and the accessor
// reports it.
//
// IMPORTANT: this covers the NewConfigFromInternal path only. The binary does
// NOT take it -- internal/node.buildDingoConfig composes dingo.Config via
// dingo.NewConfig from an explicit With... list, and a field missing from that
// list is dropped no matter how green this test is. The runtime composition
// path is covered by TestBuildDingoConfigWiresForgeTolerances in
// internal/node; presence of a field at each layer is not wiring.
func TestForgeStalenessBoundsAreOperatorTunable(t *testing.T) {
	t.Run("explicit values survive every hop", func(t *testing.T) {
		loaded := &internalconfig.Config{
			ForgeUpstreamStalenessSlots:      41,
			ForgeAppliedTipStalenessSlots:    42,
			ForgeEndorserBlockStalenessSlots: 43,
		}
		c := &Config{cfg: loaded}
		// syncCompatFields is what the loaded-config constructor runs to
		// project the parsed config onto the fields the node reads.
		c.syncCompatFields()

		require.Equal(t, uint64(41), c.ForgeUpstreamStalenessSlots())
		require.Equal(t, uint64(41), c.forgeUpstreamStalenessSlots)
		require.Equal(t, uint64(42), c.ForgeAppliedTipStalenessSlots())
		require.Equal(t, uint64(42), c.forgeAppliedTipStalenessSlots)
		require.Equal(t, uint64(43), c.ForgeEndorserBlockStalenessSlots())
		require.Equal(
			t,
			uint64(43),
			c.forgeEndorserBlockStalenessSlots,
			"the node Config snapshot the forger reads must carry it",
		)
	})

	t.Run("option funcs set them", func(t *testing.T) {
		c := NewConfig(
			WithForgeUpstreamStalenessSlots(11),
			WithForgeAppliedTipStalenessSlots(12),
			WithForgeEndorserBlockStalenessSlots(13),
		)
		require.Equal(t, uint64(11), c.ForgeUpstreamStalenessSlots())
		require.Equal(t, uint64(12), c.ForgeAppliedTipStalenessSlots())
		require.Equal(t, uint64(13), c.ForgeEndorserBlockStalenessSlots())

		c.syncCompatFields()
		require.Equal(t, uint64(11), c.forgeUpstreamStalenessSlots)
		require.Equal(t, uint64(12), c.forgeAppliedTipStalenessSlots)
		require.Equal(t, uint64(13), c.forgeEndorserBlockStalenessSlots)
	})

	// All three are opt-in. ApplyDefaults must leave them at 0, because 0
	// means "disabled" for them rather than "unset": a default-on bound on any
	// of the three refuses leader slots during ordinary operation.
	t.Run("defaults leave every bound disabled", func(t *testing.T) {
		loaded := internalconfig.Config{}
		loaded.ApplyDefaults()

		require.Zero(t, loaded.ForgeUpstreamStalenessSlots)
		require.Zero(t, loaded.ForgeAppliedTipStalenessSlots)
		require.Zero(
			t,
			loaded.ForgeEndorserBlockStalenessSlots,
			"the endorser-block bound gates a network-stage watermark "+
				"against the local applied tip; defaulting it on would "+
				"withhold leader slots with every local indicator healthy",
		)
		require.Zero(
			t,
			uint64(internalconfig.DefaultForgeEndorserBlockStalenessSlots),
			"the documented default must not drift silently",
		)
	})

	t.Run("explicit values are not overwritten by defaults", func(t *testing.T) {
		loaded := internalconfig.Config{
			ForgeUpstreamStalenessSlots:      7,
			ForgeAppliedTipStalenessSlots:    8,
			ForgeEndorserBlockStalenessSlots: 9,
		}
		loaded.ApplyDefaults()

		require.Equal(t, uint64(7), loaded.ForgeUpstreamStalenessSlots)
		require.Equal(t, uint64(8), loaded.ForgeAppliedTipStalenessSlots)
		require.Equal(t, uint64(9), loaded.ForgeEndorserBlockStalenessSlots)
	})
}
