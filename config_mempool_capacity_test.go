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
	"slices"
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// Only the Leios capacity that a default chose is marked as following the
// committee's quorum; every explicit capacity stays fixed.
func TestNewConfigMarksDefaultedLeiosMempoolCapacity(t *testing.T) {
	t.Parallel()
	musashi := ouroboros.NetworkCardanoMusashi.Name
	praos := int64(internalconfig.DefaultMempoolCapacityPraos)
	leiosMode := WithRunMode(string(internalconfig.RunModeLeios))
	cases := []struct {
		name string
		opts []ConfigOptionFunc
		want bool
	}{
		{"leios run mode", []ConfigOptionFunc{leiosMode}, true},
		{"musashi", []ConfigOptionFunc{WithNetwork(musashi)}, true},
		{"serve", nil, false},
		{
			"explicit capacity under leios",
			[]ConfigOptionFunc{leiosMode, WithMempoolCapacity(5242880)},
			false,
		},
		{
			"explicit capacity set before the run mode",
			[]ConfigOptionFunc{WithMempoolCapacity(5242880), leiosMode},
			false,
		},
		{
			"explicit praos-sized capacity on musashi",
			[]ConfigOptionFunc{WithNetwork(musashi), WithMempoolCapacity(praos)},
			false,
		},
		{
			"plugin selection with a capacity",
			[]ConfigOptionFunc{
				WithNetwork(musashi),
				WithPluginSelection(
					hostplugin.CapabilityMempool,
					hostplugin.Selection{
						Provider: "fifo",
						Config:   map[string]any{"capacity": praos},
					},
				),
			},
			false,
		},
		{
			"plugin selection without a capacity",
			[]ConfigOptionFunc{
				WithNetwork(musashi),
				WithPluginSelection(
					hostplugin.CapabilityMempool,
					hostplugin.Selection{Provider: "fifo"},
				),
			},
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := NewConfig(tc.opts...)
			require.Equal(t, tc.want, cfg.mempoolCapacityLeiosDefault)
		})
	}
}

func TestNewConfigFromInternalCarriesDefaultedMempoolCapacity(t *testing.T) {
	t.Parallel()
	defaulted := internalconfig.Config{RunMode: internalconfig.RunModeLeios}
	fixed := internalconfig.Config{RunMode: internalconfig.RunModeLeios}
	fixed.Plugins.Mempool.Config = map[string]any{
		"capacity": int64(internalconfig.DefaultMempoolCapacityLeios),
	}
	for _, tc := range []struct {
		name string
		cfg  *internalconfig.Config
		want bool
	}{
		{"defaulted by the run mode", &defaulted, true},
		{"configured, even at the leios size", &fixed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.cfg.ApplyDefaults()
			cfg, err := NewConfigFromInternal(tc.cfg, nil, nil, nil, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.mempoolCapacityLeiosDefault)
		})
	}
}

// A built-in mempool selection that carries options but no capacity takes the
// run-mode default, as a selection without any config does, and the caller's
// map is left as it was.
func TestWithPluginSelectionDefaultsAnAbsentMempoolCapacity(t *testing.T) {
	t.Parallel()
	musashi := ouroboros.NetworkCardanoMusashi.Name
	for _, tc := range []struct {
		name      string
		opts      []ConfigOptionFunc
		want      int64
		defaulted bool
	}{
		{"serve", nil, int64(internalconfig.DefaultMempoolCapacityPraos), false},
		{
			"musashi",
			[]ConfigOptionFunc{WithNetwork(musashi)},
			int64(internalconfig.DefaultMempoolCapacityLeios),
			true,
		},
	} {
		for _, provider := range []string{"default", "fifo", "dag"} {
			t.Run(tc.name+"/"+provider, func(t *testing.T) {
				t.Parallel()
				options := map[string]any{"evictionWatermark": 0.5}
				opts := append(
					slices.Clone(tc.opts),
					WithPluginSelection(
						hostplugin.CapabilityMempool,
						hostplugin.Selection{Provider: provider, Config: options},
					),
				)
				cfg := NewConfig(opts...)
				selection := cfg.pluginSelections[hostplugin.CapabilityMempool]
				require.Equal(t, tc.want, pluginInt64(selection.Config["capacity"]))
				require.InDelta(t, 0.5, selection.Config["evictionWatermark"], 0)
				require.Equal(t, tc.defaulted, cfg.mempoolCapacityLeiosDefault)
				require.Equal(
					t, map[string]any{"evictionWatermark": 0.5}, options,
					"the caller's map is unchanged",
				)
			})
		}
	}
}
