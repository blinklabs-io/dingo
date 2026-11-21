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

package config

import (
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// Only a capacity that ApplyDefaults chose for Leios or Musashi is marked, so
// that an operator's explicit value stays fixed.
func TestApplyDefaultsMarksDefaultedLeiosMempoolCapacity(t *testing.T) {
	t.Parallel()
	explicit := func(capacity int64) PluginsConfig {
		var plugins PluginsConfig
		plugins.Mempool.Config = map[string]any{"capacity": capacity}
		return plugins
	}
	cases := []struct {
		name         string
		cfg          Config
		wantCapacity int64
		wantMarked   bool
	}{
		{
			"leios run mode",
			Config{RunMode: RunModeLeios},
			DefaultMempoolCapacityLeios, true,
		},
		{
			"musashi by name",
			Config{Network: ouroboros.NetworkCardanoMusashi.Name},
			DefaultMempoolCapacityLeios, true,
		},
		{
			"musashi by magic",
			Config{NetworkMagic: ouroboros.NetworkCardanoMusashi.NetworkMagic},
			DefaultMempoolCapacityLeios, true,
		},
		{
			"serve keeps the praos capacity",
			Config{RunMode: RunModeServe},
			DefaultMempoolCapacityPraos, false,
		},
		{
			"explicit capacity under leios",
			Config{RunMode: RunModeLeios, Plugins: explicit(5242880)},
			5242880, false,
		},
		{
			"explicit leios-sized capacity under leios",
			Config{
				RunMode: RunModeLeios,
				Plugins: explicit(DefaultMempoolCapacityLeios),
			},
			DefaultMempoolCapacityLeios, false,
		},
		{
			"explicit praos-sized capacity on musashi",
			Config{
				Network: ouroboros.NetworkCardanoMusashi.Name,
				Plugins: explicit(DefaultMempoolCapacityPraos),
			},
			DefaultMempoolCapacityPraos, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.cfg
			cfg.ApplyDefaults()
			capacity, _, _ := cfg.MempoolSettings()
			require.Equal(t, tc.wantCapacity, capacity)
			require.Equal(t, tc.wantMarked, cfg.MempoolCapacityDefaulted())
		})
	}
}
