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

package cardano

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type adaptiveSettings struct {
	decrease, increase, minScale float64
	enabled                      bool
}

func TestLeiosAdaptiveEndorsementsSettings(t *testing.T) {
	t.Parallel()
	off := adaptiveSettings{}
	defaults := adaptiveSettings{0.8, 0.02, 0.1, true}
	cases := []struct {
		name string
		json string
		want adaptiveSettings
	}{
		{"absent is off", `{}`, off},
		{
			"disabled is off",
			`{"LeiosAdaptiveEndorsements": {"Enabled": false}}`,
			off,
		},
		{
			"a disabled section keeps no settings",
			`{"LeiosAdaptiveEndorsements": {"Enabled": false, "Decrease": 0.5}}`,
			off,
		},
		{
			"enabled takes the defaults",
			`{"LeiosAdaptiveEndorsements": {"Enabled": true}}`,
			defaults,
		},
		{
			"an empty section is on",
			`{"LeiosAdaptiveEndorsements": {}}`,
			defaults,
		},
		{
			"every key",
			`{"LeiosAdaptiveEndorsements": {"Enabled": true, "Decrease": 0.5, "Increase": 0.05, "MinScale": 0.25}}`,
			adaptiveSettings{0.5, 0.05, 0.25, true},
		},
		{
			"keys left out take their default",
			`{"LeiosAdaptiveEndorsements": {"Increase": 0.05}}`,
			adaptiveSettings{0.8, 0.05, 0.1, true},
		},
		{
			"a zero is kept, not defaulted",
			`{"LeiosAdaptiveEndorsements": {"MinScale": 0}}`,
			adaptiveSettings{0.8, 0.02, 0, true},
		},
		{
			"the keys of the previous release are ignored",
			`{"LeiosAdaptiveEndorsements": {"GainP": 3, "Attack": 0.2, "Release": 0.1, "AliveWindow": 5}}`,
			defaults,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := NewCardanoNodeConfigFromReader(
				strings.NewReader(tc.json),
			)
			require.NoError(t, err)
			var got adaptiveSettings
			got.decrease, got.increase, got.minScale, got.enabled = cfg.LeiosAdaptiveEndorsementsSettings()
			require.Equal(t, tc.want, got)
		})
	}
	var missing *CardanoNodeConfig
	_, _, _, enabled := missing.LeiosAdaptiveEndorsementsSettings()
	require.False(t, enabled)
}

func TestMusashiConfigEnablesLeiosAdaptiveEndorsements(t *testing.T) {
	t.Parallel()
	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		EmbeddedConfigPath("musashi"),
	)
	require.NoError(t, err)
	decrease, increase, minScale, enabled := cfg.LeiosAdaptiveEndorsementsSettings()
	require.True(t, enabled)
	require.Equal(t, DefaultLeiosAdaptiveDecrease, decrease)
	require.Equal(t, DefaultLeiosAdaptiveIncrease, increase)
	require.Equal(t, DefaultLeiosAdaptiveMinScale, minScale)
}
