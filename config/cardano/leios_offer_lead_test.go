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

func TestLeiosMinOfferLeadSlots(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		json string
		want uint64
	}{
		{"unset uses the reference default", `{}`, 3600},
		{"configured", `{"LeiosMinOfferLead": 30}`, 30},
		{"explicit zero is kept", `{"LeiosMinOfferLead": 0}`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := NewCardanoNodeConfigFromReader(
				strings.NewReader(tc.json),
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.LeiosMinOfferLeadSlots())
		})
	}
	var missing *CardanoNodeConfig
	require.Equal(t, DefaultLeiosMinOfferLead, missing.LeiosMinOfferLeadSlots())
}

func TestMusashiConfigCarriesLeiosMinOfferLead(t *testing.T) {
	t.Parallel()
	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		EmbeddedConfigPath("musashi"),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1200), cfg.LeiosMinOfferLeadSlots())
}
