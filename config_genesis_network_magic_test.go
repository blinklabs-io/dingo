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
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestConfigValidateRejectsByronNetworkMagicMismatch is a regression test for
// issue #3528: a loaded genesis network magic must be cross-checked against
// the requested network. configValidate already cross-checks the Shelley
// genesis's NetworkMagic against the configured/requested network magic;
// this proves the same cross-check applies to the Byron genesis's own
// ProtocolConsts.ProtocolMagic field, which previously loaded and was used
// for Byron-era validation without ever being compared against the
// configured network.
func TestConfigValidateRejectsByronNetworkMagicMismatch(t *testing.T) {
	const shelleyMagic = 42

	shelleyGenesisJSON := `{
		"networkMagic": ` + strconv.Itoa(shelleyMagic) + `,
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	tests := []struct {
		name               string
		byronProtocolMagic int
		wantErr            string
	}{
		{
			name:               "mismatched Byron protocol magic is rejected",
			byronProtocolMagic: shelleyMagic + 1,
			wantErr:            "doesn't match value from Byron genesis",
		},
		{
			name:               "matching Byron protocol magic is accepted",
			byronProtocolMagic: shelleyMagic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byronGenesisJSON := `{
				"startTime": 1666656000,
				"protocolConsts": {"protocolMagic": ` + strconv.Itoa(
				tt.byronProtocolMagic,
			) + `}
			}`

			nodeCfg := &cardano.CardanoNodeConfig{}
			require.NoError(
				t,
				nodeCfg.LoadShelleyGenesisFromReader(
					strings.NewReader(shelleyGenesisJSON),
				),
			)
			require.NoError(
				t,
				nodeCfg.LoadByronGenesisFromReader(
					strings.NewReader(byronGenesisJSON),
				),
			)

			cfg := NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetworkMagic(shelleyMagic),
				WithCardanoNodeConfig(nodeCfg),
			)
			n, err := New(cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}
