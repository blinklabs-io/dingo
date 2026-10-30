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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/require"
)

func TestGenesisControlGroupYAMLOwnership(t *testing.T) {
	// Not t.Parallel: configuration loading replaces the process-global snapshot.
	for _, tc := range []struct {
		name, body string
		nilGroups  bool
		count      int
	}{
		{"omitted", "{}", true, 0},
		{"empty", "genesisBootstrap:\n  controlGroups: []\n", false, 0},
		{"configured", "genesisBootstrap:\n  controlGroups:\n    - identity: custom\n      accessPoints:\n        - address: relay.example\n          port: 3001\n", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalConfig()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0600))
			cfg, err := LoadConfig(path)
			require.NoError(t, err)
			require.Equal(
				t,
				tc.nilGroups,
				cfg.GenesisBootstrap.ControlGroups == nil,
			)
			require.Len(t, cfg.GenesisBootstrap.ControlGroups, tc.count)
			copy := cloneConfig(cfg)
			if tc.count > 0 {
				require.NoError(
					t,
					copy.GenesisBootstrap.ControlGroups[0].Validate(),
				)
				copy.GenesisBootstrap.ControlGroups[0].AccessPoints[0].Address = "changed"
				require.Equal(
					t,
					"relay.example",
					cfg.GenesisBootstrap.ControlGroups[0].AccessPoints[0].Address,
				)
			}
		})
	}
}

func TestValidateGenesisControlGroups(t *testing.T) {
	t.Parallel()
	cfg := newDefaultConfig()
	cfg.GenesisBootstrap.ControlGroups = []topology.PeerControlGroup{
		{
			Identity: "operator",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: "relay.example", Port: 0},
			},
		},
	}
	require.ErrorContains(
		t,
		cfg.Validate(RunModeServe),
		"genesisBootstrap.controlGroups[0]",
	)
}
