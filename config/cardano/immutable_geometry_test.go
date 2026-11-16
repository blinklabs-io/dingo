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
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestImmutableChunkGeometry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		network string
		slots   uint64
		ebb     bool
	}{
		{"mainnet", 21600, true},
		{"preprod", 21600, true},
		{"preview", 4320, true},
	} {
		t.Run(tc.network, func(t *testing.T) {
			t.Parallel()
			cfg, err := LoadCardanoNodeConfigWithFallback(
				EmbeddedConfigPath(tc.network), tc.network, EmbeddedConfigFS,
			)
			require.NoError(t, err)
			slots, ebb, err := cfg.ImmutableChunkGeometry()
			require.NoError(t, err)
			require.Equal(t, tc.slots, slots)
			require.Equal(t, tc.ebb, ebb)
		})
	}
	_, _, err := (&CardanoNodeConfig{}).ImmutableChunkGeometry()
	require.Error(t, err, "a config with no genesis has no geometry")
	_, _, err = (*CardanoNodeConfig)(nil).ImmutableChunkGeometry()
	require.Error(t, err)
}

func TestImmutableChunkGeometryWithoutByronGenesis(t *testing.T) {
	t.Parallel()
	cfg := &CardanoNodeConfig{
		shelleyGenesis: &shelley.ShelleyGenesis{
			SecurityParam: 432, EpochLength: 86400,
		},
	}
	slots, ebb, err := cfg.ImmutableChunkGeometry()
	require.NoError(t, err)
	require.Equal(t, uint64(4320), slots, "10 * k, not the epoch length")
	require.True(t, ebb)
}
