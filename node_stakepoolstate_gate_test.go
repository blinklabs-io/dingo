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
	"github.com/stretchr/testify/assert"
)

// TestLedgerStateConfigStakePoolStateOnlyForMusashi pins the gate on the
// GetStakePoolState local state query. Released nodes do not support it, so a
// mainnet, preprod, preview or sancho node (or a devnet) must keep answering
// it as unsupported whatever its era or run mode.
func TestLedgerStateConfigStakePoolStateOnlyForMusashi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		runMode      internalconfig.RunMode
		startEra     internalconfig.StartEra
		want         bool
	}{
		{name: "musashi by name", network: "musashi", want: true},
		{name: "musashi by magic", networkMagic: 164, want: true},
		{
			name:         "musashi by name and magic",
			network:      "musashi",
			networkMagic: 164,
			want:         true,
		},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "sancho", network: "sancho", networkMagic: 4},
		{name: "devnet", network: "devnet", networkMagic: 42},
		// A devnet half-match is not an identity conflict, so either half of
		// the Musashi identity beside it still selects the prototype.
		{name: "musashi name with devnet magic", network: "musashi", networkMagic: 42, want: true},
		{name: "devnet name with musashi magic", network: "devnet", networkMagic: 164, want: true},
		{
			name:         "preview in the Dijkstra era",
			network:      "preview",
			networkMagic: 2,
			startEra:     internalconfig.StartEraDijkstra,
		},
		{
			name:         "preprod in leios run mode",
			network:      "preprod",
			networkMagic: 1,
			runMode:      internalconfig.RunModeLeios,
		},
		{
			name:         "preview with prototype magic",
			network:      "preview",
			networkMagic: 164,
		},
		{
			name:         "musashi name with mainnet magic",
			network:      "musashi",
			networkMagic: 764824073,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := &Node{config: Config{cfg: &internalconfig.Config{
				Network:      tt.network,
				NetworkMagic: tt.networkMagic,
				RunMode:      tt.runMode,
				StartEra:     tt.startEra,
			}}}
			assert.Equal(
				t,
				tt.want,
				n.ledgerStateConfig().StakePoolStateQuery,
			)
		})
	}
	assert.False(
		t,
		(&Node{config: Config{}}).ledgerStateConfig().StakePoolStateQuery,
		"a zero config leaves the query unsupported",
	)
}
