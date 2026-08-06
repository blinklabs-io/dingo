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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMusashiNetworkIdentityConflict pins the rule that decides whether a
// configuration may enable the Musashi prototype's consensus/ledger trust
// bypasses. The prototype network is identified by name ("musashi") or by
// magic (164); a configuration that mixes one of those with a *different*
// predefined network is a conflict and must be rejected, because otherwise a
// node an operator believes is on preview/preprod runs with validation off.
func TestMusashiNetworkIdentityConflict(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		wantConflict string
	}{
		// Unambiguous prototype identities: allowed.
		{name: "name only", network: "musashi"},
		{name: "name and matching magic", network: "musashi", networkMagic: 164},
		{name: "magic only", networkMagic: 164},
		{
			name:         "custom name with prototype magic",
			network:      "musashi-mirror",
			networkMagic: 164,
		},

		// Unambiguous non-prototype identities: allowed (bypasses stay off).
		{name: "preview by name", network: "preview"},
		{name: "preview by name and magic", network: "preview", networkMagic: 2},
		{name: "preprod by name and magic", network: "preprod", networkMagic: 1},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "devnet", network: "devnet", networkMagic: 42},
		{name: "custom private net", network: "private-net", networkMagic: 9999},
		{name: "unset"},

		// Conflicts: a standard network wearing the prototype's magic.
		{
			name:         "preview name with prototype magic",
			network:      "preview",
			networkMagic: 164,
			wantConflict: "preview",
		},
		{
			name:         "preprod name with prototype magic",
			network:      "preprod",
			networkMagic: 164,
			wantConflict: "preprod",
		},
		{
			name:         "mainnet name with prototype magic",
			network:      "mainnet",
			networkMagic: 164,
			wantConflict: "mainnet",
		},

		// Conflicts: the prototype name wearing a standard network's magic.
		// This is the sharper direction — the handshake uses the magic, so
		// the node actually joins preview while trusting prototype rules.
		{
			name:         "prototype name with preview magic",
			network:      "musashi",
			networkMagic: 2,
			wantConflict: "preview",
		},
		{
			name:         "prototype name with preprod magic",
			network:      "musashi",
			networkMagic: 1,
			wantConflict: "preprod",
		},
		{
			name:         "prototype name with mainnet magic",
			network:      "musashi",
			networkMagic: 764824073,
			wantConflict: "mainnet",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := MusashiNetworkIdentityConflict(
				tt.network,
				tt.networkMagic,
			)
			if tt.wantConflict == "" {
				assert.False(t, ok, "unexpected conflict %q", got)
				return
			}
			require.True(t, ok, "expected a conflict")
			assert.Equal(t, tt.wantConflict, got)
		})
	}
}

// TestMusashiPrototypeNetwork pins which identities count as the prototype
// network at all. Preview and preprod must never qualify.
//
// Note the asymmetry between the two kinds of half-match, which is the whole
// point of the rule: a half-match is a misconfiguration only when the *other*
// half names a different predefined network. A custom name on magic 164, or
// the "musashi" name on an unregistered magic, is a private prototype
// deployment rather than a mistake, so it still counts as the prototype
// network — magic 164 *is* Musashi, whatever an operator calls it locally.
func TestMusashiPrototypeNetwork(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		want         bool
	}{
		{name: "name only", network: "musashi", want: true},
		{
			name:         "name and magic",
			network:      "musashi",
			networkMagic: 164,
			want:         true,
		},
		{name: "magic only", networkMagic: 164, want: true},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "devnet", network: "devnet", networkMagic: 42},
		{name: "unset"},
		// Half-matches against a *custom* identity are private prototype
		// deployments (e.g. a Musashi mirror), not misconfigurations, so they
		// remain the prototype network.
		{
			name:         "custom name with prototype magic",
			network:      "musashi-mirror",
			networkMagic: 164,
			want:         true,
		},
		{
			name:         "prototype name with unregistered magic",
			network:      "musashi",
			networkMagic: 9999,
			want:         true,
		},
		// A conflicting identity is not the prototype network: the bypasses
		// must stay off even if startup validation was never run.
		{name: "preview with prototype magic", network: "preview", networkMagic: 164},
		{name: "prototype name with preview magic", network: "musashi", networkMagic: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(
				t,
				tt.want,
				MusashiPrototypeNetwork(tt.network, tt.networkMagic),
			)
		})
	}
}

// TestValidateRejectsMusashiIdentityConflict proves the conflict is refused at
// startup configuration validation, not merely defused at the wiring site.
func TestValidateRejectsMusashiIdentityConflict(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		wantErr      string
	}{
		{
			name:         "preview with prototype magic",
			network:      "preview",
			networkMagic: 164,
			// The message names both configured fields, so the operator can
			// see which of the two they need to change.
			wantErr: `network identity conflict: network "preview" with ` +
				`networkMagic 164 identifies both the "preview" network and ` +
				`the Musashi prototype network`,
		},
		{
			name:         "preprod with prototype magic",
			network:      "preprod",
			networkMagic: 164,
			wantErr: `network identity conflict: network "preprod" with ` +
				`networkMagic 164 identifies both the "preprod" network and ` +
				`the Musashi prototype network`,
		},
		{
			// The reverse direction must not blame "preview", which the
			// operator never configured: it reports musashi/2 as supplied.
			name:         "prototype name with preview magic",
			network:      "musashi",
			networkMagic: 2,
			wantErr: `network identity conflict: network "musashi" with ` +
				`networkMagic 2 identifies both the "preview" network and ` +
				`the Musashi prototype network`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.Network = tt.network
			cfg.NetworkMagic = tt.networkMagic
			err := cfg.Validate(RunModeServe)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestValidateAllowsUnambiguousNetworks guards against the new rule rejecting
// legitimate configurations, including Musashi itself.
func TestValidateAllowsUnambiguousNetworks(t *testing.T) {
	for _, tt := range []struct {
		name         string
		network      string
		networkMagic uint32
	}{
		{name: "musashi by name", network: "musashi"},
		{name: "musashi by name and magic", network: "musashi", networkMagic: 164},
		{name: "musashi by magic only", network: "", networkMagic: 164},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "devnet", network: "devnet", networkMagic: 42},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.Network = tt.network
			cfg.NetworkMagic = tt.networkMagic
			require.NoError(t, cfg.Validate(RunModeServe))
		})
	}
}
