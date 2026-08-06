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
	"reflect"
	"strings"
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrototypeTrustBypassesRejectedOnStandardNetworks proves a node cannot be
// constructed with a configuration that would hand the Musashi prototype's
// consensus/ledger trust bypasses to preview, preprod, or mainnet.
func TestPrototypeTrustBypassesRejectedOnStandardNetworks(t *testing.T) {
	tests := []struct {
		name    string
		opts    []ConfigOptionFunc
		wantErr string
	}{
		{
			name: "preview cannot borrow the prototype magic",
			opts: []ConfigOptionFunc{
				WithNetwork("preview"),
				WithNetworkMagic(164),
			},
			wantErr: `network identity conflict: network "preview" with networkMagic 164`,
		},
		{
			name: "preprod cannot borrow the prototype magic",
			opts: []ConfigOptionFunc{
				WithNetwork("preprod"),
				WithNetworkMagic(164),
			},
			wantErr: `network identity conflict: network "preprod" with networkMagic 164`,
		},
		{
			name: "mainnet cannot borrow the prototype magic",
			opts: []ConfigOptionFunc{
				WithNetwork("mainnet"),
				WithNetworkMagic(164),
			},
			wantErr: `network identity conflict: network "mainnet" with networkMagic 164`,
		},
		{
			// The handshake uses the magic, so this configuration actually
			// joins preview while claiming the prototype's trust rules.
			name: "prototype name cannot borrow preview's magic",
			opts: []ConfigOptionFunc{
				WithNetwork("musashi"),
				WithNetworkMagic(2),
			},
			wantErr: `network identity conflict: network "musashi" with networkMagic 2`,
		},
		{
			name: "prototype name cannot borrow preprod's magic",
			opts: []ConfigOptionFunc{
				WithNetwork("musashi"),
				WithNetworkMagic(1),
			},
			wantErr: `network identity conflict: network "musashi" with networkMagic 1`,
		},
		{
			name: "musashi by name is still accepted",
			opts: []ConfigOptionFunc{WithNetwork("musashi")},
		},
		{
			name: "musashi by name and matching magic is still accepted",
			opts: []ConfigOptionFunc{
				WithNetwork("musashi"),
				WithNetworkMagic(164),
			},
		},
		{
			name: "preview is still accepted",
			opts: []ConfigOptionFunc{WithNetwork("preview")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []ConfigOptionFunc{
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
			}
			opts = append(opts, tt.opts...)
			n, err := New(NewConfig(opts...))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

// TestPrototypeTrustBypassesEnabledOnlyForMusashi asserts the predicate that
// gates SkipLeaderStakeThresholdCheck and SkipDijkstraTxValidation. This is
// the last line of defence: an embedder that constructs a Config directly and
// never runs startup validation still must not get the bypasses on a standard
// network.
func TestPrototypeTrustBypassesEnabledOnlyForMusashi(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		want         bool
	}{
		{name: "musashi by name", network: "musashi", want: true},
		{
			name:         "musashi by name and magic",
			network:      "musashi",
			networkMagic: 164,
			want:         true,
		},
		{name: "musashi by magic only", networkMagic: 164, want: true},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "devnet", network: "devnet", networkMagic: 42},
		// Conflicting identities never enable the bypasses, even unvalidated.
		{name: "preview with prototype magic", network: "preview", networkMagic: 164},
		{name: "preprod with prototype magic", network: "preprod", networkMagic: 164},
		{name: "musashi name with preview magic", network: "musashi", networkMagic: 2},
		{name: "musashi name with preprod magic", network: "musashi", networkMagic: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{cfg: &internalconfig.Config{
				Network:      tt.network,
				NetworkMagic: tt.networkMagic,
			}}
			assert.Equal(
				t,
				tt.want,
				c.prototypeTrustBypassesEnabled(),
				"prototypeTrustBypassesEnabled",
			)
		})
	}
}

// TestPrototypeTrustBypassesOffWithoutConfig guards the nil-config path used by
// zero-value Config values in tests and embedders.
func TestPrototypeTrustBypassesOffWithoutConfig(t *testing.T) {
	c := &Config{}
	assert.False(t, c.prototypeTrustBypassesEnabled())
}

// TestMusashiProfileTrustBypassScope is the change-bar guard for the Musashi
// prototype profile: it documents exactly which LedgerStateConfig settings
// relax validation, and which of those the network profile is allowed to
// switch on by itself.
//
// The accepted non-validating behaviour on Musashi is limited to two settings:
//
//   - SkipLeaderStakeThresholdCheck downgrades a failed stake-derived leader
//     eligibility check to a warning. Every cryptographic header check (KES,
//     VRF proof, registered-VRF-key binding, opcert) still applies.
//   - SkipDijkstraTxValidation skips the per-transaction rule set for
//     Dijkstra-era transactions only; earlier eras are still validated (see
//     ledger.TestSkipDijkstraTxValidationScope).
//
// TrustedReplay is listed as known but is *not* network-derived: it is set by
// internal/node/load.go when replaying blocks this node already validated
// locally, which is a different trust context from following an untrusted
// network.
//
// A new Skip*/Trust*/Unsafe* field on LedgerStateConfig fails this test on
// purpose. Adding one is a deliberate widening of where dingo stops validating,
// and it should be classified here — prototype-only or not — rather than
// picking up a network default silently.
func TestMusashiProfileTrustBypassScope(t *testing.T) {
	// Settings that relax validation, and whether the Musashi network profile
	// is permitted to enable them on its own.
	knownTrustSettings := map[string]bool{
		"SkipLeaderStakeThresholdCheck": true,
		"SkipDijkstraTxValidation":      true,
		"TrustedReplay":                 false,
	}

	cfgType := reflect.TypeFor[ledger.LedgerStateConfig]()
	found := make(map[string]bool)
	for field := range cfgType.Fields() {
		name := field.Name
		if strings.HasPrefix(name, "Skip") ||
			strings.HasPrefix(name, "Trust") ||
			strings.HasPrefix(name, "Unsafe") {
			found[name] = true
		}
	}
	for name := range found {
		assert.Contains(
			t,
			knownTrustSettings,
			name,
			"new validation-relaxing setting %q must be classified as "+
				"prototype-only or not; see this test's doc comment",
			name,
		)
	}
	for name := range knownTrustSettings {
		assert.Contains(
			t,
			found,
			name,
			"%q no longer exists; drop it from the known set",
			name,
		)
	}
}
