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

package tlsutil

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestServerConfig verifies that server TLS configurations enforce TLS 1.2 as
// the minimum while preserving configurations that require TLS 1.3.
func TestServerConfig(t *testing.T) {
	// Cover newly allocated, zero-valued, insecure legacy, and stricter TLS
	// configurations so both the default and caller-supplied paths are tested.
	tests := []struct {
		name       string
		tlsConfig  *tls.Config
		minVersion uint16
		maxVersion uint16
	}{
		{
			name:       "nil config",
			minVersion: tls.VersionTLS12,
		},
		{
			name:       "default minimum",
			tlsConfig:  new(tls.Config),
			minVersion: tls.VersionTLS12,
		},
		{
			name: "lower minimum",
			tlsConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
			minVersion: tls.VersionTLS12,
		},
		{
			name: "higher minimum",
			tlsConfig: &tls.Config{
				MinVersion: tls.VersionTLS13,
			},
			minVersion: tls.VersionTLS13,
		},
		{
			// A caller-capped MaxVersion below the raised floor must not be
			// left in place, or the resulting config can never negotiate a
			// handshake (MinVersion > MaxVersion).
			name: "max version below raised floor",
			tlsConfig: &tls.Config{
				MaxVersion: tls.VersionTLS11,
			},
			minVersion: tls.VersionTLS12,
			maxVersion: tls.VersionTLS12,
		},
		{
			// A MaxVersion above the floor is left untouched.
			name: "max version above raised floor",
			tlsConfig: &tls.Config{
				MaxVersion: tls.VersionTLS13,
			},
			minVersion: tls.VersionTLS12,
			maxVersion: tls.VersionTLS13,
		},
		{
			// ECH is TLS 1.3-only; an unset minimum must be raised to 1.3,
			// not the general 1.2 floor, or Go rejects the config.
			name: "ECH keys with unset minimum",
			tlsConfig: &tls.Config{
				EncryptedClientHelloKeys: []tls.EncryptedClientHelloKey{{}},
			},
			minVersion: tls.VersionTLS13,
		},
	}

	// Apply the shared server policy and verify the resulting minimum version
	// is never lower than TLS 1.2 (or 1.3 for ECH) and that MaxVersion never
	// ends up below the raised MinVersion.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := ServerConfig(tc.tlsConfig)

			require.NotNil(t, config)
			require.Equal(t, tc.minVersion, config.MinVersion)
			require.Equal(t, tc.maxVersion, config.MaxVersion)
		})
	}
}
