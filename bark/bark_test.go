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

package bark

import (
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfigureServerTLS verifies that server TLS configurations enforce TLS
// 1.2 as the minimum while preserving configurations that require TLS 1.3.
func TestConfigureServerTLS(t *testing.T) {
	tests := []struct {
		name       string
		tlsConfig  *tls.Config
		minVersion uint16
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := &http.Server{TLSConfig: tc.tlsConfig}

			configureServerTLS(server)

			require.NotNil(t, server.TLSConfig)
			require.Equal(t, tc.minVersion, server.TLSConfig.MinVersion)
		})
	}
}
