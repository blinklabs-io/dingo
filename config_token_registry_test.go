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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package dingo

import (
	"net/http"
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestTokenRegistryConfigReachesRuntimeFromYAML covers the path YAML, env, and
// CLI all land on: internal config only. The syncer reads the runtime mirror,
// so without syncCompatFields carrying it across, an operator's tokenRegistry
// block would parse and then be silently ignored.
func TestTokenRegistryConfigReachesRuntimeFromYAML(t *testing.T) {
	cfg, err := NewConfigFromInternal(
		&internalconfig.Config{
			TokenRegistry: internalconfig.TokenRegistryConfig{
				Enabled:               true,
				SourceURL:             "https://mirror.example.test/reg.tar.gz",
				Interval:              2 * time.Hour,
				RequestTimeout:        9 * time.Minute,
				UserAgent:             "custom-agent/9",
				MaxBytes:              123,
				MaxEntryBytes:         45,
				StoreLogos:            true,
				AllowPrivateAddresses: true,
			},
		},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)

	require.True(t, cfg.tokenRegistry.Enabled)
	require.Equal(
		t,
		"https://mirror.example.test/reg.tar.gz",
		cfg.tokenRegistry.SourceURL,
	)
	require.Equal(t, 2*time.Hour, cfg.tokenRegistry.Interval)
	require.Equal(t, 9*time.Minute, cfg.tokenRegistry.RequestTimeout)
	require.Equal(t, "custom-agent/9", cfg.tokenRegistry.UserAgent)
	require.Equal(t, int64(123), cfg.tokenRegistry.MaxBytes)
	require.Equal(t, int64(45), cfg.tokenRegistry.MaxEntryBytes)
	require.True(t, cfg.tokenRegistry.StoreLogos)
	require.True(t, cfg.tokenRegistry.AllowPrivateAddresses)
}

// TestTokenRegistryConfigDisabledByDefault pins the deliberate default: the
// mainnet registry is a roughly 240MB download, so an upgrade must not start
// one on its own.
func TestTokenRegistryConfigDisabledByDefault(t *testing.T) {
	cfg := NewConfig()

	require.False(t, cfg.tokenRegistry.Enabled)
	require.False(t, cfg.TokenRegistry().Enabled)
}

// TestWithTokenRegistryConfigPreservesHTTPClient guards the one field that
// cannot round-trip through internal config: the programmatic HTTP client is
// runtime-only, and syncCompatFields runs after options are applied.
func TestWithTokenRegistryConfigPreservesHTTPClient(t *testing.T) {
	client := &http.Client{}

	cfg := NewConfig(WithTokenRegistryConfig(TokenRegistryConfig{
		Enabled:    true,
		HTTPClient: client,
		UserAgent:  "programmatic/1",
	}))

	require.Same(t, client, cfg.tokenRegistry.HTTPClient)
	require.True(t, cfg.tokenRegistry.Enabled)
	require.Equal(t, "programmatic/1", cfg.tokenRegistry.UserAgent)
	require.Equal(t, "programmatic/1", cfg.TokenRegistry().UserAgent)
}
