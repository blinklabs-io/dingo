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

package node

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestBuildDingoConfigCarriesTokenRegistry exercises the composition the node
// actually uses. buildDingoConfig maps internal config onto dingo.With*
// options field by field, so a family with no With* call here is silently
// dropped no matter how well it parses -- an operator's tokenRegistry block
// would be read, validated, and then ignored.
//
// dingo.NewConfigFromInternal is a different path and cannot catch this.
func TestBuildDingoConfigCarriesTokenRegistry(t *testing.T) {
	cfg := &config.Config{
		TokenRegistry: config.TokenRegistryConfig{
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
	}

	built := buildDingoConfig(
		cfg,
		nil,
		nil,
		nil,
		false,
		dingo.StorageModeAPI,
		time.Minute,
		time.Minute,
		0,
	)

	got := built.TokenRegistry()
	require.True(t, got.Enabled, "tokenRegistry.enabled must reach the node")
	require.Equal(t, "https://mirror.example.test/reg.tar.gz", got.SourceURL)
	require.Equal(t, 2*time.Hour, got.Interval)
	require.Equal(t, 9*time.Minute, got.RequestTimeout)
	require.Equal(t, "custom-agent/9", got.UserAgent)
	require.Equal(t, int64(123), got.MaxBytes)
	require.Equal(t, int64(45), got.MaxEntryBytes)
	require.True(t, got.StoreLogos)
	require.True(t, got.AllowPrivateAddresses)
}

// TestBuildDingoConfigTokenRegistryDefaultsOff pins the deliberate default
// through the same production path.
func TestBuildDingoConfigTokenRegistryDefaultsOff(t *testing.T) {
	built := buildDingoConfig(
		&config.Config{},
		nil,
		nil,
		nil,
		false,
		dingo.StorageModeAPI,
		time.Minute,
		time.Minute,
		0,
	)

	require.False(t, built.TokenRegistry().Enabled)
}
