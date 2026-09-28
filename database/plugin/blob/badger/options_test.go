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

package badger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := prometheus.NewRegistry()
	store, err := New(
		WithDataDir(t.TempDir()),
		WithBlockCacheSize(123),
		WithIndexCacheSize(456),
		WithGc(false),
		WithCompressionEnabled(true),
		WithCompressionLevel(2),
		WithLogger(logger),
		WithPromRegistry(registry),
		WithDeferOpen(),
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(123), store.blockCacheSize)
	assert.Equal(t, uint64(456), store.indexCacheSize)
	assert.False(t, store.gcEnabled)
	assert.True(t, store.compressionEnabled)
	assert.Equal(t, 2, store.compressionLevel)
	assert.Same(t, logger, store.logger)
	assert.Same(t, registry, store.promRegistry)
}

func TestProviderDefaults(t *testing.T) {
	tests := []struct {
		name        string
		runMode     string
		storageMode string
		expected    bool
	}{
		{
			name:        "serve core",
			runMode:     "serve",
			storageMode: "core",
			expected:    true,
		},
		{
			name:        "leios core",
			runMode:     "leios",
			storageMode: "core",
			expected:    true,
		},
		{
			name:        "load core",
			runMode:     "load",
			storageMode: "core",
			expected:    false,
		},
		{
			name:        "serve api",
			runMode:     "serve",
			storageMode: "api",
			expected:    false,
		},
		{
			name:        "empty run mode",
			runMode:     "",
			storageMode: "core",
			expected:    false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(
				t,
				test.expected,
				useCompactBlockMetadata(test.runMode, test.storageMode),
			)
		})
	}
}

func TestStartWithoutRegistryLeavesMetricsDisabled(t *testing.T) {
	store, err := New(
		WithDataDir(t.TempDir()),
		WithGc(false),
		WithDeferOpen(),
	)
	require.NoError(t, err)
	assert.NoError(t, store.Start())
	t.Cleanup(func() { assert.NoError(t, store.Stop()) })
	assert.Nil(t, store.promRegistry)
}
