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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestProviderDataDirPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		override bool
	}{
		{name: "database path shortcut"},
		{name: "provider override", override: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			injected := t.TempDir()
			want := injected
			config := map[string]any(nil)
			if tt.override {
				want = t.TempDir()
				config = map[string]any{"dataDir": want}
			}
			host := plugin.NewHost()
			require.NoError(t, RegisterProvider(host))
			t.Cleanup(func() {
				require.NoError(t, host.Stop(context.Background()))
			})

			store, err := plugin.Resolve[*BlobStoreBadger](
				context.Background(), host,
				plugin.CapabilityStorageBlob, "badger", config,
				blob.ProviderDependencies{DataDir: injected},
			)
			require.NoError(t, err)
			require.Equal(t, want, store.dataDir)
		})
	}
}

// resolveBadgerProvider registers and resolves the badger blob provider through
// a plugin host, returning the started store. Resolution runs the provider
// factory, so the returned store's fields reflect the storage-mode/run-mode
// derivation performed in RegisterProvider.
func resolveBadgerProvider(
	t *testing.T,
	config map[string]any,
	deps blob.ProviderDependencies,
) *BlobStoreBadger {
	t.Helper()
	if deps.DataDir == "" {
		deps.DataDir = t.TempDir()
	}
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	t.Cleanup(func() {
		require.NoError(t, host.Stop(context.Background()))
	})
	store, err := plugin.Resolve[*BlobStoreBadger](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", config, deps,
	)
	require.NoError(t, err)
	return store
}

// TestProviderStorageModeRunModeDefaults verifies the cache-size, compression,
// GC, compact-block-metadata, and compression-level values the badger provider
// derives from the injected storage mode and run mode when the provider config
// leaves them unset. This derivation lives in RegisterProvider and is otherwise
// unexercised by tests.
func TestProviderStorageModeRunModeDefaults(t *testing.T) {
	tests := []struct {
		name             string
		storageMode      string
		runMode          string
		wantBlockCache   uint64
		wantIndexCache   uint64
		wantCompression  bool
		wantGC           bool
		wantCompactBlock bool
	}{
		{
			name:             "core serve",
			storageMode:      "core",
			runMode:          "serve",
			wantBlockCache:   DefaultCoreBlockCacheSize,
			wantIndexCache:   DefaultCoreIndexCacheSize,
			wantCompression:  DefaultCoreCompressionEnabled,
			wantGC:           true,
			wantCompactBlock: true,
		},
		{
			name:             "api serve",
			storageMode:      "api",
			runMode:          "serve",
			wantBlockCache:   DefaultAPIBlockCacheSize,
			wantIndexCache:   DefaultAPIIndexCacheSize,
			wantCompression:  DefaultAPICompressionEnabled,
			wantGC:           true,
			wantCompactBlock: false,
		},
		{
			name:             "core load disables gc",
			storageMode:      "core",
			runMode:          "load",
			wantBlockCache:   DefaultCoreBlockCacheSize,
			wantIndexCache:   DefaultCoreIndexCacheSize,
			wantCompression:  DefaultCoreCompressionEnabled,
			wantGC:           false,
			wantCompactBlock: false,
		},
		{
			name:             "core leios keeps compact metadata",
			storageMode:      "core",
			runMode:          "leios",
			wantBlockCache:   DefaultCoreBlockCacheSize,
			wantIndexCache:   DefaultCoreIndexCacheSize,
			wantCompression:  DefaultCoreCompressionEnabled,
			wantGC:           true,
			wantCompactBlock: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := resolveBadgerProvider(t, nil, blob.ProviderDependencies{
				StorageMode: tt.storageMode,
				RunMode:     tt.runMode,
			})
			require.Equal(t, tt.wantBlockCache, store.blockCacheSize)
			require.Equal(t, tt.wantIndexCache, store.indexCacheSize)
			require.Equal(t, tt.wantCompression, store.compressionEnabled)
			require.Equal(t, tt.wantGC, store.gcEnabled)
			require.Equal(t, tt.wantCompactBlock, store.compactBlockMetadata)
			// Unset provider config uses the default compression level.
			require.Equal(t, DefaultCompressionLevel, store.compressionLevel)
		})
	}
}

// TestProviderExplicitConfigOverridesDefaults verifies that explicit provider
// config values win over the storage-mode/run-mode-derived defaults. API mode
// with the load run mode would otherwise force large caches, compression on,
// and GC off, so every explicit value below must override those defaults.
func TestProviderExplicitConfigOverridesDefaults(t *testing.T) {
	store := resolveBadgerProvider(t, map[string]any{
		"blockCacheSize":   uint64(4096),
		"indexCacheSize":   uint64(8192),
		"compression":      false,
		"gc":               true,
		"compressionLevel": 7,
	}, blob.ProviderDependencies{
		StorageMode: "api",
		RunMode:     "load",
	})
	require.Equal(t, uint64(4096), store.blockCacheSize)
	require.Equal(t, uint64(8192), store.indexCacheSize)
	require.False(t, store.compressionEnabled)
	require.True(t, store.gcEnabled)
	require.Equal(t, 7, store.compressionLevel)
}

func TestProviderStopDeadlineDuringValueLogGC(t *testing.T) {
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	store, err := plugin.Resolve[*BlobStoreBadger](
		context.Background(),
		host,
		plugin.CapabilityStorageBlob,
		"badger",
		map[string]any{"gc": true},
		blob.ProviderDependencies{DataDir: t.TempDir()},
	)
	require.NoError(t, err)

	gcStarted := make(chan struct{})
	releaseGC := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseGC) }) }
	defer func() {
		release()
		_ = store.Close()
	}()

	var attempts atomic.Int32
	store.runValueLogGC = func(float64) error {
		if attempts.Add(1) == 1 {
			close(gcStarted)
			<-releaseGC
			return nil
		}
		return badgerdb.ErrNoRewrite
	}
	gcTicks := make(chan time.Time, 1)
	store.gcWg.Add(1)
	go store.blobGc(gcTicks, store.gcStopCh)
	gcTicks <- time.Time{}
	testutil.RequireReceive(
		t, gcStarted, 5*time.Second,
		"value-log GC did not start",
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()
	stopDone := make(chan error, 1)
	stopStarted := time.Now()
	go func() {
		stopDone <- host.Stop(ctx)
	}()
	stopErr := testutil.RequireReceive(
		t, stopDone, 5*time.Second,
		"provider stop exceeded its context",
	)
	require.Less(
		t,
		time.Since(stopStarted),
		250*time.Millisecond,
		"provider stop did not honor its deadline promptly",
	)

	release()
	require.Eventually(
		t,
		store.DB().IsClosed,
		5*time.Second,
		10*time.Millisecond,
		"provider-owned close did not finish after value-log GC drained",
	)

	require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	require.Equal(t, int32(1), attempts.Load())
	require.True(t, store.DB().IsClosed())
	require.ErrorIs(
		t,
		host.Stop(context.Background()),
		context.DeadlineExceeded,
	)
}
