// Copyright 2025 Blink Labs Software
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

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/prometheus/client_golang/prometheus"
)

func TestWithDataDir(t *testing.T) {
	b := &BlobStoreBadger{}
	option := WithDataDir("/tmp/test")

	option(b)

	if b.dataDir != "/tmp/test" {
		t.Errorf("Expected dataDir to be '/tmp/test', got '%s'", b.dataDir)
	}
}

func TestWithBlockCacheSize(t *testing.T) {
	b := &BlobStoreBadger{}
	option := WithBlockCacheSize(123456789)

	option(b)

	if b.blockCacheSize != 123456789 {
		t.Errorf(
			"Expected blockCacheSize to be 123456789, got %d",
			b.blockCacheSize,
		)
	}
}

func TestWithIndexCacheSize(t *testing.T) {
	b := &BlobStoreBadger{}
	option := WithIndexCacheSize(987654321)

	option(b)

	if b.indexCacheSize != 987654321 {
		t.Errorf(
			"Expected indexCacheSize to be 987654321, got %d",
			b.indexCacheSize,
		)
	}
}

func TestWithLogger(t *testing.T) {
	b := &BlobStoreBadger{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	option := WithLogger(logger)

	option(b)

	if b.logger != logger {
		t.Errorf("Expected logger to be set correctly")
	}
}

func TestWithPromRegistry(t *testing.T) {
	b := &BlobStoreBadger{}
	registry := prometheus.NewRegistry()
	option := WithPromRegistry(registry)

	option(b)

	if b.promRegistry != registry {
		t.Errorf("Expected promRegistry to be set correctly")
	}
}

func TestWithGc(t *testing.T) {
	b := &BlobStoreBadger{}
	option := WithGc(true)

	option(b)

	if !b.gcEnabled {
		t.Errorf("Expected gcEnabled to be true, got %v", b.gcEnabled)
	}

	// Test disabling GC
	option2 := WithGc(false)
	option2(b)

	if b.gcEnabled {
		t.Errorf("Expected gcEnabled to be false, got %v", b.gcEnabled)
	}
}

func TestOptionsCombination(t *testing.T) {
	b := &BlobStoreBadger{}

	// Apply multiple options
	WithDataDir("/tmp/combined")(b)
	WithBlockCacheSize(1000000)(b)
	WithIndexCacheSize(2000000)(b)

	if b.dataDir != "/tmp/combined" {
		t.Errorf("Expected dataDir to be '/tmp/combined', got '%s'", b.dataDir)
	}

	if b.blockCacheSize != 1000000 {
		t.Errorf(
			"Expected blockCacheSize to be 1000000, got %d",
			b.blockCacheSize,
		)
	}

	if b.indexCacheSize != 2000000 {
		t.Errorf(
			"Expected indexCacheSize to be 2000000, got %d",
			b.indexCacheSize,
		)
	}
}

func TestApplyStorageModeDefaultsAPIWhenUnset(t *testing.T) {
	blockCache := uint64(DefaultCoreBlockCacheSize)
	indexCache := uint64(DefaultCoreIndexCacheSize)
	compressionEnabled := DefaultCoreCompressionEnabled

	applyStorageModeDefaults(
		"api",
		&blockCache,
		&indexCache,
		&compressionEnabled,
		false,
		false,
		false,
	)

	if blockCache != DefaultAPIBlockCacheSize {
		t.Fatalf("expected api block cache size %d, got %d", DefaultAPIBlockCacheSize, blockCache)
	}
	if indexCache != DefaultAPIIndexCacheSize {
		t.Fatalf("expected api index cache size %d, got %d", DefaultAPIIndexCacheSize, indexCache)
	}
	if compressionEnabled != DefaultAPICompressionEnabled {
		t.Fatalf(
			"expected api compression %v, got %v",
			DefaultAPICompressionEnabled,
			compressionEnabled,
		)
	}
}

func TestApplyStorageModeDefaultsCoreWhenUnset(t *testing.T) {
	blockCache := uint64(DefaultBlockCacheSize)
	indexCache := uint64(DefaultIndexCacheSize)
	compressionEnabled := true

	applyStorageModeDefaults(
		"core",
		&blockCache,
		&indexCache,
		&compressionEnabled,
		false,
		false,
		false,
	)

	if blockCache != DefaultCoreBlockCacheSize {
		t.Fatalf(
			"expected core block cache size %d, got %d",
			DefaultCoreBlockCacheSize,
			blockCache,
		)
	}
	if indexCache != DefaultCoreIndexCacheSize {
		t.Fatalf(
			"expected core index cache size %d, got %d",
			DefaultCoreIndexCacheSize,
			indexCache,
		)
	}
	if compressionEnabled != DefaultCoreCompressionEnabled {
		t.Fatalf(
			"expected core compression %v, got %v",
			DefaultCoreCompressionEnabled,
			compressionEnabled,
		)
	}
}

func TestUseCompactBlockMetadata(t *testing.T) {
	tests := []struct {
		name        string
		runMode     string
		storageMode string
		expected    bool
	}{
		{name: "serve core", runMode: "serve", storageMode: "core", expected: true},
		{name: "leios core", runMode: "leios", storageMode: "core", expected: true},
		{name: "load core", runMode: "load", storageMode: "core", expected: false},
		{name: "serve api", runMode: "serve", storageMode: "api", expected: false},
		{name: "empty run mode", runMode: "", storageMode: "core", expected: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := useCompactBlockMetadata(test.runMode, test.storageMode); got != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, got)
			}
		})
	}
}

func TestApplyStorageModeDefaultsPreservesExplicitOverrides(t *testing.T) {
	blockCache := uint64(DefaultBlockCacheSize)
	indexCache := uint64(DefaultIndexCacheSize)
	compressionEnabled := true
	cmdlineOptionsMutex.Lock()
	saved := cmdlineOptions
	cmdlineOptionsMutex.Unlock()
	t.Cleanup(func() {
		cmdlineOptionsMutex.Lock()
		cmdlineOptions = saved
		cmdlineOptionsMutex.Unlock()
	})

	applyStorageModeDefaults(
		"core",
		&blockCache,
		&indexCache,
		&compressionEnabled,
		true,
		true,
		true,
	)

	if blockCache != DefaultBlockCacheSize {
		t.Fatalf(
			"expected explicit block cache override %d, got %d",
			DefaultBlockCacheSize,
			blockCache,
		)
	}
	if indexCache != DefaultIndexCacheSize {
		t.Fatalf(
			"expected explicit index cache override %d, got %d",
			DefaultIndexCacheSize,
			indexCache,
		)
	}
	if !compressionEnabled {
		t.Fatal("expected explicit compression override to remain enabled")
	}
}

func TestNewFromCmdlineOptionsUsesAPIDefaultsWhenCompressionUnset(t *testing.T) {
	cmdlineOptionsMutex.Lock()
	saved := cmdlineOptions
	cmdlineOptionsMutex.Unlock()
	t.Cleanup(func() {
		cmdlineOptionsMutex.Lock()
		cmdlineOptions = saved
		cmdlineOptionsMutex.Unlock()
	})
	initCmdlineOptions()

	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		"badger",
		"storage-mode",
		"api",
	); err != nil {
		t.Fatalf("set storage-mode: %v", err)
	}

	p := NewFromCmdlineOptions()
	b, ok := p.(*BlobStoreBadger)
	if !ok {
		t.Fatal("expected *BlobStoreBadger from NewFromCmdlineOptions")
	}
	if b.blockCacheSize != DefaultAPIBlockCacheSize {
		t.Fatalf(
			"blockCacheSize: want api default %d, got %d",
			DefaultAPIBlockCacheSize,
			b.blockCacheSize,
		)
	}
	if b.indexCacheSize != DefaultAPIIndexCacheSize {
		t.Fatalf(
			"indexCacheSize: want api default %d, got %d",
			DefaultAPIIndexCacheSize,
			b.indexCacheSize,
		)
	}
	if b.compressionEnabled != DefaultAPICompressionEnabled {
		t.Fatalf(
			"compressionEnabled: want api default %v, got %v",
			DefaultAPICompressionEnabled,
			b.compressionEnabled,
		)
	}
}

func TestNewFromCmdlineOptionsPreservesExplicitCompressionDisableInAPIStorageMode(t *testing.T) {
	cmdlineOptionsMutex.Lock()
	saved := cmdlineOptions
	cmdlineOptionsMutex.Unlock()
	t.Cleanup(func() {
		cmdlineOptionsMutex.Lock()
		cmdlineOptions = saved
		cmdlineOptionsMutex.Unlock()
	})
	initCmdlineOptions()

	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		"badger",
		"storage-mode",
		"api",
	); err != nil {
		t.Fatalf("set storage-mode: %v", err)
	}
	if err := plugin.ProcessConfig(map[string]map[string]map[string]any{
		"blob": {
			"badger": {
				"compression": false,
			},
		},
	}); err != nil {
		t.Fatalf("process plugin config: %v", err)
	}

	p := NewFromCmdlineOptions()
	b, ok := p.(*BlobStoreBadger)
	if !ok {
		t.Fatal("expected *BlobStoreBadger from NewFromCmdlineOptions")
	}
	if b.blockCacheSize != DefaultAPIBlockCacheSize {
		t.Fatalf(
			"blockCacheSize: want api default %d, got %d",
			DefaultAPIBlockCacheSize,
			b.blockCacheSize,
		)
	}
	if b.compressionEnabled {
		t.Fatal("compressionEnabled: expected explicit false override")
	}
}

func TestNewFromCmdlineOptionsDoesNotMutateGlobals(t *testing.T) {
	cmdlineOptionsMutex.Lock()
	saved := cmdlineOptions
	cmdlineOptionsMutex.Unlock()
	t.Cleanup(func() {
		cmdlineOptionsMutex.Lock()
		cmdlineOptions = saved
		cmdlineOptionsMutex.Unlock()
	})
	initCmdlineOptions()
	cmdlineOptionsMutex.Lock()
	cmdlineOptions.storageMode = "core"
	cmdlineOptionsMutex.Unlock()

	p1 := NewFromCmdlineOptions()
	defer func() {
		if stopper, ok := p1.(interface{ Stop() error }); ok {
			_ = stopper.Stop()
		}
	}()

	// Capture global state after first call
	cmdlineOptionsMutex.Lock()
	blockAfterFirst := cmdlineOptions.blockCacheSize
	indexAfterFirst := cmdlineOptions.indexCacheSize
	compressionAfterFirst := cmdlineOptions.compressionEnabled
	cmdlineOptionsMutex.Unlock()

	if blockAfterFirst != DefaultCoreBlockCacheSize {
		t.Fatalf(
			"blockCacheSize mutated after first call: want %d, got %d",
			DefaultCoreBlockCacheSize,
			blockAfterFirst,
		)
	}
	if indexAfterFirst != DefaultCoreIndexCacheSize {
		t.Fatalf(
			"indexCacheSize mutated after first call: want %d, got %d",
			DefaultCoreIndexCacheSize,
			indexAfterFirst,
		)
	}
	if compressionAfterFirst != DefaultCoreCompressionEnabled {
		t.Fatalf(
			"compressionEnabled mutated after first call: want %v, got %v",
			DefaultCoreCompressionEnabled,
			compressionAfterFirst,
		)
	}

	b1, ok := p1.(*BlobStoreBadger)
	if !ok {
		t.Fatal("expected *BlobStoreBadger from first NewFromCmdlineOptions")
	}
	if b1.blockCacheSize != DefaultCoreBlockCacheSize {
		t.Fatalf(
			"first call blockCacheSize: want %d, got %d",
			DefaultCoreBlockCacheSize,
			b1.blockCacheSize,
		)
	}
	if b1.compressionEnabled != DefaultCoreCompressionEnabled {
		t.Fatalf(
			"first call compressionEnabled: want %v, got %v",
			DefaultCoreCompressionEnabled,
			b1.compressionEnabled,
		)
	}

	cmdlineOptionsMutex.Lock()
	cmdlineOptions.storageMode = "core"
	cmdlineOptions.blockCacheSize = DefaultBlockCacheSize
	cmdlineOptions.blockCacheSizeSet = true
	cmdlineOptions.compressionEnabled = true
	cmdlineOptions.compressionEnabledSet = true
	cmdlineOptionsMutex.Unlock()

	p2 := NewFromCmdlineOptions()
	defer func() {
		if stopper, ok := p2.(interface{ Stop() error }); ok {
			_ = stopper.Stop()
		}
	}()

	b2, ok := p2.(*BlobStoreBadger)
	if !ok {
		t.Fatal("expected *BlobStoreBadger from NewFromCmdlineOptions")
	}
	if b2.blockCacheSize != DefaultBlockCacheSize {
		t.Fatalf(
			"second call blockCacheSize: want explicit override %d, got %d",
			DefaultBlockCacheSize,
			b2.blockCacheSize,
		)
	}
	if !b2.compressionEnabled {
		t.Fatal("second call compressionEnabled: expected explicit true override")
	}
}
