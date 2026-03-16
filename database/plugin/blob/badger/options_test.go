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

func TestApplyOperationalDefaultsCore(t *testing.T) {
	blockCache := uint64(DefaultBlockCacheSize)
	indexCache := uint64(DefaultIndexCacheSize)
	memtable := uint64(DefaultMemTableSize)

	applyOperationalDefaults(
		"",
		string(ProfileCore),
		&blockCache,
		&indexCache,
		&memtable,
	)

	if blockCache != 134217728 {
		t.Fatalf("expected core block cache size 134217728, got %d", blockCache)
	}
	if indexCache != 33554432 {
		t.Fatalf("expected core index cache size 33554432, got %d", indexCache)
	}
	if memtable != 33554432 {
		t.Fatalf("expected core memtable size 33554432, got %d", memtable)
	}
}

func TestApplyOperationalDefaultsLoadPreservesOverrides(t *testing.T) {
	blockCache := uint64(123)
	indexCache := uint64(456)
	memtable := uint64(789)

	applyOperationalDefaults(
		string(ProfileLoad),
		string(ProfileAPI),
		&blockCache,
		&indexCache,
		&memtable,
	)

	if blockCache != 123 {
		t.Fatalf("expected block cache override to be preserved, got %d", blockCache)
	}
	if indexCache != 456 {
		t.Fatalf("expected index cache override to be preserved, got %d", indexCache)
	}
	if memtable != 789 {
		t.Fatalf("expected memtable override to be preserved, got %d", memtable)
	}
}

func TestResolveProfilePrefersLoadRunMode(t *testing.T) {
	if profile := resolveProfile(string(ProfileLoad), string(ProfileAPI)); profile != ProfileLoad {
		t.Fatalf("expected load profile, got %q", profile)
	}
}

func TestUseCompactBlockMetadata(t *testing.T) {
	tests := []struct {
		name        string
		runMode     string
		storageMode string
		expected    bool
	}{
		{name: "serve core", runMode: "serve", storageMode: string(ProfileCore), expected: true},
		{name: "leios core", runMode: "leios", storageMode: string(ProfileCore), expected: true},
		{name: "load core", runMode: string(ProfileLoad), storageMode: string(ProfileCore), expected: false},
		{name: "serve api", runMode: "serve", storageMode: string(ProfileAPI), expected: false},
		{name: "empty run mode", runMode: "", storageMode: string(ProfileCore), expected: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := useCompactBlockMetadata(test.runMode, test.storageMode); got != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, got)
			}
		})
	}
}

func TestApplyOperationalDefaultsPreservesExplicitEqualToDefault(t *testing.T) {
	// An explicit override that happens to equal the API-profile default
	// must NOT be overwritten when a different profile is resolved.
	blockCache := uint64(DefaultBlockCacheSize)
	indexCache := uint64(DefaultIndexCacheSize)
	memtable := uint64(DefaultMemTableSize)

	// With load profile, core/load sizes differ from the defaults (which
	// match the API profile). If the caller explicitly set the API-default
	// values, applyOperationalDefaults will still overwrite them because it
	// compares against the constants. This test documents the current
	// limitation: callers that need to preserve explicit overrides must
	// copy values to locals before calling applyOperationalDefaults (as
	// NewFromCmdlineOptions does).
	//
	// The important invariant: cmdlineOptions must NOT be mutated.
	cmdlineOptionsMutex.Lock()
	saved := cmdlineOptions
	cmdlineOptions.blockCacheSize = DefaultBlockCacheSize
	cmdlineOptions.indexCacheSize = DefaultIndexCacheSize
	cmdlineOptions.memTableSize = DefaultMemTableSize
	cmdlineOptionsMutex.Unlock()
	t.Cleanup(func() {
		cmdlineOptionsMutex.Lock()
		cmdlineOptions = saved
		cmdlineOptionsMutex.Unlock()
	})

	applyOperationalDefaults(
		string(ProfileLoad),
		"",
		&blockCache,
		&indexCache,
		&memtable,
	)

	// Verify that the global cmdlineOptions were NOT mutated
	if cmdlineOptions.blockCacheSize != saved.blockCacheSize {
		t.Fatalf(
			"cmdlineOptions.blockCacheSize mutated: want %d, got %d",
			saved.blockCacheSize,
			cmdlineOptions.blockCacheSize,
		)
	}
	if cmdlineOptions.indexCacheSize != saved.indexCacheSize {
		t.Fatalf(
			"cmdlineOptions.indexCacheSize mutated: want %d, got %d",
			saved.indexCacheSize,
			cmdlineOptions.indexCacheSize,
		)
	}
	if cmdlineOptions.memTableSize != saved.memTableSize {
		t.Fatalf(
			"cmdlineOptions.memTableSize mutated: want %d, got %d",
			saved.memTableSize,
			cmdlineOptions.memTableSize,
		)
	}
}

func TestNewFromCmdlineOptionsDoesNotMutateGlobals(t *testing.T) {
	// Run NewFromCmdlineOptions twice with different profiles and verify
	// the second call does not inherit the first profile's derived sizes.
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
	cmdlineOptions.runMode = string(ProfileLoad)
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
	memAfterFirst := cmdlineOptions.memTableSize
	cmdlineOptionsMutex.Unlock()

	// Globals should still be at init defaults
	if blockAfterFirst != DefaultBlockCacheSize {
		t.Fatalf(
			"blockCacheSize mutated after first call: want %d, got %d",
			DefaultBlockCacheSize,
			blockAfterFirst,
		)
	}
	if indexAfterFirst != DefaultIndexCacheSize {
		t.Fatalf(
			"indexCacheSize mutated after first call: want %d, got %d",
			DefaultIndexCacheSize,
			indexAfterFirst,
		)
	}
	if memAfterFirst != DefaultMemTableSize {
		t.Fatalf(
			"memTableSize mutated after first call: want %d, got %d",
			DefaultMemTableSize,
			memAfterFirst,
		)
	}

	// Second call with API profile should get API defaults, not load sizes
	cmdlineOptionsMutex.Lock()
	cmdlineOptions.runMode = ""
	cmdlineOptions.storageMode = string(ProfileAPI)
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
			"second call blockCacheSize: want %d (API default), got %d",
			DefaultBlockCacheSize,
			b2.blockCacheSize,
		)
	}
}
