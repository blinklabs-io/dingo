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
