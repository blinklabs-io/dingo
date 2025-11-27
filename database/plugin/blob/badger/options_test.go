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

package badger_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/prometheus/client_golang/prometheus"
)

func TestWithDataDir(t *testing.T) {
	b := &badger.BlobStoreBadger{}
	option := badger.WithDataDir("/tmp/test")

	option(b)

	if b.DataDir != "/tmp/test" {
		t.Errorf("Expected dataDir to be '/tmp/test', got '%s'", b.DataDir)
	}
}

func TestWithBlockCacheSize(t *testing.T) {
	b := &badger.BlobStoreBadger{}
	option := badger.WithBlockCacheSize(123456789)

	option(b)

	if b.BlockCacheSize != 123456789 {
		t.Errorf(
			"Expected blockCacheSize to be 123456789, got %d",
			b.BlockCacheSize,
		)
	}
}

func TestWithIndexCacheSize(t *testing.T) {
	b := &badger.BlobStoreBadger{}
	option := badger.WithIndexCacheSize(987654321)

	option(b)

	if b.IndexCacheSize != 987654321 {
		t.Errorf(
			"Expected indexCacheSize to be 987654321, got %d",
			b.IndexCacheSize,
		)
	}
}

func TestWithLogger(t *testing.T) {
	b := &badger.BlobStoreBadger{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	option := badger.WithLogger(logger)

	option(b)

	if b.Logger != logger {
		t.Errorf("Expected logger to be set correctly")
	}
}

func TestWithPromRegistry(t *testing.T) {
	b := &badger.BlobStoreBadger{}
	registry := prometheus.NewRegistry()
	option := badger.WithPromRegistry(registry)

	option(b)

	if b.PromRegistry != registry {
		t.Errorf("Expected promRegistry to be set correctly")
	}
}

func TestWithGc(t *testing.T) {
	b := &badger.BlobStoreBadger{}
	option := badger.WithGc(true)

	option(b)

	if !b.GcEnabled {
		t.Errorf("Expected gcEnabled to be true, got %v", b.GcEnabled)
	}

	// Test disabling GC
	option2 := badger.WithGc(false)
	option2(b)

	if b.GcEnabled {
		t.Errorf("Expected gcEnabled to be false, got %v", b.GcEnabled)
	}
}

func TestOptionsCombination(t *testing.T) {
	b := &badger.BlobStoreBadger{}

	// Apply multiple options
	badger.WithDataDir("/tmp/combined")(b)
	badger.WithBlockCacheSize(1000000)(b)
	badger.WithIndexCacheSize(2000000)(b)

	if b.DataDir != "/tmp/combined" {
		t.Errorf("Expected dataDir to be '/tmp/combined', got '%s'", b.DataDir)
	}

	if b.BlockCacheSize != 1000000 {
		t.Errorf(
			"Expected blockCacheSize to be 1000000, got %d",
			b.BlockCacheSize,
		)
	}

	if b.IndexCacheSize != 2000000 {
		t.Errorf(
			"Expected indexCacheSize to be 2000000, got %d",
			b.IndexCacheSize,
		)
	}
}
