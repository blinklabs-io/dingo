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

package node

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/config"
)

// TestBuildDingoConfigWiresForgeEBCaps follows the composition path the
// binary actually takes -- buildDingoConfig -> dingo.NewConfig -> the
// With... option -> the dingo.Config field the forger is built from. A yaml
// tag, a default and a getter prove nothing on their own: a missing With...
// call here silently drops the field and the cap never reaches the forge
// loop.
func TestBuildDingoConfigWiresForgeEBCaps(t *testing.T) {
	t.Parallel()

	refs, maxBytes := uint64(1234), uint64(567890)
	cfg := &config.Config{
		ForgeEBMaxTxRefs: &refs,
		ForgeEBMaxBytes:  &maxBytes,
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBMaxTxRefs(); got == nil || *got != 1234 {
		t.Fatalf("expected forgeEbMaxTxRefs 1234 to flow through, got %v", got)
	}
	if got := built.ForgeEBMaxBytes(); got == nil || *got != 567890 {
		t.Fatalf("expected forgeEbMaxBytes 567890 to flow through, got %v", got)
	}
}

// TestBuildDingoConfigPreservesExplicitZeroForgeEBCaps carries the
// zero-means-disabled contract through the composition path: an operator
// who wrote 0 must not have it replaced by the default on the way to the
// forger.
func TestBuildDingoConfigPreservesExplicitZeroForgeEBCaps(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	cfg := &config.Config{ForgeEBMaxTxRefs: &zero, ForgeEBMaxBytes: &zero}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBMaxTxRefs(); got == nil || *got != 0 {
		t.Fatalf("explicit zero forgeEbMaxTxRefs must survive, got %v", got)
	}
	if got := built.ForgeEBMaxBytes(); got == nil || *got != 0 {
		t.Fatalf("explicit zero forgeEbMaxBytes must survive, got %v", got)
	}
}

// TestBuildDingoConfigDefaultsUnsetForgeEBCaps covers a Config built
// directly rather than loaded: Load applies the defaults, so nil here
// means nobody did, and the cap must still be the backstop rather than
// zero (which would mean "disabled").
func TestBuildDingoConfigDefaultsUnsetForgeEBCaps(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	built := buildDingoConfig(
		&config.Config{},
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBMaxTxRefs(); got == nil ||
		*got != config.DefaultForgeEBMaxTxRefs {
		t.Fatalf("unset forgeEbMaxTxRefs must take the default, got %v", got)
	}
	if got := built.ForgeEBMaxBytes(); got == nil ||
		*got != config.DefaultForgeEBMaxBytes {
		t.Fatalf("unset forgeEbMaxBytes must take the default, got %v", got)
	}
}

// TestBuildDingoConfigWiresForgeEBSelectionReserve follows the same
// composition path for the selection reserve. Without the With... call
// here the field is dropped between the loaded configuration and the
// forger, so every deployment silently runs the built-in default however
// the operator set it.
func TestBuildDingoConfigWiresForgeEBSelectionReserve(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{ForgeEBSelectionReserve: 750 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBSelectionReserve(); got != 750*time.Millisecond {
		t.Fatalf("expected forgeEbSelectionReserve 750ms to flow through, got %s", got)
	}
}
