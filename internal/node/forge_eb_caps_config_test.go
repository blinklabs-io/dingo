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

	cfg := &config.Config{
		ForgeEBMaxTxRefs: 1234,
		ForgeEBMaxBytes:  567890,
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

	if got := built.ForgeEBMaxTxRefs(); got != 1234 {
		t.Fatalf("expected forgeEBMaxTxRefs 1234 to flow through, got %d", got)
	}
	if got := built.ForgeEBMaxBytes(); got != 567890 {
		t.Fatalf("expected forgeEBMaxBytes 567890 to flow through, got %d", got)
	}
}
