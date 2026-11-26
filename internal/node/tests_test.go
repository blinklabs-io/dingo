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
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// namedMissingAllManager answers the full-manifest lister and records the log
// as BuildDeferredIndexes sees it, so the ordering assertion below reads what
// was logged before the build.
type namedMissingAllManager struct {
	missing []string
	logged  string
	log     *bytes.Buffer
}

func (m *namedMissingAllManager) DropDeferredIndexes() error { return nil }

func (m *namedMissingAllManager) BuildDeferredIndexes() error {
	m.logged = m.log.String()
	return nil
}

func (m *namedMissingAllManager) HasDeferredIndexesPending() (bool, error) {
	return false, nil
}

func (m *namedMissingAllManager) MissingDeferredIndexes() ([]string, error) {
	return m.missing, nil
}

// TestEnsureAllDeferredIndexesNamesMissingBeforeBuilding covers the repair a
// restored database takes: the full manifest is rebuilt, and that rebuild is
// silent while it runs. The names must reach the log before the build is
// entered.
func TestEnsureAllDeferredIndexesNamesMissingBeforeBuilding(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	manager := &namedMissingAllManager{
		missing: []string{"idx_asset_fingerprint", "idx_utxo_added_slot"},
		log:     &buf,
	}

	require.NoError(t, ensureAllDeferredIndexes(t.Context(), manager, logger))

	require.Contains(
		t,
		manager.logged,
		"rebuilding missing deferred metadata indexes",
		"the rebuild must be announced before it starts",
	)
	require.Contains(t, manager.logged, "idx_asset_fingerprint")
	require.Contains(t, manager.logged, "idx_utxo_added_slot")
	require.Contains(
		t,
		buf.String(),
		"deferred metadata index check complete",
	)
	require.Contains(t, buf.String(), "duration")
}

// TestEnsureAllDeferredIndexesQuietWhenManifestComplete keeps the healthy
// path silent, matching the critical-subset check.
func TestEnsureAllDeferredIndexesQuietWhenManifestComplete(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	manager := &namedMissingAllManager{log: &buf}

	require.NoError(t, ensureAllDeferredIndexes(t.Context(), manager, logger))

	require.Empty(t, buf.String())
}

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

// TestBuildDingoConfigWiresLocalStateQueryViewMaxLifetime follows the
// composition path for the LocalStateQuery snapshot lifetime: a missing
// With... call would drop the operator's value and leave the default.
func TestBuildDingoConfigWiresLocalStateQueryViewMaxLifetime(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{LocalStateQueryViewMaxLifetime: "7m"}
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

	require.Equal(
		t,
		7*time.Minute,
		built.LocalStateQueryViewMaxLifetimeDuration(),
	)
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

// TestBuildDingoConfigCarriesTokenRegistry exercises the composition the node
// actually uses. buildDingoConfig maps internal config onto dingo.With*
// options field by field, so a family with no With* call here is silently
// dropped no matter how well it parses -- an operator's tokenRegistry block
// would be read, validated, and then ignored.
//
// dingo.NewConfigFromInternal is a different path and cannot catch this.
func TestBuildDingoConfigCarriesTokenRegistry(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		TokenRegistry: config.TokenRegistryConfig{
			Enabled:               true,
			SourceURL:             "https://mirror.example.test/reg.tar.gz",
			Interval:              2 * time.Hour,
			RequestTimeout:        9 * time.Minute,
			UserAgent:             "custom-agent/9",
			MaxBytes:              123,
			MaxDecompressedBytes:  456,
			MaxEntryBytes:         45,
			MaxArchiveEntries:     67,
			MaxAcceptedEntries:    34,
			MaxBatchBytes:         89,
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
	require.Equal(t, int64(456), got.MaxDecompressedBytes)
	require.Equal(t, int64(45), got.MaxEntryBytes)
	require.Equal(t, 67, got.MaxArchiveEntries)
	require.Equal(t, 34, got.MaxAcceptedEntries)
	require.Equal(t, int64(89), got.MaxBatchBytes)
	require.True(t, got.StoreLogos)
	require.True(t, got.AllowPrivateAddresses)
}

func TestBuildDingoConfigCarriesConsensusProvider(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		Plugins: config.PluginsConfig{
			Consensus: plugin.Selection{Provider: "custom-consensus"},
		},
	}
	built := buildDingoConfig(
		cfg,
		nil,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		time.Minute,
		time.Minute,
		0,
	)

	require.Equal(t, "custom-consensus", built.ConsensusPlugin())
}

// TestBuildDingoConfigTokenRegistryDefaultsOff pins the deliberate default
// through the same production path.
func TestBuildDingoConfigTokenRegistryDefaultsOff(t *testing.T) {
	t.Parallel()

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

// buildDingoConfig maps fields one by one, so plannerStatsRefreshEnabled from
// YAML, the environment or the CLI reaches the node only through an explicit
// option; without it the node keeps NewConfig's default and the escape hatch
// does nothing.
func TestBuildDingoConfigWiresPlannerStatsRefresh(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	for _, enabled := range []bool{false, true} {
		built := buildDingoConfig(
			&config.Config{PlannerStatsRefreshEnabled: enabled},
			logger,
			nil,
			nil,
			false,
			dingo.StorageModeCore,
			30*time.Second,
			chainsync.DefaultStallTimeout,
			chainsync.HeaderSyncStrategyPrimary,
		)
		require.Equal(t, enabled, built.PlannerStatsRefreshEnabled())
	}
}

// TestBuildDingoConfigCarriesDefaultedMempoolCapacity follows the path the
// binary takes: the mempool plugin selection always carries a capacity by
// then, so only the recorded default tells a Leios capacity nobody set from
// one the operator pinned.
func TestBuildDingoConfigCarriesDefaultedMempoolCapacity(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	build := func(cfg *config.Config) bool {
		cfg.ApplyDefaults()
		built := buildDingoConfig(
			cfg,
			logger,
			nil,
			nil,
			false,
			dingo.StorageModeCore,
			time.Minute,
			time.Minute,
			0,
		)
		return built.MempoolCapacityLeiosDefault()
	}

	require.True(t, build(&config.Config{RunMode: config.RunModeLeios}))

	var pinned config.Config
	pinned.RunMode = config.RunModeLeios
	pinned.Plugins.Mempool.Config = map[string]any{
		"capacity": int64(config.DefaultMempoolCapacityLeios),
	}
	require.False(t, build(&pinned))

	require.False(t, build(&config.Config{}))
}
