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

package config

import (
	"log/slog"
	"net"
	"strconv"
)

// EffectiveConfigSourceFields are the dotted Config field paths the
// EffectiveLogAttrs allowlist reads. Every path must be classified logPlain
// in the redaction table (pinned by TestEffectiveConfigFieldsArePlain), so a
// secret-bearing field can never reach the startup effective-configuration
// line.
var EffectiveConfigSourceFields = []string{
	"BindAddr",
	"BlockPipelineEnabled",
	"BlockPipelineValidateEnabled",
	"DebugBindAddr",
	"DebugPort",
	"GenesisBootstrap.CorroborationPeers",
	"GenesisBootstrap.Enabled",
	"GenesisBootstrap.WindowSlots",
	"HealthPort",
	"HealthReadyGapSlots",
	"IntersectTip",
	"Logging.Format",
	"Logging.Level",
	"MetricsBindAddr",
	"MetricsPort",
	"PrivateBindAddr",
	"PrivatePort",
	"StorageMode",
	"StrictUtxoValidation",
	"ValidateHistorical",
}

// EffectiveHealthReadyGapSlots returns the readiness tip-gap threshold the
// health listener applies: HealthReadyGapSlots, with 0 mapped to
// DefaultHealthReadyGapSlots.
func (c *Config) EffectiveHealthReadyGapSlots() uint64 {
	if c.HealthReadyGapSlots == 0 {
		return DefaultHealthReadyGapSlots
	}
	return uint64(c.HealthReadyGapSlots)
}

// EffectiveLogAttrs returns the post-default, non-secret settings that
// distributed-test harnesses and operators compare against their
// expectations, as log attributes for one startup Info line. It is an
// allowlist over EffectiveConfigSourceFields only. A bind address is empty
// when its listener is disabled (port 0).
//
// genesisSelectionMode is the caller's resolved Genesis chain-selection
// mode, which also depends on the Cardano node configuration this package
// does not load.
func (c *Config) EffectiveLogAttrs(genesisSelectionMode bool) []slog.Attr {
	listenAddr := func(host string, port uint) string {
		if port == 0 {
			return ""
		}
		return net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
	}
	debugHost := c.DebugBindAddr
	if debugHost == "" {
		debugHost = DefaultDebugBindAddr
	}
	metricsHost := c.MetricsBindAddr
	if metricsHost == "" {
		metricsHost = DefaultMetricsBindAddr
	}
	storageMode := c.StorageMode
	if storageMode == "" {
		storageMode = storageModeCore
	}
	return []slog.Attr{
		slog.Bool("genesis_selection_mode", genesisSelectionMode),
		slog.Bool("genesis_bootstrap_enabled", c.GenesisBootstrap.Enabled),
		slog.Uint64("genesis_window_slots_override", c.GenesisBootstrap.WindowSlots),
		slog.Int("genesis_corroboration_peers", c.GenesisBootstrap.CorroborationPeers),
		slog.Bool("intersect_tip", c.IntersectTip),
		slog.Bool("validate_historical", c.ValidateHistorical),
		slog.Bool("strict_utxo_validation", c.StrictUtxoValidation),
		slog.Bool("block_pipeline_enabled", c.BlockPipelineEnabled),
		slog.Bool("block_pipeline_validate_enabled", c.BlockPipelineValidateEnabled),
		slog.String("storage_mode", storageMode),
		slog.Uint64("health_ready_gap_slots", c.EffectiveHealthReadyGapSlots()),
		slog.String("logging_level", c.Logging.Level),
		slog.String("logging_format", c.Logging.Format),
		slog.String("metrics_addr", listenAddr(metricsHost, c.MetricsPort)),
		slog.String("debug_addr", listenAddr(debugHost, c.DebugPort)),
		slog.String("health_addr", listenAddr(c.BindAddr, c.HealthPort)),
		slog.Bool("private_ntc_tcp_enabled", c.PrivatePort > 0),
		slog.String("private_addr", listenAddr(c.PrivateBindAddr, c.PrivatePort)),
	}
}
