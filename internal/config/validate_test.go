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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validTestConfig returns a minimal configuration that passes
// validation, mirroring the production defaults.
func validTestConfig() *Config {
	return &Config{
		Network:              "preview",
		RunMode:              RunModeServe,
		StorageMode:          storageModeCore,
		EvictionWatermark:    DefaultEvictionWatermark,
		RejectionWatermark:   DefaultRejectionWatermark,
		MempoolCapacity:      DefaultMempoolCapacityPraos,
		RelayPort:            3001,
		PrivatePort:          3002,
		MetricsPort:          12798,
		UtxorpcPort:          9090,
		BlockfrostPort:       3000,
		MeshPort:             8080,
		ShutdownTimeout:      DefaultShutdownTimeout,
		LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
		Cache:                DefaultCacheConfig(),
		Chainsync:            DefaultChainsyncConfig(),
		Midnight:             DefaultMidnightConfig(),
		Mithril: MithrilConfig{
			Enabled: true,
			Backend: "v2",
		},
	}
}

func TestValidateDefaultsPass(t *testing.T) {
	cfg := validTestConfig()
	assert.NoError(t, cfg.validate(cfg.RunMode, minUnprivilegedPort))
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name:    "invalid run mode",
			modify:  func(c *Config) { c.RunMode = "batch" },
			wantErr: "invalid runMode",
		},
		{
			name:    "invalid start era",
			modify:  func(c *Config) { c.StartEra = "byron" },
			wantErr: "invalid startEra",
		},
		{
			name:    "invalid storage mode",
			modify:  func(c *Config) { c.StorageMode = "full" },
			wantErr: "invalid storageMode",
		},
		{
			name:    "load mode without immutable db path",
			modify:  func(c *Config) { c.RunMode = RunModeLoad },
			wantErr: "requires immutableDbPath",
		},
		{
			name: "load mode with immutable db path",
			modify: func(c *Config) {
				c.RunMode = RunModeLoad
				c.ImmutableDbPath = "/data/immutable"
			},
		},
		{
			name: "load mode allows unset listener ports",
			modify: func(c *Config) {
				c.RunMode = RunModeLoad
				c.ImmutableDbPath = "/data/immutable"
				c.RelayPort = 0
				c.PrivatePort = 0
				c.MetricsPort = 0
			},
		},
		{
			name:    "serve mode still requires listener ports",
			modify:  func(c *Config) { c.RelayPort = 0 },
			wantErr: "port (relay/NtN) must be set",
		},
		{
			name: "port above maximum",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.UtxorpcPort = 99999999
			},
			wantErr: "invalid utxorpcPort: 99999999 (must be at most 65535)",
		},
		{
			name: "privileged port without privileges",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.BlockfrostPort = 443
			},
			wantErr: "privileged port",
		},
		{
			name:    "required port set to zero",
			modify:  func(c *Config) { c.RelayPort = 0 },
			wantErr: "port (relay/NtN) must be set",
		},
		{
			name:    "metrics port set to zero",
			modify:  func(c *Config) { c.MetricsPort = 0 },
			wantErr: "metricsPort must be set",
		},
		{
			name: "optional port disabled with zero",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.UtxorpcPort = 0
			},
		},
		{
			name:    "duplicate port assignment",
			modify:  func(c *Config) { c.PrivatePort = c.RelayPort },
			wantErr: "is assigned to both",
		},
		{
			name: "duplicate zero ports do not collide",
			modify: func(c *Config) {
				c.DebugPort = 0
				c.BarkPort = 0
			},
		},
		{
			// UTxORPC/Blockfrost/Mesh/Midnight bind only under API storage
			// mode; in core mode their ports never bind, so even an
			// out-of-range or privileged value must not be rejected.
			name: "core mode skips inactive API port validation",
			modify: func(c *Config) {
				c.StorageMode = storageModeCore
				c.UtxorpcPort = 99999999
				c.BlockfrostPort = 443
			},
		},
		{
			// An API port colliding with an active serving port is not a
			// real clash in core mode because the API listener is inactive.
			name: "core mode ignores API/serving port collision",
			modify: func(c *Config) {
				c.StorageMode = storageModeCore
				c.MetricsPort = c.MeshPort
			},
		},
		{
			// Under API storage the same collision is real: both listeners
			// bind, so it must be reported.
			name: "api mode rejects API/serving port collision",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.MetricsPort = c.MeshPort
			},
			wantErr: "is assigned to both",
		},
		{
			// Listeners bound to distinct specific addresses can legally
			// share a port; only overlapping bind addresses collide.
			name: "distinct bind addresses may share a port",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.BindAddr = "127.0.0.1"
				c.PrivateBindAddr = "127.0.0.1"
				c.DebugPort = 13000
				c.Midnight.Host = "127.0.0.2"
				c.Midnight.Port = 13000
			},
		},
		{
			// A wildcard bind address contends with every specific one.
			name: "wildcard bind address collides with specific",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.BindAddr = "0.0.0.0"
				c.DebugPort = 13000
				c.Midnight.Host = "127.0.0.2"
				c.Midnight.Port = 13000
			},
			wantErr: "is assigned to both",
		},
		{
			name: "cardano config path traversal",
			modify: func(c *Config) {
				c.CardanoConfig = "configs/../../etc/passwd"
			},
			wantErr: "must not contain \"..\"",
		},
		{
			name:    "cardano config bare parent reference",
			modify:  func(c *Config) { c.CardanoConfig = ".." },
			wantErr: "must not contain \"..\"",
		},
		{
			// An inner ".." would clean away, but the contract is that
			// no ".." component appears at all: cleaning first would
			// let "configs/../secret.json" through.
			name: "cardano config inner dotdot rejected",
			modify: func(c *Config) {
				c.CardanoConfig = "/etc/dingo/../dingo/config.json"
			},
			wantErr: "must not contain \"..\"",
		},
		{
			name: "cardano config inner dotdot that cleans inside the tree",
			modify: func(c *Config) {
				c.CardanoConfig = "configs/../secret.json"
			},
			wantErr: "must not contain \"..\"",
		},
		{
			name:   "cardano config absolute path",
			modify: func(c *Config) { c.CardanoConfig = "/etc/dingo/config.json" },
		},
		{
			name:    "tls cert without key",
			modify:  func(c *Config) { c.TlsCertFilePath = "/certs/tls.crt" },
			wantErr: "must both be set",
		},
		{
			name:    "tls key without cert",
			modify:  func(c *Config) { c.TlsKeyFilePath = "/certs/tls.key" },
			wantErr: "must both be set",
		},
		{
			name: "tls cert and key together",
			modify: func(c *Config) {
				c.TlsCertFilePath = "/certs/tls.crt"
				c.TlsKeyFilePath = "/certs/tls.key"
			},
		},
		{
			name:    "negative mempool capacity",
			modify:  func(c *Config) { c.MempoolCapacity = -1 },
			wantErr: "invalid mempoolCapacity",
		},
		{
			name:    "eviction watermark out of range",
			modify:  func(c *Config) { c.EvictionWatermark = 1.5 },
			wantErr: "invalid evictionWatermark",
		},
		{
			name:    "rejection watermark out of range",
			modify:  func(c *Config) { c.RejectionWatermark = 1.5 },
			wantErr: "invalid rejectionWatermark",
		},
		{
			// Every ordered comparison with NaN is false, so a plain
			// out-of-range check would let NaN through (e.g. from
			// --eviction-watermark NaN, which strconv parses).
			name:    "NaN eviction watermark",
			modify:  func(c *Config) { c.EvictionWatermark = math.NaN() },
			wantErr: "invalid evictionWatermark",
		},
		{
			name:    "NaN rejection watermark",
			modify:  func(c *Config) { c.RejectionWatermark = math.NaN() },
			wantErr: "invalid rejectionWatermark",
		},
		{
			name: "eviction above rejection",
			modify: func(c *Config) {
				c.EvictionWatermark = 0.95
				c.RejectionWatermark = 0.90
			},
			wantErr: "must be less than rejectionWatermark",
		},
		{
			name:    "block producer missing key paths",
			modify:  func(c *Config) { c.BlockProducer = true },
			wantErr: "missing required key paths",
		},
		{
			name: "block producer with all key paths",
			modify: func(c *Config) {
				c.BlockProducer = true
				c.ShelleyVRFKey = "/keys/vrf.skey"
				c.ShelleyKESKey = "/keys/kes.skey"
				c.ShelleyOperationalCertificate = "/keys/node.cert"
			},
		},
		{
			name: "no network and no magic",
			modify: func(c *Config) {
				c.Network = ""
				c.NetworkMagic = 0
			},
			wantErr: "network or networkMagic must be set",
		},
		{
			name: "network magic without network name",
			modify: func(c *Config) {
				c.Network = ""
				c.NetworkMagic = 2
			},
		},
		{
			name:    "network name with traversal characters",
			modify:  func(c *Config) { c.Network = "../mainnet" },
			wantErr: "invalid network name",
		},
		{
			name:    "unparseable shutdown timeout",
			modify:  func(c *Config) { c.ShutdownTimeout = "thirty" },
			wantErr: "invalid shutdownTimeout",
		},
		{
			name:    "negative shutdown timeout",
			modify:  func(c *Config) { c.ShutdownTimeout = "-5s" },
			wantErr: "invalid shutdownTimeout \"-5s\": must be positive",
		},
		{
			name:    "unparseable ledger catchup timeout",
			modify:  func(c *Config) { c.LedgerCatchupTimeout = "1 hour" },
			wantErr: "invalid ledgerCatchupTimeout",
		},
		{
			name:    "unparseable chainsync stall timeout",
			modify:  func(c *Config) { c.Chainsync.StallTimeout = "soon" },
			wantErr: "invalid chainsync.stallTimeout",
		},
		{
			name: "negative mithril idle timeout allowed",
			modify: func(c *Config) {
				c.Mithril.DownloadIdleTimeout = "-1s"
			},
		},
		{
			name: "unparseable mithril idle timeout",
			modify: func(c *Config) {
				c.Mithril.DownloadIdleTimeout = "later"
			},
			wantErr: "invalid mithril.downloadIdleTimeout",
		},
		{
			name:    "invalid chainsync strategy",
			modify:  func(c *Config) { c.Chainsync.Strategy = "fastest" },
			wantErr: "invalid chainsync.strategy",
		},
		{
			name:   "chainsync strategy round_robin alias",
			modify: func(c *Config) { c.Chainsync.Strategy = "round_robin" },
		},
		{
			name:    "negative chainsync max clients",
			modify:  func(c *Config) { c.Chainsync.MaxClients = -1 },
			wantErr: "invalid chainsync.maxClients",
		},
		{
			name:    "invalid mithril backend",
			modify:  func(c *Config) { c.Mithril.Backend = "v3" },
			wantErr: "invalid mithril.backend",
		},
		{
			name:   "empty mithril backend",
			modify: func(c *Config) { c.Mithril.Backend = "" },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.modify(cfg)
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestValidatePrivilegedPortAllowedWhenBindable covers a process that
// may bind any port (root, Windows, or CAP_NET_BIND_SERVICE):
// minBindable is 0, so a sub-1024 port passes.
func TestValidatePrivilegedPortAllowedWhenBindable(t *testing.T) {
	cfg := validTestConfig()
	cfg.StorageMode = storageModeAPI
	cfg.BlockfrostPort = 443
	assert.NoError(t, cfg.validate(cfg.RunMode, 0))
}

// TestValidateLoweredPrivilegedPortCutoff covers a Linux deployment
// with net.ipv4.ip_unprivileged_port_start lowered to a nonzero value:
// ports at or above the cutoff must pass while ports below it are
// still rejected.
func TestValidateLoweredPrivilegedPortCutoff(t *testing.T) {
	cfg := validTestConfig()
	cfg.StorageMode = storageModeAPI
	cfg.BlockfrostPort = 80
	assert.NoError(t, cfg.validate(cfg.RunMode, 80))
	cfg.BlockfrostPort = 79
	err := cfg.validate(cfg.RunMode, 80)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "privileged port")
}

// TestValidateUtilityModesRelaxListenerAndSource verifies that the
// one-shot sync and mithril invocations neither require the serving
// listener ports nor an ImmutableDB source, even though the configured
// runMode is the default serve. Their metrics/debug listeners accept an
// unset port, which the runtime binds ephemerally.
func TestValidateUtilityModesRelaxListenerAndSource(t *testing.T) {
	for _, mode := range []RunMode{RunModeSync, RunModeMithril} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := validTestConfig()
			cfg.RelayPort = 0
			cfg.PrivatePort = 0
			cfg.MetricsPort = 0
			cfg.DebugPort = 0
			cfg.ImmutableDbPath = ""
			assert.NoError(t, cfg.validate(mode, minUnprivilegedPort))
		})
	}
}

// TestValidateSyncModeIgnoresInactiveListenerCollision reproduces
// `dingo --metrics-port 8080 sync`: the metrics port matches the default
// mesh port, but sync starts no Mesh listener, so there is no real
// collision and validation must pass.
func TestValidateSyncModeIgnoresInactiveListenerCollision(t *testing.T) {
	cfg := validTestConfig()
	cfg.MetricsPort = cfg.MeshPort // 8080, the default mesh port
	cfg.RelayPort = 0
	cfg.PrivatePort = 0
	cfg.ImmutableDbPath = ""
	assert.NoError(t, cfg.validate(RunModeSync, minUnprivilegedPort))
}

// TestValidateSyncModeValidatesMetricsPort verifies that the Mithril sync
// operation still validates the metrics port it starts, even though it
// skips the serving and API listener ports.
func TestValidateSyncModeValidatesMetricsPort(t *testing.T) {
	cfg := validTestConfig()
	cfg.MetricsPort = 99999999
	err := cfg.validate(RunModeSync, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid metricsPort")
}

// TestValidateMithrilReadOnlyModeSkipsAuxPorts is a regression test for
// the read-only Mithril subcommands (`mithril list`, `mithril show`),
// which query the aggregator and start no listeners: a bad metrics or
// debug port must not block them.
func TestValidateMithrilReadOnlyModeSkipsAuxPorts(t *testing.T) {
	cfg := validTestConfig()
	cfg.MetricsPort = 99999999
	cfg.DebugPort = 99999999
	assert.NoError(t, cfg.validate(RunModeMithril, minUnprivilegedPort))
}

// TestValidateDevConfigViaServeChecksApiPorts covers `dingo serve` with
// a configured runMode of "dev": the effective mode is serve, but
// node.Run keys dev behavior off the configured mode and forces API
// storage, so the API listener ports bind and must still be validated.
func TestValidateDevConfigViaServeChecksApiPorts(t *testing.T) {
	cfg := validTestConfig()
	cfg.RunMode = RunModeDev
	cfg.StorageMode = storageModeCore
	cfg.UtxorpcPort = 99999999
	err := cfg.validate(RunModeServe, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid utxorpcPort")
}

// TestValidateLoadModeSkipsAllListenerPorts verifies that load, which
// starts no listeners, does not reject otherwise out-of-range listener
// ports: they never bind during an import.
func TestValidateLoadModeSkipsAllListenerPorts(t *testing.T) {
	cfg := validTestConfig()
	cfg.RunMode = RunModeLoad
	cfg.ImmutableDbPath = "/data/immutable"
	cfg.MetricsPort = 99999999
	cfg.UtxorpcPort = 99999999
	assert.NoError(t, cfg.validate(RunModeLoad, minUnprivilegedPort))
}

// TestValidateInvalidModeStillReportsListeners verifies that when the
// bare root falls back to an effective serve mode for an invalid
// configured runMode, the listener-port violations are reported
// alongside the invalid-runMode error rather than being suppressed.
func TestValidateInvalidModeStillReportsListeners(t *testing.T) {
	cfg := validTestConfig()
	cfg.RunMode = "batch"
	cfg.RelayPort = 0
	// cmd/dingo passes RunModeServe as the effective mode for an invalid
	// configured runMode at the bare root.
	err := cfg.validate(RunModeServe, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid runMode")
	assert.Contains(t, err.Error(), "port (relay/NtN) must be set")
}

func TestValidateAggregatesAllErrors(t *testing.T) {
	cfg := validTestConfig()
	cfg.RunMode = RunModeLoad
	cfg.ImmutableDbPath = ""
	cfg.EvictionWatermark = 2.0
	cfg.Chainsync.Strategy = "fastest"
	err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires immutableDbPath")
	assert.Contains(t, err.Error(), "invalid evictionWatermark")
	assert.Contains(t, err.Error(), "invalid chainsync.strategy")
}
