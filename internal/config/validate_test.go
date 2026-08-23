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
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validTestConfig returns a minimal configuration that passes
// validation, mirroring the production defaults.
func validTestConfig() *Config {
	cfg := &Config{
		Plugins:              defaultPluginsConfig(),
		Network:              "preview",
		RunMode:              RunModeServe,
		StorageMode:          storageModeCore,
		RelayPort:            3001,
		PrivatePort:          3002,
		MetricsPort:          12798,
		ShutdownTimeout:      DefaultShutdownTimeout,
		LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
		Cache:                DefaultCacheConfig(),
		Chainsync:            DefaultChainsyncConfig(),
		HistoryExpiry:        DefaultHistoryExpiryConfig(),
		Midnight:             DefaultMidnightConfig(),
		Mithril: MithrilConfig{
			Enabled: true,
			Backend: "v2",
		},
	}
	cfg.Plugins.Mempool.Config["capacity"] = int64(DefaultMempoolCapacityPraos)
	return cfg
}

func setPluginPort(selection *hostplugin.Selection, port any) {
	selection.Config["port"] = port
}

func setMempoolSetting(c *Config, name string, value any) {
	c.Plugins.Mempool.Config[name] = value
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
			name: "negative history expiry frequency",
			modify: func(c *Config) {
				c.HistoryExpiry.Frequency = -time.Second
			},
			wantErr: "invalid historyExpiry.frequency",
		},
		{
			name: "zero history expiry frequency",
			modify: func(c *Config) {
				c.HistoryExpiry.Frequency = 0
			},
			wantErr: "invalid historyExpiry.frequency",
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
				setPluginPort(&c.Plugins.API.Utxorpc, 99999999)
			},
			wantErr: "invalid plugins.api.utxorpc.config.port: 99999999 (must be at most 65535)",
		},
		{
			name: "privileged port without privileges",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				setPluginPort(&c.Plugins.API.Blockfrost, 443)
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
				setPluginPort(&c.Plugins.API.Utxorpc, 0)
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
				setPluginPort(&c.Plugins.API.Utxorpc, 99999999)
				setPluginPort(&c.Plugins.API.Blockfrost, 443)
			},
		},
		{
			// An API port colliding with an active serving port is not a
			// real clash in core mode because the API listener is inactive.
			name: "core mode ignores API/serving port collision",
			modify: func(c *Config) {
				c.StorageMode = storageModeCore
				c.MetricsPort = APIPluginPort(c.Plugins.API.Mesh)
			},
		},
		{
			// Under API storage the same collision is real: both listeners
			// bind, so it must be reported.
			name: "api mode rejects API/serving port collision",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.MetricsPort = APIPluginPort(c.Plugins.API.Mesh)
			},
			wantErr: "is assigned to both",
		},
		{
			// Listeners bound to distinct specific addresses can legally
			// share a port; only overlapping bind addresses collide.
			name: "distinct bind addresses may share a port",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.BindAddr = "127.0.0.1"
				c.PrivateBindAddr = "127.0.0.1"
				c.DebugPort = 13000
				c.Midnight.Host = "127.0.0.2"
				c.Midnight.Port = 13000
			},
		},
		{
			name: "bark on distinct bind address may share a port",
			modify: func(c *Config) {
				c.BindAddr = "127.0.0.1"
				c.BarkHost = "127.0.0.2"
				c.BarkPort = c.MetricsPort
			},
		},
		{
			// A wildcard bind address contends with every specific one.
			name: "wildcard bind address collides with specific",
			modify: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
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
			modify:  func(c *Config) { setMempoolSetting(c, "capacity", -1) },
			wantErr: "invalid plugins.mempool.config.capacity",
		},
		{
			name:    "eviction watermark out of range",
			modify:  func(c *Config) { setMempoolSetting(c, "evictionWatermark", 1.5) },
			wantErr: "invalid plugins.mempool.config.evictionWatermark",
		},
		{
			name:    "rejection watermark out of range",
			modify:  func(c *Config) { setMempoolSetting(c, "rejectionWatermark", 1.5) },
			wantErr: "invalid plugins.mempool.config.rejectionWatermark",
		},
		{
			name: "non-positive mempool revalidation delta cap",
			modify: func(c *Config) {
				setMempoolSetting(c, "revalidationDeltaCap", 0)
			},
			wantErr: "invalid plugins.mempool.config.revalidationDeltaCap",
		},
		{
			// Every ordered comparison with NaN is false, so a plain
			// out-of-range check would let NaN through (e.g. from
			// --eviction-watermark NaN, which strconv parses).
			name:    "NaN eviction watermark",
			modify:  func(c *Config) { setMempoolSetting(c, "evictionWatermark", math.NaN()) },
			wantErr: "invalid plugins.mempool.config.evictionWatermark",
		},
		{
			name:    "NaN rejection watermark",
			modify:  func(c *Config) { setMempoolSetting(c, "rejectionWatermark", math.NaN()) },
			wantErr: "invalid plugins.mempool.config.rejectionWatermark",
		},
		{
			name: "eviction above rejection",
			modify: func(c *Config) {
				setMempoolSetting(c, "evictionWatermark", 0.95)
				setMempoolSetting(c, "rejectionWatermark", 0.90)
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
			name: "full pot rewards reject standard network by name",
			modify: func(c *Config) {
				c.FullPotRewardsEnabled = true
			},
			wantErr: "fullPotRewardsEnabled is not permitted on standard network \"preview\"",
		},
		{
			name: "full pot rewards reject standard network by magic",
			modify: func(c *Config) {
				c.Network = "private-preview-mirror"
				c.NetworkMagic = 2
				c.FullPotRewardsEnabled = true
			},
			wantErr: "fullPotRewardsEnabled is not permitted on standard network \"preview\"",
		},
		{
			name: "full pot rewards allow standard network with unsafe opt-in",
			modify: func(c *Config) {
				c.FullPotRewardsEnabled = true
				c.UnsafeFullPotRewardsOnStandardNetworks = true
			},
		},
		{
			name: "full pot rewards allow custom network",
			modify: func(c *Config) {
				c.Network = "private-net"
				c.NetworkMagic = 9_999
				c.FullPotRewardsEnabled = true
			},
		},
		{
			name: "full pot rewards allow devnet",
			modify: func(c *Config) {
				c.Network = "devnet"
				c.NetworkMagic = 42
				c.FullPotRewardsEnabled = true
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
			name: "negative genesis corroboration peers",
			modify: func(c *Config) {
				c.GenesisBootstrap.CorroborationPeers = -1
			},
			wantErr: "invalid genesisBootstrap.corroborationPeers",
		},
		{
			name: "zero genesis corroboration peers allowed",
			modify: func(c *Config) {
				c.GenesisBootstrap.CorroborationPeers = 0
			},
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

func TestValidatePledgeLeverage(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		l       uint
		wantErr string
	}{
		{name: "disabled ignores out-of-range value", enabled: false, l: 0},
		{name: "enabled at minimum", enabled: true, l: 1},
		{name: "enabled within range", enabled: true, l: 100},
		{name: "enabled at maximum", enabled: true, l: 10_000},
		{
			name:    "enabled below minimum",
			enabled: true,
			l:       0,
			wantErr: "pledgeLeverage",
		},
		{
			name:    "enabled above maximum",
			enabled: true,
			l:       10_001,
			wantErr: "pledgeLeverage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.PledgeLeverageEnabled = tt.enabled
			cfg.PledgeLeverage = tt.l
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

// TestValidateMidnightEnabled pins the contradiction check: the Midnight
// indexer needs the api-mode indexes to function, so midnight.enabled
// requires storageMode "api". Dev mode is exempted because node.Run
// force-upgrades storage mode to api at startup regardless of what is
// configured (see TestValidateMidnightEnabledAllowedInDevMode).
func TestValidateMidnightEnabled(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		storageMode string
		wantErr     string
	}{
		{
			name:        "disabled with core mode",
			enabled:     false,
			storageMode: storageModeCore,
		},
		{
			name:        "disabled with api mode",
			enabled:     false,
			storageMode: storageModeAPI,
		},
		{
			name:        "enabled with api mode",
			enabled:     true,
			storageMode: storageModeAPI,
		},
		{
			name:        "enabled with core mode",
			enabled:     true,
			storageMode: storageModeCore,
			wantErr:     `midnight.enabled requires storageMode "api"`,
		},
		{
			name:        "enabled with unset storage mode",
			enabled:     true,
			storageMode: "",
			wantErr:     `midnight.enabled requires storageMode "api"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.Midnight.Enabled = tt.enabled
			cfg.StorageMode = tt.storageMode
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

// TestValidateMidnightEnabledAllowedInDevMode verifies that dev mode's
// storage-mode force-upgrade (node.Run) is honored the same way it already
// is for the API listener ports above: midnight.enabled with a configured
// core storage mode must not be rejected when runMode is dev, because the
// node will actually start in api mode.
func TestValidateMidnightEnabledAllowedInDevMode(t *testing.T) {
	cfg := validTestConfig()
	cfg.RunMode = RunModeDev
	cfg.StorageMode = storageModeCore
	cfg.Midnight.Enabled = true
	assert.NoError(t, cfg.validate(RunModeServe, minUnprivilegedPort))
	assert.NoError(t, cfg.validate(RunModeDev, minUnprivilegedPort))
}

func TestValidateMidnightServerPolicy(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
		wantErr   string
	}{
		{
			name: "disabled server ignores configured port",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.Port = maxPort + 1
			},
		},
		{
			name: "disabled server ignores listener collision",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.Host = c.BindAddr
				c.Midnight.Port = c.RelayPort
			},
		},
		{
			name: "disabled server ignores remote plaintext host",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.Host = "0.0.0.0"
			},
		},
		{
			name: "enabled server requires api storage",
			configure: func(c *Config) {
				c.Midnight.ServerEnabled = true
			},
			wantErr: `midnight.serverEnabled requires storageMode "api"`,
		},
		{
			name: "enabled server requires a port",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Port = 0
			},
			wantErr: "midnight.port must be set",
		},
		{
			name: "enabled server allowed in dev mode",
			configure: func(c *Config) {
				c.RunMode = RunModeDev
				c.Midnight.ServerEnabled = true
			},
		},
		{
			name: "reflection requires server",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ReflectionEnabled = true
			},
			wantErr: "midnight.reflectionEnabled requires midnight.serverEnabled",
		},
		{
			name: "loopback ipv4 plaintext",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "127.0.0.1"
			},
		},
		{
			name: "loopback ipv6 plaintext",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "::1"
			},
		},
		{
			name: "localhost plaintext",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "localhost"
			},
		},
		{
			name: "remote plaintext denied",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "192.0.2.1"
			},
			wantErr: "midnight.allowInsecureRemote",
		},
		{
			name: "wildcard ipv4 plaintext denied",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "0.0.0.0"
			},
			wantErr: "midnight.allowInsecureRemote",
		},
		{
			name: "wildcard ipv6 plaintext denied",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "::"
			},
			wantErr: "midnight.allowInsecureRemote",
		},
		{
			name: "unspecified plaintext denied",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = ""
			},
			wantErr: "midnight.allowInsecureRemote",
		},
		{
			name: "remote plaintext explicit override",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "192.0.2.1"
				c.Midnight.AllowInsecureRemote = true
			},
		},
		{
			name: "remote tls",
			configure: func(c *Config) {
				c.StorageMode = storageModeAPI
				c.Midnight.ServerEnabled = true
				c.Midnight.Host = "192.0.2.1"
				c.TlsCertFilePath = "/tls/server.crt"
				c.TlsKeyFilePath = "/tls/server.key"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.configure(cfg)
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestValidateDelegatorInactivity pins the CIP-0163 range check: the
// inactivity window is only validated when the gate is enabled, and must
// fall in [1, 10000] when it is.
func TestValidateDelegatorInactivity(t *testing.T) {
	tests := []struct {
		name       string
		enabled    bool
		inactivity uint64
		wantErr    string
	}{
		{
			name:       "disabled ignores out-of-range value",
			enabled:    false,
			inactivity: 10_001,
		},
		{name: "enabled at minimum", enabled: true, inactivity: 1},
		{name: "enabled within range", enabled: true, inactivity: 90},
		{name: "enabled at maximum", enabled: true, inactivity: 10_000},
		{
			name:       "enabled below minimum",
			enabled:    true,
			inactivity: 0,
			wantErr:    "delegatorInactivity",
		},
		{
			name:       "enabled above maximum",
			enabled:    true,
			inactivity: 10_001,
			wantErr:    "delegatorInactivity",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.DelegatorInactivityEnabled = tt.enabled
			cfg.DelegatorInactivity = tt.inactivity
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

// TestValidateDatabaseLifecycleSnapshotCloudDestination guards a config
// that reaches Manager.Start with a malformed SnapshotCloudDestination:
// without this check, the only place a bad URI ever surfaced was a
// logged-and-swallowed failure inside handleEpochTransitionEvent, up to a
// full epoch after the node had already started running with it.
func TestValidateDatabaseLifecycleSnapshotCloudDestination(t *testing.T) {
	tests := []struct {
		name    string
		dest    string
		wantErr string
	}{
		{name: "empty is fine, no cloud mirroring configured", dest: ""},
		{
			name:    "well-formed s3 URI",
			dest:    "s3://bucket/prefix",
			wantErr: unsupportedCloudSchemeTestError("s3"),
		},
		{
			name:    "well-formed gcs URI",
			dest:    "gcs://bucket/prefix",
			wantErr: unsupportedCloudSchemeTestError("gcs"),
		},
		{
			name:    "typoed scheme",
			dest:    "s33://bucket/prefix",
			wantErr: "snapshotCloudDestination",
		},
		{
			name:    "missing scheme separator",
			dest:    "s3bucket/prefix",
			wantErr: "snapshotCloudDestination",
		},
		{
			name:    "scheme with no host",
			dest:    "s3://",
			wantErr: "snapshotCloudDestination",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.DatabaseLifecycle.SnapshotCloudDestination = tt.dest
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

// TestValidateDatabaseLifecycleSnapshotDirWritability guards against a raw
// filesystem permission error surfacing deep inside a snapshot attempt
// instead of a clean, actionable one at startup -- the failure mode for a
// --db-snapshot-dir bind-mounted from a host directory the Docker image's
// non-root user doesn't own (see dingo.yaml.example's snapshotDir entry).
func TestValidateDatabaseLifecycleSnapshotDirWritability(t *testing.T) {
	t.Run(
		"writable directory passes and is created if missing",
		func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "nested", "snapshots")
			cfg := validTestConfig()
			cfg.DatabaseLifecycle.SnapshotEnabled = true
			cfg.DatabaseLifecycle.SnapshotDir = dir
			require.NoError(t, cfg.validate(cfg.RunMode, minUnprivilegedPort))
			info, err := os.Stat(dir)
			require.NoError(t, err)
			require.True(t, info.IsDir())
		},
	)

	t.Run(
		"unwritable directory fails with an actionable error",
		func(t *testing.T) {
			if os.Geteuid() == 0 {
				t.Skip(
					"root can write anywhere regardless of mode -- skip when running as root",
				)
			}
			parent := t.TempDir()
			dir := filepath.Join(parent, "readonly")
			require.NoError(t, os.Mkdir(dir, 0o555))
			if runtime.GOOS == "windows" {
				testutil.MakeDirectoryUnwritable(t, dir)
			} else {
				t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			}
			cfg := validTestConfig()
			cfg.DatabaseLifecycle.SnapshotEnabled = true
			cfg.DatabaseLifecycle.SnapshotDir = dir
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "snapshotDir")
			// Asserted on every platform: the hint is a static string in
			// validate.go's snapshotDir wrap, so if the error is produced at
			// all it carries the hint, and that hint is the actionable half of
			// the message.
			assert.Contains(t, err.Error(), "1000:1000")
		},
	)

	t.Run(
		"disabled snapshots skip the writability check when bark is also disabled",
		func(t *testing.T) {
			cfg := validTestConfig()
			cfg.DatabaseLifecycle.SnapshotEnabled = false
			cfg.BarkPort = 0
			cfg.DatabaseLifecycle.SnapshotDir = filepath.Join(
				t.TempDir(), "never-created",
			)
			require.NoError(t, cfg.validate(cfg.RunMode, minUnprivilegedPort))
		},
	)

	// TestValidateDatabaseLifecycleSnapshotDirWritability/bark-enabled
	// guards against a real gap: snapshotDir also backs Bark's
	// DatabaseService CreateSnapshot/Restore RPCs whenever bark is
	// enabled (barkPort > 0) with a snapshotDir configured -- regardless
	// of whether automatic epoch-boundary snapshots (snapshotEnabled) are
	// on. Checking only snapshotEnabled left that combination (Bark
	// snapshots without automatic ones) completely unvalidated.
	t.Run(
		"unwritable directory fails even with automatic snapshots disabled, when bark is enabled",
		func(t *testing.T) {
			if os.Geteuid() == 0 {
				t.Skip(
					"root can write anywhere regardless of mode -- skip when running as root",
				)
			}
			parent := t.TempDir()
			dir := filepath.Join(parent, "readonly")
			require.NoError(t, os.Mkdir(dir, 0o555))
			if runtime.GOOS == "windows" {
				testutil.MakeDirectoryUnwritable(t, dir)
			} else {
				t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			}
			cfg := validTestConfig()
			cfg.DatabaseLifecycle.SnapshotEnabled = false
			cfg.DatabaseLifecycle.SnapshotDir = dir
			cfg.BarkPort = 8091
			cfg.BarkClientCAFilePath = "/certs/ca.crt"
			cfg.TlsCertFilePath = "/certs/tls.crt"
			cfg.TlsKeyFilePath = "/certs/tls.key"
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "snapshotDir")
			if runtime.GOOS != "windows" {
				assert.Contains(t, err.Error(), "1000:1000")
			}
		},
	)
}

func TestValidateDatabaseLifecycleSnapshotCloudDestinationPrefix(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{name: "empty", prefix: ""},
		{name: "safe segment", prefix: "node-a"},
		{name: "parent", prefix: "..", wantErr: true},
		{name: "current directory", prefix: ".", wantErr: true},
		{name: "forward slash", prefix: "nodes/a", wantErr: true},
		{name: "backslash", prefix: `nodes\a`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.DatabaseLifecycle.SnapshotCloudDestinationPrefix = tt.prefix
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			if tt.wantErr {
				require.ErrorContains(
					t,
					err,
					"snapshotCloudDestinationPrefix",
				)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestValidatePrivilegedPortAllowedWhenBindable covers a process that
// may bind any port (root, Windows, or CAP_NET_BIND_SERVICE):
// minBindable is 0, so a sub-1024 port passes.
func TestValidatePrivilegedPortAllowedWhenBindable(t *testing.T) {
	cfg := validTestConfig()
	cfg.StorageMode = storageModeAPI
	setPluginPort(&cfg.Plugins.API.Blockfrost, 443)
	assert.NoError(t, cfg.validate(cfg.RunMode, 0))
}

// TestValidateLoweredPrivilegedPortCutoff covers a Linux deployment
// with net.ipv4.ip_unprivileged_port_start lowered to a nonzero value:
// ports at or above the cutoff must pass while ports below it are
// still rejected.
func TestValidateLoweredPrivilegedPortCutoff(t *testing.T) {
	cfg := validTestConfig()
	cfg.StorageMode = storageModeAPI
	setPluginPort(&cfg.Plugins.API.Blockfrost, 80)
	assert.NoError(t, cfg.validate(cfg.RunMode, 80))
	setPluginPort(&cfg.Plugins.API.Blockfrost, 79)
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
	cfg.MetricsPort = APIPluginPort(cfg.Plugins.API.Mesh)
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
	setPluginPort(&cfg.Plugins.API.Utxorpc, 99999999)
	err := cfg.validate(RunModeServe, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid plugins.api.utxorpc.config.port")
}

// TestValidateLoadModeSkipsAllListenerPorts verifies that load, which
// starts no listeners, does not reject otherwise out-of-range listener
// ports: they never bind during an import.
func TestValidateLoadModeSkipsAllListenerPorts(t *testing.T) {
	cfg := validTestConfig()
	cfg.RunMode = RunModeLoad
	cfg.ImmutableDbPath = "/data/immutable"
	cfg.MetricsPort = 99999999
	setPluginPort(&cfg.Plugins.API.Utxorpc, 99999999)
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
	setMempoolSetting(cfg, "evictionWatermark", 2.0)
	cfg.Chainsync.Strategy = "fastest"
	err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires immutableDbPath")
	assert.Contains(
		t,
		err.Error(),
		"invalid plugins.mempool.config.evictionWatermark",
	)
	assert.Contains(t, err.Error(), "invalid chainsync.strategy")
}

func TestValidateMinPoolMargin(t *testing.T) {
	tests := []struct {
		name    string
		v       uint
		wantErr string
	}{
		{name: "zero disabled", v: 0},
		{name: "within range", v: 150},
		{name: "at maximum", v: 10_000},
		{name: "above maximum", v: 10_001, wantErr: "minPoolMargin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.MinPoolMargin = tt.v
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
