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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	minUnprivilegedPort = 1024
	maxPort             = 65535
)

// Validate checks the fully merged configuration (defaults, YAML,
// environment, CLI flags) for invalid values and nonsensical
// combinations. Every problem found is returned, joined into a single
// error, so the operator can fix them all in one pass. It is called
// from cmd/dingo after CLI flags have been applied, before any
// services start; LoadConfig alone does not see CLI flag values.
func (c *Config) Validate() error {
	return c.validate(os.Geteuid() == 0)
}

// validate is the deterministic core of Validate. privileged reports
// whether the process may bind ports below 1024 (i.e. running as
// root); it is a parameter so tests do not depend on the effective
// UID of the test runner.
func (c *Config) validate(privileged bool) error {
	var errs []error

	// Mode enums
	if !c.RunMode.Valid() {
		errs = append(errs, fmt.Errorf(
			"invalid runMode: %q (must be 'serve', 'load', 'dev', or 'leios')",
			c.RunMode,
		))
	}
	if !c.StartEra.Valid() {
		errs = append(errs, fmt.Errorf(
			"invalid startEra: %q (must be empty or 'dijkstra')",
			c.StartEra,
		))
	}
	switch c.StorageMode {
	case "", storageModeCore, storageModeAPI:
	default:
		errs = append(errs, fmt.Errorf(
			"invalid storageMode %q: must be %q or %q",
			c.StorageMode, storageModeCore, storageModeAPI,
		))
	}

	// Load mode requires a source ImmutableDB
	if c.RunMode == RunModeLoad && c.ImmutableDbPath == "" {
		errs = append(errs, errors.New(
			"runMode \"load\" requires immutableDbPath to be set "+
				"(config, DINGO_IMMUTABLE_DB_PATH, or --immutable-db-path)",
		))
	}

	// Ports
	ports := []struct {
		setting  string
		port     uint
		required bool
	}{
		{"port (relay/NtN)", c.RelayPort, true},
		{"privatePort", c.PrivatePort, true},
		{"metricsPort", c.MetricsPort, true},
		{"debugPort", c.DebugPort, false},
		{"utxorpcPort", c.UtxorpcPort, false},
		{"blockfrostPort", c.BlockfrostPort, false},
		{"meshPort", c.MeshPort, false},
		{"barkPort", c.BarkPort, false},
		{"midnight.port", c.Midnight.Port, false},
	}
	for _, p := range ports {
		if err := validatePort(p.setting, p.port, p.required, privileged); err != nil {
			errs = append(errs, err)
		}
	}

	// Path traversal guard on the Cardano node config path, matching
	// the network-name guard (INT-03). The path may arrive via env or
	// YAML, so a ".." component could escape an expected config root.
	if err := validatePathNoTraversal("cardanoConfig", c.CardanoConfig); err != nil {
		errs = append(errs, err)
	}

	// TLS cert and key only work as a pair
	if (c.TlsCertFilePath == "") != (c.TlsKeyFilePath == "") {
		errs = append(errs, errors.New(
			"tlsCertFilePath and tlsKeyFilePath must both be set to enable TLS "+
				"(only one is set)",
		))
	}

	// Mempool
	if c.MempoolCapacity < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid mempoolCapacity: %d (must not be negative)",
			c.MempoolCapacity,
		))
	}
	if c.EvictionWatermark <= 0 || c.EvictionWatermark >= 1.0 {
		errs = append(errs, fmt.Errorf(
			"invalid evictionWatermark: %f (must be in range (0, 1))",
			c.EvictionWatermark,
		))
	}
	if c.RejectionWatermark <= 0 || c.RejectionWatermark > 1.0 {
		errs = append(errs, fmt.Errorf(
			"invalid rejectionWatermark: %f (must be in range (0, 1])",
			c.RejectionWatermark,
		))
	}
	if c.EvictionWatermark >= c.RejectionWatermark {
		errs = append(errs, fmt.Errorf(
			"evictionWatermark (%f) must be less than rejectionWatermark (%f)",
			c.EvictionWatermark,
			c.RejectionWatermark,
		))
	}

	// Block production needs all three credential paths
	if c.BlockProducer {
		var missing []string
		if c.ShelleyVRFKey == "" {
			missing = append(missing, "shelleyVrfKey")
		}
		if c.ShelleyKESKey == "" {
			missing = append(missing, "shelleyKesKey")
		}
		if c.ShelleyOperationalCertificate == "" {
			missing = append(missing, "shelleyOperationalCertificate")
		}
		if len(missing) > 0 {
			errs = append(errs, fmt.Errorf(
				"blockProducer enabled but missing required key paths: %v",
				missing,
			))
		}
	}

	// Network identity
	if c.Network == "" {
		if c.NetworkMagic == 0 {
			errs = append(errs, errors.New(
				"network or networkMagic must be set",
			))
		}
	} else if err := ValidateNetworkName(c.Network); err != nil {
		errs = append(errs, err)
	}

	// Duration strings, parsed downstream at use; fail at startup
	// instead with the setting named
	for _, d := range []struct {
		setting      string
		value        string
		mustPositive bool
	}{
		{"shutdownTimeout", c.ShutdownTimeout, true},
		{"ledgerCatchupTimeout", c.LedgerCatchupTimeout, true},
		{"chainsync.stallTimeout", c.Chainsync.StallTimeout, true},
		// Negative disables Mithril download idle detection
		{"mithril.downloadIdleTimeout", c.Mithril.DownloadIdleTimeout, false},
	} {
		if d.value == "" {
			continue
		}
		parsed, err := time.ParseDuration(d.value)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"invalid %s %q: %w", d.setting, d.value, err,
			))
			continue
		}
		if d.mustPositive && parsed <= 0 {
			errs = append(errs, fmt.Errorf(
				"invalid %s %q: must be positive", d.setting, d.value,
			))
		}
	}

	// Chainsync; accepted strategy names must match
	// chainsync.ParseHeaderSyncStrategy (not imported here to keep
	// internal/config free of node subsystem dependencies)
	switch strings.ToLower(strings.TrimSpace(c.Chainsync.Strategy)) {
	case "", "primary", "parallel", "round-robin", "roundrobin", "round_robin":
	default:
		errs = append(errs, fmt.Errorf(
			"invalid chainsync.strategy %q (want primary, parallel, or round-robin)",
			c.Chainsync.Strategy,
		))
	}
	if c.Chainsync.MaxClients < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid chainsync.maxClients: %d (must not be negative)",
			c.Chainsync.MaxClients,
		))
	}

	// Mithril backend; accepted values must match
	// cmd/dingo resolveMithrilBackend (empty selects v2)
	switch c.Mithril.Backend {
	case "", "v1", "v2":
	default:
		errs = append(errs, fmt.Errorf(
			"invalid mithril.backend %q: must be \"v1\" or \"v2\"",
			c.Mithril.Backend,
		))
	}

	return errors.Join(errs...)
}

// validatePort checks a configured TCP port. Ports are uints, so
// values above 65535 are representable but unbindable; ports below
// 1024 need elevated privileges; and 0 either disables the component
// (required=false) or is nonsense for a mandatory listener
// (required=true, binding port 0 picks a random port).
func validatePort(setting string, port uint, required, privileged bool) error {
	if port == 0 {
		if required {
			return fmt.Errorf("%s must be set (port 0 is not valid)", setting)
		}
		return nil
	}
	if port > maxPort {
		return fmt.Errorf(
			"invalid %s: %d (must be at most %d)",
			setting, port, maxPort,
		)
	}
	if port < minUnprivilegedPort && !privileged {
		return fmt.Errorf(
			"invalid %s: %d is a privileged port and the process is not "+
				"running as root (use %d-%d)",
			setting, port, minUnprivilegedPort, maxPort,
		)
	}
	return nil
}

// validatePathNoTraversal rejects paths containing a ".." component.
// Values can arrive via YAML or environment, where a traversal-shaped
// path is more likely an injection than an intent; absolute paths
// express any legitimate target without "..".
func validatePathNoTraversal(setting, path string) error {
	if path == "" {
		return nil
	}
	cleaned := filepath.Clean(path)
	for part := range strings.SplitSeq(filepath.ToSlash(cleaned), "/") {
		if part == ".." {
			return fmt.Errorf(
				"invalid %s %q: path must not contain \"..\" "+
					"(use an absolute path instead)",
				setting, path,
			)
		}
	}
	return nil
}
