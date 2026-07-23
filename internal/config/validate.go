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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

const (
	minUnprivilegedPort = 1024
	maxPort             = 65535
)

// AcceptedChainsyncStrategies mirrors
// chainsync.AcceptedHeaderSyncStrategyNames (the accepted-name list
// chainsync.ParseHeaderSyncStrategy is derived from). internal/config
// cannot import chainsync without pulling node subsystems into the
// config package, so the two lists are kept in sync by a parity test in
// cmd/dingo (which can import both).
var AcceptedChainsyncStrategies = []string{
	"", "primary", "parallel", "round-robin", "roundrobin", "round_robin",
}

// AcceptedMithrilBackends mirrors mithril.AcceptedBackends plus the
// empty string (which selects the default v2), matching cmd/dingo's
// resolveMithrilBackend. Kept in sync by the same parity test in
// cmd/dingo as AcceptedChainsyncStrategies.
var AcceptedMithrilBackends = []string{"", "v1", "v2"}

// FullPotRewardsStandardNetwork reports whether network/networkMagic identifies
// a predefined non-devnet network where CIP-0163 full-pot rewards must not be
// enabled accidentally. Network magic is checked too, so a custom name cannot
// opt in while still pointing at a public network magic.
func FullPotRewardsStandardNetwork(
	network string,
	networkMagic uint32,
) (string, bool) {
	if network != "" {
		if known, ok := ouroboros.NetworkByName(network); ok &&
			known.Name != ouroboros.NetworkDevnet.Name {
			return known.Name, true
		}
	}
	if networkMagic != 0 {
		if known, ok := ouroboros.NetworkByNetworkMagic(networkMagic); ok &&
			known.Name != ouroboros.NetworkDevnet.Name {
			return known.Name, true
		}
	}
	return "", false
}

// Validate checks the fully merged configuration (defaults, YAML,
// environment, CLI flags) for invalid values and nonsensical
// combinations. Every problem found is returned, joined into a single
// error, so the operator can fix them all in one pass. It is called
// from cmd/dingo after CLI flags have been applied and ApplyDefaults
// has filled in derived defaults, before any services start;
// LoadConfig alone does not see CLI flag values.
//
// effectiveMode is the run mode the invocation will actually execute.
// For the bare `dingo` process it is c.RunMode, but the one-shot
// subcommands (load, sync, mithril) run a fixed operation regardless of
// the configured runMode, so cmd/dingo passes the mode reflecting what
// the command does. It governs which listeners and sources are required.
func (c *Config) Validate(effectiveMode RunMode) error {
	return c.validate(effectiveMode, minBindablePort())
}

// minBindablePort returns the lowest TCP port this process may bind;
// configured ports below it are privileged and rejected. Root (euid 0)
// may bind any port, and Windows has no privileged-port restriction
// (os.Geteuid also returns -1 there, which would otherwise misclassify
// every Windows process as unprivileged). On Linux the restriction is
// lifted entirely by CAP_NET_BIND_SERVICE (setcap, systemd
// AmbientCapabilities), and the cutoff is otherwise the kernel's
// net.ipv4.ip_unprivileged_port_start — 1024 by default, 0 inside
// containers under runtimes such as Docker, and possibly any other
// value an operator has set — so the actual sysctl value is used. On
// other Unixes the traditional 1024 cutoff applies.
func minBindablePort() uint {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		return 0
	}
	if runtime.GOOS != "linux" {
		return minUnprivilegedPort
	}
	if hasCapNetBindService() {
		return 0
	}
	if start, err := readProcUint(
		"/proc/sys/net/ipv4/ip_unprivileged_port_start",
	); err == nil && start <= maxPort {
		return uint(start)
	}
	return minUnprivilegedPort
}

// readProcUint reads a procfs file containing a single unsigned
// decimal value.
func readProcUint(path string) (uint64, error) {
	data, err := os.ReadFile(path) //nolint:gosec // fixed procfs paths
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

// hasCapNetBindService reports whether the process's effective
// capability set includes CAP_NET_BIND_SERVICE (bit 10), read from the
// CapEff line of /proc/self/status.
func hasCapNetBindService() bool {
	const capNetBindService = 10
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return false
	}
	for line := range strings.Lines(string(data)) {
		rest, ok := strings.CutPrefix(line, "CapEff:")
		if !ok {
			continue
		}
		mask, err := strconv.ParseUint(strings.TrimSpace(rest), 16, 64)
		if err != nil {
			return false
		}
		return mask&(1<<capNetBindService) != 0
	}
	return false
}

// validate is the deterministic core of Validate. minBindable is the
// lowest port the process may bind (0 for a privileged process, 1024
// for a typical unprivileged one); it is a parameter so tests do not
// depend on the effective UID or kernel settings of the test runner.
func (c *Config) validate(effectiveMode RunMode, minBindable uint) error {
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
	if effectiveMode == RunModeLoad && c.ImmutableDbPath == "" {
		errs = append(errs, errors.New(
			"runMode \"load\" requires immutableDbPath to be set "+
				"(config, DINGO_IMMUTABLE_DB_PATH, or --immutable-db-path)",
		))
	}

	// Ports. Only listeners this invocation actually starts are
	// range-checked, privilege-checked, and checked against each other for
	// collisions: a port configured for a listener that stays inactive
	// cannot bind and so cannot conflict. The active set is derived from
	// the effective run mode plus the storage mode, mirroring the gating in
	// (*dingo.Node).Start and cmd/dingo's node.Run/mithril paths:
	//   - relay, private: serving modes only (required there);
	//   - metrics, debug: serving modes and the Mithril sync operation
	//     (RunModeSync); the read-only Mithril subcommands start neither;
	//   - bark: serving modes only (not storage-gated);
	//   - UTxORPC, Blockfrost, Mesh, Midnight: serving modes under API
	//     storage. Dev mode forces API storage on at startup, and node.Run
	//     keys that off the *configured* runMode — `dingo serve` with
	//     runMode "dev" still runs dev — so the configured mode is
	//     consulted alongside the effective one.
	// The load and read-only Mithril invocations start no listeners, so
	// their ports may be unset (0) and are not checked.
	serving := effectiveMode.RequiresListeners()
	auxListeners := serving || effectiveMode == RunModeSync
	apiListeners := serving &&
		(effectiveMode == RunModeDev || c.RunMode.IsDevMode() ||
			c.StorageMode == storageModeAPI)
	// Each entry's host is the bind address the listener actually uses
	// at runtime: bindAddr for most, privateBindAddr for the private
	// listener, midnight.host for Midnight, and all interfaces for bark
	// (which is started without a host).
	ports := []struct {
		setting  string
		host     string
		port     uint
		active   bool
		required bool
	}{
		{"port (relay/NtN)", c.BindAddr, c.RelayPort, serving, serving},
		{"privatePort", c.PrivateBindAddr, c.PrivatePort, serving, serving},
		{"metricsPort", c.BindAddr, c.MetricsPort, auxListeners, serving},
		{"debugPort", c.BindAddr, c.DebugPort, auxListeners, false},
		{"barkPort", "", c.BarkPort, serving, false},
		{"utxorpcPort", c.BindAddr, c.UtxorpcPort, apiListeners, false},
		{"blockfrostPort", c.BindAddr, c.BlockfrostPort, apiListeners, false},
		{"meshPort", c.BindAddr, c.MeshPort, apiListeners, false},
		{"midnight.port", c.Midnight.Host, c.Midnight.Port, apiListeners, false},
	}
	// Two active listeners contending for a port only fails at bind
	// time; catch it here. Zero ports are disabled or OS-assigned, so
	// they don't clash, and a port is only a conflict when the bind
	// addresses overlap: listeners on distinct specific addresses (e.g.
	// 127.0.0.1 and 127.0.0.2) may legally share a port.
	type boundListener struct {
		setting string
		host    string
	}
	seenPorts := make(map[uint][]boundListener, len(ports))
	for _, p := range ports {
		if !p.active {
			continue
		}
		if err := validatePort(p.setting, p.port, p.required, minBindable); err != nil {
			errs = append(errs, err)
		}
		if p.port == 0 {
			continue
		}
		for _, other := range seenPorts[p.port] {
			if !bindAddrsOverlap(other.host, p.host) {
				continue
			}
			errs = append(errs, fmt.Errorf(
				"port %d is assigned to both %s and %s "+
					"on overlapping bind addresses",
				p.port, other.setting, p.setting,
			))
			break
		}
		seenPorts[p.port] = append(
			seenPorts[p.port],
			boundListener{setting: p.setting, host: p.host},
		)
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
	if c.MempoolImplementation != "" &&
		c.MempoolImplementation != "fifo" {
		errs = append(errs, fmt.Errorf(
			"invalid mempoolImplementation %q (must be \"fifo\")",
			c.MempoolImplementation,
		))
	}
	if c.MempoolCapacity < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid mempoolCapacity: %d (must not be negative)",
			c.MempoolCapacity,
		))
	}
	// NaN is checked explicitly: every ordered comparison with NaN is
	// false, so a NaN watermark would slip through the range checks
	// alone and reach mempool threshold arithmetic.
	if math.IsNaN(c.EvictionWatermark) ||
		c.EvictionWatermark <= 0 || c.EvictionWatermark >= 1.0 {
		errs = append(errs, fmt.Errorf(
			"invalid evictionWatermark: %f (must be in range (0, 1))",
			c.EvictionWatermark,
		))
	}
	if math.IsNaN(c.RejectionWatermark) ||
		c.RejectionWatermark <= 0 || c.RejectionWatermark > 1.0 {
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

	// CIP-23 minimum pool margin is basis points; must be within [0, 10000].
	if c.MinPoolMargin > 10_000 {
		errs = append(errs, fmt.Errorf(
			"minPoolMargin (%d) must be in [0, 10000] basis points",
			c.MinPoolMargin,
		))
	}

	// CIP-50 pledge leverage L must be within [1, 10000] when enabled.
	if c.PledgeLeverageEnabled &&
		(c.PledgeLeverage < 1 || c.PledgeLeverage > 10000) {
		errs = append(errs, fmt.Errorf(
			"pledgeLeverage (%d) must be in [1, 10000] when pledgeLeverageEnabled",
			c.PledgeLeverage,
		))
	}

	if c.FullPotRewardsEnabled && !c.UnsafeFullPotRewardsOnStandardNetworks {
		if network, ok := FullPotRewardsStandardNetwork(
			c.Network,
			c.NetworkMagic,
		); ok {
			errs = append(errs, fmt.Errorf(
				"fullPotRewardsEnabled is not permitted on standard network %q "+
					"without unsafeFullPotRewardsOnStandardNetworks",
				network,
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

	// History expiry cadence. ApplyDefaults only fills in an unset
	// (zero) frequency, so a non-positive value here was configured
	// explicitly — reject it rather than let the expiry worker start
	// on a cadence the operator did not choose.
	if c.HistoryExpiry.Frequency <= 0 {
		errs = append(errs, fmt.Errorf(
			"invalid historyExpiry.frequency: %s (must be positive)",
			c.HistoryExpiry.Frequency,
		))
	}

	// Chainsync; accepted strategy names come from
	// AcceptedChainsyncStrategies, kept in sync with
	// chainsync.ParseHeaderSyncStrategy by a parity test in cmd/dingo.
	strategy := strings.ToLower(strings.TrimSpace(c.Chainsync.Strategy))
	if !slices.Contains(AcceptedChainsyncStrategies, strategy) {
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

	// Mithril backend; accepted values come from
	// AcceptedMithrilBackends, kept in sync with cmd/dingo's
	// resolveMithrilBackend by a parity test in cmd/dingo (empty
	// selects v2).
	if !slices.Contains(AcceptedMithrilBackends, c.Mithril.Backend) {
		errs = append(errs, fmt.Errorf(
			"invalid mithril.backend %q: must be \"v1\" or \"v2\"",
			c.Mithril.Backend,
		))
	}

	return errors.Join(errs...)
}

// validatePort checks a configured TCP port. Ports are uints, so
// values above 65535 are representable but unbindable; ports below
// minBindable are privileged ports the process may not bind; and 0
// either disables the component (required=false) or is nonsense for a
// mandatory listener (required=true, binding port 0 picks a random
// port).
func validatePort(
	setting string,
	port uint,
	required bool,
	minBindable uint,
) error {
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
	if port < minBindable {
		return fmt.Errorf(
			"invalid %s: %d is a privileged port this process may not "+
				"bind (use %d-%d, or grant the privilege, e.g. root or "+
				"CAP_NET_BIND_SERVICE)",
			setting, port, minBindable, maxPort,
		)
	}
	return nil
}

// bindAddrsOverlap reports whether two listener bind addresses can
// contend for the same port: equal addresses always do, and a wildcard
// address overlaps every other address. Hostname aliases for the same
// interface (e.g. "localhost" vs "127.0.0.1") are not resolved; such
// conflicts surface at bind time instead.
func bindAddrsOverlap(a, b string) bool {
	if a == b {
		return true
	}
	return isWildcardAddr(a) || isWildcardAddr(b)
}

// isWildcardAddr reports whether a bind address selects all interfaces.
func isWildcardAddr(addr string) bool {
	switch addr {
	case "", "0.0.0.0", "::", "[::]":
		return true
	default:
		return false
	}
}

// validatePathNoTraversal rejects paths containing a ".." component.
// Values can arrive via YAML or environment, where a traversal-shaped
// path is more likely an injection than an intent; absolute paths
// express any legitimate target without "..". The original path is
// inspected component-by-component, deliberately without cleaning it
// first: cleaning would erase an inner ".." (e.g.
// "configs/../secret.json"), and the contract is that no ".."
// component appears at all.
func validatePathNoTraversal(setting, path string) error {
	if path == "" {
		return nil
	}
	for part := range strings.SplitSeq(filepath.ToSlash(path), "/") {
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
