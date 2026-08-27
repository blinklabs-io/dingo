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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
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

// MusashiNetworkIdentityConflict reports whether network/networkMagic mixes the
// experimental Musashi network (the IOG Leios prototype) with a *different*
// predefined network, returning the name of the network it collides with.
//
// This matters because Musashi is identified by either half of its identity —
// the name "musashi" or network magic 164 — and that identity switches on
// consensus/ledger trust bypasses (SkipLeaderStakeThresholdCheck,
// SkipDijkstraTxValidation). Either half alone is enough to enable them, so a
// half-matching configuration is dangerous in both directions:
//
//   - network "preview" with magic 164 runs the prototype's non-validating
//     rules on a node the operator configured as preview; and
//   - network "musashi" with magic 2 is worse still, because the handshake
//     uses the magic: the node actually joins preview while trusting the
//     prototype's rules.
//
// A custom name or an unregistered magic is not a conflict — those are private
// prototype deployments (e.g. a Musashi mirror). Devnet is excluded for the
// same reason it is excluded from FullPotRewardsStandardNetwork: it is a local
// test network, not a production-like profile.
func MusashiNetworkIdentityConflict(
	network string,
	networkMagic uint32,
) (string, bool) {
	nameIsMusashi := network == ouroboros.NetworkCardanoMusashi.Name
	magicIsMusashi := networkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic
	if nameIsMusashi && !magicIsMusashi && networkMagic != 0 {
		if known, ok := ouroboros.NetworkByNetworkMagic(networkMagic); ok &&
			known.Name != ouroboros.NetworkCardanoMusashi.Name &&
			known.Name != ouroboros.NetworkDevnet.Name {
			return known.Name, true
		}
	}
	if magicIsMusashi && !nameIsMusashi && network != "" {
		if known, ok := ouroboros.NetworkByName(network); ok &&
			known.Name != ouroboros.NetworkCardanoMusashi.Name &&
			known.Name != ouroboros.NetworkDevnet.Name {
			return known.Name, true
		}
	}
	return "", false
}

// PeerSnapshotNetworkMismatch reports whether a topology peer snapshot names a
// different network than the node is configured for.
//
// cardano-node writes the snapshot's own NetworkMagic into the file, so a
// snapshot taken on another network is self-identifying. It matters because
// the snapshot's relays *replace* the configured bootstrap peers during
// Genesis selection: accepting a foreign one aims the node at another
// network's relays and throws away the only addresses that could have worked.
// Every one of those relays is then denied at the handshake on a network-magic
// mismatch, leaving the node with no peers and nothing to fall back to.
//
// A zero magic on either side is "unspecified" rather than a network -- no
// real network uses magic 0, and a hand-written or older snapshot may omit the
// field -- so it is not treated as a mismatch.
func PeerSnapshotNetworkMismatch(
	snapshotMagic uint32,
	networkMagic uint32,
) bool {
	return snapshotMagic != 0 &&
		networkMagic != 0 &&
		snapshotMagic != networkMagic
}

// MusashiPrototypeNetwork reports whether network/networkMagic unambiguously
// identifies the Musashi prototype network, and is therefore permitted to run
// with the prototype's consensus/ledger trust bypasses.
//
// A conflicting identity (see MusashiNetworkIdentityConflict) is deliberately
// *not* the prototype network. Startup validation rejects those configurations
// outright, but returning false here keeps the bypasses off even for an
// embedder that builds a Config directly and never calls Validate.
func MusashiPrototypeNetwork(network string, networkMagic uint32) bool {
	if _, conflict := MusashiNetworkIdentityConflict(
		network,
		networkMagic,
	); conflict {
		return false
	}
	return network == ouroboros.NetworkCardanoMusashi.Name ||
		networkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic
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
	midnightServer := apiListeners && c.Midnight.ServerEnabled
	utxorpcPort := APIPluginPort(c.Plugins.API.Utxorpc)
	blockfrostPort := APIPluginPort(c.Plugins.API.Blockfrost)
	meshPort := APIPluginPort(c.Plugins.API.Mesh)
	// Each entry's host is the bind address the listener actually uses
	// at runtime: bindAddr for public listeners, privateBindAddr for the
	// private listener, debugBindAddr for pprof, midnight.host for Midnight,
	// and BarkHost for bark.
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
		{"debugPort", c.DebugBindAddr, c.DebugPort, auxListeners, false},
		{"barkPort", c.BarkHost, c.BarkPort, serving, false},
		{
			"plugins.api.utxorpc.config.port",
			c.BindAddr,
			utxorpcPort,
			apiListeners,
			false,
		},
		{
			"plugins.api.blockfrost.config.port",
			c.BindAddr,
			blockfrostPort,
			apiListeners,
			false,
		},
		{
			"plugins.api.mesh.config.port",
			c.BindAddr,
			meshPort,
			apiListeners,
			false,
		},
		{
			"midnight.port",
			c.Midnight.Host,
			c.Midnight.Port,
			midnightServer,
			midnightServer,
		},
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

	// The shared api.tls/api.auth mode enums are checked here so a typo is
	// caught once, with a single clear message, rather than surfacing
	// identically from every one of the three API providers that inherit
	// it. Certificate/key (and token) presence is deliberately NOT
	// checked here: a provider legitimately may supply only its own
	// certFilePath/keyFilePath while inheriting just `mode: server` from
	// this shared default (see internal/apiconfig.MergeTLS), so
	// completeness can only be judged after node.go merges this default
	// into each provider's own plugins.api.<name>.config.tls/auth --
	// which is where the full pair-completeness check runs, before that
	// provider's listener starts.
	if err := validateAPIMode(
		"api.tls.mode", c.API.TLS.Mode,
		string(apiconfig.TLSModeDisabled), string(apiconfig.TLSModeServer),
	); err != nil {
		errs = append(errs, err)
	}
	if err := validateAPIMode(
		"api.auth.mode", c.API.Auth.Mode,
		string(apiconfig.AuthModeDisabled), string(apiconfig.AuthModeToken),
	); err != nil {
		errs = append(errs, err)
	}

	// Bark's DatabaseService is mounted whenever bark is enabled with a snapshot
	// directory configured. Every method must authenticate its caller, and its
	// destructive methods additionally require explicit operator authorization.
	// Validate both policies here rather than failing deep in Bark.Start.
	if serving && c.BarkPort > 0 && c.DatabaseLifecycle.SnapshotDir != "" &&
		c.BarkClientCAFilePath == "" {
		errs = append(errs, errors.New(
			"barkClientCaFilePath is required when bark is enabled "+
				"(barkPort) alongside databaseLifecycle.snapshotDir: its "+
				"DatabaseService RPCs must not be mounted without a way to "+
				"authenticate callers",
		))
	}
	if serving && c.BarkPort > 0 && c.DatabaseLifecycle.SnapshotDir != "" &&
		len(c.BarkOperatorCertificateFingerprints) == 0 {
		errs = append(errs, errors.New(
			"barkOperatorCertificateFingerprints requires at least one SHA-256 "+
				"client certificate fingerprint when bark is enabled (barkPort) "+
				"alongside databaseLifecycle.snapshotDir: verified identity alone "+
				"does not authorize destructive DatabaseService RPCs",
		))
	}
	for idx, fingerprint := range c.BarkOperatorCertificateFingerprints {
		normalized := strings.ReplaceAll(strings.TrimSpace(fingerprint), ":", "")
		decoded, err := hex.DecodeString(normalized)
		if err != nil || len(decoded) != sha256.Size {
			errs = append(errs, fmt.Errorf(
				"barkOperatorCertificateFingerprints[%d] must be a %d-byte "+
					"SHA-256 certificate fingerprint encoded as hexadecimal",
				idx,
				sha256.Size,
			))
		}
	}
	// mTLS client verification also needs the server's own TLS pair --
	// without it, bark.Bark.Start's own equivalent check (independent of
	// Lifecycle, since it applies to any TlsClientCAFilePath) would fail
	// deep inside node startup instead of here. Checked independently of
	// the barkPort/snapshotDir gate above so a barkClientCaFilePath set by
	// mistake without barkPort/snapshotDir still gets flagged.
	if serving && c.BarkClientCAFilePath != "" &&
		(c.TlsCertFilePath == "" || c.TlsKeyFilePath == "") {
		errs = append(errs, errors.New(
			"barkClientCaFilePath requires tlsCertFilePath and tlsKeyFilePath "+
				"to also be set for mTLS client verification",
		))
	}

	// Mempool
	mempoolCapacity, evictionWatermark, rejectionWatermark := c.MempoolSettings()
	if mempoolCapacity < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid plugins.mempool.config.capacity: %d (must not be negative)",
			mempoolCapacity,
		))
	}
	// NaN is checked explicitly: every ordered comparison with NaN is
	// false, so a NaN watermark would slip through the range checks
	// alone and reach mempool threshold arithmetic.
	// EvictionWatermark may be 0 to disable eviction, or a value in (0, 1).
	if math.IsNaN(evictionWatermark) ||
		evictionWatermark < 0 || evictionWatermark >= 1.0 {
		errs = append(errs, fmt.Errorf(
			"invalid plugins.mempool.config.evictionWatermark: %f (must be 0 or in range (0, 1))",
			evictionWatermark,
		))
	}
	if math.IsNaN(rejectionWatermark) ||
		rejectionWatermark <= 0 || rejectionWatermark > 1.0 {
		errs = append(errs, fmt.Errorf(
			"invalid plugins.mempool.config.rejectionWatermark: %f (must be in range (0, 1])",
			rejectionWatermark,
		))
	}
	// Only enforce ordering if eviction is enabled (non-zero).
	if evictionWatermark > 0 && evictionWatermark >= rejectionWatermark {
		errs = append(errs, fmt.Errorf(
			"plugins.mempool.config.evictionWatermark (%f) must be less than rejectionWatermark (%f)",
			evictionWatermark,
			rejectionWatermark,
		))
	}
	if value, ok := c.Plugins.Mempool.Config["revalidationDeltaCap"]; ok {
		revalidationDeltaCap := pluginInt64(value)
		if revalidationDeltaCap <= 0 {
			errs = append(errs, fmt.Errorf(
				"invalid plugins.mempool.config.revalidationDeltaCap: %d (must be positive)",
				revalidationDeltaCap,
			))
		}
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

	// The Musashi prototype network's identity switches on consensus/ledger
	// trust bypasses, so it must never be half-claimed by a configuration
	// that also names or addresses a standard network. Rejected outright
	// rather than defused silently: the operator asked for two mutually
	// exclusive networks and only one of them can be what they meant.
	if network, ok := MusashiNetworkIdentityConflict(
		c.Network,
		c.NetworkMagic,
	); ok {
		errs = append(errs, fmt.Errorf(
			"network identity conflict: network %q with networkMagic %d "+
				"identifies both the %q network and the Musashi prototype "+
				"network (name %q, magic %d); the Musashi prototype disables "+
				"consensus and ledger validation and must not be reachable "+
				"from a standard network configuration",
			c.Network,
			c.NetworkMagic,
			network,
			ouroboros.NetworkCardanoMusashi.Name,
			ouroboros.NetworkCardanoMusashi.NetworkMagic,
		))
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

	// Genesis corroboration is a security gate: a negative value must fail
	// closed rather than silently disabling it (only 0 disables it).
	if c.GenesisBootstrap.CorroborationPeers < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid genesisBootstrap.corroborationPeers: %d "+
				"(must not be negative; 0 disables corroboration)",
			c.GenesisBootstrap.CorroborationPeers,
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

	if c.DelegatorInactivityEnabled &&
		(c.DelegatorInactivity < 1 || c.DelegatorInactivity > 10000) {
		errs = append(errs, fmt.Errorf(
			"delegatorInactivity (%d) must be in [1, 10000] when delegatorInactivityEnabled",
			c.DelegatorInactivity,
		))
	}

	// The Midnight indexer needs the api-mode indexes to function, so
	// midnight.enabled requires storageMode "api". Reject the contradiction
	// up front rather than letting it start indexer-less/silently. Dev mode
	// force-upgrades storage mode to api at startup (node.Run), so it is
	// exempted here the same way apiListeners above is.
	if c.Midnight.Enabled && c.StorageMode != storageModeAPI &&
		effectiveMode != RunModeDev && !c.RunMode.IsDevMode() {
		errs = append(errs, fmt.Errorf(
			"midnight.enabled requires storageMode %q, got %q: "+
				"set storageMode to %q or disable midnight.enabled",
			storageModeAPI, c.StorageMode, storageModeAPI,
		))
	}
	if c.Midnight.ServerEnabled && c.StorageMode != storageModeAPI &&
		effectiveMode != RunModeDev && !c.RunMode.IsDevMode() {
		errs = append(errs, fmt.Errorf(
			"midnight.serverEnabled requires storageMode %q, got %q: "+
				"set storageMode to %q or disable midnight.serverEnabled",
			storageModeAPI, c.StorageMode, storageModeAPI,
		))
	}
	if c.Midnight.ReflectionEnabled && !c.Midnight.ServerEnabled {
		errs = append(errs, errors.New(
			"midnight.reflectionEnabled requires midnight.serverEnabled",
		))
	}
	// Plaintext is safe by default only on loopback. Wildcard, unspecified,
	// hostname, and concrete remote addresses require either the configured
	// TLS keypair or an explicit escape hatch acknowledging that transport
	// security is supplied outside Dingo.
	useMidnightTLS := c.TlsCertFilePath != "" && c.TlsKeyFilePath != ""
	if midnightServer && !useMidnightTLS &&
		!c.Midnight.AllowInsecureRemote &&
		!isLoopbackAddr(c.Midnight.Host) {
		errs = append(errs, fmt.Errorf(
			"midnight.host %q is not loopback: configure TLS or set "+
				"midnight.allowInsecureRemote to acknowledge plaintext exposure",
			c.Midnight.Host,
		))
	}

	if c.DatabaseLifecycle.SnapshotEnabled &&
		c.DatabaseLifecycle.SnapshotDir == "" {
		errs = append(errs, errors.New(
			"databaseLifecycle.snapshotDir is required when databaseLifecycle.snapshotEnabled is true",
		))
	}
	// snapshotDir is only actually live for a run mode that starts the
	// full node (serving): that's the only path wiring up dblifecycle.
	// Manager, whose Start reads SnapshotEnabled and whose Bark-triggered
	// CreateSnapshot/Restore needs barkPort > 0 (matching the
	// barkClientCaFilePath gate above) -- neither ever runs under a
	// one-shot load/sync/mithril/database invocation, even if the same
	// shared config file has snapshotEnabled or Bark turned on for its
	// normal serve deployment. Gating on serving avoids those one-shot
	// commands eagerly creating the directory and probe-writing into it
	// (see checkDirWritable) for a subsystem they never start.
	if serving &&
		(c.DatabaseLifecycle.SnapshotEnabled || c.BarkPort > 0) &&
		c.DatabaseLifecycle.SnapshotDir != "" {
		if err := checkDirWritable(c.DatabaseLifecycle.SnapshotDir); err != nil {
			errs = append(errs, fmt.Errorf(
				"databaseLifecycle.snapshotDir %q is not usable: %w (if "+
					"running the official Docker image, a directory outside "+
					"/data/db must be pre-chowned on the host to the "+
					"container's UID:GID, 1000:1000)",
				c.DatabaseLifecycle.SnapshotDir, err,
			))
		}
	}
	if c.DatabaseLifecycle.SnapshotRetention < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid databaseLifecycle.snapshotRetention: %d (must not be negative)",
			c.DatabaseLifecycle.SnapshotRetention,
		))
	}
	if c.DatabaseLifecycle.SnapshotEveryNEpochs < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid databaseLifecycle.snapshotEveryNEpochs: %d (must not be negative)",
			c.DatabaseLifecycle.SnapshotEveryNEpochs,
		))
	}
	if dest := c.DatabaseLifecycle.SnapshotCloudDestination; dest != "" {
		u, err := url.Parse(dest)
		if err != nil || u.Scheme == "" || u.Host == "" {
			errs = append(errs, fmt.Errorf(
				"invalid databaseLifecycle.snapshotCloudDestination %q: must be a URI like s3://bucket/prefix or gcs://bucket/prefix",
				dest,
			))
		} else if !snapshotCloudSchemeSupported(u.Scheme) {
			errs = append(errs, fmt.Errorf(
				"invalid databaseLifecycle.snapshotCloudDestination %q: cloud scheme %q is unavailable in this build (s3/gcs require -tags dingo_extra_plugins)",
				dest,
				u.Scheme,
			))
		}
	}
	if prefix := c.DatabaseLifecycle.SnapshotCloudDestinationPrefix; prefix != "" {
		if prefix == "." || prefix == ".." ||
			strings.ContainsAny(prefix, `/\`) {
			errs = append(errs, fmt.Errorf(
				"invalid databaseLifecycle.snapshotCloudDestinationPrefix %q: "+
					"must be one path segment, not '.' or '..', and contain no '/' or '\\'",
				prefix,
			))
		}
	}

	// Koios parity observer (dingo #3098): network mirrors the same
	// preview/preprod restriction internal/koiosparity.NewObserver enforces
	// at construction time, checked here too so a bad value fails fast at
	// config-validation time instead of only once the node reaches
	// startKoiosParityObserver. Empty is valid -- it defers to the node's
	// own configured Network.
	if net := c.KoiosParity.Network; net != "" && net != "preview" &&
		net != "preprod" {
		errs = append(errs, fmt.Errorf(
			"invalid koiosParity.network %q: must be empty, \"preview\", or \"preprod\"",
			net,
		))
	}
	// ApplyDefaults only fills in an unset (zero) GraceHours, so a negative
	// value here was configured explicitly -- reject it the same way
	// historyExpiry.frequency's equivalent check does, rather than let it
	// silently change the grace-window semantics CompareEpochAggregates/
	// CompareEpochTotals apply.
	if c.KoiosParity.GraceHours < 0 {
		errs = append(errs, fmt.Errorf(
			"invalid koiosParity.graceHours: %d (must not be negative; 0 selects the default of 24)",
			c.KoiosParity.GraceHours,
		))
	}

	return errors.Join(errs...)
}

// validateAPIMode rejects a mode value that is neither unset (inherit/
// default) nor one of the two accepted enum values.
func validateAPIMode(
	setting string,
	mode *string,
	disabled, enabled string,
) error {
	if mode == nil || *mode == "" {
		return nil
	}
	if *mode == disabled || *mode == enabled {
		return nil
	}
	return fmt.Errorf(
		"%s: invalid mode %q (must be %q or %q)",
		setting, *mode, disabled, enabled,
	)
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

// isLoopbackAddr recognizes only explicit loopback literals and localhost.
// It deliberately performs no DNS lookup: accepting an arbitrary hostname
// based on a mutable resolution would turn startup validation into a TOCTOU
// exposure check. Empty and wildcard addresses are therefore remote.
func isLoopbackAddr(addr string) bool {
	if strings.EqualFold(addr, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(addr, "[]"))
	return ip != nil && ip.IsLoopback()
}

// checkDirWritable ensures dir exists (creating it if needed) and that this
// process can actually create files in it, surfacing a clear, actionable
// error at startup instead of a raw filesystem permission error surfacing
// later, deep inside a snapshot attempt. This is the common failure mode
// for a --db-snapshot-dir bind-mounted from a host directory the
// container's non-root user doesn't own (see the Docker image's pinned
// UID:GID note in dingo.yaml.example).
func checkDirWritable(dir string) (err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	probe, err := os.CreateTemp(dir, ".dingo-writable-check-*")
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}
	name := probe.Name()
	// Deferred (rather than a plain call after Close) so the probe file is
	// still cleaned up on every path, including one a later change might
	// add between Close and here that returns early.
	defer func() {
		if removeErr := os.Remove(name); removeErr != nil && err == nil {
			err = fmt.Errorf("remove writability probe file: %w", removeErr)
		}
	}()
	return probe.Close()
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
