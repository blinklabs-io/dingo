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

// Package settingsresolve lets a data directory's already-persisted node
// settings supply the effective value for any override-eligible gate an
// operator left at its built-in default, before the rest of configuration
// resolution runs. This is the mechanism behind "dingo -n preprod", a stop,
// then a bare "dingo" resuming preprod instead of failing to sync from
// scratch -- while "dingo -n preview" against that same database is still a
// fatal error naming the conflict, because an operator-supplied value is
// never silently discarded.
//
// Apply opens only the metadata store -- read-only in effect, since it
// persists nothing itself; the real database.New does that -- and only
// long enough to read the legacy node_settings row and the
// node_settings_gate table. It never touches the blob store: badger's
// exclusive directory lock would block the real open moments later, and
// merely opening it would mint a blob_store_id as a side effect of reading
// configuration.
package settingsresolve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/plugin"
)

// Apply reads cfg.DatabasePath's persisted node settings and, for every
// override-eligible gate (database/nodesettings.Gates) the operator did not
// set explicitly, replaces cfg's built-in default with the persisted value.
// An operator-supplied value that conflicts with what the database already
// is returns an error naming the gate, both values, and which configuration
// source supplied the value -- it never gets silently overridden.
//
// Apply is a no-op, returning nil, whenever there is nothing safe or useful
// to resolve: a database directory that doesn't exist yet (a first run), one
// that exists but is empty (a pre-created volume mount, or a `dingo database
// restore` target), or one this process could not open for any reason. A
// corrupt or in-use database is database.New's problem to report properly;
// failing here would only mask its better error behind a worse one.
//
// On every nil-returning path -- including the no-op ones above, where cfg
// is left completely unchanged -- Apply publishes cfg via
// config.PublishConfig before returning. This matters even when nothing was
// overridden: LoadConfig and ApplyFlags already publish their own results
// to the same process-wide snapshot, and cmd/dingo's later
// LoadTopologyConfig call reads that snapshot via config.GetConfig rather
// than the *Config pointer being threaded through main.go's
// PersistentPreRunE. Without Apply publishing too, an override it made
// (e.g. resuming Network from a persisted gate) would be visible on cfg
// itself but invisible to GetConfig, so topology would resolve against the
// stale pre-override network while every other consumer of cfg used the
// resumed one -- a real, previously-shipped bug (a bare resume dialed the
// wrong network's relays while handshaking with the resumed network's
// magic) that Apply changing cfg without republishing caused.
func Apply(cfg *config.Config) (err error) {
	defer func() {
		if err == nil {
			config.PublishConfig(cfg)
		}
	}()

	if _, statErr := os.Stat(cfg.DatabasePath); statErr != nil {
		// No database directory: a first run, or a path about to be
		// created as part of this very start. Either way there is
		// nothing persisted to resume from. A non-IsNotExist stat error
		// (e.g. permission denied) is deliberately treated the same way:
		// database.New will hit and report the identical problem properly
		// moments later.
		slog.Debug(
			"settingsresolve: database path not accessible; nothing to resume from",
			"path",
			cfg.DatabasePath,
			"error",
			statErr,
		)
		return nil
	}

	// An existing-but-empty directory (a freshly mounted container/k8s
	// volume, or a `dingo database restore` target) is, for this purpose,
	// no different from one that does not exist yet: there is nothing
	// persisted to resume from. Without this check, readPersistedGateValues
	// below would resolve a metadata provider, which runs its migration
	// registry as a side effect of merely starting it -- silently creating a
	// database in a directory a caller like `dingo database restore` needs
	// to still find empty (lifecycle.RestoreValidated's
	// requireEmptyOrAbsent check would then reject it), and wastefully
	// double-migrating on a first `dingo serve` into a pre-created
	// directory.
	if entries, readErr := os.ReadDir(cfg.DatabasePath); readErr != nil {
		// Same reasoning as the stat error above: database.New will hit
		// and report this properly.
		slog.Debug(
			"settingsresolve: database path not readable; nothing to resume from",
			"path",
			cfg.DatabasePath,
			"error",
			readErr,
		)
		return nil
	} else if len(entries) == 0 {
		slog.Debug(
			"settingsresolve: database path exists but is empty; nothing to resume from",
			"path", cfg.DatabasePath,
		)
		return nil
	}

	if err := checkMetadataPluginSidecar(cfg); err != nil {
		return err
	}

	persisted, ok := readPersistedGateValues(cfg)
	if !ok {
		return nil
	}

	provenance := cfg.Provenance()
	eligible := overrideEligibleGateNames()
	bindings := make(map[string]gateBinding, len(eligible))
	configured := make(nodesettings.Values, len(eligible))
	explicit := make(map[string]bool, len(eligible))
	for _, binding := range gateBindings() {
		if !eligible[binding.name] {
			continue
		}
		value, ok := binding.configuredValue(cfg)
		if !ok {
			continue
		}
		bindings[binding.name] = binding
		configured[binding.name] = value
		explicit[binding.name] = explicitSource(
			provenance, binding.provenance,
		) != config.SourceDefault
	}

	result := nodesettings.Evaluate(persisted, configured, explicit)
	if len(result.Mismatches) > 0 {
		return mismatchError(result.Mismatches, bindings, provenance)
	}
	for name, effective := range result.Effective {
		if effective == configured[name] {
			continue
		}
		if binding, ok := bindings[name]; ok {
			binding.applyEffective(cfg, effective)
		}
	}
	return nil
}

// checkMetadataPluginSidecar reads the dbinfo sidecar before anything else
// touches the data directory. This ordering is mandatory: resolving a
// metadata store runs its migration registry as a side effect of merely
// starting it, so opening the wrong provider would silently create a fresh,
// empty database beside the real one -- precisely the silent fresh sync the
// sidecar exists to prevent. A missing or unreadable sidecar is not this
// function's problem to report; it only ever blocks on a sidecar that is
// present and names a plugin other than the one about to be opened.
func checkMetadataPluginSidecar(cfg *config.Config) error {
	info, err := dbinfo.Read(cfg.DatabasePath)
	if err != nil {
		// ErrIncompleteSidecar is not one of the advisory cases below: it
		// means a sidecar file exists but was never completed with a
		// plugin name (interrupted write, hand edit, or a future writer
		// bug), which is indistinguishable from "sidecar absent" unless
		// this is checked explicitly -- and "absent" is exactly what falls
		// through to the no-op default below. Warning and proceeding on
		// this specific error would resolve whatever metadata plugin is
		// configured and run its migrations as a side effect, silently
		// creating a fresh, empty database beside the real one if that
		// configured plugin happens to be wrong -- precisely the outcome
		// this whole check exists to prevent. So this one fails startup
		// instead.
		if errors.Is(err, dbinfo.ErrIncompleteSidecar) {
			return fmt.Errorf(
				"settingsresolve: %w; refusing to guess a metadata plugin "+
					"for %q",
				err, cfg.DatabasePath,
			)
		}
		// dbinfo.Read returns a nil error for a simply-missing sidecar (the
		// common, unremarkable case for a database that predates it); every
		// other error here means a sidecar that is present but corrupt or
		// from a FormatVersion this build does not understand -- a
		// deliberate forward-compatibility choice, not a case to fail
		// startup over. That silently and permanently disables this
		// pre-open check, so it is worth a Warn rather than a Debug: an
		// operator should be able to see why database.New's mismatch
		// detection stopped catching a wrong-plugin start before it creates
		// a fresh, empty database.
		slog.Warn(
			"settingsresolve: dbinfo sidecar present but unreadable; "+
				"pre-open metadata plugin check disabled",
			"error",
			err,
		)
		return nil
	}
	configuredPlugin := cfg.Plugins.Storage.Metadata.Provider
	if info.MetadataPlugin == "" || info.MetadataPlugin == configuredPlugin {
		return nil
	}
	return fmt.Errorf(
		"database at %q was created with metadata plugin %q but the "+
			"configured metadata plugin is %q",
		cfg.DatabasePath, info.MetadataPlugin, configuredPlugin,
	)
}

// readPersistedGateValues opens only the metadata store at cfg.DatabasePath
// and merges the legacy node_settings row with node_settings_gate the same
// way database/commit_timestamp.go's persistedGateValues does: the legacy
// row is read first and node_settings_gate is copied on top of it, since
// node_settings_gate is authoritative for every gate including storage_mode
// and network. The store and its plugin host are closed on every path,
// including every error path, before this returns -- leaving either open
// would block the real database.New that follows.
//
// ok is false whenever the store could not be opened or read for any
// reason, in which case the caller treats this as "nothing to resolve" and
// leaves database.New to report the real problem properly.
func readPersistedGateValues(
	cfg *config.Config,
) (values nodesettings.Values, ok bool) {
	ctx := context.Background()
	host, err := internalplugins.NewHost()
	if err != nil {
		slog.Debug(
			"settingsresolve: failed to build plugin host",
			"error", err,
		)
		return nil, false
	}
	defer func() {
		if stopErr := host.Stop(ctx); stopErr != nil {
			slog.Debug(
				"settingsresolve: failed to stop plugin host",
				"error", stopErr,
			)
		}
	}()

	store, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		cfg.Plugins.Storage.Metadata.Provider,
		cfg.Plugins.Storage.Metadata.Config,
		metadata.ProviderDependencies{
			DataDir:        cfg.DatabasePath,
			StorageMode:    cfg.StorageMode,
			MaxConnections: 1,
			Logger:         slog.Default(),
		},
	)
	if err != nil {
		slog.Debug(
			"settingsresolve: failed to open metadata store; leaving configuration unresolved",
			"error",
			err,
		)
		return nil, false
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			slog.Debug(
				"settingsresolve: failed to close metadata store",
				"error", closeErr,
			)
		}
	}()

	legacy, err := store.GetNodeSettings()
	if err != nil {
		slog.Debug(
			"settingsresolve: failed to read legacy node settings",
			"error", err,
		)
		return nil, false
	}
	gates, err := store.GetNodeSettingsGates()
	if err != nil {
		slog.Debug(
			"settingsresolve: failed to read node settings gates",
			"error", err,
		)
		return nil, false
	}

	values = make(nodesettings.Values, len(gates)+2)
	if legacy != nil {
		if legacy.StorageMode != "" {
			values["storage_mode"] = legacy.StorageMode
		}
		if legacy.Network != "" {
			values["network"] = legacy.Network
		}
	}
	maps.Copy(values, gates)
	return values, true
}

// overrideEligibleGateNames returns the set of gate names Apply is allowed
// to resolve from persisted state, straight from the registry rather than a
// separately maintained list, so a future registry change is picked up here
// automatically instead of silently going stale.
func overrideEligibleGateNames() map[string]bool {
	names := make(map[string]bool)
	for _, gate := range nodesettings.Gates() {
		if gate.OverrideEligible {
			names[gate.Name] = true
		}
	}
	return names
}

// gateBinding connects one override-eligible gate to the Config field(s)
// that carry its configured value and provenance.
type gateBinding struct {
	name string
	// provenance lists the dotted Config field path(s) (see
	// internal/config.GatedFieldPaths) whose provenance determines whether
	// this gate's configured value was operator-supplied. More than one path
	// applies to a gate encoding both an enabled flag and a companion value
	// (e.g. pledge_leverage: PledgeLeverageEnabled and PledgeLeverage).
	provenance []string
	// configuredValue returns cfg's current value for this gate, encoded
	// the same way database/nodesettings expects it to be persisted. ok is
	// false when this path cannot supply a meaningful value at all (only
	// network_magic today: a zero magic means "not known yet", the same
	// convention database/commit_timestamp.go's phase1GateValues uses, so
	// ApplyDefaults can still derive it from a Network this very call may
	// have just resumed).
	configuredValue func(cfg *config.Config) (string, bool)
	// applyEffective writes an overridden value back onto cfg.
	applyEffective func(cfg *config.Config, value string)
}

func gateBindings() []gateBinding {
	return []gateBinding{
		{
			name:       "network",
			provenance: []string{"Network"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return cfg.Network, true
			},
			applyEffective: func(cfg *config.Config, value string) {
				cfg.Network = value
			},
		},
		{
			name:       "network_magic",
			provenance: []string{"NetworkMagic"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				if cfg.NetworkMagic == 0 {
					return "", false
				}
				return strconv.FormatUint(
					uint64(cfg.NetworkMagic), 10,
				), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				magic, err := strconv.ParseUint(value, 10, 32)
				if err != nil {
					return
				}
				cfg.NetworkMagic = uint32(magic)
			},
		},
		{
			name:       "start_era",
			provenance: []string{"StartEra"},
			// cfg is always a fully-resolved *config.Config here, never one
			// of database.Config's partial callers, so an empty StartEra is
			// unambiguously "no start era" -- unlike phase1GateValues
			// (database/commit_timestamp.go), which has to guard against
			// a partial caller's zero value too. Emit the same
			// nodesettings.NoStartEra sentinel that writer uses so a
			// persisted "no start era" round-trips through Apply's override
			// path without decoding to two different things.
			configuredValue: func(cfg *config.Config) (string, bool) {
				if cfg.StartEra == "" {
					return nodesettings.NoStartEra, true
				}
				return string(cfg.StartEra), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				if value == nodesettings.NoStartEra {
					cfg.StartEra = ""
					return
				}
				cfg.StartEra = config.StartEra(value)
			},
		},
		{
			name:       "storage_mode",
			provenance: []string{"StorageMode"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return cfg.StorageMode, true
			},
			applyEffective: func(cfg *config.Config, value string) {
				cfg.StorageMode = value
			},
		},
		{
			name:       "history_expiry_active",
			provenance: []string{"HistoryExpiry.Enabled"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return nodesettings.EncodeLatchBool(
					cfg.HistoryExpiry.Enabled, "",
				), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				enabled, _ := decodeLatchBool(value)
				cfg.HistoryExpiry.Enabled = enabled
			},
		},
		{
			name:       "pledge_leverage",
			provenance: []string{"PledgeLeverageEnabled", "PledgeLeverage"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return nodesettings.EncodeLatchBool(
					cfg.PledgeLeverageEnabled,
					strconv.FormatUint(uint64(cfg.PledgeLeverage), 10),
				), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				enabled, carried := decodeLatchBool(value)
				cfg.PledgeLeverageEnabled = enabled
				if carried == "" {
					return
				}
				// strconv.IntSize, not 64: the destination is uint,
				// which is 32 bits on a 32-bit platform, so parsing
				// at 64 would silently truncate instead of erroring.
				if parsed, err := strconv.ParseUint(
					carried, 10, strconv.IntSize,
				); err == nil {
					cfg.PledgeLeverage = uint(parsed)
				}
			},
		},
		{
			name:       "full_pot_rewards",
			provenance: []string{"FullPotRewardsEnabled"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return nodesettings.EncodeLatchBool(
					cfg.FullPotRewardsEnabled, "",
				), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				enabled, _ := decodeLatchBool(value)
				cfg.FullPotRewardsEnabled = enabled
			},
		},
		{
			name: "delegator_inactivity",
			provenance: []string{
				"DelegatorInactivityEnabled", "DelegatorInactivity",
			},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return nodesettings.EncodeLatchBool(
					cfg.DelegatorInactivityEnabled,
					strconv.FormatUint(cfg.DelegatorInactivity, 10),
				), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				enabled, carried := decodeLatchBool(value)
				cfg.DelegatorInactivityEnabled = enabled
				if carried == "" {
					return
				}
				if parsed, err := strconv.ParseUint(
					carried, 10, 64,
				); err == nil {
					cfg.DelegatorInactivity = parsed
				}
			},
		},
		{
			name:       "min_pool_margin",
			provenance: []string{"MinPoolMargin"},
			configuredValue: func(cfg *config.Config) (string, bool) {
				return nodesettings.EncodeLatchBool(
					cfg.MinPoolMargin != 0,
					strconv.FormatUint(uint64(cfg.MinPoolMargin), 10),
				), true
			},
			applyEffective: func(cfg *config.Config, value string) {
				enabled, carried := decodeLatchBool(value)
				if !enabled {
					cfg.MinPoolMargin = 0
					return
				}
				// strconv.IntSize for the same reason as
				// pledge_leverage above: MinPoolMargin is a uint.
				if parsed, err := strconv.ParseUint(
					carried, 10, strconv.IntSize,
				); err == nil {
					cfg.MinPoolMargin = uint(parsed)
				}
			},
		},
	}
}

// decodeLatchBool is the inverse of nodesettings.EncodeLatchBool.
func decodeLatchBool(value string) (enabled bool, carried string) {
	if value == nodesettings.LatchOn {
		return true, ""
	}
	if after, ok := strings.CutPrefix(
		value, nodesettings.LatchOn+":",
	); ok {
		return true, after
	}
	return false, ""
}

// explicitSource returns the highest-precedence Source recorded across
// paths -- CLI flag outranks environment, which outranks YAML, which
// outranks the built-in default, matching Source's own declaration order
// (internal/config/provenance.go) -- so a gate backed by more than one
// Config field (e.g. pledge_leverage) reports whichever field the operator
// actually touched.
func explicitSource(
	provenance config.Provenance, paths []string,
) config.Source {
	best := config.SourceDefault
	for _, path := range paths {
		if source := provenance[path]; source > best {
			best = source
		}
	}
	return best
}

// mismatchError renders every mismatch Evaluate reported, each naming the
// gate, both values, the reason, and which configuration source supplied
// the value that conflicts -- so the operator knows which input to change.
func mismatchError(
	mismatches []nodesettings.Mismatch,
	bindings map[string]gateBinding,
	provenance config.Provenance,
) error {
	messages := make([]string, 0, len(mismatches))
	for _, mismatch := range mismatches {
		source := "the built-in default"
		if binding, ok := bindings[mismatch.Gate]; ok {
			source = sourceLabel(
				explicitSource(provenance, binding.provenance),
			)
		}
		messages = append(messages, fmt.Sprintf(
			"%s (configured via %s)", mismatch.String(), source,
		))
	}
	return errors.New(
		"persisted node settings conflict with configuration: " +
			strings.Join(messages, "; "),
	)
}

// sourceLabel renders a Source for an operator-facing error message.
func sourceLabel(source config.Source) string {
	switch source {
	case config.SourceFlag:
		return "a CLI flag"
	case config.SourceEnv:
		return "an environment variable"
	case config.SourceYAML:
		return "the YAML config file"
	case config.SourceDefault:
		return "the built-in default"
	default:
		return "the built-in default"
	}
}
