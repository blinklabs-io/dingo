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

// Package nodesettings holds the policy for settings that are persisted on
// first start and enforced on every subsequent start. It has no database or
// configuration dependencies so the policy can be tested in isolation.
package nodesettings

// Class determines how a gate reacts when the configured value differs from
// the persisted one.
type Class int

const (
	// Frozen requires the configured value to equal the persisted one.
	Frozen Class = iota
	// FrozenFillOnce behaves like Frozen but treats an empty configured
	// value as "not known yet" and an empty persisted value as fillable.
	FrozenFillOnce
	// LatchEnum permits movement forward through Ordered only.
	LatchEnum
	// LatchBool permits off to on only. Values are LatchOff, LatchOn, or
	// LatchOn plus a carried value (see EncodeLatchBool).
	LatchBool
	// Taint is a sticky bit: once on it stays on and stops being compared,
	// and it may never be turned on for a database that lacks it.
	Taint
)

// Latch and taint value encodings.
const (
	LatchOff = "off"
	LatchOn  = "on"
)

// NoStartEra is the canonical persisted value recording that a database was
// created (or last confirmed) with no start era override. start_era's class
// is FrozenFillOnce, which treats an empty configured value as "not known
// yet" (see evaluate.go) -- correct for a caller that genuinely cannot
// supply the gate, but wrong for the ordinary case of a full node startup
// with no --start-era flag, where "" meant "nothing to compare" and the
// gate was silently never recorded. Every writer that can distinguish "no
// start era" from "don't know" must persist this sentinel instead of "",
// so a later --start-era dijkstra against that database is compared against
// something rather than filling in for free.
const NoStartEra = "none"

// EncodeLatchBool renders a LatchBool value. carried is the associated
// setting, such as a pledge leverage factor, and is empty for gates that
// carry nothing.
func EncodeLatchBool(enabled bool, carried string) string {
	if !enabled {
		return LatchOff
	}
	if carried == "" {
		return LatchOn
	}
	return LatchOn + ":" + carried
}

// Gate describes one persisted setting.
type Gate struct {
	// Name is the key stored in node_settings_gate.
	Name string
	// Label is the operator-facing name used in error messages. It defaults
	// to Name when empty. storage_mode and network must label themselves
	// "storage mode" and "network" to preserve the message wording that
	// existing tests in database/storage_mode_test.go assert against.
	Label string
	// Class selects the enforcement rule.
	Class Class
	// Ordered lists permitted LatchEnum values in transition order. It is
	// unused by every other class.
	Ordered []string
	// OverrideEligible allows a persisted value to supply the effective
	// value when the configured one was not set explicitly. A gate is
	// eligible only when its persisted value is self-sufficient, needing
	// no companion configuration that is not itself persisted.
	OverrideEligible bool
	// Remedy overrides the operator-facing fix description used in a
	// Mismatch's Reason for the Frozen and FrozenFillOnce classes, whose
	// default reason ("changing it requires re-syncing from scratch") is
	// wrong for a gate whose real fix is not a resync. Empty uses the
	// default.
	Remedy string
}

// defaultRemedy is the reason text every Frozen/FrozenFillOnce gate uses
// unless it sets Remedy.
const defaultRemedy = "changing it requires re-syncing from scratch"

// remedy returns g's operator-facing fix description, falling back to
// defaultRemedy when Remedy is unset.
func (g Gate) remedy() string {
	if g.Remedy != "" {
		return g.Remedy
	}
	return defaultRemedy
}

// label returns the operator-facing name, falling back to the stored name.
func (g Gate) label() string {
	if g.Label != "" {
		return g.Label
	}
	return g.Name
}

// Gates returns the registry. Adding an entry here plus its test cases is the
// entire cost of gating a new setting; no schema change is needed.
func Gates() []Gate {
	return []Gate{
		{
			Name:             "network",
			Label:            "network",
			Class:            FrozenFillOnce,
			OverrideEligible: true,
		},
		{
			Name:             "network_magic",
			Label:            "network magic",
			Class:            FrozenFillOnce,
			OverrideEligible: true,
		},
		{
			Name:             "start_era",
			Label:            "start era",
			Class:            FrozenFillOnce,
			OverrideEligible: true,
		},
		{
			Name: "storage_mode",
			// "storage mode", not "storage_mode": existing tests in
			// database/storage_mode_test.go assert this wording.
			Label:            "storage mode",
			Class:            LatchEnum,
			Ordered:          []string{"api", "core"},
			OverrideEligible: true,
		},
		{
			Name:             "history_expiry_active",
			Label:            "history expiry",
			Class:            LatchBool,
			OverrideEligible: true,
		},
		{
			Name:             "pledge_leverage",
			Label:            "pledge leverage",
			Class:            LatchBool,
			OverrideEligible: true,
		},
		// full_pot_rewards is deliberately NOT override-eligible, unlike the
		// other four ledger LatchBools below: its companion,
		// UnsafeFullPotRewardsOnStandardNetworks, is neither gated nor
		// persisted, so restoring this gate alone from a database would
		// enable full-pot rewards without the flag that makes them usable on
		// a standard network -- a configured-but-unusable state, exactly
		// what OverrideEligible's own doc comment above requires a gate not
		// have. Do not re-add OverrideEligible here without also gating and
		// persisting that companion flag.
		{
			Name:  "full_pot_rewards",
			Label: "full-pot rewards",
			Class: LatchBool,
		},
		{
			Name:             "delegator_inactivity",
			Label:            "delegator inactivity",
			Class:            LatchBool,
			OverrideEligible: true,
		},
		{
			Name:             "min_pool_margin",
			Label:            "minimum pool margin",
			Class:            LatchBool,
			OverrideEligible: true,
		},
		// Taints are records, not settings to restore, so they are never
		// override-eligible: overriding from a set taint would force relaxed
		// validation onto a resume.
		{
			Name:  "historical_validation_relaxed",
			Label: "historical validation",
			Class: Taint,
		},
		{
			Name:  "strict_utxo_validation_relaxed",
			Label: "strict UTxO validation",
			Class: Taint,
		},
		// Genesis hashes derive from the configured files rather than being
		// independently settable, so they are validate-only.
		{
			Name:  "byron_genesis_hash",
			Label: "Byron genesis hash",
			Class: FrozenFillOnce,
		},
		{
			Name:  "shelley_genesis_hash",
			Label: "Shelley genesis hash",
			Class: FrozenFillOnce,
		},
		{
			Name:  "alonzo_genesis_hash",
			Label: "Alonzo genesis hash",
			Class: FrozenFillOnce,
		},
		{
			Name:  "conway_genesis_hash",
			Label: "Conway genesis hash",
			Class: FrozenFillOnce,
		},
		{
			Name:  "dijkstra_genesis_hash",
			Label: "Dijkstra genesis hash",
			Class: FrozenFillOnce,
		},
		// metadata_plugin holds the settings, so it cannot be selected by
		// them. blob_plugin's bucket and credentials are not persisted, so
		// restoring the name alone would produce an unusable store.
		{Name: "metadata_plugin", Label: "metadata plugin", Class: Frozen},
		{Name: "blob_plugin", Label: "blob plugin", Class: Frozen},
		{
			Name:  "blob_store_id",
			Label: "blob store ID",
			Class: Frozen,
			Remedy: "point this node at the blob store this database was " +
				"created with, rather than re-syncing",
		},
	}
}
