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

package nodesettings_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// allExplicit marks every configured gate as explicitly set, which is how
// database.New calls Evaluate.
func allExplicit(configured nodesettings.Values) map[string]bool {
	explicit := make(map[string]bool, len(configured))
	for name := range configured {
		explicit[name] = true
	}
	return explicit
}

func TestEvaluateFirstStartWritesEverything(t *testing.T) {
	configured := nodesettings.Values{
		"network":      "preprod",
		"storage_mode": "api",
	}
	result := nodesettings.Evaluate(nil, configured, allExplicit(configured))
	require.Empty(t, result.Mismatches)
	require.Equal(t, "preprod", result.Writes["network"])
	require.Equal(t, "api", result.Writes["storage_mode"])
	require.Equal(t, "preprod", result.Effective["network"])
}

func TestEvaluateFrozenMismatchIsFatal(t *testing.T) {
	persisted := nodesettings.Values{"metadata_plugin": "postgres"}
	configured := nodesettings.Values{"metadata_plugin": "sqlite"}
	result := nodesettings.Evaluate(
		persisted, configured, allExplicit(configured),
	)
	require.Len(t, result.Mismatches, 1)
	require.Equal(t, "metadata_plugin", result.Mismatches[0].Gate)
	require.Equal(t, "postgres", result.Mismatches[0].Persisted)
	require.Equal(t, "sqlite", result.Mismatches[0].Configured)
	require.Empty(t, result.Writes)
}

func TestMismatchMessageUsesOperatorLabel(t *testing.T) {
	// database/storage_mode_test.go asserts the message contains
	// "storage mode" with a space, so the label must not leak the
	// underscored storage key.
	persisted := nodesettings.Values{"storage_mode": "core"}
	configured := nodesettings.Values{"storage_mode": "api"}
	result := nodesettings.Evaluate(
		persisted, configured, map[string]bool{"storage_mode": true},
	)
	require.Len(t, result.Mismatches, 1)
	require.Contains(t, result.Mismatches[0].String(), "storage mode")
	require.NotContains(t, result.Mismatches[0].String(), "storage_mode")
}

func TestEvaluateFillOnceSkipsEmptyConfigured(t *testing.T) {
	persisted := nodesettings.Values{"network": "preprod"}
	configured := nodesettings.Values{"network": ""}
	result := nodesettings.Evaluate(
		persisted, configured, allExplicit(configured),
	)
	require.Empty(t, result.Mismatches)
	require.Empty(t, result.Writes)
	require.Equal(t, "preprod", result.Effective["network"])
}

func TestEvaluateFillOnceFillsEmptyPersisted(t *testing.T) {
	persisted := nodesettings.Values{"network": ""}
	configured := nodesettings.Values{"network": "preprod"}
	result := nodesettings.Evaluate(
		persisted, configured, allExplicit(configured),
	)
	require.Empty(t, result.Mismatches)
	require.Equal(t, "preprod", result.Writes["network"])
}

func TestEvaluateOverrideResumesPersistedWhenNotExplicit(t *testing.T) {
	// The operator scenario: `dingo -n preprod`, then bare `dingo`, whose
	// network came from the "preview" default.
	persisted := nodesettings.Values{"network": "preprod"}
	configured := nodesettings.Values{"network": "preview"}
	result := nodesettings.Evaluate(
		persisted, configured, map[string]bool{"network": false},
	)
	require.Empty(t, result.Mismatches)
	require.Empty(t, result.Writes)
	require.Equal(t, "preprod", result.Effective["network"])
}

func TestEvaluateExplicitConflictIsFatal(t *testing.T) {
	// `dingo -n preview` against a preprod database.
	persisted := nodesettings.Values{"network": "preprod"}
	configured := nodesettings.Values{"network": "preview"}
	result := nodesettings.Evaluate(
		persisted, configured, map[string]bool{"network": true},
	)
	require.Len(t, result.Mismatches, 1)
	require.Equal(t, "network", result.Mismatches[0].Gate)
}

func TestEvaluateStorageModeLatchNeedsExplicitToPrune(t *testing.T) {
	persisted := nodesettings.Values{"storage_mode": "api"}
	configured := nodesettings.Values{"storage_mode": "core"}

	// Bare `dingo`: "core" is the built-in default, so the database resumes
	// api mode rather than pruning.
	resumed := nodesettings.Evaluate(
		persisted, configured, map[string]bool{"storage_mode": false},
	)
	require.Empty(t, resumed.Mismatches)
	require.Equal(t, "api", resumed.Effective["storage_mode"])
	require.Empty(t, resumed.Writes)

	// Explicit --storage-mode core: the permitted api to core transition.
	pruned := nodesettings.Evaluate(
		persisted, configured, map[string]bool{"storage_mode": true},
	)
	require.Empty(t, pruned.Mismatches)
	require.Equal(t, "core", pruned.Effective["storage_mode"])
	require.Equal(t, "core", pruned.Writes["storage_mode"])
}

func TestEvaluateStorageModeReverseLatchIsFatal(t *testing.T) {
	persisted := nodesettings.Values{"storage_mode": "core"}
	configured := nodesettings.Values{"storage_mode": "api"}
	result := nodesettings.Evaluate(
		persisted, configured, map[string]bool{"storage_mode": true},
	)
	require.Len(t, result.Mismatches, 1)
	require.Equal(t, "storage_mode", result.Mismatches[0].Gate)
}

func TestEvaluateLatchBoolForwardAndReverse(t *testing.T) {
	on := nodesettings.EncodeLatchBool(true, "")
	off := nodesettings.EncodeLatchBool(false, "")

	forward := nodesettings.Evaluate(
		nodesettings.Values{"history_expiry_active": off},
		nodesettings.Values{"history_expiry_active": on},
		map[string]bool{"history_expiry_active": true},
	)
	require.Empty(t, forward.Mismatches)
	require.Equal(t, on, forward.Writes["history_expiry_active"])

	reverse := nodesettings.Evaluate(
		nodesettings.Values{"history_expiry_active": on},
		nodesettings.Values{"history_expiry_active": off},
		map[string]bool{"history_expiry_active": true},
	)
	require.Len(t, reverse.Mismatches, 1)
}

func TestEvaluateLatchBoolCarriedValueChangeIsFatal(t *testing.T) {
	result := nodesettings.Evaluate(
		nodesettings.Values{
			"delegator_inactivity": nodesettings.EncodeLatchBool(true, "26"),
		},
		nodesettings.Values{
			"delegator_inactivity": nodesettings.EncodeLatchBool(true, "30"),
		},
		map[string]bool{"delegator_inactivity": true},
	)
	require.Len(t, result.Mismatches, 1)
	require.Equal(t, "delegator_inactivity", result.Mismatches[0].Gate)
}

func TestEvaluateTaintStaysSetWhenConfigTightens(t *testing.T) {
	// The database was synced relaxed. Tightening now is allowed and must
	// not clear the bit, because the old range is still unverified.
	result := nodesettings.Evaluate(
		nodesettings.Values{
			"historical_validation_relaxed": nodesettings.LatchOn,
		},
		nodesettings.Values{
			"historical_validation_relaxed": nodesettings.LatchOff,
		},
		map[string]bool{"historical_validation_relaxed": true},
	)
	require.Empty(t, result.Mismatches)
	require.Empty(t, result.Writes)
	require.Equal(
		t,
		nodesettings.LatchOn,
		result.Effective["historical_validation_relaxed"],
	)
}

func TestEvaluateTaintCannotBeSetOnUntaintedDatabase(t *testing.T) {
	result := nodesettings.Evaluate(
		nodesettings.Values{
			"historical_validation_relaxed": nodesettings.LatchOff,
		},
		nodesettings.Values{
			"historical_validation_relaxed": nodesettings.LatchOn,
		},
		map[string]bool{"historical_validation_relaxed": true},
	)
	require.Len(t, result.Mismatches, 1)
	require.Equal(t, "historical_validation_relaxed", result.Mismatches[0].Gate)
}

func TestEvaluateTaintOnFirstStartIsRecordedNotFatal(t *testing.T) {
	configured := nodesettings.Values{
		"historical_validation_relaxed": nodesettings.LatchOn,
	}
	result := nodesettings.Evaluate(nil, configured, allExplicit(configured))
	require.Empty(t, result.Mismatches)
	require.Equal(
		t,
		nodesettings.LatchOn,
		result.Writes["historical_validation_relaxed"],
	)
}

func TestEvaluateIgnoresGatesAbsentFromConfigured(t *testing.T) {
	persisted := nodesettings.Values{"byron_genesis_hash": "abc"}
	result := nodesettings.Evaluate(
		persisted, nodesettings.Values{}, map[string]bool{},
	)
	require.Empty(t, result.Mismatches)
	require.Empty(t, result.Writes)
}

func TestEvaluateCollectsEveryMismatch(t *testing.T) {
	persisted := nodesettings.Values{
		"network":         "preprod",
		"metadata_plugin": "postgres",
	}
	configured := nodesettings.Values{
		"network":         "preview",
		"metadata_plugin": "sqlite",
	}
	result := nodesettings.Evaluate(
		persisted, configured, allExplicit(configured),
	)
	require.Len(t, result.Mismatches, 2)
}

// TestGatesRegistryIsComplete fails when a gate is added without test
// coverage, so no gate ships unexercised.
func TestGatesRegistryIsComplete(t *testing.T) {
	covered := map[string]bool{
		"network":                        true,
		"network_magic":                  true,
		"start_era":                      true,
		"storage_mode":                   true,
		"history_expiry_active":          true,
		"pledge_leverage":                true,
		"full_pot_rewards":               true,
		"delegator_inactivity":           true,
		"min_pool_margin":                true,
		"historical_validation_relaxed":  true,
		"strict_utxo_validation_relaxed": true,
		"byron_genesis_hash":             true,
		"shelley_genesis_hash":           true,
		"alonzo_genesis_hash":            true,
		"conway_genesis_hash":            true,
		"dijkstra_genesis_hash":          true,
		"metadata_plugin":                true,
		"blob_plugin":                    true,
		"blob_store_id":                  true,
	}
	for _, gate := range nodesettings.Gates() {
		require.True(
			t, covered[gate.Name],
			"gate %q has no test coverage; add cases then list it here",
			gate.Name,
		)
	}
	require.Len(t, nodesettings.Gates(), len(covered))
}

// TestFullPotRewardsIsNotOverrideEligible pins the project owner's ruling
// that full_pot_rewards must never be OverrideEligible: its companion,
// UnsafeFullPotRewardsOnStandardNetworks, is neither gated nor persisted, so
// resuming this gate alone from a database would enable full-pot rewards
// without the flag that makes them usable on a standard network -- a
// configured-but-unusable state that surfaces as a startup error naming a
// flag the operator never passed and never mentioning that the value came
// from the database. Re-adding OverrideEligible here without also gating
// and persisting that companion flag reintroduces exactly that bug.
func TestFullPotRewardsIsNotOverrideEligible(t *testing.T) {
	for _, gate := range nodesettings.Gates() {
		if gate.Name != "full_pot_rewards" {
			continue
		}
		require.False(t, gate.OverrideEligible)
		return
	}
	t.Fatal("full_pot_rewards gate not found in registry")
}
