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

package dingo

import (
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// TestNodeSettingsGateValuesAssemblesLedgerAndGenesisGates covers the one
// piece of the phase 2 gate-enforcement wiring that is reachable without
// booting a full Node: nodeSettingsGateValues, the assembly function Run
// calls from both the normal-startup call site and the deferred,
// post-recovery call site. Testing it here is exactly what makes "factor
// so both call sites use the same values" a real guarantee rather than an
// aspiration -- a future edit that changes one call site's inputs without
// updating this function would be caught here.
//
// This does not, and cannot without booting a real Node through Run,
// exercise the control flow itself: that dbNeedsRecovery defers the call
// rather than skipping it, and that the deferred call runs immediately
// after RecoverCommitTimestampConflict and before history expiry, the
// Midnight indexer, or any network listener starts. Run is a single large
// method whose body constructs the ledger state, event bus, chain
// selector, and every network listener as a side effect of reaching that
// code, so isolating just the recovery-then-enforce sequence would require
// either duplicating most of Run's setup or refactoring Run to extract a
// narrower seam -- out of scope for this fix. That gap is a known,
// explicitly accepted one (see the coordinator's note deferring a
// DevNet/integration-level test of the real Node.Run wiring), not a gap
// this test is pretending to close.
func TestNodeSettingsGateValuesAssemblesLedgerAndGenesisGates(t *testing.T) {
	n := &Node{
		config: Config{
			validateHistorical:         false, // relaxed: taint "on"
			strictUtxoValidation:       true,  // not relaxed: taint "off"
			historyExpiry:              HistoryExpiryConfig{Enabled: true},
			pledgeLeverageEnabled:      true,
			pledgeLeverage:             3,
			fullPotRewardsEnabled:      true,
			delegatorInactivityEnabled: true,
			delegatorInactivity:        5,
			minPoolMargin:              10,
			cardanoNodeConfig: &cardano.CardanoNodeConfig{
				ByronGenesisHash:    "byronhash",
				ShelleyGenesisHash:  "shelleyhash",
				AlonzoGenesisHash:   "alonzohash",
				ConwayGenesisHash:   "conwayhash",
				DijkstraGenesisHash: "",
			},
		},
	}
	values := n.nodeSettingsGateValues()

	require.Equal(
		t,
		nodesettings.LatchOn,
		values["historical_validation_relaxed"],
	)
	require.Equal(
		t,
		nodesettings.LatchOff,
		values["strict_utxo_validation_relaxed"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, ""),
		values["history_expiry_active"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, "3"),
		values["pledge_leverage"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, ""),
		values["full_pot_rewards"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, "5"),
		values["delegator_inactivity"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, "10"),
		values["min_pool_margin"],
	)
	require.Equal(t, "byronhash", values["byron_genesis_hash"])
	require.Equal(t, "shelleyhash", values["shelley_genesis_hash"])
	require.Equal(t, "alonzohash", values["alonzo_genesis_hash"])
	require.Equal(t, "conwayhash", values["conway_genesis_hash"])
	// Left empty by the loaded cardano config, so this is passed through
	// as "" rather than omitted: EnforceNodeSettings's FrozenFillOnce
	// class treats an empty configured value as "not known yet," not a
	// mismatch, which is exactly what an era whose hash an older dingo
	// build didn't know about needs.
	require.Equal(t, "", values["dijkstra_genesis_hash"])
}

// TestNodeSettingsGateValuesOmitsGenesisHashesWithoutCardanoConfig covers
// the guard mirrored from config.go's own nil check
// (`n.config.CardanoNodeConfig() != nil`): a caller with no loaded cardano
// config -- true for every gate-enforcement call before the config is
// parsed, and the reason phase 2 cannot run any earlier than it does --
// must not synthesize genesis-hash keys at all, matching
// nodesettings.Evaluate's "absent from configured is skipped" rule rather
// than passing five empty strings that would incorrectly resolve like a
// config that loaded but left every hash unset.
func TestNodeSettingsGateValuesOmitsGenesisHashesWithoutCardanoConfig(
	t *testing.T,
) {
	n := &Node{config: Config{}}
	values := n.nodeSettingsGateValues()

	for _, gate := range []string{
		"byron_genesis_hash",
		"shelley_genesis_hash",
		"alonzo_genesis_hash",
		"conway_genesis_hash",
		"dijkstra_genesis_hash",
	} {
		_, present := values[gate]
		require.False(t, present, "gate %q should be absent, not empty", gate)
	}
}
