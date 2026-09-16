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

package main

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/stretchr/testify/assert"
)

// TestSplitStakeMismatches pins the from-genesis report's separation of a
// real Dingo/Koios divergence from a KoiosFault entry (an unparseable koios
// active_stake value): a human reviewer's finding was that counting the
// latter toward stakeMismatches pages on Koios's own data quality rather
// than a real Dingo bug, mirroring the wrong outcome DetermineStatus
// already prevents on the protocol-params side.
func TestSplitStakeMismatches(t *testing.T) {
	real := nodeparity.StakeMismatch{
		PoolIDBech32: "pool1real",
		DingoStake:   100,
		KoiosStake:   "50",
		DiffLovelace: 50,
	}
	fault := nodeparity.StakeMismatch{
		PoolIDBech32: "pool1fault",
		DingoStake:   100,
		KoiosStake:   "not-a-number",
		Reason:       "unparseable koios active_stake value",
		KoiosFault:   true,
	}

	t.Run("empty input", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(nil)
		assert.Empty(t, gotReal)
		assert.Empty(t, gotFaults)
	})

	t.Run("real mismatch only", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(
			[]nodeparity.StakeMismatch{real},
		)
		assert.Equal(t, []nodeparity.StakeMismatch{real}, gotReal)
		assert.Empty(t, gotFaults)
	})

	t.Run("koios fault only", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(
			[]nodeparity.StakeMismatch{fault},
		)
		assert.Empty(t, gotReal)
		assert.Equal(t, []nodeparity.StakeMismatch{fault}, gotFaults)
	})

	t.Run("mixed keeps both, in order", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(
			[]nodeparity.StakeMismatch{real, fault},
		)
		assert.Equal(t, []nodeparity.StakeMismatch{real}, gotReal)
		assert.Equal(t, []nodeparity.StakeMismatch{fault}, gotFaults)
	})
}

// TestFromGenesisCounters_RecordEpoch drives recordEpoch directly with a
// synthetic EpochResult and asserts on the resulting counters -- the
// regression a human reviewer found (Chris Guiney, dingo#4319):
// TestSplitStakeMismatches above proves the partition helper is correct in
// isolation, but reverting recordEpoch's stake branch to the old
// "stakeMismatches++ for any non-empty StakeMismatches" shape left that
// test green, since it never calls recordEpoch at all. Also covers the
// Incomplete counters (dingo#4319): without them, a run whose every check
// was degraded reports 0 mismatches across the board, indistinguishable
// from a clean run.
func TestFromGenesisCounters_RecordEpoch(t *testing.T) {
	logger := discardLogger()

	t.Run("clean epoch: no counters move", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{Epoch: 1, UTxOAttempted: true}, logger)
		assert.Equal(t, fromGenesisCounters{epochsChecked: 1}, c)
	})

	t.Run("real stake mismatch counts as a mismatch, not incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1,
			StakeMismatches: []nodeparity.StakeMismatch{{
				PoolIDBech32: "pool1real", DingoStake: 100, KoiosStake: "50", DiffLovelace: 50,
			}},
			UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 1, c.stakeMismatches)
		assert.Equal(t, 0, c.stakeIncomplete)
	})

	t.Run("koios-fault-only stake mismatch counts as incomplete, not a mismatch", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1,
			StakeMismatches: []nodeparity.StakeMismatch{{
				PoolIDBech32: "pool1fault", DingoStake: 100, KoiosStake: "not-a-number",
				Reason: "unparseable koios active_stake value", KoiosFault: true,
			}},
			UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.stakeMismatches,
			"a Koios data fault must not be counted as a Dingo divergence")
		assert.Equal(t, 1, c.stakeIncomplete)
	})

	t.Run("StakeErr counts as incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, StakeErr: assert.AnError, UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.stakeMismatches)
		assert.Equal(t, 1, c.stakeIncomplete)
	})

	t.Run("ProtocolParamsErr counts as incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, ProtocolParamsErr: assert.AnError, UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.ppMismatches)
		assert.Equal(t, 1, c.ppIncomplete)
	})

	t.Run("UTxOErr counts as incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, UTxOAttempted: true, UTxOErr: assert.AnError,
		}, logger)
		assert.Equal(t, 0, c.utxoMismatches)
		assert.Equal(t, 1, c.utxoIncomplete)
	})

	t.Run("UTxO never attempted counts as incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{Epoch: 1, UTxOAttempted: false}, logger)
		assert.Equal(t, 0, c.utxoMismatches)
		assert.Equal(t, 1, c.utxoIncomplete)
	})

	t.Run("real UTxO mismatch counts as a mismatch, not incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, UTxOAttempted: true, UTxOMissing: []string{"abc#0"},
		}, logger)
		assert.Equal(t, 1, c.utxoMismatches)
		assert.Equal(t, 0, c.utxoIncomplete)
	})

	t.Run("epochsChecked increments once per call, across multiple epochs", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{Epoch: 1, UTxOAttempted: true}, logger)
		c.recordEpoch(nodeparity.EpochResult{Epoch: 2, UTxOAttempted: true}, logger)
		assert.Equal(t, 2, c.epochsChecked)
	})
}
