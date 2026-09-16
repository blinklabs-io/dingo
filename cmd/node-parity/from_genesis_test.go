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
