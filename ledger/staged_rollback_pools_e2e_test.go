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

package ledger_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A pool the boundary removed above a staged rollback's target is a candidate
// of the pool walk that applies the next boundary, though the set's pool rows
// no longer hold it.
func TestStagedRollbackRestoresAReapedPoolToTheBoundaryPoolWalk(t *testing.T) {
	t.Parallel()
	scenario, set, _ := setModeScenario(t)
	slots := scenario.Pools(func() {})
	states := map[string]bool{}
	require.NoError(t, set.PoolStates(func(st *ledger.PoolState) error {
		states[string(st.Pool)] = true
		return nil
	}))
	require.False(t, states[string(slots.Retired)], "the boundary reaped it")
	require.Contains(t, scenario.PoolKeysInsideRollback(), slots.Retired)
}
