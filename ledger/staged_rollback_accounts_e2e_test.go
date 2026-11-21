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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// An account whose delegation a staged rollback restores is a candidate of
// every account list read, though the set's pool and DRep indexes name it no
// longer: a transaction that staged the rollback reads what the committed
// rollback leaves.
func TestStagedRollbackRestoresReapedDelegationsToAccountListReads(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, "set")
	h.scenario.ApplyBoundary()
	drep := bytes.Repeat([]byte{0x5d}, 28)
	before := h.scenario.CommittedAccountReads(h.pool, drep)
	require.Len(t, h.requireAccountListReads(t, "initial", drep), 2)
	h.scenario.Reap(h.pool)
	require.Empty(t, h.requireAccountListReads(t, "after reap", drep))
	staged := h.scenario.StagedRollbackAccountReads(h.pool, drep)
	h.scenario.RollbackBeforeBoundary()
	committed := h.scenario.CommittedAccountReads(h.pool, drep)
	require.Equal(t, committed, staged,
		"the staged rollback view must match the committed rollback")
	require.Equal(t, before, committed, "the rollback restores the delegations")
}
