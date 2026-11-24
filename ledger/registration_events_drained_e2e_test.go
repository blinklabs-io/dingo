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

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// newDrainableRewardRoundRun is a ledger whose boundary into epoch 6 applies a
// real reward round over blocks a Mithril anchor leaves imported, so every
// graduated family can be drained from it.
func newDrainableRewardRoundRun(
	t *testing.T,
	g metadata.StatementGuard,
) *rewardBlocksRun {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewRewardBlocksScenarioForTest(
		t, set, "set", g,
		ledger.RewardBlocksOptions{Imported: true, Round: true},
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	r := &rewardBlocksRun{scenario: scenario, set: set, imported: true}
	scenario.Collateral(0xc0, 1_000)
	scenario.Block(9, scenario.PoolB)
	scenario.Fee(9, 200)
	scenario.InvalidTotal(9, 300)
	scenario.Block(10, scenario.PoolA)
	scenario.Fee(10, 50)
	scenario.InvalidInputs(10, 0xc0, 400)
	scenario.Block(11, scenario.PoolB)
	scenario.Fee(11, 25)
	stakeUp(r)
	scenario.ConfigureLeiosValidator()
	scenario.Tip(11)
	return r
}

// drainedRewardRoundRun is a reward round over a ledger with every graduated
// family drained and the statement guard armed.
func drainedRewardRoundRun(
	t *testing.T,
) *rewardBlocksRun {
	t.Helper()
	r := newDrainableRewardRoundRun(t, nil)
	s := r.scenario
	s.InstallDrainedSources()
	r.set.ServeLedgerOnly()
	return r
}

func requireMemberUnspendable(
	t *testing.T,
	s ledger.RewardBlocksScenario,
) {
	t.Helper()
	out := s.Outputs(3)
	require.NoError(t, out.Err)
	var spendable, unspendable int
	for _, o := range out.Accounts {
		if o.Spendable {
			spendable++
		} else {
			unspendable++
			require.Equal(t, "member", o.RewardType)
		}
	}
	require.Equal(t, 1, spendable)
	require.Equal(t, 1, unspendable, "the deregistered member is unspendable")
	_, pending := s.Balance(bytes.Repeat([]byte{0x72}, 28))
	require.Zero(t, pending)
}

// With every graduated family drained, the boundary that applies a
// precomputed reward round finds a credential that deregistered after the
// round began from the registration events the set keeps.
func TestEveryGraduatedFamilyDrainedRewardRoundRechecksRegistrations(
	t *testing.T,
) {
	t.Parallel()
	r := drainedRewardRoundRun(t)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.DeregisterMemberInSet()
	s.Boundary()
	requireMemberUnspendable(t, s)
}

// The same after a rollback between the precompute and the boundary, which
// moves the set's rollback counter, so the recheck also rereads every
// credential with an output.
func TestEveryGraduatedFamilyDrainedRewardRoundRechecksAfterARollback(
	t *testing.T,
) {
	t.Parallel()
	r := drainedRewardRoundRun(t)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.RollbackBeforeBoundary()
	s.DeregisterMemberInSet()
	s.Tip(11)
	s.Boundary()
	requireMemberUnspendable(t, s)
}
