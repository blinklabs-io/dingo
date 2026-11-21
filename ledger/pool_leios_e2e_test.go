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

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func certifiedPoolScenario(
	t *testing.T,
	reads string,
	beforeBoundary func(ledger.BoundaryEpochStateScenario, ledgerdb.LedgerDB),
) (ledger.BoundaryEpochStateScenario, ledgerdb.LedgerDB, ledger.CertifiedPoolSlots) {
	t.Helper()
	scenario, _, store, s := certifiedPoolScenarioWithSet(
		t, reads, beforeBoundary,
	)
	return scenario, store, s
}

func certifiedPoolScenarioWithSet(
	t *testing.T,
	reads string,
	beforeBoundary func(ledger.BoundaryEpochStateScenario, ledgerdb.LedgerDB),
) (
	ledger.BoundaryEpochStateScenario,
	*utxoset.Set,
	ledgerdb.LedgerDB,
	ledger.CertifiedPoolSlots,
) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.CertifyLeios()
	var hook func()
	if beforeBoundary != nil {
		hook = func() { beforeBoundary(scenario, store) }
	}
	return scenario, set, store, scenario.CertifiedPools(hook)
}

// The certificates differ in effective and applied slot as the scenario
// intends: the scenario is not a pre-Leios run in disguise.
func TestCertifiedPoolScenarioSeparatesEffectiveFromAppliedSlots(t *testing.T) {
	t.Parallel()
	scenario, _, s := certifiedPoolScenario(t, "set", nil)
	regs, err := scenario.LedgerDB().GetPoolRegistrationsAtSlot(
		[]lcommon.PoolKeyHash{lcommon.PoolKeyHash(s.Pool)}, s.RegB, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, s.VrfA, regs[0].VrfKeyHash,
		"the newer certificate is the earlier in effect")
	require.Equal(t, s.CtxA, regs[0].AddedSlot)
}
