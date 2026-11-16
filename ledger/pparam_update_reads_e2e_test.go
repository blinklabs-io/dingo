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

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

func newPParamUpdateReadsScenario(
	t *testing.T,
) ledger.ClassicUpdateBoundaryScenario {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set, ledger.UtxoSetReadsVerify,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	return scenario
}

// TestPParamUpdateReadsAgreeWithTheLiveUtxoSet requires the proposals the
// set holds to be the ones the metadata store hands the update selection,
// for the forecast and for the boundary's enactment, through stored
// proposals of both kinds of transaction, a boundary and a rollback.
func TestPParamUpdateReadsAgreeWithTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario := newPParamUpdateReadsScenario(t)
	verified0, mismatches0 := scenario.PParamUpdateReads()
	reads := func(stage string) float64 {
		t.Helper()
		v, m := scenario.PParamUpdateReads()
		require.Equal(
			t,
			mismatches0,
			m,
			"%s: the set's proposals differ",
			stage,
		)
		n := v - verified0
		verified0 = v
		return n
	}

	require.NoError(t, scenario.Forecast(1))
	require.Equal(t, float64(1), reads("forecast of the seeded proposals"))

	at := scenario.RollbackTarget.Slot
	for _, p := range []ledger.UpdateProposal{
		{Slot: at, Epoch: 1, Index: 0, Keys: []byte{1, 2, 3}, MaxTxSize: 20_000},
		{Slot: at + 1, Epoch: 1, Index: 0, Keys: []byte{1, 2}, MaxTxSize: 21_000},
		{Slot: at + 1, Epoch: 1, Index: 1, Keys: []byte{1}, MaxTxSize: 22_000, Batched: true},
		{Slot: at + 2, Epoch: 1, Index: 0, Keys: []byte{4}, MaxTxSize: 23_000, Invalid: true},
		{Slot: at + 3, Epoch: 1, Index: 0, Keys: []byte{5}, MaxTxSize: 24_000, Batched: true},
	} {
		scenario.Propose(p)
	}
	require.NoError(t, scenario.Forecast(2))
	require.Equal(t, float64(1), reads("forecast of the proposed updates"))

	// The boundary's own reads are counted with the enactment's.
	require.NoError(t, scenario.EnactInsideTxn(1))
	require.Equal(t, float64(1), reads("enactment"))
	scenario.ApplyBoundary()
	require.Positive(t, reads("boundary"), "the boundary compared no read")

	scenario.RollbackBeforeBoundary()
	reads("rollback")
	require.NoError(t, scenario.Forecast(2))
	require.Equal(t, float64(1), reads("forecast after the rollback"))
}

// TestPParamUpdateReadsSeeTheirTransactionsStagedProposals requires the
// forecast in a write transaction to see the proposal that transaction has
// staged, as the metadata store does.
func TestPParamUpdateReadsSeeTheirTransactionsStagedProposals(t *testing.T) {
	t.Parallel()
	scenario := newPParamUpdateReadsScenario(t)
	at := scenario.RollbackTarget.Slot
	require.NoError(t, scenario.ForecastInsideTxn(ledger.UpdateProposal{
		Slot: at + 4, Epoch: 1, Keys: []byte{9}, MaxTxSize: 25_000,
	}, 2))
	verified, mismatches := scenario.PParamUpdateReads()
	require.Equal(t, float64(1), verified)
	require.Zero(t, mismatches, "the set lists the staged proposal")
}

// TestPParamUpdateReadVerificationCountsADivergence requires a verify-mode
// read to count a mismatch when the metadata store holds a proposal the set
// does not.
func TestPParamUpdateReadVerificationCountsADivergence(t *testing.T) {
	t.Parallel()
	scenario := newPParamUpdateReadsScenario(t)
	at := scenario.RollbackTarget.Slot
	scenario.ProposeAroundTheLedger(ledger.UpdateProposal{
		Slot: at + 1, Epoch: 1, Keys: []byte{7}, MaxTxSize: 26_000,
	})
	require.NoError(t, scenario.Forecast(2))
	verified, mismatches := scenario.PParamUpdateReads()
	require.Equal(t, float64(1), verified)
	require.Equal(t, float64(1), mismatches, "the set has no such proposal")
}
