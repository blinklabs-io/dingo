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
		t, set,
	)
	return scenario
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
}
