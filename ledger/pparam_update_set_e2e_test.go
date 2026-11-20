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
	"fmt"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var pparamUpdateReadNames = []string{
	"GetPParamUpdates",
	"ForecastPParamUpdates",
}

// pparamUpdateOutcome runs the proposal scenario in reads mode and returns
// what the forecasts and proposal reads answered. With the live set the store's
// proposal reads fail, and the count of reads that reached it is returned.
func pparamUpdateOutcome(
	t *testing.T,
	reads string,
) (out []string, reached int64) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(t, set)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	count := func() int64 { return 0 }
	if reads == "set" {
		count = scenario.HideStoreReads(pparamUpdateReadNames...)
	}
	read := func(stage string, epoch uint64) {
		t.Helper()
		pp, err := scenario.ForecastRendered(epoch)
		require.NoError(t, err, stage)
		out = append(out, fmt.Sprintf("%s forecast %d %s", stage, epoch, pp))
		for _, e := range []uint64{epoch - 1, epoch} {
			rows, err := scenario.Updates(e)
			require.NoError(t, err, stage)
			// Per genesis key, newest first; the order between keys is
			// not the store's to promise.
			var lines []string
			for _, u := range rows {
				lines = append(lines, fmt.Sprintf("%x %d %d %x",
					u.GenesisHash, u.AddedSlot, u.Epoch, u.Cbor))
			}
			slices.Sort(lines)
			out = append(out, fmt.Sprintf("%s updates %d %v", stage, e, lines))
		}
	}
	read("seeded", 1)
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
	read("proposed", 2)
	require.NoError(t, scenario.ForecastInsideTxn(ledger.UpdateProposal{
		Slot: at + 4, Epoch: 1, Keys: []byte{9}, MaxTxSize: 25_000,
	}, 2))
	require.NoError(t, scenario.EnactInsideTxn(1))
	scenario.ApplyBoundary()
	read("boundary", 2)
	scenario.RollbackBeforeBoundary()
	read("rollback", 2)
	return out, count()
}

// TestPParamUpdateReadsFromTheLiveUtxoSet requires the forecast and the
// proposal read to give, from the live set with the store's reads failing, the
// answers they give with metadata reads available, through proposals of both kinds of
// transaction, an invalid transaction's, a boundary and a rollback.
func TestPParamUpdateReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	visible, _ := pparamUpdateOutcome(t, "metadata-visible")
	setMode, reached := pparamUpdateOutcome(t, "set")
	require.Equal(t, visible, setMode)
	require.Zero(t, reached, "a proposal read reached the metadata store")
}

// TestPParamUpdateReadsUseTheSetAfterAnUntrackedStoreWrite requires an
// update written outside the ledger to stay out of the set's answer.
func TestPParamUpdateReadsUseTheSetAfterAnUntrackedStoreWrite(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	at := scenario.RollbackTarget.Slot
	scenario.ProposeAroundTheLedger(ledger.UpdateProposal{
		Slot: at + 1, Epoch: 1, Keys: []byte{7}, MaxTxSize: 26_000,
	})
	rows, err := scenario.Updates(1)
	require.NoError(t, err)
	require.False(t, slices.ContainsFunc(rows, func(u models.PParamUpdate) bool {
		return u.GenesisHash[0] == 7
	}), "an untracked store proposal is absent from the set")
}
