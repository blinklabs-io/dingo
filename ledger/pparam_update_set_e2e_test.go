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
	"maps"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var pparamUpdateReadNames = []string{
	"GetPParamUpdates",
	"ForecastPParamUpdates",
}

// TestPParamUpdateReadsFollowProposalsABoundaryAndARollback reads the
// forecast and the stored proposals through the ledger's LedgerDB with the
// metadata store's proposal reads failing, through proposals of both kinds of
// transaction, an invalid transaction's, a boundary and a rollback. Valid
// proposals become visible, the boundary keeps them, and the rollback removes
// exactly the ones stored after its target.
func TestPParamUpdateReadsFollowProposalsABoundaryAndARollback(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(t, set)
	reached := scenario.HideStoreReads(pparamUpdateReadNames...)
	// read returns the forecast for epoch and the proposals stored for it
	// and the epoch before, one line each.
	read := func(stage string, epoch uint64) (string, []string) {
		t.Helper()
		pp, err := scenario.ForecastRendered(epoch)
		require.NoError(t, err, stage)
		// A read answers its epoch and the one before, so the two reads
		// overlap.
		seen := map[string]struct{}{}
		for _, e := range []uint64{epoch - 1, epoch} {
			rows, err := scenario.Updates(e)
			require.NoError(t, err, stage)
			for _, u := range rows {
				seen[fmt.Sprintf("%d %x %d %x",
					u.AddedSlot, u.GenesisHash, u.Epoch, u.Cbor)] = struct{}{}
			}
		}
		lines := slices.Sorted(maps.Keys(seen))
		return pp, lines
	}
	at := scenario.RollbackTarget.Slot
	storedAfter := func(lines []string, slot uint64) int {
		n := 0
		for _, l := range lines {
			var added uint64
			_, err := fmt.Sscanf(l, "%d", &added)
			require.NoError(t, err)
			if added > slot {
				n++
			}
		}
		return n
	}
	_, seeded := read("seeded", 2)
	require.Zero(t, storedAfter(seeded, at-1),
		"the scenario seeds no proposal at or after the rollback target")
	for _, p := range []ledger.UpdateProposal{
		{Slot: at, Epoch: 1, Index: 0, Keys: []byte{1, 2, 3}, MaxTxSize: 20_000},
		{Slot: at + 1, Epoch: 1, Index: 0, Keys: []byte{1, 2}, MaxTxSize: 21_000},
		{Slot: at + 1, Epoch: 1, Index: 1, Keys: []byte{1}, MaxTxSize: 22_000, Batched: true},
		{Slot: at + 2, Epoch: 1, Index: 0, Keys: []byte{4}, MaxTxSize: 23_000, Invalid: true},
		{Slot: at + 3, Epoch: 1, Index: 0, Keys: []byte{5}, MaxTxSize: 24_000, Batched: true},
	} {
		scenario.Propose(p)
	}
	proposedForecast, proposed := read("proposed", 2)
	// Seven valid proposals, one per genesis key of each valid transaction.
	require.Len(t, proposed, len(seeded)+7,
		"an invalid transaction's proposals are not stored")
	require.NoError(t, scenario.ForecastInsideTxn(ledger.UpdateProposal{
		Slot: at + 4, Epoch: 1, Keys: []byte{9}, MaxTxSize: 25_000,
	}, 2))
	require.NoError(t, scenario.EnactInsideTxn(1))
	scenario.ApplyBoundary()
	_, boundary := read("boundary", 2)
	require.Equal(t, proposed, boundary, "the boundary keeps every proposal")
	scenario.RollbackBeforeBoundary()
	rolledForecast, rolled := read("rollback", 2)
	require.Len(t, rolled, len(seeded)+3,
		"the rollback keeps the proposals stored at its target")
	require.Zero(t, storedAfter(rolled, at),
		"the rollback removes the proposals stored after its target")
	require.NotEqual(t, proposedForecast, rolledForecast,
		"the forecast follows the proposals the rollback removed")
	require.Zero(t, reached(), "a proposal read reached the metadata store")
}
