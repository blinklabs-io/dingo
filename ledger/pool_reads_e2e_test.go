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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type poolReadsHarness struct {
	scenario ledger.BoundaryEpochStateScenario
	store    *dbadapter.Adapter
	pool     []byte
	cur      models.Epoch
}

func newPoolReadsHarness(t *testing.T) *poolReadsHarness {
	t.Helper()
	return newPoolReadsHarnessWithReads(t, "metadata-visible")
}

func newPoolReadsHarnessWithReads(
	t *testing.T,
	reads string,
) *poolReadsHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	// The epoch the ledger is in after the boundary has the room for
	// the scenarios' slots.
	scenario.ApplyBoundary()
	// The harness commits through the LedgerDB alone, so the set has
	// recorded no tip; a ledger's commits record theirs.
	tip, err := store.GetTip(nil)
	require.NoError(t, err)
	require.NoError(t, set.ApplyUtxoChanges(&tip.Point, nil))
	epochs, err := store.GetEpochs(nil)
	require.NoError(t, err)
	var cur models.Epoch
	for _, e := range epochs {
		if e.EpochId > cur.EpochId {
			cur = e
		}
	}
	h := &poolReadsHarness{
		scenario: scenario,
		store:    &store,
		pool:     bytes.Repeat([]byte{0xd1}, 28),
		cur:      cur,
	}
	return h
}

func (h *poolReadsHarness) nextStart() uint64 {
	return h.cur.StartSlot + uint64(h.cur.LengthInSlots)
}

// retiring reads the pools retiring at epoch through the ledger's LedgerDB.
func (h *poolReadsHarness) retiring(
	t *testing.T,
	epoch uint64,
) []models.PoolRetirementRefund {
	t.Helper()
	got, err := h.scenario.LedgerDB().GetPoolsRetiringAtEpoch(
		t.Context(), epoch, h.nextStart(), nil,
	)
	require.NoError(t, err)
	return got
}

// active reads the active pools at slot through the ledger's LedgerDB. When
// atTip is set the tip is at slot's state, and the read at the tip must
// agree.
func (h *poolReadsHarness) active(
	t *testing.T,
	slot uint64,
	atTip bool,
) [][]byte {
	t.Helper()
	got, err := h.scenario.LedgerDB().GetActivePoolKeyHashesAtSlot(slot, nil)
	require.NoError(t, err)
	if atTip {
		gotTip, err := h.scenario.LedgerDB().
			GetActivePoolKeyHashes(t.Context(), nil)
		require.NoError(t, err)
		require.ElementsMatch(t, got, gotTip)
	}
	return got
}

// TestPoolRetirementReadSeesItsTransactionsStagedRetirement requires the
// retirement read in a write transaction to see the certificate that
// transaction has staged, as the metadata store does.
func TestPoolRetirementReadSeesItsTransactionsStagedRetirement(t *testing.T) {
	t.Parallel()
	h := newPoolReadsHarness(t)
	got := h.scenario.RetireInsideTxn(h.pool, h.cur.EpochId+1, h.nextStart())
	require.Len(t, got, 1)
	require.Equal(t, h.pool, got[0].PoolKeyHash)
}

// TestPoolReadUsesSetStateAfterUntrackedRetirements pins the set result when
// metadata-only retirement rows do not appear in its pool history.
func TestPoolReadUsesSetStateAfterUntrackedRetirements(t *testing.T) {
	t.Parallel()
	h := newPoolReadsHarness(t)
	require.NoError(t, h.scenario.DB.Metadata().RetirePools(
		nil, [][]byte{h.pool}, h.cur.EpochId+1, h.cur.StartSlot+5,
	))
	require.NoError(t, h.scenario.DB.Metadata().RetirePools(
		nil, [][]byte{h.pool}, h.cur.EpochId, h.cur.StartSlot+6,
	))
	reached, _ := h.scenario.HideStoreReads(
		"GetPoolsRetiringAtEpoch", "GetActivePoolKeyHashesAtSlot",
		"GetActivePoolKeyHashes",
	)
	ldb := h.scenario.LedgerDB()
	retiring, err := ldb.GetPoolsRetiringAtEpoch(
		t.Context(), h.cur.EpochId+1, h.nextStart(), nil,
	)
	require.NoError(t, err)
	require.Empty(t, retiring)
	active, err := ldb.GetActivePoolKeyHashesAtSlot(h.cur.StartSlot+1000, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{h.pool}, active)
	require.Zero(t, reached(), "pool reads reached the metadata store")
}
