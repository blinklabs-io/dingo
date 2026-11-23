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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type witnessHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	slots    ledger.WitnessSlots
}

// newWitnessHarness runs the witness scenario over a set that follows the
// ledger's writes.
func newWitnessHarness(t *testing.T) *witnessHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &witnessHarness{set: set}
	h.scenario = ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(h.scenario.DB),
	))
	h.slots = h.scenario.Witnesses(false)
	return h
}

func (h *witnessHarness) refs() []models.StakeCredentialRef {
	return []models.StakeCredentialRef{
		h.slots.Payee, h.slots.C,
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xe9}, 28)),
	}
}

// requireWitnessSlots requires the greatest witness slots of the scenario's
// credentials at a few bounds to be the ones its writes make, so a set and a
// metadata store that agree on a wrong answer do not pass.
func requireWitnessSlots(
	t *testing.T,
	db ledgerdb.LedgerDB,
	s ledger.WitnessSlots,
) {
	t.Helper()
	for _, tc := range []struct {
		name string
		ref  models.StakeCredentialRef
		max  uint64
		want uint64
		ok   bool
	}{
		{"payee before the kept delegation", s.Payee, s.Kept - 1, 0, false},
		{"payee at the kept delegation", s.Payee, s.Kept, s.Kept, true},
		{"payee before any withdrawal", s.Payee, s.Credit, s.Kept, true},
		{"payee at a withdrawal", s.Payee, s.Withdraw, s.Withdraw, true},
		{"payee after a skipped zero withdrawal", s.Payee, s.ZeroSkipped, s.Withdraw, true},
		{"payee at a recorded zero withdrawal", s.Payee, s.ZeroRecorded, s.ZeroRecorded, true},
		{"c before registering", s.C, s.Register - 1, 0, false},
		{"c at registering", s.C, s.Register, s.Register, true},
		{"c at the delegation", s.C, s.Delegate, s.Delegate, true},
		{"c at the vote delegation", s.C, s.VoteDelegate, s.VoteDelegate, true},
		{"c between certificates", s.C, s.Deregister - 1, s.VoteDelegate, true},
		{"c at deregistering", s.C, math.MaxInt64, s.Deregister, true},
	} {
		got, err := db.AccountLastWitnessSlots(
			t.Context(), []models.StakeCredentialRef{tc.ref}, tc.max, nil,
		)
		require.NoError(t, err, tc.name)
		last, ok := got[tc.ref.MapKey()]
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.want, last, tc.name)
	}
}

// TestWitnessSlotReadsFromTheLiveUtxoSet reads the witness slots through the
// ledger's LedgerDB with the metadata store's read failing, across
// delegation-only certificates, withdrawals with the witness write elided and
// recorded, and a rollback.
func TestWitnessSlotReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newWitnessHarness(t)
	reached, _ := h.scenario.HideStoreReads("AccountLastWitnessSlots")
	ldb := h.scenario.LedgerDB()
	requireWitnessSlots(t, ldb, h.slots)

	h.scenario.RollbackBeforeBoundary()
	got, err := ldb.AccountLastWitnessSlots(
		t.Context(), h.refs(), math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		map[string]uint64{h.slots.Payee.MapKey(): h.slots.Kept}, got,
		"only the witness at the rollback target survives")
	require.Zero(t, reached(), "a witness read reached the metadata store")
}

// TestWitnessSlotViewsSeeTheirTransaction requires a read in a write
// transaction to see a witness the transaction staged, and one cut back by a
// staged rollback to see only the witnesses at or before its target.
func TestWitnessSlotViewsSeeTheirTransaction(t *testing.T) {
	t.Parallel()
	h := newWitnessHarness(t)
	reached, _ := h.scenario.HideStoreReads("AccountLastWitnessSlots")
	key := bytes.Repeat([]byte{0xea}, 28)
	slot := h.scenario.EpochStartSlot() + 90
	last, ok := h.scenario.WitnessesInsideTxn(key, slot)
	require.True(t, ok, "the transaction's own certificate is a witness")
	require.Equal(t, slot, last)
	got := h.scenario.WitnessesInsideRollback(h.refs())
	require.Equal(t,
		map[string]uint64{h.slots.Payee.MapKey(): h.slots.Kept}, got,
		"a staged rollback keeps the committed witnesses at its target")
	require.Zero(t, reached())
}
