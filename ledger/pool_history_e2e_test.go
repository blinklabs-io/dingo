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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type poolHistoryHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	slots    ledger.PoolSlots
	// refunds is what GetPoolsRetiringAtEpoch returned just before the
	// boundary, read through the ledger's LedgerDB.
	refunds []models.PoolRetirementRefund
}

// newPoolHistoryHarness runs the pool scenario with metadata reads available over a set
// that loaded its history first.
func newPoolHistoryHarness(t *testing.T) *poolHistoryHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &poolHistoryHarness{set: set}
	h.scenario = ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	h.store = dbadapter.New(h.scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), h.store))
	h.slots = h.scenario.Pools(func() {
		epochs, err := h.store.GetEpochs(nil)
		require.NoError(t, err)
		var cur models.Epoch
		for _, e := range epochs {
			if e.EpochId > cur.EpochId {
				cur = e
			}
		}
		h.refunds, err = h.scenario.LedgerDB().GetPoolsRetiringAtEpoch(
			t.Context(), cur.EpochId+1, cur.StartSlot+23, nil,
		)
		require.NoError(t, err)
	})
	return h
}

func vrfAt(
	t *testing.T,
	l ledgerdb.LedgerDB,
	pool []byte,
	slot uint64,
	earliest bool,
) ([]byte, bool) {
	t.Helper()
	read := l.GetPoolVrfKeyHashAtSlot
	if earliest {
		read = l.GetPoolEarliestVrfKeyHashAtSlot
	}
	v, ok, err := read(pool, slot, nil)
	require.NoError(t, err)
	return v, ok
}

// TestPoolHistoryReadsAgreeAcrossAVRFRotation requires every verified pool
// history read to agree with the metadata store for a pool that rotates its
// VRF key mid-epoch and one that retires, and the set to keep the retired
// pool's events after the boundary reaps it.
func TestPoolHistoryReadsAgreeAcrossAVRFRotation(t *testing.T) {
	t.Parallel()
	h := newPoolHistoryHarness(t)
	s := h.slots
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pool := lcommon.PoolKeyHash(s.Pool)

	for _, tc := range []struct {
		slot     uint64
		earliest bool
		want     []byte
		found    bool
	}{
		{s.Register - 1, false, nil, false},
		{s.Register, false, s.VrfA, true},
		{s.Rotate - 1, false, s.VrfA, true},
		{s.Rotate, false, s.VrfB, true},
		{s.Rotate, true, s.VrfA, true},
		{s.Register - 1, true, nil, false},
	} {
		got, found := vrfAt(t, ldb, s.Pool, tc.slot, tc.earliest)
		require.Equal(t, tc.found, found, "slot %d earliest=%v", tc.slot, tc.earliest)
		require.Equal(t, tc.want, got, "slot %d earliest=%v", tc.slot, tc.earliest)
	}
	state, err := ldb.GetPoolVrfState(ctx, pool, nil)
	require.NoError(t, err)
	require.Equal(t, s.VrfB, state.Registration[0].VrfKeyHash)
	certs, err := ldb.GetPoolRegistrations(pool, nil)
	require.NoError(t, err)
	require.Len(t, certs, 2)
	require.Equal(t, s.VrfB, certs[0].VrfKeyHash[:], "newest first")
	_, err = ldb.GetPool(ctx, pool, true, nil)
	require.NoError(t, err)
	// The window the rotation falls in: the first registration is in
	// effect until a window starts after the second.
	for _, w := range []struct {
		start, ended, snapshot uint64
		pool                   []byte
		vrf                    []byte
	}{
		{s.Rotate, 5, math.MaxInt64, s.Pool, s.VrfA},
		{s.Rotate + 1, 5, math.MaxInt64, s.Pool, s.VrfB},
		{s.Register, 5, s.Rotate, s.Pool, s.VrfA},
		{s.Register, 5, s.Register - 1, s.Pool, nil},
		{s.Retire + 1, s.RetireEpoch, math.MaxInt64, s.Retired, nil},
		{s.Retire + 1, s.RetireEpoch - 1, math.MaxInt64, s.Retired, []byte{}},
	} {
		pkh := lcommon.PoolKeyHash(w.pool)
		got, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
			[]lcommon.PoolKeyHash{pkh}, w.start, w.ended, w.snapshot, nil,
		)
		require.NoError(t, err)
		if w.vrf == nil {
			require.Empty(t, got, "window %+v", w)
			continue
		}
		require.Len(t, got, 1, "window %+v", w)
		if len(w.vrf) > 0 {
			require.Equal(t, w.vrf, got[0].VrfKeyHash, "window %+v", w)
		}
	}

	require.Len(t, h.refunds, 1)
	require.Equal(t, s.Retired, h.refunds[0].PoolKeyHash)

	// The retired pool is reaped from the current state, not the history.
	st, err := h.set.PoolState(s.Retired)
	require.NoError(t, err)
	require.Nil(t, st, "the boundary reaped the pool")
	events, err := h.set.PoolHistory(s.Retired)
	require.NoError(t, err)
	require.Len(t, events, 2, "a registration and the retirement")
	require.NotNil(t, events[0].Registration)
	require.Nil(t, events[1].Registration)
	require.Equal(t, s.RetireEpoch, events[1].RetiringEpoch)
	_, found := vrfAt(t, ldb, s.Retired, s.Boundary, false)
	require.True(t, found, "a reaped pool's registration still answers")

}

// TestPoolHistoryRollsBackAcrossAReRegistration requires a rollback to
// remove the registrations and retirement after its target from the set's
// history, and the reads after it to agree with the metadata store.
func TestPoolHistoryRollsBackAcrossAReRegistration(t *testing.T) {
	t.Parallel()
	h := newPoolHistoryHarness(t)
	s := h.slots
	events, err := h.set.PoolHistory(s.Pool)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, s.VrfA, events[0].Registration.VrfKeyHash)
	require.Equal(t, s.VrfB, events[1].Registration.VrfKeyHash)
	before, err := h.set.PoolHistory(s.Retired)
	require.NoError(t, err)
	require.Len(t, before, 2)

	h.scenario.RollbackBeforeBoundary()

	events, err = h.set.PoolHistory(s.Pool)
	require.NoError(t, err)
	require.Empty(t, events, "the rollback removes both registrations")
	after, err := h.set.PoolHistory(s.Retired)
	require.NoError(t, err)
	require.Len(t, after, 1, "the rollback removes the retirement only")
	require.NotNil(t, after[0].Registration)

	_, found := vrfAt(t, h.scenario.LedgerDB(), s.Pool, s.Boundary, false)
	require.False(t, found)
	_, found = vrfAt(t, h.scenario.LedgerDB(), s.Retired, s.Boundary, false)
	require.True(t, found)
}
