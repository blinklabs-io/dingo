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
	"github.com/blinklabs-io/dingo/database/types"
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

// TestPoolHistoryLoadsFromTheMetadataStore requires a set from before it kept
// pool history to load every registration and retirement from the metadata
// store, as the set that followed the certificates holds them.
func TestPoolHistoryLoadsFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	h := newPoolHistoryHarness(t)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	loaded, err := fresh.PoolHistoryLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, fresh.LoadLedgerState(t.Context(), h.store))
	loaded, err = fresh.PoolHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	type shape struct {
		slot       uint64
		vrf        []byte
		retirement bool
		epoch      uint64
	}
	shapes := func(set *utxoset.Set, pool []byte) []shape {
		t.Helper()
		events, err := set.PoolHistory(pool)
		require.NoError(t, err)
		var out []shape
		for _, e := range events {
			sh := shape{slot: e.Slot, retirement: e.Registration == nil, epoch: e.RetiringEpoch}
			if e.Registration != nil {
				sh.vrf = e.Registration.VrfKeyHash
			}
			out = append(out, sh)
		}
		return out
	}
	for _, pool := range [][]byte{h.slots.Pool, h.slots.Retired} {
		want := shapes(h.set, pool)
		require.NotEmpty(t, want)
		require.Equal(t, want, shapes(fresh, pool), "pool %x", pool)
	}
	require.Equal(t, []shape{
		{slot: h.slots.Register, vrf: h.slots.VrfA},
		{slot: h.slots.Rotate, vrf: h.slots.VrfB},
	}, shapes(fresh, h.slots.Pool))
}

// TestPoolHistoryReadUsesSetStateAfterAnUntrackedRegistration pins the set
// result when a registration exists only in metadata.
func TestPoolHistoryReadUsesSetStateAfterAnUntrackedRegistration(t *testing.T) {
	t.Parallel()
	h := newPoolHistoryHarness(t)
	pool := bytes.Repeat([]byte{0x7e}, 28)
	vrf := bytes.Repeat([]byte{0x7f}, 32)
	require.NoError(t, h.scenario.DB.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pool, VrfKeyHash: vrf},
		&models.PoolRegistration{
			PoolKeyHash: pool, VrfKeyHash: vrf, AddedSlot: 5,
			Pledge: types.Uint64(1), Cost: types.Uint64(1),
		},
		nil,
	))
	reached, _ := h.scenario.HideStoreReads("GetPoolVrfKeyHashAtSlot")
	got, found := vrfAt(t, h.scenario.LedgerDB(), pool, 10, false)
	require.False(t, found)
	require.Nil(t, got)
	require.Zero(t, reached(), "pool history read reached the metadata store")
}
