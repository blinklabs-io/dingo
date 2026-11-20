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
	"math"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

var poolLeiosReadNames = []string{
	"GetActivePoolKeyHashesAtSlot",
	"GetPoolRegistrationsAtSlot", "GetPoolRegistrationsEffectiveForEpoch",
	"GetPoolsRetiringAtEpoch", "GetEpochBoundaryActivePoolKeyHashes",
}

func renderPoolRegistrations(regs []models.PoolRegistration) []string {
	var out []string
	for _, r := range regs {
		out = append(out, fmt.Sprintf(
			"%x vrf=%x pledge=%d added=%d",
			r.PoolKeyHash, r.VrfKeyHash, r.Pledge, r.AddedSlot,
		))
	}
	slices.Sort(out)
	return out
}

func renderHashes(hs [][]byte) []string {
	out := []string{}
	for _, h := range hs {
		out = append(out, fmt.Sprintf("%x", h))
	}
	slices.Sort(out)
	return out
}

// poolLeiosReads runs every Leios-aware pool read of l over slots around the
// certified certificates, which differ in effective and applied slot, and
// returns the rendered answers by read.
//
// A drained family cannot refund a pool the boundary already reaped, so
// afterReap omits the retirements of the epoch the harness pool left.
func poolLeiosReads(
	t *testing.T,
	l ledgerdb.LedgerDB,
	s ledger.CertifiedPoolSlots,
	afterReap bool,
) map[string]any {
	t.Helper()
	ctx := t.Context()
	out := map[string]any{}
	pools := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(s.Pool), lcommon.PoolKeyHash(s.Retired),
	}
	slots := []uint64{
		s.EpochStart + 7, s.CtxB, s.CtxA, s.CtxA + 1, s.CtxRetP,
		s.CtxRetP + 1, s.RegA, s.RegB, s.RegB + 1, s.RetP, s.RetP + 1,
		s.Boundary - 4, s.Boundary - 3, s.Boundary - 2, s.Boundary - 1,
	}
	for _, slot := range slots {
		a, err := l.GetActivePoolKeyHashesAtSlot(slot, nil)
		require.NoError(t, err, "active at %d", slot)
		out[fmt.Sprintf("active@%d", slot)] = renderHashes(a)
		regs, err := l.GetPoolRegistrationsAtSlot(pools, slot, nil)
		require.NoError(t, err, "registrations at %d", slot)
		out[fmt.Sprintf("regs@%d", slot)] = renderPoolRegistrations(regs)
	}
	for _, w := range []struct{ start, ended, snap uint64 }{
		{s.EpochStart, s.NextEpoch - 1, math.MaxInt64},
		{s.EpochStart + 9, s.NextEpoch - 1, s.EpochStart + 25},
		{s.EpochStart + 11, s.NextEpoch - 1, s.RegB},
		{s.Boundary, s.NextEpoch, math.MaxInt64},
		{s.Boundary - 2, s.NextEpoch, math.MaxInt64},
		{s.Boundary, s.NextEpoch + 1, math.MaxInt64},
	} {
		regs, err := l.GetPoolRegistrationsEffectiveForEpoch(
			pools, w.start, w.ended, w.snap, nil,
		)
		require.NoError(t, err, "effective %+v", w)
		out[fmt.Sprintf("effective%+v", w)] = renderPoolRegistrations(regs)
	}
	for _, w := range []struct{ epoch, boundary uint64 }{
		{s.RetireEpoch, s.Boundary},
		{s.RetireEpoch, s.Boundary - 3},
		{s.RetireEpoch, s.CtxRetP},
		{s.RetireEpoch, s.CtxRetP + 1},
		{s.RetireEpoch + 1, s.Boundary + 20},
		{s.RetireEpoch + 1, s.Boundary + 1},
	} {
		if afterReap && w.epoch == s.RetireEpoch {
			continue
		}
		refunds, err := l.GetPoolsRetiringAtEpoch(ctx, w.epoch, w.boundary, nil)
		require.NoError(t, err, "retiring %+v", w)
		var rows []string
		for _, r := range refunds {
			rows = append(rows, fmt.Sprintf(
				"%x %x %d", r.PoolKeyHash, r.RewardAccount, r.DepositHeld,
			))
		}
		slices.Sort(rows)
		out[fmt.Sprintf("retiring%+v", w)] = rows
	}
	for _, w := range [][2]uint64{
		{s.Boundary - 3, s.Boundary}, {s.Boundary + 6, s.Boundary},
	} {
		a, err := l.GetEpochBoundaryActivePoolKeyHashes(w[0], w[1], nil)
		if err != nil {
			out[fmt.Sprintf("boundary%v", w)] = err.Error()
			continue
		}
		out[fmt.Sprintf("boundary%v", w)] = renderHashes(a)
	}
	return out
}

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

// Pool certificates of Leios closures take effect at their context slot, not
// the slot of the block that certified them. With pools-history drained, the
// set answers every pool read the store's Leios-aware queries answer, without
// the metadata store, over a registration and re-registration whose effective
// order is the reverse of their apply order, a retirement, and a retirement
// applied after the epoch boundary whose effective slot lies before it.
func TestCertifiedPoolReadsFromTheSetMatchTheStore(t *testing.T) {
	t.Parallel()
	scenario, store, s := certifiedPoolScenario(t, "set", nil)
	want := poolLeiosReads(t, store, s, true)
	require.NotEmpty(t, want)

	_, hide := scenario.HideStoreReads(poolLeiosReadNames...)
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPoolsHistory),
	)
	hide(true)
	reached, _ := scenario.HideStoreReads(poolLeiosReadNames...)
	got := poolLeiosReads(t, scenario.LedgerDB(), s, true)

	require.Equal(t, want, got)
	require.Zero(t, reached(), "a read reached the metadata store")
}

// The live set and metadata store must retain the same certified pool state.
func TestCertifiedPoolReadsMatchTheStore(t *testing.T) {
	t.Parallel()
	// Before the boundary reaps the harness pool, its refund is read in both.
	var before, beforeStore []models.PoolRetirementRefund
	scenario, store, s := certifiedPoolScenario(
		t, "metadata-visible",
		func(sc ledger.BoundaryEpochStateScenario, store ledgerdb.LedgerDB) {
			epochs, err := store.GetEpochs(nil)
			require.NoError(t, err)
			var newest models.Epoch
			for _, e := range epochs {
				if e.EpochId > newest.EpochId {
					newest = e
				}
			}
			epoch, boundary := newest.EpochId+1, newest.StartSlot+1000
			before, err = sc.LedgerDB().GetPoolsRetiringAtEpoch(
				t.Context(), epoch, boundary, nil,
			)
			require.NoError(t, err)
			beforeStore, err = store.GetPoolsRetiringAtEpoch(
				t.Context(), epoch, boundary, nil,
			)
			require.NoError(t, err)
		},
	)
	require.NotEmpty(t, before, "the harness pool is retiring")
	require.Equal(t, beforeStore, before)

	got := poolLeiosReads(t, scenario.LedgerDB(), s, true)

	require.Equal(t, poolLeiosReads(t, store, s, true), got)
}

// The certificates differ in effective and applied slot as the scenario
// intends: the scenario is not a pre-Leios run in disguise.
func TestCertifiedPoolScenarioSeparatesEffectiveFromAppliedSlots(t *testing.T) {
	t.Parallel()
	_, store, s := certifiedPoolScenario(t, "set", nil)
	regs, err := store.GetPoolRegistrationsAtSlot(
		[]lcommon.PoolKeyHash{lcommon.PoolKeyHash(s.Pool)}, s.RegB, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, s.VrfA, regs[0].VrfKeyHash,
		"the newer certificate is the earlier in effect")
	require.Equal(t, s.CtxA, regs[0].AddedSlot)
}

// A set that loads its pool history from the metadata store gets the
// effective slot of each certified certificate, which the store derives from
// the closure's context, so it holds the same events as the set that followed
// the certificates.
func TestCertifiedPoolHistoryLoadsItsEffectiveSlots(t *testing.T) {
	t.Parallel()
	_, followed, store, s := certifiedPoolScenarioWithSet(
		t, "set", nil,
	)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.LoadLedgerState(t.Context(), store))

	type shape struct {
		slot, effective uint64
		retirement      bool
	}
	shapes := func(set *utxoset.Set, pool []byte) []shape {
		t.Helper()
		events, err := set.PoolHistory(pool)
		require.NoError(t, err)
		var out []shape
		for _, e := range events {
			out = append(
				out,
				shape{e.Slot, e.EffectiveSlot, e.Registration == nil},
			)
		}
		return out
	}
	for _, pool := range [][]byte{s.Pool, s.Retired} {
		require.Equal(t, shapes(followed, pool), shapes(fresh, pool))
	}
	require.Equal(t, []shape{
		{s.RegA, s.CtxA, false}, {s.RegB, s.CtxB, false},
		{s.RetN, s.CtxRetN, true},
	}, shapes(fresh, s.Pool))
}

// GetPoolStatus from the set answers as the store does for pools whose
// certificates take effect at a Leios context slot.
func TestCertifiedPoolStatusFromTheSetMatchesTheStore(t *testing.T) {
	t.Parallel()
	scenario, store, s := certifiedPoolScenario(t, "set", nil)
	// The store reads the current epoch at its tip, which the drained set
	// reads as the newest epoch row; align them.
	tip, err := scenario.DB.GetTip(nil)
	require.NoError(t, err)
	tip.Point.Slot = s.Boundary + 6
	require.NoError(t, scenario.DB.SetTip(tip, nil))
	read := func(l ledgerdb.LedgerDB) map[string]models.PoolStatus {
		out := map[string]models.PoolStatus{}
		for _, p := range [][]byte{s.Pool, s.Retired} {
			status, err := l.GetPoolStatus(lcommon.PoolKeyHash(p), nil)
			require.NoError(t, err)
			out[fmt.Sprintf("%x", p)] = status
		}
		return out
	}
	want := read(store)
	require.True(t, want[fmt.Sprintf("%x", s.Pool)].Exists)

	_, hide := scenario.HideStoreReads("GetPoolStatus")
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPoolsHistory),
	)
	hide(true)
	reached, _ := scenario.HideStoreReads("GetPoolStatus")
	got := read(scenario.LedgerDB())

	require.Equal(t, want, got)
	require.Zero(t, reached(), "a read reached the metadata store")
}
