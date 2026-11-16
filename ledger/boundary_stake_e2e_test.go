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
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var boundaryStakeReads = []string{
	"GetEpochBoundaryActivePoolKeyHashes",
	"GetEpochBoundaryDelegatedPoolKeyHashes",
	"GetEpochBoundaryStakeByPools",
	"GetEpochBoundaryRewardStakeInputsForPools",
	"GetPointerStakeInputsForPools",
}

type boundaryStakeHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
}

func newBoundaryStakeHarness(t *testing.T, reads string) *boundaryStakeHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set, reads)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
	return &boundaryStakeHarness{set: set, scenario: scenario}
}

func sortedInputs(rows []*models.RewardStakeInput) []models.RewardStakeInput {
	out := make([]models.RewardStakeInput, 0, len(rows))
	for _, r := range rows {
		out = append(out, *r)
	}
	slices.SortFunc(out, func(a, b models.RewardStakeInput) int {
		if c := bytes.Compare(a.PoolKeyHash, b.PoolKeyHash); c != 0 {
			return c
		}
		return bytes.Compare(a.StakingKey, b.StakingKey)
	})
	return out
}

// requireSameBoundaryStake requires every answer of got to be want's.
func requireSameBoundaryStake(
	t *testing.T,
	want, got ledger.BoundaryStakeAnswers,
) {
	t.Helper()
	require.ElementsMatch(t, want.Active, got.Active, "active pools")
	require.ElementsMatch(t, want.Delegated, got.Delegated, "delegated pools")
	require.Equal(t, want.Stakes, got.Stakes, "pool stake")
	require.Equal(t, want.Delegators, got.Delegators, "delegator counts")
	require.Equal(
		t,
		sortedInputs(want.Inputs),
		sortedInputs(got.Inputs),
		"reward stake inputs",
	)
	require.Equal(
		t,
		sortedInputs(want.Pointers),
		sortedInputs(got.Pointers),
		"pointer stake inputs",
	)
}

// requireFixtureFigures requires the answers to hold the figures the fixture
// was built for: the pool's two harness delegators with 20000000 each and the
// pointed credential with its base and pointer outputs, its balance without
// the credit applied after SNAP, and its pending credits, and the
// re-registered credential with its base output only: the pointer naming its
// earlier registration, the dangling pointer and the enterprise output count
// for nothing.
func requireFixtureFigures(
	t *testing.T,
	f ledger.BoundaryStakeFixture,
	a ledger.BoundaryStakeAnswers,
) {
	t.Helper()
	pointed := f.PointedBase + f.PointedPointer + f.PointedRw
	require.Equal(t, 45_000_000+pointed, a.Stakes[string(f.Pool)])
	require.Equal(t, uint64(4), a.Delegators[string(f.Pool)])
	require.Zero(t, a.Stakes[string(f.Idle)])
	require.ElementsMatch(t, [][]byte{f.Pool}, a.Delegated)
	require.ElementsMatch(t, [][]byte{f.Pool}, a.Active)
	require.Len(t, a.Pointers, 1)
	require.Equal(t, f.Pointed.Key, a.Pointers[0].StakingKey)
	require.Equal(t, f.PointedPointer, uint64(a.Pointers[0].Stake))
	var got, again uint64
	for _, in := range a.Inputs {
		switch {
		case bytes.Equal(in.StakingKey, f.Pointed.Key):
			got = uint64(in.Stake)
		case bytes.Equal(in.StakingKey, f.Dereg.Key):
			again = uint64(in.Stake)
		}
	}
	require.Equal(t, uint64(5_000_000), again)
	require.Equal(t, pointed, got)
	require.Len(t, a.Inputs, 4)
}

// TestBoundaryStakeReadsAgreeWithTheMetadataStore requires the set's
// epoch-boundary stake, built from its accounts, pools, UTxO stake sums,
// pointer sums and reward rows, to equal the metadata store's CTEs at a
// boundary, and for a boundary the set has moved past, from its journals. The
// fixture holds delegations, pointer outputs, an account that deregistered and
// registered again, whose pointer dangles, a retiring pool and pending and post-snapshot credits.
func TestBoundaryStakeReadsAgreeWithTheMetadataStore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		retire uint64
		past   bool
	}{
		{"at the boundary", 0, false},
		{"pool retiring next epoch", 6, false},
		{"pool retired", 5, false},
		{"past boundary", 0, true},
		{"past boundary with a retired pool", 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsVerify)
			f := h.scenario.BoundaryStake(tc.retire)
			if tc.past {
				h.scenario.AfterBoundaryStake(f)
				tip, _, err := h.set.Tip()
				require.NoError(t, err)
				require.Greater(t, tip.Slot, f.Slot)
			}
			verified, _ := h.scenario.LiveStakeReads()
			poolVerified, _ := h.scenario.PoolReads()
			want := h.scenario.ReadBoundaryStake(f, true)
			got := h.scenario.ReadBoundaryStake(f, false)
			if tc.retire == 5 {
				// A retirement for the epoch its certificate is in is
				// never reaped, so the delegations it would clear are
				// not comparable; the active pool filter is.
				require.ElementsMatch(t, want.Active, got.Active)
				require.Empty(t, got.Active)
				pv, pm := h.scenario.PoolReads()
				require.Equal(t, poolVerified+1, pv)
				require.Zero(t, pm)
				return
			}
			requireSameBoundaryStake(t, want, got)
			requireFixtureFigures(t, f, got)
			v, mismatches := h.scenario.LiveStakeReads()
			require.Equal(t, verified+4, v, "four stake reads are compared")
			require.Zero(t, mismatches)
			pv, pm := h.scenario.PoolReads()
			require.Equal(
				t,
				poolVerified+1,
				pv,
				"the active pool read is compared",
			)
			require.Zero(t, pm)
		})
	}
}

// TestBoundaryStakeReadsAnswerWithTheStoreBlind requires set mode to answer
// the five reads without reaching the metadata store, with the answers the
// store gave, at a boundary and past it.
func TestBoundaryStakeReadsAnswerWithTheStoreBlind(t *testing.T) {
	t.Parallel()
	for _, past := range []bool{false, true} {
		name := "at the boundary"
		if past {
			name = "past boundary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsSet)
			f := h.scenario.BoundaryStake(0)
			if past {
				h.scenario.AfterBoundaryStake(f)
			}
			want := h.scenario.ReadBoundaryStake(f, true)
			reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
			hide(true)
			got := h.scenario.ReadBoundaryStake(f, false)
			require.Zero(t, reached(), "a read reached the metadata store")
			requireSameBoundaryStake(t, want, got)
			requireFixtureFigures(t, f, got)
		})
	}
}

// TestBoundaryStakeReadsServeFromTheSetWithALeiosValidatorBeforeDijkstra
// requires a node that configures a Leios certificate validator, as every
// node does, to answer the boundary reads from the set while its chain is in
// an era that certifies nothing.
func TestBoundaryStakeReadsServeFromTheSetWithALeiosValidatorBeforeDijkstra(
	t *testing.T,
) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsSet)
	h.scenario.ConfigureLeiosValidator()
	f := h.scenario.BoundaryStake(0)
	want := h.scenario.ReadBoundaryStake(f, true)
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	got := h.scenario.ReadBoundaryStake(f, false)
	require.Zero(t, reached(), "a read reached the metadata store")
	requireSameBoundaryStake(t, want, got)
}

// TestBoundaryStakeReadsServeFromTheSetWhenTheSetOnlyHoldsTheState requires a
// node whose metadata store holds no ledger state, on a chain that certifies
// Leios closures, to answer the five reads from the set, with the answers the
// store gave before, and without a read of the store.
func TestBoundaryStakeReadsServeFromTheSetWhenTheSetOnlyHoldsTheState(
	t *testing.T,
) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsSet)
	f := h.scenario.BoundaryStake(0)
	h.scenario.CertifyLeios()
	want := h.scenario.ReadBoundaryStake(f, true)
	h.scenario.LedgerStateSetOnly(true)
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	got := h.scenario.ReadBoundaryStake(f, false)
	require.Zero(t, reached(), "a read reached the metadata store")
	requireSameBoundaryStake(t, want, got)
	requireFixtureFigures(t, f, got)
}

// TestBoundaryStakeVerificationCountsADivergence requires a verify-mode read
// to count a mismatch when the set holds stake the metadata store does not.
func TestBoundaryStakeVerificationCountsADivergence(t *testing.T) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsVerify)
	f := h.scenario.BoundaryStake(0)
	h.scenario.AddSetStake(f.Pointed.Key, 50)
	want := h.scenario.ReadBoundaryStake(f, true)
	got := h.scenario.ReadBoundaryStake(f, false)
	requireSameBoundaryStake(t, want, got)
	verified, mismatches := h.scenario.LiveStakeReads()
	require.Equal(t, float64(4), verified)
	require.Equal(
		t,
		float64(2),
		mismatches,
		"pool stake and reward inputs differ",
	)
}

// TestBoundaryStakeReadsFollowTheBoundary requires the set to agree with the
// store through the boundary's own snapshot, which reads the pointer stake and
// active pools in the boundary transaction.
func TestBoundaryStakeReadsFollowTheBoundary(t *testing.T) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsVerify)
	h.scenario.BoundaryStake(0)
	h.scenario.ApplyBoundary()
	_, mismatches := h.scenario.LiveStakeReads()
	require.Zero(t, mismatches)
	_, poolMismatches := h.scenario.PoolReads()
	require.Zero(t, poolMismatches)
	verified, _ := h.scenario.PoolReads()
	stakeVerified, _ := h.scenario.LiveStakeReads()
	// Without the set's answers the boundary compares 3 pool and 2 live
	// stake reads; the active pools and pointer stake add one each.
	require.GreaterOrEqual(t, verified, float64(4))
	require.GreaterOrEqual(t, stakeVerified, float64(3))
}

// TestBoundaryStakeReadsAreServedAtTheBoundary requires the boundary's
// snapshot to read its active pools and pointer stake from the set, with the
// metadata store's reads of them failing.
func TestBoundaryStakeReadsAreServedAtTheBoundary(t *testing.T) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsSet)
	h.scenario.BoundaryStake(0)
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	h.scenario.ApplyBoundary()
	require.Zero(t, reached(), "the boundary read the metadata store")
}

// boundaryStakeReadFuncs are the five epoch-boundary stake reads for f's
// boundary, through db, by name.
func boundaryStakeReadFuncs(
	db ledgerdb.LedgerDB,
	f ledger.BoundaryStakeFixture,
) map[string]func() error {
	pools := [][]byte{f.Pool}
	return map[string]func() error{
		"GetEpochBoundaryActivePoolKeyHashes": func() error {
			_, err := db.GetEpochBoundaryActivePoolKeyHashes(
				f.Slot, f.Boundary, nil,
			)
			return err
		},
		"GetEpochBoundaryDelegatedPoolKeyHashes": func() error {
			_, err := db.GetEpochBoundaryDelegatedPoolKeyHashes(
				f.Slot, f.Boundary, nil,
			)
			return err
		},
		"GetEpochBoundaryStakeByPools": func() error {
			_, _, err := db.GetEpochBoundaryStakeByPools(
				pools, f.Slot, f.Boundary, nil, 0, 0, nil,
			)
			return err
		},
		"GetEpochBoundaryRewardStakeInputsForPools": func() error {
			_, err := db.GetEpochBoundaryRewardStakeInputsForPools(
				pools, f.Slot, f.Boundary, nil, 0, 0, nil,
			)
			return err
		},
		"GetPointerStakeInputsForPools": func() error {
			_, err := db.GetPointerStakeInputsForPools(
				pools, f.Slot, f.Boundary, 0, nil,
			)
			return err
		},
	}
}

// TestBoundaryStakeReadsServeFromTheSetOnceAFamilyIsDrained turns the
// set-only switch on, on a chain that certifies Leios closures, and requires
// every read to be the set's, without a read of the store, as each family the
// reads need is drained in turn.
func TestBoundaryStakeReadsServeFromTheSetOnceAFamilyIsDrained(t *testing.T) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsSet)
	f := h.scenario.BoundaryStake(0)
	h.scenario.CertifyLeios()
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	db := h.scenario.LedgerDB()
	reads := boundaryStakeReadFuncs(db, f)
	require.Len(t, reads, len(boundaryStakeReads))
	h.scenario.SetOnlySwitch(true)

	for _, drained := range []nodesettings.Family{
		nodesettings.FamilyNonces,
		nodesettings.FamilyLiveStake,
		nodesettings.FamilyAccounts,
		nodesettings.FamilyPoolsHistory,
	} {
		h.scenario.DrainFamilies(nodesettings.FamilyMask(0).With(drained))
		for _, name := range boundaryStakeReads {
			require.NoError(
				t, reads[name](), "%s with %s drained", name, drained,
			)
			require.Zero(t, reached(), "%s with %s drained", name, drained)
		}
	}
}

// Every epoch-boundary stake read of the metadata store reads the protocol
// parameters that decide whether the snapshot follows the enactment. The set
// keeps them, so each is the set's once the pparams-updates family is drained,
// without a read of the store.
func TestBoundaryStakeReadsServeFromTheSetOnceProtocolParametersAreDrained(
	t *testing.T,
) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, ledger.UtxoSetReadsSet)
	f := h.scenario.BoundaryStake(0)
	h.scenario.CertifyLeios()
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	reads := boundaryStakeReadFuncs(h.scenario.LedgerDB(), f)
	h.scenario.SetOnlySwitch(true)
	h.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPParamsUpdates),
	)
	for _, name := range boundaryStakeReads {
		require.NoError(t, reads[name](), name)
		require.Zero(t, reached(), name)
	}
}
