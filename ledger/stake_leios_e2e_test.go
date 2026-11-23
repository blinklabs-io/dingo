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

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

func certifiedStakeScenario(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, *utxoset.Set, ledger.CertifiedStakeSlots) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.CertifyLeios()
	return scenario, set, scenario.CertifiedStake()
}

type stakeShape struct {
	slot, effective uint64
	registered      bool
}

func stakeShapes(
	t *testing.T,
	set *utxoset.Set,
	ref models.StakeCredentialRef,
) []stakeShape {
	t.Helper()
	events, err := set.StakeHistory(ref)
	require.NoError(t, err)
	var out []stakeShape
	for _, e := range events {
		out = append(out, stakeShape{e.Slot, e.EffectiveSlot, e.Registered})
	}
	return out
}

// Stake registrations and deregistrations of Leios closures keep the slot they
// take effect at, the closure's context slot, beside the slot they were
// applied at; a certificate outside a closure has no separate effective slot.
func TestCertifiedStakeEventsKeepTheirEffectiveSlots(t *testing.T) {
	t.Parallel()
	_, followed, s := certifiedStakeScenario(t)

	require.Equal(t, []stakeShape{
		{s.Reg1, s.Ctx1, true}, {s.Dereg, s.CtxDereg, false},
		{s.Reg2, s.Ctx2, true},
	}, stakeShapes(t, followed, s.C))
	require.Equal(t, []stakeShape{{s.Plain, 0, true}},
		stakeShapes(t, followed, s.D))
}

// The derivation ranks the events by effective slot: at a slot between the
// effective slots the registration holds, after the deregistration's effective
// slot it does not, though the registration was applied last.
func TestLatestStakeEventEffectiveAtFollowsEffectiveOrder(t *testing.T) {
	t.Parallel()
	_, followed, s := certifiedStakeScenario(t)
	events, err := followed.StakeHistory(s.C)
	require.NoError(t, err)
	// In effective order: Reg1 at Ctx1, Reg2 at Ctx2, Dereg at CtxDereg.
	require.Less(t, s.Ctx1, s.Ctx2)
	require.Less(t, s.Ctx2, s.CtxDereg)
	for _, tc := range []struct {
		slot              uint64
		found, registered bool
	}{
		{0, false, false},
		{s.Ctx1 - 1, false, false},
		{s.Ctx1, true, true},
		{s.Ctx2 - 1, true, true},
		{s.Ctx2, true, true},
		{s.CtxDereg - 1, true, true},
		{s.CtxDereg, true, false},
		{s.Reg1, true, false},
		{s.Dereg, true, false},
		{s.Reg2, true, false},
		{math.MaxInt64, true, false},
	} {
		got := ledger.LatestStakeEventEffectiveAt(events, tc.slot)
		require.Equal(t, tc.found, got != nil, "slot %d", tc.slot)
		if got != nil {
			require.Equal(t, tc.registered, got.Registered, "slot %d", tc.slot)
		}
	}
}

// A witness of a Leios closure takes effect at the closure's context slot,
// while the applied-slot reads keep counting it at the slot it was applied at,
// over a registration, a deregistration and a zero withdrawal certified after
// their context slots.
func TestCertifiedWitnessesKeepTheirEffectiveSlots(t *testing.T) {
	t.Parallel()
	_, followed, s := certifiedStakeScenario(t)

	type witness struct{ slot, effective uint64 }
	witnesses := map[string][]witness{
		s.Payee.MapKey(): {{s.Withdraw, s.CtxWithdraw}},
		s.C.MapKey(): {
			{s.Reg1, s.Ctx1}, {s.Dereg, s.CtxDereg}, {s.Reg2, s.Ctx2},
		},
		s.D.MapKey(): {{s.Plain, s.Plain}},
	}
	for _, ref := range []models.StakeCredentialRef{s.Payee, s.C, s.D} {
		for _, max := range []uint64{
			s.Ctx1, s.Ctx1 + 1, s.Ctx2, s.CtxDereg, s.CtxWithdraw - 1,
			s.CtxWithdraw, s.Plain, s.Reg2, s.Withdraw, math.MaxInt64,
		} {
			var wantEff, wantApplied uint64
			var foundEff, foundApplied bool
			for _, w := range witnesses[ref.MapKey()] {
				if w.effective <= max && (!foundEff || w.effective > wantEff) {
					wantEff, foundEff = w.effective, true
				}
				if w.slot <= max && (!foundApplied || w.slot > wantApplied) {
					wantApplied, foundApplied = w.slot, true
				}
			}
			gotEff, ok, err := followed.LastWitnessEffectiveSlot(ref, max)
			require.NoError(t, err)
			require.Equal(t, foundEff, ok, "%x max %d", ref.Key, max)
			require.Equal(t, wantEff, gotEff, "%x max %d", ref.Key, max)
			gotApplied, ok, err := followed.LastWitnessSlot(ref, max)
			require.NoError(t, err)
			require.Equal(t, foundApplied, ok, "%x max %d", ref.Key, max)
			require.Equal(t, wantApplied, gotApplied, "%x max %d", ref.Key, max)
		}
	}
}

type delegationShape struct {
	slot, effective uint64
	pool            string
}

func delegationShapes(
	t *testing.T,
	set *utxoset.Set,
	ref models.StakeCredentialRef,
) []delegationShape {
	t.Helper()
	events, err := set.DelegationHistory(ref)
	require.NoError(t, err)
	var out []delegationShape
	for _, e := range events {
		out = append(out, delegationShape{e.Slot, e.EffectiveSlot, string(e.Pool)})
	}
	return out
}

func certifiedDelegationScenario(
	t *testing.T,
) (*utxoset.Set, ledger.CertifiedDelegationSlots) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.CertifyLeios()
	return set, scenario.CertifiedDelegations()
}

// Pool delegations of Leios closures keep the slot they take effect at beside
// the slot they were applied at; a delegation outside a closure has no
// separate effective slot.
func TestCertifiedDelegationsKeepTheirEffectiveSlots(t *testing.T) {
	t.Parallel()
	followed, s := certifiedDelegationScenario(t)

	require.Equal(t, []delegationShape{
		{s.D1, s.E1, string(s.PoolA)}, {s.D2, s.E2, string(s.PoolB)},
	}, delegationShapes(t, followed, s.X))
	require.Equal(t, []delegationShape{{s.Plain, 0, string(s.PoolA)}},
		delegationShapes(t, followed, s.Y))
}

// The derivation ranks the delegations by effective slot: at a slot between
// the effective slots the first pool holds, from the later one's effective slot
// the second does, though the first was applied earlier than the second and
// the order of the two is reversed in effective terms.
func TestLatestDelegationEffectiveAtFollowsEffectiveOrder(t *testing.T) {
	t.Parallel()
	followed, s := certifiedDelegationScenario(t)
	events, err := followed.DelegationHistory(s.X)
	require.NoError(t, err)
	pool := func(slot uint64) string {
		if e := ledger.LatestDelegationEffectiveAt(events, slot); e != nil {
			return string(e.Pool)
		}
		return ""
	}
	require.Equal(t, "", pool(s.E2-1))
	require.Equal(t, string(s.PoolB), pool(s.E2))
	require.Equal(t, string(s.PoolB), pool(s.E1-1))
	require.Equal(t, string(s.PoolA), pool(s.E1),
		"the first delegation is the later in effect")
	require.Equal(t, string(s.PoolA), pool(s.D2))
	require.Equal(t, string(s.PoolA), pool(math.MaxInt64))
}
