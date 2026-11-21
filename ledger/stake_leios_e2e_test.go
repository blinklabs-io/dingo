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
	"github.com/stretchr/testify/require"
)

func certifiedStakeScenario(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, *utxoset.Set, ledgerdb.LedgerDB, ledger.CertifiedStakeSlots) {
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
	return scenario, set, store, scenario.CertifiedStake()
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
// take effect at, which the store derives from the closure's context: the set
// that followed the certificates, the set loaded from the store, and the
// store's own rows agree on every event's applied and effective slot.
func TestCertifiedStakeEventsKeepTheirEffectiveSlots(t *testing.T) {
	t.Parallel()
	_, followed, store, s := certifiedStakeScenario(t)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.LoadLedgerState(t.Context(), store))

	require.Equal(t, []stakeShape{
		{s.Reg1, s.Ctx1, true}, {s.Dereg, s.CtxDereg, false},
		{s.Reg2, s.Ctx2, true},
	}, stakeShapes(t, followed, s.C))
	require.Equal(t, []stakeShape{{s.Plain, 0, true}},
		stakeShapes(t, followed, s.D))
	for _, ref := range []models.StakeCredentialRef{s.C, s.D} {
		require.Equal(t, stakeShapes(t, followed, ref), stakeShapes(t, fresh, ref))
		rows, err := store.GetAccountRegistrationHistoryByCredential(
			t.Context(), ref.Tag, ref.Key, 0, 0, "asc", nil,
		)
		require.NoError(t, err)
		var fromRows []stakeShape
		for _, r := range rows {
			fromRows = append(fromRows, stakeShape{
				r.AddedSlot, r.EffectiveSlot, r.Action == "registered",
			})
		}
		require.Equal(t, fromRows, stakeShapes(t, followed, ref))
	}
}

// The derivation ranks the events by effective slot: at a slot between the
// effective slots the registration holds, after the deregistration's effective
// slot it does not, though the registration was applied last. It matches the
// ranking the store's rows give.
func TestLatestStakeEventEffectiveAtFollowsEffectiveOrder(t *testing.T) {
	t.Parallel()
	_, followed, store, s := certifiedStakeScenario(t)
	events, err := followed.StakeHistory(s.C)
	require.NoError(t, err)
	rows, err := store.GetAccountRegistrationHistoryByCredential(
		t.Context(), s.C.Tag, s.C.Key, 0, 0, "asc", nil,
	)
	require.NoError(t, err)
	oracle := func(slot uint64) (bool, bool) {
		var best *models.AccountRegistrationHistoryRow
		eff := func(r *models.AccountRegistrationHistoryRow) uint64 {
			if r.EffectiveSlot != 0 {
				return r.EffectiveSlot
			}
			return r.AddedSlot
		}
		for i := range rows {
			r := &rows[i]
			if eff(r) > slot {
				continue
			}
			if best == nil || eff(r) > eff(best) ||
				(eff(r) == eff(best) && r.AddedSlot > best.AddedSlot) {
				best = r
			}
		}
		if best == nil {
			return false, false
		}
		return best.Action == "registered", true
	}
	for _, slot := range []uint64{
		0, s.Ctx1 - 1, s.Ctx1, s.Ctx2 - 1, s.Ctx2, s.CtxDereg - 1, s.CtxDereg,
		s.Reg1, s.Dereg, s.Reg2, math.MaxInt64,
	} {
		want, wantFound := oracle(slot)
		got := ledger.LatestStakeEventEffectiveAt(events, slot)
		require.Equal(t, wantFound, got != nil, "slot %d", slot)
		if got != nil {
			require.Equal(t, want, got.Registered, "slot %d", slot)
		}
	}
	last := ledger.LatestStakeEventEffectiveAt(events, math.MaxInt64)
	require.False(t, last.Registered,
		"the deregistration is the last in effect though applied before Reg2")
}

// A witness of a Leios closure takes effect at the closure's context slot,
// while the applied-slot reads keep counting it at the slot it was applied at.
// The set that followed the certificates, the set loaded from the store and
// the store's own witness rows agree on both, over a registration, a
// deregistration and a zero withdrawal certified after their context slots.
func TestCertifiedWitnessesKeepTheirEffectiveSlots(t *testing.T) {
	t.Parallel()
	_, followed, store, s := certifiedStakeScenario(t)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.LoadLedgerState(t.Context(), store))

	type witness struct{ slot, effective uint64 }
	fromStore := map[string][]witness{}
	require.NoError(t, store.IterateAccountWitnessEffectiveSlots(nil,
		func(ref models.StakeCredentialRef, slot, effective uint64) error {
			fromStore[ref.MapKey()] = append(
				fromStore[ref.MapKey()], witness{slot, effective},
			)
			return nil
		}))
	require.Contains(t, fromStore[s.Payee.MapKey()],
		witness{s.Withdraw, s.CtxWithdraw})
	require.Contains(t, fromStore[s.C.MapKey()], witness{s.Reg2, s.Ctx2})
	require.Contains(t, fromStore[s.D.MapKey()], witness{s.Plain, s.Plain})

	for _, set := range []*utxoset.Set{followed, fresh} {
		for _, ref := range []models.StakeCredentialRef{s.Payee, s.C, s.D} {
			for _, max := range []uint64{
				s.Ctx1, s.Ctx1 + 1, s.Ctx2, s.CtxDereg, s.CtxWithdraw - 1,
				s.CtxWithdraw, s.Plain, s.Reg2, s.Withdraw, math.MaxInt64,
			} {
				var wantEff, wantApplied uint64
				var foundEff, foundApplied bool
				for _, w := range fromStore[ref.MapKey()] {
					if w.effective <= max && (!foundEff || w.effective > wantEff) {
						wantEff, foundEff = w.effective, true
					}
					if w.slot <= max && (!foundApplied || w.slot > wantApplied) {
						wantApplied, foundApplied = w.slot, true
					}
				}
				gotEff, ok, err := set.LastWitnessEffectiveSlot(ref, max)
				require.NoError(t, err)
				require.Equal(t, foundEff, ok, "%x max %d", ref.Key, max)
				require.Equal(t, wantEff, gotEff, "%x max %d", ref.Key, max)
				gotApplied, ok, err := set.LastWitnessSlot(ref, max)
				require.NoError(t, err)
				require.Equal(t, foundApplied, ok, "%x max %d", ref.Key, max)
				require.Equal(t, wantApplied, gotApplied, "%x max %d", ref.Key, max)
			}
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
) (*utxoset.Set, ledgerdb.LedgerDB, ledger.CertifiedDelegationSlots) {
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
	return set, store, scenario.CertifiedDelegations()
}

// Pool delegations of Leios closures keep the slot they take effect at,
// staged through the ledger and loaded from the store alike: the set that
// followed the certificates, the set loaded from the store, and the store's
// own rows agree on each certificate's applied slot, effective slot and pool.
func TestCertifiedDelegationsKeepTheirEffectiveSlots(t *testing.T) {
	t.Parallel()
	followed, store, s := certifiedDelegationScenario(t)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.LoadLedgerState(t.Context(), store))

	require.Equal(t, []delegationShape{
		{s.D1, s.E1, string(s.PoolA)}, {s.D2, s.E2, string(s.PoolB)},
	}, delegationShapes(t, followed, s.X))
	require.Equal(t, []delegationShape{{s.Plain, 0, string(s.PoolA)}},
		delegationShapes(t, followed, s.Y))
	fromStore := map[string][]delegationShape{}
	require.NoError(t, store.IterateStakeDelegationEvents(nil,
		func(r models.StakeDelegationEventRow) error {
			k := models.NewStakeCredentialRef(r.CredentialTag, r.StakingKey).MapKey()
			fromStore[k] = append(fromStore[k], delegationShape{
				r.AddedSlot, r.EffectiveSlot, string(r.PoolKeyHash),
			})
			return nil
		}))
	for _, ref := range []models.StakeCredentialRef{s.X, s.Y} {
		require.Equal(t, delegationShapes(t, followed, ref),
			delegationShapes(t, fresh, ref))
		require.ElementsMatch(t, fromStore[ref.MapKey()],
			delegationShapes(t, followed, ref))
	}
}

// The derivation ranks the delegations by effective slot: at a slot between
// the effective slots the first pool holds, from the later one's effective slot
// the second does, though the first was applied earlier than the second and
// the order of the two is reversed in effective terms.
func TestLatestDelegationEffectiveAtFollowsEffectiveOrder(t *testing.T) {
	t.Parallel()
	followed, _, s := certifiedDelegationScenario(t)
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
