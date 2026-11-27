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

// leiosBoundaryAnswers are the four epoch-boundary stake reads at one
// snapshot slot.
type leiosBoundaryAnswers struct {
	Delegated          [][]byte
	Stakes, Delegators map[string]uint64
	Inputs, Pointers   []*models.RewardStakeInput
}

func readLeiosBoundary(
	t *testing.T,
	db ledgerdb.LedgerDB,
	pools [][]byte,
	slot uint64,
) (leiosBoundaryAnswers, error) {
	t.Helper()
	var a leiosBoundaryAnswers
	var err error
	if a.Delegated, err = db.GetEpochBoundaryDelegatedPoolKeyHashes(
		slot, 0, nil,
	); err != nil {
		return a, err
	}
	if a.Stakes, a.Delegators, err = db.GetEpochBoundaryStakeByPools(
		pools, slot, 0, nil, 0, 0, nil,
	); err != nil {
		return a, err
	}
	if a.Inputs, err = db.GetEpochBoundaryRewardStakeInputsForPools(
		pools, slot, 0, nil, 0, 0, nil,
	); err != nil {
		return a, err
	}
	a.Pointers, err = db.GetPointerStakeInputsForPools(pools, slot, 0, 0, nil)
	return a, err
}

type leiosBoundaryHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	slots    ledger.CertifiedBoundaryStakeSlots
}

func newLeiosBoundaryHarness(
	t *testing.T,
	reads string,
	withdrawAcrossSnapshot bool,
) *leiosBoundaryHarness {
	t.Helper()
	return newLeiosBoundaryHarnessWith(t, reads, ledger.CertifiedBoundaryOptions{
		WithdrawAcrossSnapshot: withdrawAcrossSnapshot,
	})
}

func newLeiosBoundaryHarnessWith(
	t *testing.T,
	reads string,
	opts ledger.CertifiedBoundaryOptions,
) *leiosBoundaryHarness {
	t.Helper()
	h := newBoundaryStakeHarness(t, reads)
	h.scenario.CertifyLeios()
	return &leiosBoundaryHarness{
		set: h.set, scenario: h.scenario,
		store: dbadapter.New(h.scenario.DB),
		slots: h.scenario.CertifiedBoundaryStakeWith(opts),
	}
}

// requireLeiosBoundaryFigures requires the figures derived by hand from the
// closures' effective order:
// at the snapshot slot, between the effective and applied slots of the
// closures, and after every transaction.
func requireLeiosBoundaryFigures(
	t *testing.T,
	s ledger.CertifiedBoundaryStakeSlots,
	a leiosBoundaryAnswers,
	current bool,
) {
	t.Helper()
	wStake := s.WEarly + s.WCreated
	delegatedA := uint64(6) // two harness accounts, X2, Q, W and U
	delegated := [][]byte{s.A, s.B, s.D}
	delegatorsD := uint64(1)
	if current {
		wStake += s.WLate
		delegated = [][]byte{s.A, s.B}
		delegatorsD = 0
	}
	require.ElementsMatch(t, delegated, a.Delegated)
	require.Equal(t, 40_000_000+wStake, a.Stakes[string(s.A)])
	require.Equal(t, delegatedA, a.Delegators[string(s.A)])
	require.Equal(t, uint64(4_000_000), a.Stakes[string(s.B)])
	require.Equal(t, uint64(1), a.Delegators[string(s.B)])
	require.Zero(t, a.Stakes[string(s.C)])
	require.Zero(t, a.Delegators[string(s.C)])
	require.Zero(t, a.Stakes[string(s.D)])
	require.Equal(t, delegatorsD, a.Delegators[string(s.D)])
	byKey := map[string]uint64{}
	for _, in := range a.Inputs {
		byKey[string(in.StakingKey)] = uint64(in.Stake)
	}
	require.Equal(t, wStake, byKey[string(s.W.Key)])
	require.Equal(t, uint64(4_000_000), byKey[string(s.X.Key)])
	require.NotContains(t, byKey, string(s.Y.Key), "deregistered in effect")
	require.NotContains(t, byKey, string(s.R.Key), "deregistered in effect")
	require.NotContains(t, byKey, string(s.V.Key), "pool retired in effect")
}

// The set derives the four epoch-boundary stake reads over Leios closures
// applied after the slot they take effect at: re-delegations in
// reverse effective order, registrations and deregistrations, staked outputs
// created and spent, pools retired by their effective slot, each applied after
// the snapshot slot and effective before it. The reads are also made after
// every transaction, where only the effective order separates the answers.
func TestLeiosBoundaryStakeReadsFollowTheEffectiveOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		current bool
	}{
		{"snapshot between effective and applied slots", false},
		{"snapshot after every transaction", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newLeiosBoundaryHarness(t, "metadata-visible", false)
			s := h.slots
			slot := s.Snapshot
			if tc.current {
				slot = s.Current
			}
			tip, _, err := h.set.Tip()
			require.NoError(t, err)
			require.Equal(t, tc.current, tip.Slot <= slot)
			got, err := readLeiosBoundary(
				t, h.scenario.LedgerDB(), s.Pools, slot,
			)
			require.NoError(t, err)
			requireLeiosBoundaryFigures(t, s, got, tc.current)
		})
	}
}

// A closure applied through LedgerDelta.apply, as block processing applies
// one, reaches the set with the slot it takes effect at: its created and
// spent outputs as closure effects, and its registrations, delegations and
// witnesses with their effective slots. Nothing here is injected into the set.
func TestClosureDeltaReachesTheSetWithItsEffectiveSlots(t *testing.T) {
	t.Parallel()
	h := newLeiosBoundaryHarness(t, "set", false)
	s := h.slots

	type effect struct {
		applied, effective uint64
		spent              bool
		lovelace           uint64
	}
	var effects []effect
	require.NoError(t, h.set.ClosureUtxoEffectsAfter(0,
		func(e ledger.ClosureUtxoEffect) error {
			effects = append(effects, effect{
				e.Applied, e.Effective, e.Spent, e.Lovelace,
			})
			return nil
		}))
	require.Contains(t, effects, effect{
		s.WCreatedApplied, s.WCreatedEff, false, s.WCreated,
	}, "the output a closure created")
	var spent []effect
	for _, e := range effects {
		if e.spent {
			spent = append(spent, e)
		}
	}
	require.Len(t, spent, 1, "the output a closure spent")
	require.Equal(t, s.WSpent, spent[0].lovelace)
	require.Less(t, spent[0].effective, spent[0].applied)
	for _, e := range effects {
		require.Less(t, e.effective, e.applied,
			"only closures that take effect before they are applied")
	}

	require.Equal(t, []delegationShape{
		{s.XApplied1, s.XEff1, string(s.A)},
		{s.XApplied2, s.XEff2, string(s.B)},
	}, delegationShapes(t, h.set, s.X))
	require.Equal(t, []delegationShape{
		{s.X2AppliedA, s.X2EffA, string(s.A)},
		{s.X2AppliedB, s.X2EffB, string(s.B)},
	}, delegationShapes(t, h.set, s.X2))
	stake := stakeShapes(t, h.set, s.Q)
	require.Equal(t, []stakeShape{{s.QApplied, s.QEff, true}}, stake)
	effective, ok, err := h.set.LastWitnessEffectiveSlot(s.Q, math.MaxInt64)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, s.QEff, effective)
}

// The set keeps no amount for a withdrawal, so a closure withdrawal applied
// after the snapshot slot and effective before it leaves the reward balance
// the store reads at that slot out of its reach. A read of such a snapshot
// fails closed when the set cannot serve the snapshot; a snapshot after the
// withdrawal is applied is served.
func TestLeiosBoundaryStakeRefusesAClosureWithdrawalAcrossTheSnapshot(
	t *testing.T,
) {
	t.Parallel()
	h := newLeiosBoundaryHarness(t, "set", true)
	s := h.slots
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	_, err := readLeiosBoundary(t, h.scenario.LedgerDB(), s.Pools, s.Snapshot)
	require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable)
	require.Zero(t, reached(), "the read reached the metadata store")

	before := reached()
	_, err = readLeiosBoundary(t, h.scenario.LedgerDB(), s.Pools, s.Current)
	require.NoError(t, err)
	require.Equal(t, before, reached(), "a served read reached the store")

	_, err = readLeiosBoundary(t, h.scenario.LedgerDB(), s.Pools, s.Snapshot)
	require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable)
	require.Equal(t, before, reached(), "a refused read reached the store")
	_, err = readLeiosBoundary(t, h.scenario.LedgerDB(), s.Pools, s.Current)
	require.NoError(t, err)
}

func rewardInputStake(inputs []*models.RewardStakeInput, ref models.StakeCredentialRef) (
	uint64, bool,
) {
	for _, in := range inputs {
		if in.CredentialTag == ref.Tag && bytes.Equal(in.StakingKey, ref.Key) {
			return uint64(in.Stake), true
		}
	}
	return 0, false
}

// G withdraws from its reward account and delegates to B in one closure
// applied after the snapshot slot and effective before it. The closure's
// certificate and withdrawal witness G at the same slots, so the certificate
// cannot vouch for the withdrawal, and the balance the set reads at the
// snapshot does not count it, so the read is refused.
func TestLeiosBoundaryStakeRefusesAClosureThatWithdrawsAndDelegates(
	t *testing.T,
) {
	t.Parallel()
	opts := ledger.CertifiedBoundaryOptions{GWithdraws: true}
	t.Run("set refuses an unserved pre-closure balance", func(t *testing.T) {
		t.Parallel()
		h := newLeiosBoundaryHarnessWith(t, "set", opts)
		s := h.slots
		reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
		hide(true)
		_, err := readLeiosBoundary(
			t, h.scenario.LedgerDB(), s.Pools, s.Snapshot,
		)
		hide(false)
		require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable)
		require.Zero(t, reached(), "the read reached the metadata store")
	})
	t.Run("the set refuses what it cannot derive", func(t *testing.T) {
		t.Parallel()
		h := newLeiosBoundaryHarnessWith(t, "set", opts)
		s := h.slots
		reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
		hide(true)
		_, err := readLeiosBoundary(
			t, h.scenario.LedgerDB(), s.Pools, s.Snapshot,
		)
		require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable)
		_, err = readLeiosBoundary(
			t, h.scenario.LedgerDB(), s.Pools, s.Current,
		)
		require.NoError(t, err)
		require.Zero(t, reached(), "a read reached the metadata store")
	})
}

// An old closure witness, applied in a later epoch than it takes effect in, is
// superseded by a later ordinary witness before the snapshot. The store reads
// the expiry from the ordinary witness, as the set does, so the closure's
// witness must not make the read refuse. Before the ordinary witness is
// applied the store's pick is the closure witness and the set's expiry is
// stamped in the other epoch, which the set cannot reproduce.
func TestLeiosBoundaryStakeServesASupersededClosureWitnessUnderExpiry(
	t *testing.T,
) {
	t.Parallel()
	const expiryEpoch, inactivity = 1, 100
	h := newLeiosBoundaryHarnessWith(t, "set",
		ledger.CertifiedBoundaryOptions{KSuperseded: true})
	s := h.slots
	pools := [][]byte{s.E}
	read := func(db ledgerdb.LedgerDB, slot uint64) (
		map[string]uint64, []*models.RewardStakeInput, error,
	) {
		stakes, _, err := db.GetEpochBoundaryStakeByPools(
			pools, slot, 0, nil, expiryEpoch, inactivity, nil,
		)
		if err != nil {
			return nil, nil, err
		}
		inputs, err := db.GetEpochBoundaryRewardStakeInputsForPools(
			pools, slot, 0, nil, expiryEpoch, inactivity, nil,
		)
		return stakes, inputs, err
	}
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	_, inputs, err := read(h.scenario.LedgerDB(), s.Snapshot)
	require.NoError(t, err, "the superseded closure witness refused the read")
	_, counted := rewardInputStake(inputs, s.K)
	require.True(t, counted, "K is counted at the snapshot")
	require.Zero(t, reached(), "a read reached the metadata store")

	_, _, err = read(h.scenario.LedgerDB(), s.KApplied+1)
	require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable,
		"the closure witness is the store's pick before the ordinary one")
}
