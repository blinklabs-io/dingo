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
	"github.com/stretchr/testify/require"
)

type registrationHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	slots    ledger.RegistrationSlots
}

// newRegistrationHarness runs the registration scenario in verify mode over
// a set that loaded its history first.
func newRegistrationHarness(t *testing.T) *registrationHarness {
	t.Helper()
	return newRegistrationHarnessWith(t, ledger.UtxoSetReadsVerify, nil)
}

// newRegistrationHarnessWith is newRegistrationHarness with the given read
// mode and a wrapper around the set the ledger writes to.
func newRegistrationHarnessWith(
	t *testing.T,
	reads string,
	wrap func(*utxoset.Set) ledger.UtxoSetWriter,
) *registrationHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &registrationHarness{set: set}
	var w ledger.UtxoSetWriter = set
	if wrap != nil {
		w = wrap(set)
	}
	h.scenario = ledger.NewBoundaryEpochStateScenarioForTest(t, w, reads)
	h.store = dbadapter.New(h.scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), h.store))
	h.slots = h.scenario.Registrations()
	return h
}

//go:fix inline
func ptr(v uint64) *uint64 { return new(v) }

func stakeHistory(
	t *testing.T,
	l ledgerdb.LedgerDB,
	key []byte,
	limit, offset int,
	order string,
) []models.AccountRegistrationHistoryRow {
	t.Helper()
	rows, err := l.GetAccountRegistrationHistoryByCredential(
		t.Context(), 0, key, limit, offset, order, nil,
	)
	require.NoError(t, err)
	return rows
}

type registrationShape struct {
	slot       uint64
	registered bool
	deposit    *uint64
}

func shapesOf(rows []models.AccountRegistrationHistoryRow) []registrationShape {
	out := make([]registrationShape, 0, len(rows))
	for _, r := range rows {
		out = append(out, registrationShape{r.AddedSlot, r.Action == "registered", r.Deposit})
	}
	return out
}

// TestRegistrationHistoryReadsAgreeAcrossReRegistrations requires every
// verified registration read to agree with the metadata store for a
// credential that registers, deregisters and re-registers with different
// deposits, and for DReps that register, update and retire.
func TestRegistrationHistoryReadsAgreeAcrossReRegistrations(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	s := h.slots
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()

	require.Equal(t, []registrationShape{
		{s.S1, true, ptr(2_000_000)},
		{s.S2, false, ptr(0)},
		{s.S3, true, ptr(3_000_000)},
		{s.S4, false, ptr(3_000_000)},
		{s.S5, true, ptr(4_000_000)},
	}, shapesOf(stakeHistory(t, ldb, s.K, 0, 0, "asc")))
	require.Equal(t,
		[]registrationShape{{s.S4, false, ptr(3_000_000)}, {s.S3, true, ptr(3_000_000)}},
		shapesOf(stakeHistory(t, ldb, s.K, 2, 1, "desc")))
	require.Equal(t, []registrationShape{{s.S1, true, nil}},
		shapesOf(stakeHistory(t, ldb, s.L, 0, 0, "asc")), "an unknown deposit stays nil")
	require.Equal(t, []registrationShape{{s.S1, true, ptr(1_000_000)}},
		shapesOf(stakeHistory(t, ldb, s.M, 1, 0, "asc")))
	require.Empty(t, stakeHistory(t, ldb, bytes.Repeat([]byte{0x01}, 28), 0, 0, "asc"))

	for _, tc := range []struct {
		slot       uint64
		registered bool
		deposit    *uint64
		none       bool
	}{
		{s.S1 - 1, false, nil, true},
		{s.S2, false, ptr(0), false},
		{s.S3, true, ptr(3_000_000), false},
		{s.S4, false, ptr(3_000_000), false},
		{s.S5 + 100, true, ptr(4_000_000), false},
	} {
		row, err := ldb.GetLatestAccountRegistrationAtOrBefore(ctx, 0, s.K, tc.slot, nil)
		require.NoError(t, err)
		if tc.none {
			require.Nil(t, row, "slot %d", tc.slot)
			continue
		}
		require.NotNil(t, row, "slot %d", tc.slot)
		require.Equal(t, tc.registered, row.Action == "registered", "slot %d", tc.slot)
		require.Equal(t, tc.deposit, row.Deposit, "slot %d", tc.slot)
	}

	for key, want := range map[string]int{
		string(s.K): 1, string(s.L): 1, string(s.M): 0,
	} {
		certs, err := ldb.GetStakeRegistrationsByCredential(0, []byte(key), nil)
		require.NoError(t, err)
		require.Len(t, certs, want, "stake registration certificates of %x", key)
	}

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, s.K), models.NewStakeCredentialRef(0, s.L),
		models.NewStakeCredentialRef(0, s.M), s.Imported,
	}
	for _, tc := range []struct {
		slot uint64
		want []string
	}{
		{s.S1 - 1, nil},
		{s.S1, []string{string(s.K), string(s.L), string(s.M)}},
		{s.S2, []string{string(s.L), string(s.M)}},
		{s.S3, []string{string(s.K), string(s.L), string(s.M)}},
		{s.S4, []string{string(s.L), string(s.M)}},
		{s.S5, []string{string(s.K), string(s.L), string(s.M)}},
	} {
		active, err := ldb.GetAccountsActiveAtSlot(refs, tc.slot, nil)
		require.NoError(t, err)
		var got []string
		for _, r := range refs[:3] {
			if _, ok := active[r.MapKey()]; ok {
				got = append(got, string(r.Key))
			}
		}
		require.ElementsMatch(t, tc.want, got, "slot %d", tc.slot)
	}

	for key, want := range map[string]*uint64{
		string(s.D): ptr(600_000_000), string(s.E): ptr(700_000_000),
		string(bytes.Repeat([]byte{0x02}, 28)): nil,
	} {
		got, err := ldb.GetDrepLastRegistrationDeposit(ctx, 0, []byte(key), nil)
		require.NoError(t, err)
		require.Equal(t, want, got, "DRep %x", key)
	}
	all, err := ldb.GetDrepLastRegistrationDeposits(ctx, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]uint64{models.DrepDepositKey(0, s.D): 600_000_000}, all,
		"only the DRep that registered again is active")

	events, err := h.set.StakeHistory(models.NewStakeCredentialRef(0, s.K))
	require.NoError(t, err)
	require.Len(t, events, 5)
	require.Equal(t, ledger.StakeClassLegacy, events[0].Class)
	require.Equal(t, ledger.StakeClassOther, events[2].Class)

	verified, mismatches := h.scenario.RegistrationReads()
	require.Positive(t, verified)
	require.Zero(t, mismatches)
}

// TestRegistrationHistoryRollsBackAcrossReRegistrations requires a rollback
// to remove the registrations and deregistrations after its target and
// restore a DRep's deposit, and the reads after it to agree with the
// metadata store.
func TestRegistrationHistoryRollsBackAcrossReRegistrations(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	s := h.slots
	k := models.NewStakeCredentialRef(0, s.K)
	before, err := h.set.StakeHistory(k)
	require.NoError(t, err)
	require.Len(t, before, 5)
	deposit, err := h.set.DRepDeposit(0, s.D)
	require.NoError(t, err)
	require.Equal(t, ptr(600_000_000), deposit)

	h.scenario.RollbackBeforeBoundary()

	for _, key := range [][]byte{s.K, s.L, s.M} {
		events, err := h.set.StakeHistory(models.NewStakeCredentialRef(0, key))
		require.NoError(t, err)
		require.Empty(t, events, "credential %x", key)
	}
	for _, key := range [][]byte{s.D, s.E} {
		deposit, err = h.set.DRepDeposit(0, key)
		require.NoError(t, err)
		require.Nil(t, deposit, "the rollback removes the registration of %x", key)
	}
	ldb := h.scenario.LedgerDB()
	require.Empty(t, stakeHistory(t, ldb, s.K, 0, 0, "asc"))
	row, err := ldb.GetLatestAccountRegistrationAtOrBefore(t.Context(), 0, s.K, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Nil(t, row)
	certs, err := ldb.GetStakeRegistrationsByCredential(0, s.K, nil)
	require.NoError(t, err)
	require.Empty(t, certs)
	active, err := ldb.GetAccountsActiveAtSlot(
		[]models.StakeCredentialRef{k, s.Imported}, s.S5, nil,
	)
	require.NoError(t, err)
	require.NotContains(t, active, k.MapKey())
	got, err := ldb.GetDrepLastRegistrationDeposit(t.Context(), 0, s.D, nil)
	require.NoError(t, err)
	require.Nil(t, got)
	all, err := ldb.GetDrepLastRegistrationDeposits(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, all)

	verified, mismatches := h.scenario.RegistrationReads()
	require.Positive(t, verified)
	require.Zero(t, mismatches)
}

// TestRegistrationHistoryLoadsFromTheMetadataStore requires a set from before
// it kept registration history to load every event and DRep deposit from the
// metadata store, as the set that followed the certificates holds them.
func TestRegistrationHistoryLoadsFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	loaded, err := fresh.RegistrationHistoryLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, fresh.LoadLedgerState(t.Context(), h.store))
	loaded, err = fresh.RegistrationHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	s := h.slots
	for _, key := range [][]byte{s.K, s.L, s.M} {
		ref := models.NewStakeCredentialRef(0, key)
		want, err := h.set.StakeHistory(ref)
		require.NoError(t, err)
		require.NotEmpty(t, want)
		got, err := fresh.StakeHistory(ref)
		require.NoError(t, err)
		require.Len(t, got, len(want), "credential %x", key)
		for i := range want {
			// Which table a loaded registration came from is not in the
			// history rows; the class is compared separately.
			want[i].Class, got[i].Class = 0, 0
			require.Equal(t, want[i], got[i], "credential %x event %d", key, i)
		}
	}
	classes := func(key []byte) []ledger.StakeRegistrationClass {
		events, err := fresh.StakeHistory(models.NewStakeCredentialRef(0, key))
		require.NoError(t, err)
		var out []ledger.StakeRegistrationClass
		for _, e := range events {
			if e.Registered {
				out = append(out, e.Class)
			}
		}
		return out
	}
	require.Equal(t, []ledger.StakeRegistrationClass{ledger.StakeClassLegacy}, classes(s.L),
		"as many stake registrations as registrations")
	require.Equal(t, []ledger.StakeRegistrationClass{ledger.StakeClassOther}, classes(s.M),
		"no stake registration row")
	require.Equal(t, []ledger.StakeRegistrationClass{
		ledger.StakeClassUnknown, ledger.StakeClassUnknown, ledger.StakeClassUnknown,
	}, classes(s.K), "one stake registration among three registrations")

	for key, want := range map[string]*uint64{
		string(s.D): ptr(600_000_000), string(s.E): ptr(700_000_000),
	} {
		got, err := fresh.DRepDeposit(0, []byte(key))
		require.NoError(t, err)
		require.Equal(t, want, got, "DRep %x", key)
	}
}

// TestRegistrationHistoryVerificationCountsADivergence requires a verified
// registration read to count a mismatch when the metadata store holds a DRep
// registration the set never saw.
func TestRegistrationHistoryVerificationCountsADivergence(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	_, before := h.scenario.RegistrationReads()
	cred := bytes.Repeat([]byte{0x7d}, 28)
	// Written around the ledger's LedgerDB, so the set never sees it.
	require.NoError(t, h.scenario.DB.Metadata().ImportDrep(
		&models.Drep{CredentialTag: 0, Credential: cred, AddedSlot: 5, Active: true},
		&models.RegistrationDrep{
			CredentialTag: 0, DrepCredential: cred, AddedSlot: 5,
			DepositAmount: types.Uint64(123),
		},
		nil,
	))
	got, err := h.scenario.LedgerDB().GetDrepLastRegistrationDeposit(t.Context(), 0, cred, nil)
	require.NoError(t, err)
	require.Equal(t, ptr(123), got, "the read answers from the metadata store")
	_, after := h.scenario.RegistrationReads()
	require.Equal(t, before+1, after)
}

// TestRegistrationHistoryIsVisibleInsideAnOpenTransaction requires reads in a
// write transaction to see the transaction's own staged registrations and
// agree with the metadata store.
func TestRegistrationHistoryIsVisibleInsideAnOpenTransaction(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	_, before := h.scenario.RegistrationReads()
	rows, deposit := h.scenario.RegistrationsInsideTxn(h.slots.DReReg + 1)
	require.Equal(t, []registrationShape{{h.slots.DReReg + 1, true, ptr(2_000_000)}}, shapesOf(rows))
	require.Equal(t, ptr(500_000_000), deposit)
	verified, mismatches := h.scenario.RegistrationReads()
	require.Zero(t, mismatches)
	require.Greater(t, verified, before)
}

// TestRegistrationHistoryViewHonoursAStagedRollback requires the set's view in
// a write transaction that staged a rollback to drop the committed events and
// restore the DRep deposit as the rollback leaves them.
func TestRegistrationHistoryViewHonoursAStagedRollback(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	k := models.NewStakeCredentialRef(0, h.slots.K)
	committed, err := h.set.StakeHistory(k)
	require.NoError(t, err)
	require.Len(t, committed, 5)
	events, deposit := h.scenario.RegistrationsInsideRollback(h.slots.K, h.slots.D)
	require.Empty(t, events, "the staged rollback cuts the committed events")
	require.Nil(t, deposit, "and restores the DRep's deposit to none")
}

// TestRegistrationHistoryStagesAGenesisDRepDeposit requires the genesis
// governance write to stage the deposit of a DRep whose registration has no
// certificate, so the set answers the deposit read as the metadata store does.
func TestRegistrationHistoryStagesAGenesisDRepDeposit(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	cred := bytes.Repeat([]byte{0x7c}, 28)
	h.scenario.GenesisDrep(cred, 321)
	deposit, err := h.set.DRepDeposit(0, cred)
	require.NoError(t, err)
	require.Equal(t, ptr(321), deposit)
	_, before := h.scenario.RegistrationReads()
	got, err := h.scenario.LedgerDB().GetDrepLastRegistrationDeposit(t.Context(), 0, cred, nil)
	require.NoError(t, err)
	require.Equal(t, ptr(321), got)
	_, after := h.scenario.RegistrationReads()
	require.Equal(t, before, after, "the set agrees with the metadata store")
}
