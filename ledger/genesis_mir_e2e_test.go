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
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// genesisMIRWindow is the stability window the header reads are asked with.
const genesisMIRWindow = 5

// harnessDonation is the donation the scenario seeds in the ended epoch,
// which joins the treasury at the boundary.
const harnessDonation = 25

type genesisMIRHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	slots    ledger.GenesisMIRSlots
	reads    string
	// ownerBefore, delegatorBefore and potsBefore are read just before the
	// boundary, which settles the MIR certificates.
	ownerBefore, delegatorBefore   uint64
	reservesBefore, treasuryBefore uint64
}

func newGenesisMIRHarness(t *testing.T, reads string) *genesisMIRHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &genesisMIRHarness{set: set, reads: reads}
	h.scenario = ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	h.store = dbadapter.New(h.scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), h.store))
	h.slots = h.scenario.GenesisMIR(func() {
		reward := func(key []byte) uint64 {
			a, err := set.Account(0, key)
			require.NoError(t, err)
			require.NotNil(t, a)
			return uint64(a.Reward)
		}
		h.ownerBefore = reward(bytes71())
		h.delegatorBefore = reward(bytes72())
		state, err := h.scenario.LedgerDB().GetNetworkState(nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		h.reservesBefore, h.treasuryBefore = uint64(state.Reserves), uint64(state.Treasury)
	})
	return h
}

func bytes71() []byte { return repeated(0x71, 28) }
func bytes72() []byte { return repeated(0x72, 28) }

func repeated(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func forEachGenesisMIRRead(t *testing.T, fn func(t *testing.T, reads string)) {
	t.Helper()
	for _, reads := range []string{"set"} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			fn(t, reads)
		})
	}
}

// TestGenesisDelegationAndMIRReadsFollowTheCertificates requires the genesis
// delegation and MIR reads to answer from the certificates the ledger
// applied, from the set, and the epoch boundary that settles the MIR
// certificates to credit the registered accounts and move the pots.
func TestGenesisDelegationAndMIRReadsFollowTheCertificates(t *testing.T) {
	t.Parallel()
	forEachGenesisMIRRead(t, func(t *testing.T, reads string) {
		h := newGenesisMIRHarness(t, reads)
		s := h.slots
		ldb := h.scenario.LedgerDB()
		ctx := t.Context()

		forSlot := func(slot uint64) []byte {
			t.Helper()
			d, err := ldb.GetGenesisDelegationForSlot(s.Genesis, slot, genesisMIRWindow, nil)
			require.NoError(t, err)
			if d == nil {
				return nil
			}
			return d.GenesisDelegateHash
		}
		require.Nil(t, forSlot(s.DelegA+genesisMIRWindow-1), "not yet in effect")
		require.Nil(t, forSlot(genesisMIRWindow-1), "before the window has elapsed")
		require.Nil(t, forSlot(s.DelegA+genesisMIRWindow-1))
		require.Equal(t, s.DelegateA, forSlot(s.DelegA+genesisMIRWindow))
		require.Equal(t, s.DelegateA, forSlot(s.DelegB+genesisMIRWindow-1))
		require.Equal(t, s.DelegateB, forSlot(s.DelegB+genesisMIRWindow))
		d, err := ldb.GetGenesisDelegationForSlot(
			s.Genesis, s.DelegB+genesisMIRWindow, genesisMIRWindow, nil,
		)
		require.NoError(t, err)
		require.Equal(t, s.VrfB, d.VrfKeyHash)
		require.Equal(t, s.DelegB, d.AddedSlot)
		other, err := ldb.GetGenesisDelegationForSlot(
			repeated(0x01, 28), math.MaxInt64, genesisMIRWindow, nil,
		)
		require.NoError(t, err)
		require.Nil(t, other)

		ranged := func(from, upto uint64) int {
			t.Helper()
			rows, err := ldb.GetGenesisDelegationsInSlotRange(from, upto, nil)
			require.NoError(t, err)
			return len(rows)
		}
		require.Equal(t, 2, ranged(s.DelegA, s.DelegB), "both ends are inclusive")
		require.Equal(t, 1, ranged(s.DelegA, s.DelegA))
		require.Equal(t, 1, ranged(s.DelegA+1, s.DelegB))
		require.Equal(t, 1, ranged(s.DelegB, s.DelegB+9))
		require.Zero(t, ranged(s.DelegB+1, s.DelegB+9))
		require.Zero(t, ranged(s.DelegB, s.DelegA), "an inverted range is empty")

		effects, err := ldb.GetMIRCertsInSlotRange(ctx, s.EpochStart, s.Boundary, nil)
		require.NoError(t, err)
		require.Len(t, effects, 4)
		for _, e := range effects {
			require.Equal(t, s.Distribute, e.AddedSlot)
		}
		require.Equal(t, uint(0), effects[0].Pot)
		require.Len(t, effects[0].Rewards, 2)
		require.Equal(t, uint(1), effects[1].Pot)
		require.Len(t, effects[1].Rewards, 1)
		require.Equal(t, int64(s.TreasuryToOwner), effects[1].Rewards[0].Amount.Int64())
		require.Equal(t, s.ToTreasuryAmount, effects[2].OtherPot)
		require.Equal(t, uint(0), effects[2].Pot)
		require.Equal(t, s.ToReservesAmount, effects[3].OtherPot)
		require.Equal(t, uint(1), effects[3].Pot)
		effects, err = ldb.GetMIRCertsInSlotRange(ctx, s.EpochStart, s.Distribute, nil)
		require.NoError(t, err)
		require.Empty(t, effects, "the range excludes its end")

		// The boundary's own read and its credits and pot writes.
		owner, err := h.set.Account(0, s.Owner)
		require.NoError(t, err)
		require.Equal(t,
			h.ownerBefore+s.ReservesToOwner+s.TreasuryToOwner, uint64(owner.Reward))
		delegator, err := h.set.Account(0, s.Delegator)
		require.NoError(t, err)
		require.Equal(t, h.delegatorBefore+s.ReservesToDelegator, uint64(delegator.Reward))
		state, err := ldb.GetNetworkState(nil)
		require.NoError(t, err)
		require.Equal(t, s.Boundary, state.Slot)
		require.Equal(t,
			h.reservesBefore-s.ReservesToOwner-s.ReservesToDelegator-
				s.ToTreasuryAmount+s.ToReservesAmount,
			uint64(state.Reserves))
		require.Equal(t,
			h.treasuryBefore-s.TreasuryToOwner+s.ToTreasuryAmount-s.ToReservesAmount+
				harnessDonation,
			uint64(state.Treasury))

	})
}

// TestGenesisDelegationAndMIRRollBack requires a rollback to remove every
// genesis delegation and MIR certificate after its target from the set.
func TestGenesisDelegationAndMIRRollBack(t *testing.T) {
	t.Parallel()
	forEachGenesisMIRRead(t, func(t *testing.T, reads string) {
		h := newGenesisMIRHarness(t, reads)
		s := h.slots
		delegs, err := h.set.GenesisDelegations()
		require.NoError(t, err)
		require.Len(t, delegs, 2)
		mirs, err := h.set.MIRCerts()
		require.NoError(t, err)
		require.Len(t, mirs, 4)
		require.Equal(t, s.DelegA, delegs[0].AddedSlot)
		require.Equal(t, s.DelegB, delegs[1].AddedSlot)

		if reads == "set" {
			delegs, mirs := h.scenario.CertHistoryInsideRollback()
			require.Equal(t, 1, delegs, "a read inside the rollback sees it")
			require.Equal(t, 4, mirs, "MIR certificates remain SQL-backed during a staged set rollback")
		}
		h.scenario.RollbackBeforeBoundary()

		delegs, err = h.set.GenesisDelegations()
		require.NoError(t, err)
		require.Len(t, delegs, 1, "the delegation at the rollback target stays")
		require.Equal(t, s.DelegA, delegs[0].AddedSlot)
		mirs, err = h.set.MIRCerts()
		require.NoError(t, err)
		require.Empty(t, mirs)
		ldb := h.scenario.LedgerDB()
		d, err := ldb.GetGenesisDelegationForSlot(
			s.Genesis, math.MaxInt64, genesisMIRWindow, nil,
		)
		require.NoError(t, err)
		require.Equal(t, s.DelegateA, d.GenesisDelegateHash,
			"the later delegation is gone")
		effects, err := ldb.GetMIRCertsInSlotRange(
			t.Context(), s.EpochStart, s.Boundary, nil,
		)
		require.NoError(t, err)
		require.Empty(t, effects)
	})
}

// TestGenesisDelegationAndMIRLoadFromTheMetadataStore requires a set from
// before it kept them to load every delegation and MIR certificate from the
// metadata store, as the set that followed the certificates holds them.
func TestGenesisDelegationAndMIRLoadFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	h := newGenesisMIRHarness(t, "metadata-visible")
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	loaded, err := fresh.GenesisMIRLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, fresh.LoadLedgerState(t.Context(), h.store))
	loaded, err = fresh.GenesisMIRLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	want, err := h.set.GenesisDelegations()
	require.NoError(t, err)
	got, err := fresh.GenesisDelegations()
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, want, got, "the position of a delegation is exact")

	wantMIR, err := h.set.MIRCerts()
	require.NoError(t, err)
	gotMIR, err := fresh.MIRCerts()
	require.NoError(t, err)
	require.Len(t, gotMIR, 4)
	for i := range wantMIR {
		require.Equal(t, wantMIR[i].Slot, gotMIR[i].Slot)
		require.Equal(t, wantMIR[i].Pot, gotMIR[i].Pot)
		require.Equal(t, wantMIR[i].OtherPot, gotMIR[i].OtherPot)
		require.Len(t, gotMIR[i].Rewards, len(wantMIR[i].Rewards))
		for j := range wantMIR[i].Rewards {
			require.Equal(t, wantMIR[i].Rewards[j].Credential, gotMIR[i].Rewards[j].Credential)
			require.Zero(t, wantMIR[i].Rewards[j].Amount.Cmp(gotMIR[i].Rewards[j].Amount))
		}
	}
}

// TestGenesisDelegationAndMIRReadsIgnoreUntrackedMetadataWrites requires set
// reads to ignore certificates written directly to the metadata store.
func TestGenesisDelegationAndMIRReadsIgnoreUntrackedMetadataWrites(t *testing.T) {
	t.Parallel()
	forEachGenesisMIRRead(t, func(t *testing.T, reads string) {
		h := newGenesisMIRHarness(t, reads)
		s := h.slots
		raw, err := dbtest.RawSQLiteMetadata(t, h.scenario.DB)
		require.NoError(t, err)
		// Written around the ledger's LedgerDB, so the set never sees them.
		_, err = raw.Exec(`
INSERT INTO genesis_delegation (
    genesis_hash, genesis_delegate_hash, vrf_key_hash,
    added_slot, block_index, cert_index, certificate_id
) VALUES (?, ?, ?, ?, 0, 0, 999999)`,
			s.Genesis, s.DelegateA, s.VrfA, s.DelegB+10)
		require.NoError(t, err)
		_, err = raw.Exec(`
INSERT INTO move_instantaneous_rewards (pot, added_slot, other_pot)
VALUES (0, ?, '7')`, s.DelegB+10)
		require.NoError(t, err)

		ldb := h.scenario.LedgerDB()
		rows, err := ldb.GetGenesisDelegationsInSlotRange(s.DelegB+10, s.DelegB+10, nil)
		require.NoError(t, err)
		effects, err := ldb.GetMIRCertsInSlotRange(
			t.Context(), s.DelegB+10, s.DelegB+11, nil,
		)
		require.NoError(t, err)
		require.Empty(t, rows, "the read answers from the set")
		require.Len(t, effects, 1)
		require.Equal(t, s.DelegB+10, effects[0].AddedSlot)
		require.Equal(t, uint64(7), effects[0].OtherPot)
	})
}
