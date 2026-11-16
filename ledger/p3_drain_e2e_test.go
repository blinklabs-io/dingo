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
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

var (
	committeeTables = []string{
		"auth_committee_hot", "resign_committee_cold", "committee_member",
		"committee_quorum", "constitution",
	}
	opCertTables = []string{"pool_opcert_sequence"}
)

// p3Run is what p3DrainRun observed.
type p3Run struct {
	reads map[string][]string
	guard *statedrain.TripGuard
	db    *database.Database
}

func renderCommittee(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	colds [][]byte,
) []string {
	t.Helper()
	ctx := t.Context()
	var out []string
	c, err := ldb.GetConstitution(nil)
	require.NoError(t, err)
	if c != nil {
		out = append(out, fmt.Sprintf(
			"constitution %s %x %x %d",
			c.AnchorURL,
			c.AnchorHash,
			c.PolicyHash,
			c.AddedSlot,
		))
	}
	q, err := ldb.GetCommitteeQuorum(ctx, nil)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("quorum %v", q))
	members, err := ldb.GetCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	out = append(out, committeeMemberRows(members)...)
	all, err := ldb.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.NoError(t, err)
	out = append(out, "all")
	out = append(out, committeeMemberRows(all)...)
	active, err := ldb.GetActiveCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	out = append(out, "active")
	out = append(out, committeeAuthRows(active)...)
	n, err := ldb.GetCommitteeAuthorizedCount(ctx, nil)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("count %d", n))
	since, err := ldb.GetCommitteeHotAuthorizationsSince(ctx, 0, nil)
	require.NoError(t, err)
	out = append(out, "since")
	out = append(out, committeeAuthRows(since)...)
	var creds []models.CommitteeCredential
	for _, cold := range colds {
		creds = append(creds, models.CommitteeCredential{Credential: cold})
		a, err := ldb.GetCommitteeMember(ctx, 0, cold, 0, nil)
		if errors.Is(err, models.ErrCommitteeMemberNotFound) {
			out = append(out, fmt.Sprintf("member %x none", cold))
		} else {
			require.NoError(t, err)
			out = append(out, "member "+committeeAuthRows(
				[]*models.AuthCommitteeHot{a})[0])
		}
		r, err := ldb.IsCommitteeMemberResigned(ctx, 0, cold, 0, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("resigned %x %t", cold, r))
	}
	resigned, err := ldb.GetResignedCommitteeMembers(ctx, creds, nil)
	require.NoError(t, err)
	for _, k := range slices.Sorted(maps.Keys(resigned)) {
		out = append(out, fmt.Sprintf("resigned set %x", k))
	}
	return out
}

// renderOpCerts renders the reads the set answers in every state the
// scenario reaches: slots at or after the first observation it records.
func renderOpCerts(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	pools [][]byte,
	slots []uint64,
) []string {
	t.Helper()
	ctx := t.Context()
	var out []string
	all, err := ldb.LatestPoolOpCertSequences(ctx, nil)
	require.NoError(t, err)
	for _, k := range slices.Sorted(maps.Keys(all)) {
		out = append(out, fmt.Sprintf("latest %x %d", k, all[k]))
	}
	for _, pool := range pools {
		pkh := lcommon.PoolKeyHash(pool)
		seq, found, err := ldb.LatestPoolOpCertSequence(ctx, pkh, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("pool %x %d %t", pool, seq, found))
		for _, slot := range slots {
			seq, found, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
				ctx,
				pkh,
				slot,
				nil,
			)
			require.NoError(t, err, "pool %x at or before %d", pool, slot)
			out = append(out, fmt.Sprintf(
				"pool %x at or before %d: %d %t", pool, slot, seq, found,
			))
		}
	}
	for _, slot := range slots {
		at, err := ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, slot, nil)
		require.NoError(t, err, "every pool at or before %d", slot)
		for _, k := range slices.Sorted(maps.Keys(at)) {
			out = append(out, fmt.Sprintf("at %d %x %d", slot, k, at[k]))
		}
		seen, err := ldb.PoolOpCertSequencesExistAtSlot(ctx, slot, nil)
		require.NoError(t, err, "observed at %d", slot)
		out = append(out, fmt.Sprintf("observed %d %t", slot, seen))
	}
	return out
}

// p3DrainRun drives a family through writes, an optional drain, more
// writes, a rollback and a replay, and returns what the LedgerDB answered
// after each phase.
func p3DrainRun(
	t *testing.T,
	family nodesettings.Family,
	drain bool,
	corruptBeforeDrain func(*database.Database),
) (p3Run, error) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet, guard,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	run := p3Run{reads: map[string][]string{}, guard: guard, db: scenario.DB}
	ldb := scenario.LedgerDB()
	start := scenario.EpochStartSlot()

	var render func() []string
	var write func()
	switch family {
	case nodesettings.FamilyCommitteeConstitution:
		slots := scenario.Committee()
		colds := [][]byte{
			slots.A, slots.B, slots.C, bytes.Repeat([]byte{0xd2}, 28),
		}
		render = func() []string { return renderCommittee(t, ldb, colds) }
		write = func() {
			scenario.CommitteeLater(
				colds[3], bytes.Repeat([]byte{0xd3}, 28),
				bytes.Repeat([]byte{0xd4}, 28), start+60,
			)
		}
	case nodesettings.FamilyOpCerts:
		slots := scenario.OpCerts()
		pools := [][]byte{slots.Pool, slots.Other}
		observed := []uint64{
			slots.Raised, slots.Repeated, slots.Lowered, slots.Batched,
			start + 60, start + 61, start + 62, start + 70,
		}
		render = func() []string {
			return renderOpCerts(t, ldb, pools, observed)
		}
		write = func() {
			scenario.OpCertsLater(slots.Pool, slots.Other, start+60)
		}
	default:
		t.Fatalf("no scenario for family %s", family)
	}
	phase := func(name string) { run.reads[name] = render() }

	phase("before")
	if drain {
		if corruptBeforeDrain != nil {
			corruptBeforeDrain(scenario.DB)
		}
		src := drainTestSource{set: set, db: scenario.DB}
		var entries []statedrain.Entry
		for _, e := range append(statedrain.Graduated(src), statedrain.Staged(src)...) {
			if e.Family == family {
				entries = append(entries, e)
			}
		}
		require.Len(t, entries, 1)
		err := statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies}, Set: src,
			Guard: guard, VerifyBeforeDrain: true,
		}, entries)
		if err != nil {
			return run, err
		}
		phase("drained")
	}
	write()
	phase("after writes")
	scenario.RollbackBeforeBoundary()
	phase("rollback")
	write()
	phase("replay")
	return run, nil
}

func requireP3DrainedLikeUndrained(
	t *testing.T,
	family nodesettings.Family,
	tables []string,
) {
	t.Helper()
	undrained, err := p3DrainRun(t, family, false, nil)
	require.NoError(t, err)
	drained, err := p3DrainRun(t, family, true, nil)
	require.NoError(t, err)
	require.Empty(
		t,
		drained.guard.Trips(),
		"a drained table was read or written",
	)
	rawPlain, err := dbtest.RawSQLiteMetadata(t, undrained.db)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, drained.db)
	require.NoError(t, err)
	for _, table := range tables {
		require.Positive(
			t,
			tableRows(t, rawPlain, table),
			"undrained %s",
			table,
		)
		require.Zero(t, tableRows(t, raw, table), "drained %s", table)
	}
	mask, err := drained.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(family))
	for name, want := range undrained.reads {
		require.NotEmpty(t, want, name)
		if _, ok := drained.reads[name]; ok {
			require.Equal(t, want, drained.reads[name], name)
		}
	}
	require.Equal(t, undrained.reads["replay"], drained.reads["replay"])
	require.NotEqual(
		t,
		drained.reads["after writes"],
		drained.reads["rollback"],
		"the rollback must undo the writes",
	)
}

func TestCommitteeDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	requireP3DrainedLikeUndrained(
		t, nodesettings.FamilyCommitteeConstitution, committeeTables,
	)
}

func TestOpCertsDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	requireP3DrainedLikeUndrained(t, nodesettings.FamilyOpCerts, opCertTables)
}

// A set that differs from the metadata store refuses the drain and leaves
// the rows.
func TestP3DrainVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		family  nodesettings.Family
		table   string
		corrupt func(*database.Database)
	}{
		{
			"committee quorum",
			nodesettings.FamilyCommitteeConstitution, "committee_quorum",
			func(db *database.Database) {
				require.NoError(t, db.SetCommitteeQuorum(
					t.Context(), big.NewRat(1, 3), 1<<20, nil,
				))
			},
		},
		{
			"an unknown pool",
			nodesettings.FamilyOpCerts, "pool_opcert_sequence",
			func(db *database.Database) {
				require.NoError(t, db.UpdatePoolOpCertSequence(
					t.Context(), lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6f}, 28)),
					99, 5, nil,
				))
			},
		},
		{
			"a raised counter",
			nodesettings.FamilyOpCerts, "pool_opcert_sequence",
			func(db *database.Database) {
				counters, err := db.Metadata().LatestPoolOpCertCounters(nil)
				require.NoError(t, err)
				require.NotEmpty(t, counters)
				require.NoError(t, db.UpdatePoolOpCertSequence(
					t.Context(), lcommon.PoolKeyHash(counters[0].PoolKeyHash),
					counters[0].Sequence+10, counters[0].Slot+1, nil,
				))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run, err := p3DrainRun(t, tc.family, true, tc.corrupt)
			require.ErrorIs(t, err, statedrain.ErrGateRefused)
			require.ErrorContains(t, err, "differ")
			mask, err := run.db.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.False(t, mask.Has(tc.family))
			raw, err := dbtest.RawSQLiteMetadata(t, run.db)
			require.NoError(t, err)
			require.Positive(t, tableRows(t, raw, tc.table))
		})
	}
}

// With a family drained and the set not holding it, the LedgerDB and the
// database fail every read rather than answer from the empty SQL tables.
func TestP3DrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	// The set never loads the metadata store's rows.
	for _, f := range []nodesettings.Family{
		nodesettings.FamilyCommitteeConstitution, nodesettings.FamilyOpCerts,
	} {
		require.NoError(t, scenario.DB.RecordDrainedFamily(t.Context(), f))
		scenario.DrainFamilies(nodesettings.FamilyMask(0).With(f))
	}
	ctx := t.Context()
	ldb := scenario.LedgerDB()
	cold := bytes.Repeat([]byte{0xa1}, 28)
	pkh := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6e}, 28))
	_, err = ldb.GetConstitution(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeQuorum(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeMembers(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeMember(ctx, 0, cold, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.IsCommitteeMemberResigned(ctx, 0, cold, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetResignedCommitteeMembers(
		ctx, []models.CommitteeCredential{{Credential: cold}}, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetActiveCommitteeMembers(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeAuthorizedCount(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeHotAuthorizationsSince(ctx, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)

	_, _, err = ldb.LatestPoolOpCertSequence(ctx, pkh, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(ctx, pkh, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceAtOrBefore(ctx, pkh, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceInRange(ctx, pkh, 1, 2, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.LatestPoolOpCertSequences(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.PoolOpCertSequencesExistAtSlot(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)

	// The plain database refuses both reads and writes.
	db := scenario.DB
	_, err = db.GetCommitteeMembers(ctx, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	require.ErrorIs(t, db.SetCommitteeQuorum(
		ctx, big.NewRat(1, 2), 1, nil,
	), database.ErrFamilyDrained)
	_, err = db.GetConstitution(nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	require.ErrorIs(t, db.UpdatePoolOpCertSequence(
		ctx, pkh, 1, 1, nil,
	), database.ErrFamilyDrained)
	_, _, err = db.LatestPoolOpCertSequence(ctx, pkh, nil)
	require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
	_, _, err = db.Metadata().CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkh}, 0, 10, nil,
	)
	require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
}

// A drained op-cert read for a slot the set has no record of fails rather
// than answer from the empty table.
func TestOpCertReadOutsideTheSetsRecordFailsOnceDrained(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	slots := scenario.OpCerts()
	// A lower counter after the highest was reached: the set keeps the
	// highest sequence, so only the SQL rows say a lower one came later.
	lower := scenario.LedgerDB().Begin(t.Context(), true)
	require.NoError(t, scenario.LedgerDB().UpdatePoolOpCertSequence(
		t.Context(), lcommon.PoolKeyHash(slots.Pool), 5, slots.Batched+1, lower,
	))
	require.NoError(t, lower.Commit())
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyOpCerts,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyOpCerts),
	)
	ldb := scenario.LedgerDB()
	pkh := lcommon.PoolKeyHash(slots.Pool)
	seq, found, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		t.Context(), pkh, slots.Batched, nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(6), seq)
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(
		t.Context(), pkh, slots.Batched, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}

// A pool that keeps its counter after a slot is the common case of a node
// bootstrapped from Mithril, and the drained op-cert rows cannot answer it, so
// the set answers when the pool's latest observation repeats its highest
// counter: the highest after the slot is then that counter. A lower counter
// observed last still reaches the emptied table and fails.
func TestOpCertRepeatedCounterIsAnsweredByTheDrainedSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyOpCerts,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyOpCerts),
	)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	write := func(pool lcommon.PoolKeyHash, sequence, slot uint64) {
		txn := ldb.Begin(ctx, true)
		require.NoError(t, ldb.UpdatePoolOpCertSequence(
			ctx, pool, sequence, slot, txn,
		))
		require.NoError(t, txn.Commit())
		txn.Release()
	}
	base := scenario.OpCerts().Batched
	steady := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6f}, 28))
	for _, slot := range []uint64{base + 2, base + 3, base + 4} {
		write(steady, 7, slot)
	}
	seq, found, err := ldb.LatestPoolOpCertSequenceAfter(
		ctx,
		steady,
		base+2,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), seq)
	_, found, err = ldb.LatestPoolOpCertSequenceAfter(ctx, steady, base+4, nil)
	require.NoError(t, err)
	require.False(t, found, "nothing observed after the last row")
	seq, found, err = ldb.LatestPoolOpCertSequenceInRange(
		ctx, steady, base+3, base+4, nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), seq)

	dropped := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x70}, 28))
	write(dropped, 6, base+2)
	write(dropped, 5, base+3)
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(ctx, dropped, base+2, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceInRange(
		ctx, dropped, base+3, base+3, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}
