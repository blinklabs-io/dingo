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

// poolTables are the tables of the pools-history family, and the current-state
// tables the drain empties through their foreign key to pool.
var poolTables = []string{
	"pool", "pool_registration", "pool_registration_owner",
	"pool_registration_relay", "pool_retirement",
	"pool_current_state", "pool_current_event",
}

// poolRun is what p4DrainRun observed.
type poolRun struct {
	reads map[string][]string
	guard *statedrain.TripGuard
	db    *database.Database
	slots ledger.PoolSlots
}

func renderPool(p *models.Pool, err error) string {
	if err != nil {
		return fmt.Sprintf("error %v", err)
	}
	if p == nil {
		return "nil"
	}
	out := fmt.Sprintf(
		"pool %x vrf %x pledge %d regs %d rets %d",
		p.PoolKeyHash, p.VrfKeyHash, p.Pledge,
		len(p.Registration), len(p.Retirement),
	)
	for _, r := range p.Registration {
		out += fmt.Sprintf(
			" [reg %d %x %d]",
			r.AddedSlot,
			r.VrfKeyHash,
			r.Pledge,
		)
	}
	for _, r := range p.Retirement {
		out += fmt.Sprintf(" [ret %d %d]", r.AddedSlot, r.Epoch)
	}
	return out
}

func hexList(list [][]byte, err error) string {
	if err != nil {
		return fmt.Sprintf("error %v", err)
	}
	var out []string
	for _, p := range list {
		out = append(out, fmt.Sprintf("%x", p))
	}
	slices.Sort(out)
	return fmt.Sprint(out)
}

// requireSameLines compares two renderings line by line and reports the
// lines only one side has, rather than the two renderings.
func requireSameLines(t *testing.T, phase string, want, got []string) {
	t.Helper()
	count := map[string]int{}
	for _, l := range want {
		count[l]++
	}
	for _, l := range got {
		count[l]--
	}
	var diff []string
	for _, l := range want {
		if count[l] > 0 {
			diff = append(diff, "undrained only: "+l)
			count[l]--
		}
	}
	for _, l := range got {
		if count[l] < 0 {
			diff = append(diff, "drained only: "+l)
			count[l]++
		}
	}
	require.Empty(t, diff, phase)
}

// readPools renders every pool read the family serves, through ldb and db,
// for the pools of slots.
func readPools(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	db *database.Database,
	slots ledger.PoolSlots,
) []string {
	t.Helper()
	ctx := t.Context()
	var out []string
	add := func(format string, args ...any) {
		out = append(out, fmt.Sprintf(format, args...))
	}
	pools := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(slots.Pool), lcommon.PoolKeyHash(slots.Retired),
	}
	add("list %s", hexList(ldb.ListPoolKeyHashes(nil)))
	for _, p := range pools {
		for _, inactive := range []bool{true, false} {
			add("get %x %t %s", p[:], inactive,
				renderPool(ldb.GetPool(ctx, p, inactive, nil)))
		}
		state, err := ldb.GetPoolVrfState(ctx, p, nil)
		if err != nil || state == nil || len(state.Registration) == 0 {
			add("vrf state %x %v %v", p[:], state != nil, err)
		} else {
			add("vrf state %x %x", p[:], state.Registration[0].VrfKeyHash)
		}
		for _, slot := range []uint64{
			slots.Register - 1, slots.Register, slots.Rotate, slots.Boundary,
		} {
			for _, earliest := range []bool{false, true} {
				read := ldb.GetPoolVrfKeyHashAtSlot
				if earliest {
					read = ldb.GetPoolEarliestVrfKeyHashAtSlot
				}
				v, ok, err := read(p[:], slot, nil)
				add(
					"vrf %x slot %d %t: %x %t %v",
					p[:],
					slot,
					earliest,
					v,
					ok,
					err,
				)
			}
		}
		status, err := ldb.GetPoolStatus(p, nil)
		retiring := "none"
		if status.RetirementEpoch != nil {
			retiring = fmt.Sprint(*status.RetirementEpoch)
		}
		add("status %x exists %t active %t retiring %s %v",
			p[:], status.Exists, status.Active, retiring, err)
		certs, err := ldb.GetPoolRegistrations(p, nil)
		add("certs %x %d %v", p[:], len(certs), err)
		for _, c := range certs {
			add("cert %x pledge %d", c.VrfKeyHash[:], c.Pledge)
		}
	}
	pools2, err := ldb.GetPools(ctx, pools, nil)
	add("pools %d %v", len(pools2), err)
	for i := range pools2 {
		add("pools row %s", renderPool(&pools2[i], nil))
	}
	for _, w := range []struct{ start, ended, snapshot uint64 }{
		{slots.Rotate, 5, math.MaxInt64},
		{slots.Rotate + 1, 5, math.MaxInt64},
		{slots.Register, 5, slots.Rotate},
		{slots.Retire + 1, slots.RetireEpoch, math.MaxInt64},
	} {
		regs, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
			pools, w.start, w.ended, w.snapshot, nil,
		)
		add("effective %+v: %v", w, err)
		for _, r := range regs {
			add(
				"effective %+v row %.4x %.4x %d",
				w,
				r.PoolKeyHash,
				r.VrfKeyHash,
				r.AddedSlot,
			)
		}
	}
	relays, err := ldb.GetActivePoolRelays(ctx, nil)
	add("relays %d %v", len(relays), err)
	deposits, err := ldb.GetPoolDepositsHeld(pools[:1], nil)
	add("deposits %v %v", deposits, err)
	for _, epoch := range []uint64{slots.NextEpoch - 1, slots.NextEpoch} {
		retiring, err := ldb.GetRetiringPools(epoch, nil)
		add("retiring %d: %v %v", epoch, retiring, err)
	}
	for _, key := range [][]byte{slots.VrfA, slots.VrfB} {
		for _, start := range []uint64{slots.Rotate, slots.Rotate + 1} {
			add("by vrf %x %d %s", key, start,
				renderPool(ldb.GetPoolByVrfKeyHash(ctx, key, start, nil)))
			add("future by vrf %x %d %s", key, start,
				renderPool(ldb.GetFuturePoolByVrfKeyHash(ctx, key, start, nil)))
		}
	}
	// The database answers the same reads through its source once drained.
	dbPools, err := db.GetPools(ctx, pools, nil)
	add("db pools %d %v", len(dbPools), err)
	for i := range dbPools {
		add("db pools row %s", renderPool(&dbPools[i], nil))
	}
	p, err := db.GetPool(ctx, pools[0], true, nil)
	add("db pool %s", renderPool(p, err))
	dbCerts, err := db.GetPoolRegistrations(pools[0], nil)
	add("db certs %d %v", len(dbCerts), err)
	dbRelays, err := db.GetActivePoolRelays(ctx, nil)
	add("db relays %d %v", len(dbRelays), err)
	return out
}

// requireDrainedPoolAnswers asserts the answers of a drained run that the
// metadata store's SQL cannot give in the harness, whose tip sits outside its
// epoch rows: the active pools, the relays, and the registration of a pool
// whose retirement has not ended.
func requireDrainedPoolAnswers(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	slots ledger.PoolSlots,
) {
	t.Helper()
	active, err := ldb.GetActivePoolKeyHashes(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{slots.Pool}, active,
		"the reaped pool is gone and the re-registered pool is active")
	relays, err := ldb.GetActivePoolRelays(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, relays, 1)
	require.Equal(t, slots.Pool, relays[0].PoolKeyHash)
	require.Equal(t, "relay.example", relays[0].Hostname)
	require.EqualValues(t, 3001, relays[0].Port)
	retired := lcommon.PoolKeyHash(slots.Retired)
	regs, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash{retired},
		slots.Retire+1, slots.RetireEpoch-1, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1, "the retirement has not ended")
	regs, err = ldb.GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash{retired},
		slots.Retire+1, slots.RetireEpoch, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Empty(t, regs, "the retirement has ended")
	pool, err := ldb.GetPoolByVrfKeyHash(
		t.Context(), slots.VrfB, slots.Rotate+1, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pool)
	require.Equal(t, slots.Pool, pool.PoolKeyHash)
	pool, err = ldb.GetPoolByVrfKeyHash(
		t.Context(), slots.VrfA, slots.Rotate+1, nil,
	)
	require.NoError(t, err)
	require.Nil(t, pool, "the rotated-away key is free")
}

// p4DrainRun registers a pool, rotates its VRF key, retires another pool and
// reaps it at the boundary, rolls back before all of it and replays it. With
// drain set the pools-history family is drained, after verification, before
// the first certificate.
func p4DrainRun(
	t *testing.T,
	drain bool,
	corruptBeforeDrain func(*database.Database),
) (poolRun, error) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet, guard,
	)
	run := poolRun{reads: map[string][]string{}, guard: guard, db: scenario.DB}
	require.NoError(
		t, set.LoadLedgerState(t.Context(), dbadapter.New(scenario.DB)),
	)
	if drain {
		if corruptBeforeDrain != nil {
			corruptBeforeDrain(scenario.DB)
		}
		src := drainTestSource{set: set, db: scenario.DB}
		var entries []statedrain.Entry
		for _, e := range statedrain.Graduated(src) {
			if e.Family == nodesettings.FamilyPoolsHistory {
				entries = append(entries, e)
			}
		}
		require.Len(t, entries, 1)
		if err := statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies}, Set: src,
			Guard: guard, VerifyBeforeDrain: true,
		}, entries); err != nil {
			return run, err
		}
	}
	var refunds []models.PoolRetirementRefund
	apply := func() ledger.PoolSlots {
		return scenario.Pools(func() {
			epochs, err := dbadapter.New(scenario.DB).GetEpochs(nil)
			if err != nil {
				epochs, err = scenario.DB.GetEpochs(nil)
			}
			require.NoError(t, err)
			var cur models.Epoch
			for _, e := range epochs {
				if e.EpochId > cur.EpochId {
					cur = e
				}
			}
			refunds, err = scenario.LedgerDB().GetPoolsRetiringAtEpoch(
				t.Context(), cur.EpochId+1, cur.StartSlot+23, nil,
			)
			require.NoError(t, err)
		})
	}
	// The op-cert write keeps the pool row's counter in step in the metadata
	// store, which a drained family has no row for.
	scenario.OpCerts()
	run.slots = apply()
	require.Len(t, refunds, 1)
	require.Equal(t, run.slots.Retired, refunds[0].PoolKeyHash)
	// An undrained run reads the metadata store's rows alone, which the
	// drained run's answers must equal.
	var ldb ledgerdb.LedgerDB = dbadapter.New(scenario.DB)
	if drain {
		ldb = scenario.LedgerDB()
	}
	read := func() []string {
		return readPools(t, ldb, scenario.DB, run.slots)
	}
	run.reads["registered"] = read()
	if drain {
		requireDrainedPoolAnswers(t, scenario.LedgerDB(), run.slots)
	}
	tip, err := scenario.DB.GetTip(nil)
	require.NoError(t, err)
	scenario.RollbackBeforeBoundary()
	run.reads["rolled back"] = read()
	run.slots = apply()
	// The replay does not apply blocks, so the tip stays at the rollback
	// target; the metadata store's reads of the current epoch follow it.
	require.NoError(t, scenario.DB.SetTip(tip, nil))
	run.reads["replay"] = read()
	return run, nil
}

// A drained pools-history family answers every pool read as an undrained run
// does across a registration, a re-registration that rotates the VRF key, a
// retirement and its reap, a rollback and a replay, with the guard armed and
// the tables empty.
func TestPoolsHistoryDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	undrained, err := p4DrainRun(t, false, nil)
	require.NoError(t, err)
	drained, err := p4DrainRun(t, true, nil)
	require.NoError(t, err)
	require.Empty(t, drained.guard.Trips(), "a drained table was touched")

	raw, err := dbtest.RawSQLiteMetadata(t, drained.db)
	require.NoError(t, err)
	rawPlain, err := dbtest.RawSQLiteMetadata(t, undrained.db)
	require.NoError(t, err)
	for _, table := range poolTables {
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
	require.True(t, mask.Has(nodesettings.FamilyPoolsHistory))
	for name, want := range undrained.reads {
		require.NotEmpty(t, want, name)
		requireSameLines(t, name, want, drained.reads[name])
	}
	require.NotEqual(
		t, undrained.reads["registered"], undrained.reads["rolled back"],
		"the rollback must change the pool reads",
	)
}

// A set that differs from the metadata store refuses the drain and leaves the
// rows.
func TestPoolsHistoryDrainVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	for name, corrupt := range map[string]func(*database.Database){
		"a registration only the store holds": func(db *database.Database) {
			reg := models.PoolRegistration{
				PoolKeyHash: make([]byte, 28), VrfKeyHash: make([]byte, 32),
				RewardAccount: make([]byte, 28), AddedSlot: 7,
			}
			require.NoError(t, db.ImportPool(
				t.Context(), nil,
				&models.Pool{
					PoolKeyHash: reg.PoolKeyHash, VrfKeyHash: reg.VrfKeyHash,
					RewardAccount: reg.RewardAccount,
				}, &reg,
			))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			run, err := p4DrainRun(t, true, corrupt)
			require.ErrorIs(t, err, statedrain.ErrGateRefused)
			require.ErrorContains(t, err, "differ")
			mask, err := run.db.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.False(t, mask.Has(nodesettings.FamilyPoolsHistory))
			raw, err := dbtest.RawSQLiteMetadata(t, run.db)
			require.NoError(t, err)
			require.Positive(t, tableRows(t, raw, "pool"))
			require.Positive(t, tableRows(t, raw, "pool_registration"))
		})
	}
}

// With the family drained and the set not holding it, the LedgerDB and the
// database fail the read rather than answer from the empty SQL tables.
func TestDrainedPoolReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	// The set never loads the metadata store's rows.
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyPoolsHistory,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPoolsHistory),
	)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	pool := lcommon.PoolKeyHash(make([]byte, 28))
	_, err = ldb.ListPoolKeyHashes(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPool(ctx, pool, true, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPools(ctx, []lcommon.PoolKeyHash{pool}, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPoolVrfState(ctx, pool, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPoolRegistrations(pool, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPoolStatus(pool, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPoolByVrfKeyHash(ctx, make([]byte, 32), 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetFuturePoolByLeiosKey(ctx, make([]byte, 96), 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetActivePoolKeyHashes(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetActivePoolRelays(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPoolDepositsHeld([]lcommon.PoolKeyHash{pool}, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetRetiringPools(1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPoolsRetiringAtEpoch(ctx, 1, 10, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)

	// The database's own reads come from the same source.
	_, err = scenario.DB.GetPools(ctx, []lcommon.PoolKeyHash{pool}, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = scenario.DB.GetActivePoolRelays(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	require.ErrorIs(
		t,
		scenario.DB.ImportPool(
			ctx,
			nil,
			&models.Pool{},
			&models.PoolRegistration{},
		),
		database.ErrFamilyDrained,
	)
}
