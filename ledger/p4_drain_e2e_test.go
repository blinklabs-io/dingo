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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

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

// With the set failed, the LedgerDB and the database fail the read rather
// than answer from the empty SQL tables.
func TestDrainedPoolReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// A failed set serves no read.
	scenario.FailSet()
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
}
