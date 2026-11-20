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
	"context"
	"database/sql"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// drainStore adapts the database to statedrain.Store.
type drainStore struct {
	*database.Database
	publish func(nodesettings.FamilyMask)
}

func (s drainStore) RecordDrainedFamily(
	ctx context.Context,
	f nodesettings.Family,
) error {
	if err := s.Database.RecordDrainedFamily(ctx, f); err != nil {
		return err
	}
	mask, err := s.Database.DrainedFamilies(ctx)
	if err == nil {
		s.publish(mask)
	}
	return err
}

func (s drainStore) SQLTip(context.Context) (ocommon.Point, error) {
	tip, err := s.GetTip(nil)
	return tip.Point, err
}

func (s drainStore) GetSyncState(
	_ context.Context,
	key string,
) (string, error) {
	return s.Database.GetSyncState(key, nil)
}

func (s drainStore) SetSyncState(_ context.Context, key, value string) error {
	return s.Database.SetSyncState(key, value, nil)
}

func (s drainStore) DeleteSyncState(_ context.Context, key string) error {
	return s.Database.DeleteSyncState(key, nil)
}

func (s drainStore) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	return s.ListSyncStateKeysByPrefix(prefix, nil)
}

// drainTestSource is the statedrain.Source of a harness: the set's markers
// and comparison, with the metadata store's tip as the set's.
type drainTestSource struct {
	set *utxoset.Set
	db  *database.Database
}

func (s drainTestSource) Tip() (ocommon.Point, bool, error) {
	tip, err := s.db.GetTip(nil)
	return tip.Point, true, err
}

func (s drainTestSource) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.set)
}

func tableRows(t *testing.T, raw *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM `"+table+"`").Scan(&n),
	)
	return n
}

// p1DrainRun drives nonces, network state and donations across writes, a
// boundary and a rollback, draining the families listed in drain after the
// first writes. It returns the family rows the LedgerDB answered after each
// phase, the guard, and the harness database.
func p1DrainRun(
	t *testing.T,
	drain []nodesettings.Family,
	corruptBeforeDrain func(*database.Database),
) (map[string][]string, *statedrain.TripGuard, *database.Database, error) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, guard,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	// The harness wrote its two nonces to the metadata store only.
	fixture, err := store.GetBlockNoncesInSlotRange(0, math.MaxInt64, nil)
	require.NoError(t, err)
	scenario.StageNoncesInSet(fixture)

	out := map[string][]string{}
	phase := func(name string) {
		out[name+"/nonces"] = scenario.ReadNonces()
		out[name+"/network"] = scenario.ReadNetwork()
		out[name+"/epochs"] = scenario.ReadEpochs()
	}
	cur := scenario.CurrentPoint().Slot
	row := func(slot uint64, checkpoint bool) models.BlockNonce {
		return models.BlockNonce{
			Slot: slot, Hash: bytes.Repeat([]byte{byte(slot)}, 32),
			Nonce: bytes.Repeat(
				[]byte{byte(slot) + 1},
				32,
			), IsCheckpoint: checkpoint,
		}
	}
	scenario.WriteNonces(
		[]models.BlockNonce{row(cur+1, true), row(cur+2, false)},
	)
	scenario.SetNetworkState(500, 8_000, cur+1)
	scenario.Donate(11)
	scenario.SaveSummary(models.EpochSummary{
		Epoch: 1, TotalActiveStake: 7, TotalPoolCount: 2,
		EpochNonce: bytes.Repeat([]byte{3}, 32),
	})
	phase("before")

	var runErr error
	if len(drain) > 0 {
		if corruptBeforeDrain != nil {
			corruptBeforeDrain(scenario.DB)
		}
		src := drainTestSource{set: set, db: scenario.DB}
		var entries []statedrain.Entry
		candidates := append(
			statedrain.Graduated(src),
			statedrain.Staged(src)...)
		for _, e := range candidates {
			for _, f := range drain {
				if e.Family == f {
					entries = append(entries, e)
				}
			}
		}
		runErr = statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies}, Set: src, Guard: guard,
		}, entries)
		if runErr != nil {
			return out, guard, scenario.DB, runErr
		}
		phase("drained")
	}

	scenario.WriteNonces([]models.BlockNonce{row(cur+3, false)})
	scenario.SetNetworkState(400, 7_000, cur+3)
	scenario.Donate(5)
	// The second save keeps the readiness the first one set.
	scenario.SaveSummary(models.EpochSummary{
		Epoch: 1, TotalActiveStake: 9, TotalPoolCount: 3, SnapshotReady: true,
	})
	scenario.SaveSummary(models.EpochSummary{
		Epoch: 1, TotalActiveStake: 11, TotalPoolCount: 3,
	})
	phase("after writes")
	scenario.ApplyBoundaryQuiet()
	phase("boundary")
	scenario.PruneNonces(cur + 2)
	phase("prune")
	scenario.RollbackBeforeBoundary()
	phase("rollback")
	scenario.WriteNonces([]models.BlockNonce{row(cur+4, false)})
	scenario.ApplyBoundaryQuiet()
	phase("replay")
	return out, guard, scenario.DB, nil
}

func requireDrainedLikeUndrained(
	t *testing.T,
	family nodesettings.Family,
	tables ...string,
) {
	t.Helper()
	undrained, _, plain, err := p1DrainRun(t, nil, nil)
	require.NoError(t, err)
	drained, guard, db, err := p1DrainRun(t, []nodesettings.Family{family}, nil)
	require.NoError(t, err)
	require.Empty(t, guard.Trips(), "a drained table was read")
	rawPlain, err := dbtest.RawSQLiteMetadata(t, plain)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
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
	mask, err := db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(family))
	for name, want := range undrained {
		if _, ok := drained[name]; !ok {
			continue
		}
		require.Equal(t, want, drained[name], name)
	}
	require.NotEmpty(t, drained["replay/nonces"])
	require.NotEmpty(t, drained["replay/network"])
	require.NotEmpty(t, drained["replay/epochs"])
}

func TestNoncesDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	requireDrainedLikeUndrained(t, nodesettings.FamilyNonces, "block_nonce")
}

func TestNetworkDonationsDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	requireDrainedLikeUndrained(
		t, nodesettings.FamilyNetworkDonations,
		"network_state", "network_donation",
	)
}

// With the family drained and the set not holding it, the LedgerDB fails the
// read rather than answer from the empty SQL tables.
func TestDrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// The set never loads the metadata store's rows.
	for _, f := range []nodesettings.Family{
		nodesettings.FamilyNonces, nodesettings.FamilyNetworkDonations,
	} {
		require.NoError(t, scenario.DB.RecordDrainedFamily(t.Context(), f))
		scenario.DrainFamilies(nodesettings.FamilyMask(0).With(f))
	}
	ldb := scenario.LedgerDB()
	point := scenario.CurrentPoint()
	_, err = ldb.GetBlockNonce(point, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetBlockNoncesInSlotRange(0, 100, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.GetLatestBlockNonce(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetNetworkState(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetNetworkStateAsOfSlot(10, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.SumNetworkDonationsForEpoch(0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.ListNetworkDonations(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}

// The startup nonce prune of a drained family checks that the set holds none
// past the ledger tip instead of deleting SQL rows that do not exist.
func TestDrainedNoncePruneChecksTheSetAgainstTheTip(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	cur := scenario.CurrentPoint()
	scenario.WriteNonces([]models.BlockNonce{{
		Slot: cur.Slot + 5, Hash: bytes.Repeat([]byte{9}, 32),
		Nonce: bytes.Repeat([]byte{8}, 32),
	}})
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces),
	)
	ldb := scenario.LedgerDB()
	err = ldb.DeleteBlockNoncesAfterPoint(cur, nil)
	require.ErrorContains(t, err, "past the ledger tip")
	require.NoError(t, ldb.DeleteBlockNoncesAfterPoint(
		ocommon.NewPoint(cur.Slot+5, bytes.Repeat([]byte{9}, 32)), nil,
	))
}

func TestEpochsDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	requireDrainedLikeUndrained(t, nodesettings.FamilyEpochs, "epoch")
}

func TestSummariesDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	requireDrainedLikeUndrained(
		t, nodesettings.FamilySummaries, "epoch_summary",
	)
}

// Epochs and summaries drained together answer the same reads as neither
// drained, and the metadata store keeps none of either.
func TestEpochsAndSummariesDrainedTogetherMatchAnUndrainedRun(t *testing.T) {
	t.Parallel()
	undrained, _, _, err := p1DrainRun(t, nil, nil)
	require.NoError(t, err)
	drained, guard, db, err := p1DrainRun(t, []nodesettings.Family{
		nodesettings.FamilyEpochs, nodesettings.FamilySummaries,
	}, nil)
	require.NoError(t, err)
	require.Empty(t, guard.Trips(), "a drained table was read")
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	require.Zero(t, tableRows(t, raw, "epoch"))
	require.Zero(t, tableRows(t, raw, "epoch_summary"))
	for name, want := range undrained {
		if _, ok := drained[name]; ok {
			require.Equal(t, want, drained[name], name)
		}
	}
}

// midEpochSwitchedEpoch is the epoch the mid-epoch era switch scenario
// switches.
const midEpochSwitchedEpoch = 3

// eraSwitchRun is what p1EraSwitchRun observed.
type eraSwitchRun struct {
	reads map[string][]string
	guard *statedrain.TripGuard
	db    *database.Database
	// sqlKey is the sync_state value of the era switch and setSwitch the
	// set's record, each read after the switch applied.
	sqlKey    string
	setSwitch *database.MidEpochEraSwitch
	// sqlEra is the era of the switched epoch's metadata store row after
	// the rollback, which the LedgerDB's own reads do not show while the set
	// serves them.
	sqlEra uint
}

// p1EraSwitchRun applies a mid-epoch era switch and rolls back before it,
// draining the era-switch family first when drain is set.
func p1EraSwitchRun(
	t *testing.T,
	drain bool,
	corruptBeforeDrain func(*database.Database),
) (eraSwitchRun, error) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedMidEpochEraSwitchScenarioForTest(
		t, set, guard,
	)
	run := eraSwitchRun{
		reads: map[string][]string{}, guard: guard, db: scenario.DB,
	}
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
			if e.Family == nodesettings.FamilyEraSwitch {
				entries = append(entries, e)
			}
		}
		require.Len(t, entries, 1)
		if err := statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies}, Set: src,
			Guard: guard,
		}, entries); err != nil {
			return run, err
		}
	}
	scenario.ApplySwitch()
	run.reads["switched"] = scenario.ReadEpochs()
	run.sqlKey, err = scenario.DB.GetSyncState(
		database.MidEpochEraSwitchSyncKey, nil,
	)
	require.NoError(t, err)
	run.setSwitch, err = set.EraSwitch()
	require.NoError(t, err)
	scenario.RollbackBeforeSwitch()
	run.reads["rolled back"] = scenario.ReadEpochs()
	row, err := scenario.DB.GetEpoch(midEpochSwitchedEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
	run.sqlEra = row.EraId
	return run, nil
}

// A drained era switch lives only in the set: the metadata store keeps no
// key, and a rollback before the switch still returns the switched epoch's
// row to its source era.
func TestEraSwitchDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	undrained, err := p1EraSwitchRun(t, false, nil)
	require.NoError(t, err)
	drained, err := p1EraSwitchRun(t, true, nil)
	require.NoError(t, err)
	require.Empty(t, drained.guard.Trips())

	require.NotEmpty(t, undrained.sqlKey, "an undrained run records the switch")
	require.Empty(t, drained.sqlKey, "a drained run writes no sync key")
	require.NotNil(t, undrained.setSwitch)
	require.Equal(t, undrained.setSwitch, drained.setSwitch,
		"the set records the switch either way")
	for name, want := range undrained.reads {
		require.NotEmpty(t, want, name)
		require.Equal(t, want, drained.reads[name], name)
	}
	require.NotEqual(
		t, undrained.reads["switched"], undrained.reads["rolled back"],
		"the rollback must change the switched epoch's era",
	)
	require.Equal(t, uint(eras.BabbageEraDesc.Id), undrained.sqlEra)
	require.Equal(t, undrained.sqlEra, drained.sqlEra,
		"the rollback returns the metadata store's row to its source era")
	mask, err := drained.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyEraSwitch))
	key, err := drained.db.GetSyncState(database.MidEpochEraSwitchSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, key, "the rollback must not write the key back")
	marker, err := drained.db.GetSyncState(
		statedrain.MarkerPrefix+nodesettings.FamilyEraSwitch.String(), nil,
	)
	require.NoError(t, err)
	require.Empty(t, marker, "the drain marker is cleared")
}

// With the epochs or summaries family drained and the set not holding it, the
// LedgerDB and the database fail the read rather than answer from the empty
// SQL tables.
func TestDrainedEpochReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	for _, f := range []nodesettings.Family{
		nodesettings.FamilyEpochs, nodesettings.FamilySummaries,
		nodesettings.FamilyEraSwitch,
	} {
		require.NoError(t, scenario.DB.RecordDrainedFamily(t.Context(), f))
		scenario.DrainFamilies(nodesettings.FamilyMask(0).With(f))
	}
	ldb := scenario.LedgerDB()
	_, err = ldb.GetEpochs(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetEpoch(1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetEpochBySlot(3, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetEpochsByEra(1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetEpochSummary(1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetLatestEpochSummary(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetTotalActiveStake(1, "mark", nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	require.ErrorIs(
		t,
		ldb.SaveEpochSummary(&models.EpochSummary{Epoch: 1}, nil),
		ledger.ErrDrainedFamilyUnserved,
	)

	// The database's own reads come from the same source.
	_, err = scenario.DB.GetEpochs(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = scenario.DB.GetEpochBySlot(3, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	require.ErrorIs(
		t,
		scenario.DB.SetEpoch(0, 0, nil, nil, nil, nil, 1, 1, 1, nil),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t,
		scenario.DB.SetMidEpochEraSwitch(database.MidEpochEraSwitch{}, nil),
		database.ErrFamilyDrained,
	)
}
