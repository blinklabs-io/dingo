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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// p2Run is what p2PParamsRun observed.
type p2Run struct {
	reads map[string][]string
	guard *statedrain.TripGuard
	db    *database.Database
	// sqlMarkers counts the sync_state rows of the two synthetic PlutusV2
	// markers after real data was confirmed.
	sqlMarkers int
}

func syncStateRows(t *testing.T, db *database.Database, keys ...string) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	n := 0
	for _, key := range keys {
		var c int
		require.NoError(t, raw.QueryRow(
			"SELECT COUNT(*) FROM sync_state WHERE sync_key = ?", key,
		).Scan(&c))
		n += c
	}
	return n
}

var syntheticMarkerKeys = []string{
	database.SyntheticV2CostModelSyncKey,
	database.SyntheticV2CostModelClearedEpochSyncKey,
}

// p2PParamsRun loads a classic-update ledger into a set, marks the PlutusV2
// cost model synthetic, optionally drains the pparams-updates family, then
// proposes updates, enacts one at a boundary, rolls back across it, and
// replays it.
func p2PParamsRun(
	t *testing.T,
	drain bool,
	corruptBeforeDrain func(*database.Database),
) (p2Run, error) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedClassicUpdateBoundaryScenarioForTest(
		t, set, guard,
	)
	run := p2Run{reads: map[string][]string{}, guard: guard, db: scenario.DB}
	require.NoError(
		t, set.LoadLedgerState(t.Context(), dbadapter.New(scenario.DB)),
	)
	phase := func(name string) { run.reads[name] = scenario.ReadPParams() }
	scenario.MarkSyntheticV2()
	phase("loaded")

	if drain {
		if corruptBeforeDrain != nil {
			corruptBeforeDrain(scenario.DB)
		}
		src := drainTestSource{set: set, db: scenario.DB}
		var entries []statedrain.Entry
		for _, e := range statedrain.Graduated(src) {
			if e.Family == nodesettings.FamilyPParamsUpdates {
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
		phase("drained")
	}

	cur := scenario.RollbackTarget.Slot
	scenario.Propose(ledger.UpdateProposal{
		Slot: cur + 1, Epoch: 1, Keys: []byte{1, 2}, MaxTxSize: 21_000,
	})
	scenario.Propose(ledger.UpdateProposal{
		Slot: cur + 2, Epoch: 1, Keys: []byte{3}, MaxTxSize: 22_000,
		Batched: true,
	})
	scenario.ObserveRealV2(1)
	run.sqlMarkers = syncStateRows(t, scenario.DB, syntheticMarkerKeys...)
	phase("proposed")
	scenario.ApplyBoundary()
	phase("enacted")
	scenario.RollbackBeforeBoundary()
	phase("rolled back")
	scenario.ApplyBoundary()
	phase("replayed")
	return run, nil
}

// A drained family answers every read as an undrained run does across a
// proposal, an enactment, a rollback and a replay, keeps its SQL tables and
// sync keys empty, and never has a table read or written.
func TestPParamsUpdatesDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	undrained, err := p2PParamsRun(t, false, nil)
	require.NoError(t, err)
	drained, err := p2PParamsRun(t, true, nil)
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
	for _, table := range []string{"pparams", "pparam_update"} {
		require.Positive(
			t,
			tableRows(t, rawPlain, table),
			"undrained %s",
			table,
		)
		require.Zero(t, tableRows(t, raw, table), "drained %s", table)
	}
	require.Equal(t, 2, undrained.sqlMarkers)
	require.Zero(t, drained.sqlMarkers, "a drained run writes no marker rows")
	mask, err := drained.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyPParamsUpdates))

	for name, want := range undrained.reads {
		require.NotEmpty(t, want, name)
		require.Equal(t, want, drained.reads[name], name)
	}
	reads := drained.reads
	require.NotEqual(t, reads["proposed"], reads["enacted"],
		"the boundary must enact an update")
	require.NotEqual(t, reads["enacted"], reads["rolled back"],
		"the rollback must remove the enactment")
	require.Equal(
		t,
		parameterLines(reads["loaded"]),
		parameterLines(reads["rolled back"]),
		"the rollback leaves the parameter rows it found",
	)
	require.Contains(
		t,
		reads["proposed"],
		fmt.Sprintf(
			"marker %s %q",
			database.SyntheticV2CostModelSyncKey,
			"false",
		),
	)
	require.Contains(t, reads["proposed"],
		fmt.Sprintf("marker %s %q",
			database.SyntheticV2CostModelClearedEpochSyncKey, "1"))
	for _, key := range syntheticMarkerKeys {
		require.Contains(t, reads["rolled back"],
			fmt.Sprintf("marker %s %q", key, ""),
			"a rollback before the confirming epoch drops the marker")
	}
}

// parameterLines are the rendered protocol parameter rows and picks of
// lines, without the proposals and markers.
func parameterLines(lines []string) []string {
	var out []string
	for _, l := range lines {
		if strings.HasPrefix(l, "pparams ") || strings.HasPrefix(l, "pick ") {
			out = append(out, l)
		}
	}
	return out
}

// With the family drained and the set not holding it, the LedgerDB and the
// database fail the read rather than answer from the empty SQL tables, and
// a write the set must make is refused.
func TestDrainedPParamReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
	)
	// The set never loads the metadata store's rows.
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyPParamsUpdates,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPParamsUpdates),
	)
	ldb := scenario.LedgerDB()
	decode := func([]byte) (lcommon.ProtocolParameters, error) { return nil, nil }
	_, err = ldb.GetPParams(1, 6, decode, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.ListPParamsForEra(6, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetPParamUpdates(1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.ForecastPParamUpdates(
		2,
		1,
		&conway.ConwayProtocolParameters{},
		func([]byte) (any, error) { return nil, nil },
		func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
		func(p lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
		nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.ComputeAndApplyPParamUpdates(
		t.Context(), 1, 2, 6, 1, &conway.ConwayProtocolParameters{},
		nil, nil, nil, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)

	// The database's own reads fail the same way, and its writes are
	// refused.
	_, err = scenario.DB.GetPParams(1, 6, decode, nil)
	require.ErrorIs(t, err, database.ErrPParamsSourceNotLoaded)
	_, err = scenario.DB.ListPParamUpdates(nil)
	require.ErrorIs(t, err, database.ErrPParamsSourceNotLoaded)
	_, err = scenario.DB.GetSyncState(
		database.SyntheticV2CostModelClearedEpochSyncKey, nil,
	)
	require.ErrorIs(t, err, database.ErrPParamsSourceNotLoaded)
	require.ErrorIs(
		t, scenario.DB.SetPParams([]byte{0x80}, 1, 1, 6, nil),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t, scenario.DB.SetPParamUpdate([]byte{1}, []byte{0xa0}, 1, 1, nil),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t, scenario.DB.DeletePParamsAfterSlot(t.Context(), 1, nil),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t, scenario.DB.DeletePParamUpdatesAfterSlot(t.Context(), 1, nil),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t,
		scenario.DB.SetSyncState(
			database.SyntheticV2CostModelSyncKey,
			"true",
			nil,
		),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t,
		scenario.DB.DeleteSyncState(database.SyntheticV2CostModelSyncKey, nil),
		database.ErrFamilyDrained,
	)
}

// p2EraSwitchRun applies a mid-epoch era switch, which translates and stores
// the parameters of the new era, and rolls back before it, draining the
// pparams-updates family first when drain is set.
func p2EraSwitchRun(t *testing.T, drain bool) p2Run {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedMidEpochEraSwitchScenarioForTest(
		t, set, guard,
	)
	run := p2Run{reads: map[string][]string{}, guard: guard, db: scenario.DB}
	require.NoError(
		t, set.LoadLedgerState(t.Context(), dbadapter.New(scenario.DB)),
	)
	run.reads["loaded"] = scenario.ReadPParams()
	if drain {
		src := drainTestSource{set: set, db: scenario.DB}
		var entries []statedrain.Entry
		for _, e := range statedrain.Graduated(src) {
			if e.Family == nodesettings.FamilyPParamsUpdates {
				entries = append(entries, e)
			}
		}
		require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies}, Set: src,
			Guard: guard,
		}, entries))
	}
	scenario.ApplySwitch()
	run.reads["switched"] = scenario.ReadPParams()
	scenario.RollbackBeforeSwitch()
	run.reads["rolled back"] = scenario.ReadPParams()
	return run
}

// The parameters an era switch translates and a rollback removes read the
// same from a drained family, which keeps no SQL row.
func TestPParamsDrainMatchesAnUndrainedRunAcrossAnEraSwitch(t *testing.T) {
	t.Parallel()
	undrained := p2EraSwitchRun(t, false)
	drained := p2EraSwitchRun(t, true)
	require.Empty(
		t,
		drained.guard.Trips(),
		"a drained table was read or written",
	)
	for name, want := range undrained.reads {
		require.NotEmpty(t, want, name)
		require.Equal(t, want, drained.reads[name], name)
	}
	require.NotEqual(t, undrained.reads["loaded"], undrained.reads["switched"],
		"the switch must store the new era's parameters")
	require.NotEqual(
		t,
		undrained.reads["switched"],
		undrained.reads["rolled back"],
	)
	require.Equal(t,
		parameterLines(undrained.reads["loaded"]),
		parameterLines(undrained.reads["rolled back"]))
	raw, err := dbtest.RawSQLiteMetadata(t, drained.db)
	require.NoError(t, err)
	require.Zero(t, tableRows(t, raw, "pparams"))
	require.Zero(t, tableRows(t, raw, "pparam_update"))
	rawPlain, err := dbtest.RawSQLiteMetadata(t, undrained.db)
	require.NoError(t, err)
	require.Positive(t, tableRows(t, rawPlain, "pparams"))
}
