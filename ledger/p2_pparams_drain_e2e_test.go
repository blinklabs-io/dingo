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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// With a failed set that holds none of the ledger's state, the LedgerDB and
// the database fail the read rather than answer from the empty SQL tables,
// and the database refuses a write to them.
func TestDrainedPParamReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
	)
	unloaded, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = unloaded.Close() })
	scenario.ReplaceSet(unloaded)
	// A failed set serves no read.
	scenario.FailSet()
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
