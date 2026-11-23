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

package utxoset_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// A pending ratification record taken over from a v0.94.2 store has no
// journal entry of its own, so the load dates it at the boundary it names: a
// rollback that keeps the boundary keeps the record, and one below it drops
// the record, as it does one the ledger wrote itself.
func TestLoadedPendingRatificationRollsBackAtItsBoundary(t *testing.T) {
	t.Parallel()
	const record = `{"epoch":7,"boundary_slot":6,"id":3}`
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := releasedAt(t, slotPoint(8))
	require.NoError(t, db.Metadata().SetSyncState(
		database.PendingRatificationSyncKey, record, nil,
	))
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))

	held := func(asOf *uint64) string {
		t.Helper()
		raw, _, err := set.RatifyPending(asOf)
		require.NoError(t, err)
		return string(raw)
	}
	require.Equal(t, record, held(nil))
	below := uint64(5)
	require.Empty(t, held(&below),
		"a read as of a slot below the boundary sees the record")

	require.NoError(t, set.RollbackTo(slotPoint(6)))
	require.Equal(t, record, held(nil),
		"a rollback that keeps the boundary dropped its record")
	require.NoError(t, set.RollbackTo(slotPoint(5)))
	require.Empty(t, held(nil),
		"a rollback below the boundary kept its record")
}

// A v0.94.2 store reverted below a boundary still names it in its pending
// ratification record. The set is loaded at the reverted tip, where the
// boundary has not happened, so it takes no record over.
func TestPendingRatificationAboveTheTipIsNotLoaded(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 4)
	db := releasedAt(t, slotPoint(4))
	require.NoError(t, db.Metadata().SetSyncState(
		database.PendingRatificationSyncKey,
		`{"epoch":7,"boundary_slot":6,"id":3}`, nil,
	))
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))

	loaded, err := set.RatifyPendingLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	_, ok, err := set.RatifyPending(nil)
	require.NoError(t, err)
	require.False(t, ok)
}
