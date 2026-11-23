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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/stretchr/testify/require"
)

// The ledger's sync_state keys name either ledger state, which only the
// LedgerDB set keeps, or chain data the SQL chain store still holds. A key
// that is neither must be refused rather than written to SQL, where no
// ledger read would find it and no rollback would undo it.
func TestLedgerSyncStateKeyOutsideTheChainStoreIsRefused(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}
	fxAttachLedgerDB(t, ls)
	ldb := ls.ledgerDB()

	const unknown = "ledger_state_with_no_owner"
	require.ErrorIs(t, ldb.SetSyncState(unknown, "1", nil), errSyncStateKeyUnowned)
	_, err := ldb.GetSyncState(unknown, nil)
	require.ErrorIs(t, err, errSyncStateKeyUnowned)
	require.ErrorIs(t, ldb.DeleteSyncState(unknown, nil), errSyncStateKeyUnowned)
	stored, err := db.GetSyncState(unknown, nil)
	require.NoError(t, err)
	require.Empty(t, stored, "the refused key reached the SQL store")

	// A chain-store key still passes through to SQL.
	require.NoError(t, ldb.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey, "7", nil,
	))
	stored, err = db.GetSyncState(database.ConsumedUtxoPruneFloorSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "7", stored)
	require.NoError(t, ldb.SetSyncState(
		deferredHeaderValidationSyncStatePrefix+"1.ab", "1", nil,
	))
	got, err := ldb.GetSyncState(
		deferredHeaderValidationSyncStatePrefix+"1.ab", nil,
	)
	require.NoError(t, err)
	require.Equal(t, "1", got)
}
