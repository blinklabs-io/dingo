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
// LedgerDB set keeps, or chain data the block store holds. A key
// that is neither must be refused rather than written to a store where no
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
	require.ErrorIs(t, err, database.ErrNoMetadataStore)
	require.Empty(t, stored, "the refused key reached a store")

	// The deferred header floor is the block store's, so its sync_state
	// name is refused like any other key the ledger does not keep there.
	require.ErrorIs(
		t,
		ldb.SetSyncState(database.DeferredHeaderFloorKey, "1", nil),
		errSyncStateKeyUnowned,
	)
	_, err = ldb.ListSyncStateKeysByPrefix("deferred_header_validation:", nil)
	require.ErrorIs(t, err, errSyncStateKeyUnowned)
}
