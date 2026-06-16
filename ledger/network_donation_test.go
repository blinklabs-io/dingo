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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDonationTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck
	return db
}

func networkState(t *testing.T, db *database.Database) (treasury, reserves, slot uint64) {
	t.Helper()
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return uint64(state.Treasury), uint64(state.Reserves), state.Slot
}

// TestApplyEpochDonations verifies that the ending epoch's donations are added
// to the treasury at the boundary slot, leaving reserves untouched, and that
// only the ended epoch's donations are moved.
func TestApplyEpochDonations(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Post-withdrawal treasury/reserves baseline at an earlier slot.
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// Donations for the ending epoch (7) and a later epoch (8) that must not move.
	require.NoError(t, db.Metadata().AddNetworkDonation(60, 7, 100, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 200, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(600, 8, 999, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_300), treasury, "treasury += epoch-7 donations (100+200)")
	assert.Equal(t, uint64(5_000), reserves, "reserves untouched by donations")
	assert.Equal(t, uint64(80), slot, "updated at the boundary slot")
}

// TestApplyEpochDonations_NoDonations is a no-op when the ended epoch had none.
func TestApplyEpochDonations_NoDonations(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury)
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(t, uint64(50), slot, "no boundary row written when no donations")
}

// TestEpochDonationWithdrawalRollback exercises the acceptance scenario: a
// treasury withdrawal (modelled as a debited treasury) followed by a donation
// at the boundary, then a rollback past the boundary that restores the prior
// treasury and drops the donation rows so re-application is deterministic.
func TestEpochDonationWithdrawalRollback(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Epoch 7 starts with treasury 1_000 (slot 50).
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// A donation block lands mid-epoch.
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 300, nil))
	// At the 7->8 boundary (slot 80) a treasury withdrawal of 400 is enacted
	// first (checked against the pre-donation treasury of 1_000), debiting the
	// treasury to 600...
	require.NoError(t, db.Metadata().SetNetworkState(600, 5_000, 80, nil))
	// ...then the epoch's donations are added on top.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))
	treasury, reserves, slot := networkState(t, db)
	require.Equal(t, uint64(900), treasury, "1_000 - 400 withdrawal + 300 donation")
	require.Equal(t, uint64(5_000), reserves)
	require.Equal(t, uint64(80), slot)

	// Roll back past the boundary (to slot 60): the boundary NetworkState row
	// and the donation row are dropped, restoring epoch 7's starting treasury.
	require.NoError(t, db.DeleteNetworkStateAfterSlot(60, nil))
	require.NoError(t, db.DeleteNetworkDonationsAfterSlot(60, nil))

	treasury, reserves, slot = networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury, "treasury restored to pre-boundary value")
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(t, uint64(50), slot)
	sum, err := db.Metadata().SumNetworkDonationsForEpoch(7, nil)
	require.NoError(t, err)
	assert.Zero(t, sum, "rolled-back donation rows are gone")
}
