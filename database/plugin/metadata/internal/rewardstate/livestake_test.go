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

package rewardstate

import (
	"bytes"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newLiveStakeTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Utxo{}))
	return db
}

// liveStakeTestTxID builds a distinct 32-byte tx id for the given ordinal so
// seeded rows satisfy the (tx_id, output_idx) unique index.
func liveStakeTestTxID(n int) []byte {
	id := make([]byte, 32)
	id[0] = byte(n >> 8)
	id[1] = byte(n)
	return id
}

// liveStakeTestStakingKey builds a distinct 28-byte staking credential hash so
// each seeded UTxO resolves to its own reward-live-stake credential.
func liveStakeTestStakingKey(n int) []byte {
	return bytes.Repeat([]byte{byte(0x40 + n)}, 28)
}

// TestStakeRefsFromLiveUtxoIDsFiltersSpentRowsInGo verifies the Go-side
// liveOnly filter (which replaced the SQL `deleted_slot = 0` predicate that
// defeated the unique index): StakeRefsFromLiveUtxoIDs returns refs only for
// live (deleted_slot = 0) rows, while StakeRefsFromUtxoIDs returns refs for
// every row regardless of spend state. Live and consumed rows share the same
// (tx_id, output_idx) shape so the only thing distinguishing them is
// deleted_slot.
func TestStakeRefsFromLiveUtxoIDsFiltersSpentRowsInGo(t *testing.T) {
	t.Parallel()
	db := newLiveStakeTestDB(t)

	type seed struct {
		id          models.UtxoId
		ref         models.StakeCredentialRef
		deletedSlot uint64
		addedSlot   uint64
	}
	var seeds []seed
	// 3 live rows (deleted_slot = 0), 2 consumed rows (deleted_slot > 0).
	for i := range 5 {
		txID := liveStakeTestTxID(i + 1)
		key := liveStakeTestStakingKey(i + 1)
		var deletedSlot uint64
		if i >= 3 {
			deletedSlot = 700
		}
		addedSlot := uint64(100 + i) //nolint:gosec
		require.NoError(t, db.Create(&models.Utxo{
			TxId:          txID,
			OutputIdx:     0,
			CredentialTag: 0,
			StakingKey:    key,
			AddedSlot:     addedSlot,
			DeletedSlot:   deletedSlot,
			Amount:        types.Uint64(1_000_000),
		}).Error)
		seeds = append(seeds, seed{
			id:          models.UtxoId{Hash: txID, Idx: 0},
			ref:         models.NewStakeCredentialRef(0, key),
			deletedSlot: deletedSlot,
			addedSlot:   addedSlot,
		})
	}

	allIDs := make([]models.UtxoId, 0, len(seeds))
	for _, s := range seeds {
		allIDs = append(allIDs, s.id)
	}

	// liveOnly: only the 3 live credentials come back.
	liveRefs, err := StakeRefsFromLiveUtxoIDs(db, allIDs, 0, 499)
	require.NoError(t, err)
	wantLive := map[string]uint64{}
	for _, s := range seeds {
		if s.deletedSlot == 0 {
			wantLive[s.ref.MapKey()] = s.addedSlot
		}
	}
	require.Len(t, liveRefs, len(wantLive))
	for key, slot := range wantLive {
		got, ok := liveRefs[key]
		require.True(t, ok, "expected live credential %q", key)
		assert.Equal(t, slot, got.Slot)
	}
	// No consumed credential leaked into the live result.
	for _, s := range seeds {
		if s.deletedSlot != 0 {
			_, ok := liveRefs[s.ref.MapKey()]
			assert.False(t, ok, "spent credential %q must not appear in live refs", s.ref.MapKey())
		}
	}

	// Non-live variant: every credential comes back, consumed rows keyed to
	// their deleted_slot.
	allRefs, err := StakeRefsFromUtxoIDs(db, allIDs, 0, 499)
	require.NoError(t, err)
	require.Len(t, allRefs, len(seeds))
	for _, s := range seeds {
		got, ok := allRefs[s.ref.MapKey()]
		require.True(t, ok, "expected credential %q in full result", s.ref.MapKey())
		wantSlot := s.addedSlot
		if s.deletedSlot > 0 {
			wantSlot = s.deletedSlot
		}
		assert.Equal(t, wantSlot, got.Slot)
	}
}

// TestStakeRefsFromLiveUtxoIDsUsesTxIdOutputIndex proves the reward-stake
// batch lookup resolves the OR-chain of (tx_id, output_idx) pairs through the
// unique index tx_id_output_idx and does NOT fall back to a deleted_slot index
// scan. Before the fix an `AND deleted_slot = 0` predicate made SQLite pick
// idx_utxo_deleted_payment_script and evaluate the whole OR-chain per live
// row (O(live_utxos x terms)), wedging the node at every epoch boundary.
func TestStakeRefsFromLiveUtxoIDsUsesTxIdOutputIndex(t *testing.T) {
	t.Parallel()
	db := newLiveStakeTestDB(t)

	// Seed a mix of many live rows (which the defeated plan would have to scan)
	// plus a handful of consumed rows, then look up a batch spanning both.
	ids := make([]models.UtxoId, 0, 40)
	for i := range 40 {
		txID := liveStakeTestTxID(i + 1)
		var deletedSlot uint64
		if i%4 == 0 { // a quarter consumed
			deletedSlot = 700
		}
		require.NoError(t, db.Create(&models.Utxo{
			TxId:          txID,
			OutputIdx:     0,
			CredentialTag: 0,
			StakingKey:    liveStakeTestStakingKey(i + 1),
			AddedSlot:     uint64(100 + i), //nolint:gosec
			DeletedSlot:   deletedSlot,
			Amount:        types.Uint64(1_000_000),
		}).Error)
		ids = append(ids, models.UtxoId{Hash: txID, Idx: 0})
	}

	var capturedSQL string
	var capturedVars []any
	const callbackName = "test:capture_stake_refs_batch_sql"
	require.NoError(t, db.Callback().Query().
		After("gorm:query").
		Register(callbackName, func(tx *gorm.DB) {
			if capturedSQL != "" {
				return
			}
			sql := tx.Statement.SQL.String()
			if strings.Contains(sql, "output_idx") &&
				strings.Contains(sql, " OR ") {
				capturedSQL = sql
				capturedVars = append([]any(nil), tx.Statement.Vars...)
			}
		}))
	t.Cleanup(func() {
		_ = db.Callback().Query().Remove(callbackName)
	})

	refs, err := StakeRefsFromLiveUtxoIDs(db, ids, 0, 499)
	require.NoError(t, err)
	// 30 of 40 rows are live (i%4 != 0).
	require.Len(t, refs, 30)
	require.NotEmpty(t, capturedSQL, "did not capture the OR-chain batch query")
	// deleted_slot is still SELECTed (the Go loop reads it), but the Go-side
	// filter means the SQL must NOT carry a `deleted_slot = ...` WHERE predicate
	// (that predicate is what defeated the unique index).
	require.NotContains(t, capturedSQL, "deleted_slot =")

	planRows, err := db.
		Raw("EXPLAIN QUERY PLAN "+capturedSQL, capturedVars...).
		Rows()
	require.NoError(t, err)
	defer planRows.Close()

	var details []string
	for planRows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, planRows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, planRows.Err())
	plan := strings.Join(details, "\n")

	assert.Contains(t, plan, "tx_id_output_idx",
		"OR-chain must resolve through the unique tx_id_output_idx index; plan was:\n%s",
		plan)
	assert.NotContains(t, plan, "idx_utxo_deleted_payment_script",
		"OR-chain must not fall back to a deleted_slot index scan; plan was:\n%s",
		plan)
}
