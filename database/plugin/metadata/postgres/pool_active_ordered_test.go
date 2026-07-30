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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//go:build dingo_extra_plugins

package postgres

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestGetActivePoolKeyHashesOrderedPostgres is the cross-backend behavioral
// counterpart to the sqlite ordering test
// (sqlite/pool_active_ordered_test.go's TestNodeAdapterPoolsListOrderingAndActiveSet
// equivalent -- see api/blockfrost/pools_list_test.go for the original,
// adapter-level version of this fixture). ROW_NUMBER() window functions,
// multi-key ORDER BY, and NULL/COALESCE handling are exactly where sqlite,
// postgres, and mysql diverge, so this exercises the real query against a
// real postgres server rather than trusting sqlite behavior to generalize.
//
// This test runs against a shared, persistent integration-test database
// (POSTGRES_* env vars), not a fresh per-test database, so other tests in
// this package may leave unrelated rows in pool/pool_registration/
// pool_retirement. To stay correct regardless of that shared state, this
// filters the full result down to just the 8 pool key hashes this test
// created (by set membership) before asserting order, rather than
// asserting on the total result length.
func TestGetActivePoolKeyHashesOrderedPostgres(t *testing.T) {
	store := newTestPostgresStore(t)
	// Registered via t.Cleanup (not a plain defer) and ahead of the data
	// cleanup below so LIFO ordering runs the data cleanup BEFORE the
	// connection closes: t.Cleanup callbacks run after every plain defer
	// in this function body has already executed, so a plain
	// "defer store.Close()" here would close the connection before any
	// later-registered t.Cleanup data cleanup runs against it (that
	// exact ordering bug is why TestGetRewardStakeInputsForPoolsUsesHistoricalExpirationPostgres's
	// own epoch cleanup below silently never executes -- see the epoch_id
	// <= 5 delete below).
	t.Cleanup(func() { _ = store.Close() })
	db := store.DB()

	// A tip/epoch row is a global singleton this package's other tests
	// never write directly, so it is safe to create fresh here. Slot
	// 1_000_000 / epoch length 2_000_000 gives ample headroom above every
	// added_slot used below (max 500).
	require.NoError(t, db.
		Where("id = ?", 1).
		Attrs(models.Tip{Slot: 1_000_000}).
		FirstOrCreate(&models.Tip{ID: 1}).Error)
	// epoch_id 0-5 is TestGetRewardStakeInputsForPoolsUsesHistoricalExpirationPostgres's
	// fixture range (pool_atslot_test.go). That test's own cleanup never
	// actually runs (same plain-defer-before-t.Cleanup ordering bug
	// described above, pre-existing in that file and not touched here),
	// so epoch_id 0 persists in this shared integration database with a
	// narrow length_in_slots=100 that would make GetActivePoolKeyHashesOrdered's
	// epoch-at-tip-slot lookup pick that stale row instead of this test's
	// and fail with ErrNoEpochData. Clearing that known range before
	// creating our own epoch_id=0 makes this test self-sufficient
	// regardless of that pre-existing bug or test run order.
	require.NoError(t, db.Where("epoch_id <= ?", 5).Delete(&models.Epoch{}).Error)
	require.NoError(t, db.
		Where("epoch_id = ?", 0).
		Attrs(models.Epoch{StartSlot: 0, LengthInSlots: 2_000_000}).
		FirstOrCreate(&models.Epoch{EpochId: 0}).Error)

	reregisteredCancelledHash := bytes.Repeat([]byte{0xF1}, 28)
	oldestHash := bytes.Repeat([]byte{0xF2}, 28)
	reregisteredMarginHash := bytes.Repeat([]byte{0xF3}, 28)
	retiredFutureHash := bytes.Repeat([]byte{0xF4}, 28)
	ssBlk0Cert0Hash := bytes.Repeat([]byte{0xF5}, 28)
	ssBlk0Cert1Hash := bytes.Repeat([]byte{0xF6}, 28)
	ssBlk1Cert0Hash := bytes.Repeat([]byte{0xF7}, 28)
	retiredEffectiveHash := bytes.Repeat([]byte{0xF8}, 28)

	allHashes := [][]byte{
		reregisteredCancelledHash, oldestHash, reregisteredMarginHash,
		retiredFutureHash, ssBlk0Cert0Hash, ssBlk0Cert1Hash, ssBlk1Cert0Hash,
		retiredEffectiveHash,
	}
	cleanup := func() {
		for _, h := range allHashes {
			_ = db.Where("pool_key_hash = ?", h).Delete(&models.PoolRegistration{}).Error
			_ = db.Where("pool_key_hash = ?", h).Delete(&models.PoolRetirement{}).Error
			_ = db.Where("pool_key_hash = ?", h).Delete(&models.Pool{}).Error
		}
		// Test-specific transaction/cert IDs used for the same-slot trio
		// below (90001-90003); scoped high enough to avoid any other
		// test's fixture IDs.
		_ = db.Where("id IN (?)", []uint{90001, 90002, 90003}).
			Delete(&models.Certificate{}).Error
		_ = db.Where("id IN (?)", []uint{90001, 90002}).
			Delete(&models.Transaction{}).Error
	}
	cleanup()
	t.Cleanup(cleanup)

	seedPool := func(poolKeyHash []byte) uint {
		pool := &models.Pool{PoolKeyHash: poolKeyHash}
		require.NoError(t, db.Create(pool).Error)
		return pool.ID
	}
	seedReg := func(poolID uint, poolKeyHash []byte, addedSlot uint64, certID uint) {
		require.NoError(t, db.Create(&models.PoolRegistration{
			PoolID: poolID, PoolKeyHash: poolKeyHash,
			AddedSlot: addedSlot, CertificateID: certID,
		}).Error)
	}
	seedRet := func(poolID uint, poolKeyHash []byte, addedSlot, epoch uint64) {
		require.NoError(t, db.Create(&models.PoolRetirement{
			PoolID: poolID, PoolKeyHash: poolKeyHash,
			AddedSlot: addedSlot, Epoch: epoch,
		}).Error)
	}
	seedCert := func(txID, certID uint, slot uint64, blockIndex uint32, certIndex uint) uint {
		var existing models.Transaction
		err := db.Where("id = ?", txID).First(&existing).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			require.NoError(t, db.Create(&models.Transaction{
				ID: txID, Slot: slot, BlockIndex: blockIndex,
				Hash: bytes.Repeat([]byte{byte(txID)}, 32),
			}).Error)
		} else {
			require.NoError(t, err)
		}
		require.NoError(t, db.Create(&models.Certificate{
			ID: certID, TransactionID: txID, Slot: slot, CertIndex: certIndex,
		}).Error)
		return certID
	}

	// Same fixture as the sqlite/adapter-level test: see
	// api/blockfrost/pools_list_test.go's
	// TestNodeAdapterPoolsListOrderingAndActiveSet doc comment for the
	// full rationale of each case.
	id := seedPool(reregisteredCancelledHash)
	seedReg(id, reregisteredCancelledHash, 1, 0)
	seedRet(id, reregisteredCancelledHash, 6, 0)
	seedReg(id, reregisteredCancelledHash, 400, 0)

	id = seedPool(oldestHash)
	seedReg(id, oldestHash, 10, 0)

	id = seedPool(reregisteredMarginHash)
	seedReg(id, reregisteredMarginHash, 50, 0)
	seedReg(id, reregisteredMarginHash, 500, 0)

	id = seedPool(retiredFutureHash)
	seedReg(id, retiredFutureHash, 90, 0)
	seedRet(id, retiredFutureHash, 91, 5)

	cert1 := seedCert(90001, 90001, 100, 0, 0)
	cert2 := seedCert(90001, 90002, 100, 0, 1)
	cert3 := seedCert(90002, 90003, 100, 1, 0)

	id = seedPool(ssBlk0Cert0Hash)
	seedReg(id, ssBlk0Cert0Hash, 100, cert1)
	id = seedPool(ssBlk0Cert1Hash)
	seedReg(id, ssBlk0Cert1Hash, 100, cert2)
	id = seedPool(ssBlk1Cert0Hash)
	seedReg(id, ssBlk1Cert0Hash, 100, cert3)

	id = seedPool(retiredEffectiveHash)
	seedReg(id, retiredEffectiveHash, 5, 0)
	seedRet(id, retiredEffectiveHash, 6, 0)

	wantAsc := []string{
		hex.EncodeToString(reregisteredCancelledHash),
		hex.EncodeToString(oldestHash),
		hex.EncodeToString(reregisteredMarginHash),
		hex.EncodeToString(retiredFutureHash),
		hex.EncodeToString(ssBlk0Cert0Hash),
		hex.EncodeToString(ssBlk0Cert1Hash),
		hex.EncodeToString(ssBlk1Cert0Hash),
	}
	retiredEffectiveHex := hex.EncodeToString(retiredEffectiveHash)

	knownHex := make(map[string]bool, len(wantAsc)+1)
	for _, h := range wantAsc {
		knownHex[h] = true
	}
	knownHex[retiredEffectiveHex] = true

	result, err := store.GetActivePoolKeyHashesOrdered(nil)
	require.NoError(t, err)

	var filtered []string
	for _, pkh := range result {
		h := hex.EncodeToString(pkh)
		if knownHex[h] {
			filtered = append(filtered, h)
		}
	}

	require.Equal(
		t, wantAsc, filtered,
		"postgres GetActivePoolKeyHashesOrdered must return this fixture's "+
			"active pools in the same oldest-first order as sqlite",
	)
	require.NotContains(t, filtered, retiredEffectiveHex)
}
