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

package database_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// poolTables are the tables of the pools-history family, which statedrain
// lists only once the family graduates.
var poolTables = []string{
	"pool", "pool_registration", "pool_registration_owner",
	"pool_registration_relay", "pool_retirement",
}

func newDrainedPoolsDB(
	t *testing.T,
) (*database.Database, *statedrain.TripGuard, ocommon.Point) {
	t.Helper()
	db, guard, point := newDrainedTruncateDB(t)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyPoolsHistory,
	))
	guard.Arm(nodesettings.FamilyPoolsHistory, poolTables)
	return db, guard, point
}

// A rollback of a database that drained the pools-history family writes
// nothing to the pool tables, and the op-cert write that updates the pool row
// is skipped as well.
func TestTruncateAfterSlotWritesNothingToDrainedPoolTables(t *testing.T) {
	t.Parallel()
	db, guard, point := newDrainedPoolsDB(t)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x7a}, 28))
	require.NoError(t, db.UpdatePoolOpCertSequence(
		t.Context(), pool, 4, point.Slot+5, nil,
	))
	require.Empty(t, guard.Trips(), "the op-cert write touched the pool row")
	require.NoError(t, db.SetBlockNonce(
		point.Hash, point.Slot, bytes.Repeat([]byte{0xc2}, 32), true, nil,
	))

	_, _, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

	require.NoError(t, err)
	require.Empty(t, guard.Trips(), "a drained table was touched")
}

// With the family drained and no source installed the database's pool reads
// fail instead of answering from the empty tables, and a pool write is
// refused.
func TestDrainedPoolReadsOfTheDatabaseNeedASource(t *testing.T) {
	t.Parallel()
	db, guard, _ := newDrainedPoolsDB(t)
	ctx := t.Context()
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x7a}, 28))

	_, err := db.GetPool(ctx, pool, true, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPools(ctx, []lcommon.PoolKeyHash{pool}, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolVrfState(ctx, pool, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolByVrfKeyHash(ctx, make([]byte, 32), 1, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetFuturePoolByLeiosKey(ctx, make([]byte, 96), 1, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetActivePoolRelays(ctx, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetActivePoolKeyHashes(ctx, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolRegistrations(pool, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolsRetiringAtEpoch(ctx, 3, 100, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetActivePoolKeyHashesOrdered(ctx, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, _, err = db.GetPoolCertificateHistory(ctx, pool, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	require.ErrorIs(
		t,
		db.ImportPool(ctx, nil, &models.Pool{}, &models.PoolRegistration{}),
		database.ErrFamilyDrained,
	)
	require.Empty(t, guard.Trips(), "a refusal must not touch the tables")
}
