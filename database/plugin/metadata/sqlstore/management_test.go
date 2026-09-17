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

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newManagementTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestGetPoolByVrfKeyHashExcludesRetiredPool(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKey := make([]byte, 28)
	poolKey[0] = 1
	vrfKey := make([]byte, 32)
	vrfKey[0] = 2
	pool := &models.Pool{PoolKeyHash: poolKey, VrfKeyHash: vrfKey}
	registration := &models.PoolRegistration{
		PoolKeyHash: poolKey,
		VrfKeyHash:  vrfKey,
		AddedSlot:   10,
	}
	require.NoError(t, store.ImportPool(pool, registration, nil))

	// The pool remains in historical metadata, but a retirement effective in
	// the current epoch must no longer reserve its VRF key.
	require.NoError(t, store.RetirePools(nil, [][]byte{poolKey}, 1, 20))
	require.NoError(t, store.SetEpoch(0, 1, nil, nil, nil, nil, 0, 1, 100, nil))
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip")},
		BlockNumber: 1,
	}, nil))
	got, err := store.GetPoolByVrfKeyHash(vrfKey, 0, nil)
	require.NoError(t, err)
	require.Nil(t, got)

}

// TestGetPoolByVrfKeyHashPreservesActiveKeyDuringDeferredReRegistration is
// the regression test for issue #4352: a pool re-registering with a new VRF
// key mid-epoch must not free its old key before the epoch boundary, because
// cardano-ledger defers a re-registration through psFutureStakePoolParams
// until then.
func TestGetPoolByVrfKeyHashPreservesActiveKeyDuringDeferredReRegistration(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)

	poolKey := make([]byte, 28)
	poolKey[0] = 1
	oldVrfKey := make([]byte, 32)
	oldVrfKey[0] = 0xA
	newVrfKey := make([]byte, 32)
	newVrfKey[0] = 0xB

	// Pool P registers with key A before the current epoch begins.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: oldVrfKey},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  oldVrfKey,
			AddedSlot:   10,
		},
		nil,
	))
	// P re-registers with key B mid-epoch (added_slot 50), inside the
	// epoch that starts at slot 30. The re-registration is not yet
	// effective.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: newVrfKey},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  newVrfKey,
			AddedSlot:   50,
		},
		nil,
	))

	const epochStartSlot = 30

	// The old key must still be reported as reserved by P.
	got, err := store.GetPoolByVrfKeyHash(oldVrfKey, epochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolKey, got.PoolKeyHash)

	// The new key is already claimed by P itself as a same-epoch pending
	// registration (psVRFKeyHashes tracks it even before it becomes
	// effective). Reporting P here -- not nil -- is what lets
	// gouroboros's caller compare against PoolCurrentState and correctly
	// still allow P to keep using its own pending key.
	got, err = store.GetPoolByVrfKeyHash(newVrfKey, epochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolKey, got.PoolKeyHash)
}

// TestGetPoolByVrfKeyHashAllowsSamePoolRevertingToItsOwnKey is the control
// for the deferral fix: a pool re-registering back to a key it already
// holds must never be treated as a conflict against itself.
func TestGetPoolByVrfKeyHashAllowsSamePoolRevertingToItsOwnKey(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)

	poolKey := make([]byte, 28)
	poolKey[0] = 1
	keyA := make([]byte, 32)
	keyA[0] = 0xA
	keyB := make([]byte, 32)
	keyB[0] = 0xB

	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: keyA},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  keyA,
			AddedSlot:   10,
		},
		nil,
	))
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: keyB},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  keyB,
			AddedSlot:   50,
		},
		nil,
	))
	// Same epoch, P reverts back to key A before the boundary.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: keyA},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  keyA,
			AddedSlot:   60,
		},
		nil,
	))

	const epochStartSlot = 30
	got, err := store.GetPoolByVrfKeyHash(keyA, epochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolKey, got.PoolKeyHash)
}

// TestGetPoolByVrfKeyHashActivatesAndReleasesAtEpochBoundary confirms the
// full lifecycle: once epochStartSlot advances past a re-registration, the
// new key becomes reserved, the old key is released, and a different pool
// registering for the first time may immediately claim the released key.
func TestGetPoolByVrfKeyHashActivatesAndReleasesAtEpochBoundary(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)

	poolP := make([]byte, 28)
	poolP[0] = 1
	poolQ := make([]byte, 28)
	poolQ[0] = 2
	keyA := make([]byte, 32)
	keyA[0] = 0xA
	keyB := make([]byte, 32)
	keyB[0] = 0xB

	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolP, VrfKeyHash: keyA},
		&models.PoolRegistration{
			PoolKeyHash: poolP,
			VrfKeyHash:  keyA,
			AddedSlot:   10,
		},
		nil,
	))
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolP, VrfKeyHash: keyB},
		&models.PoolRegistration{
			PoolKeyHash: poolP,
			VrfKeyHash:  keyB,
			AddedSlot:   50,
		},
		nil,
	))

	// Once the epoch starting at slot 60 (after the re-registration)
	// begins, key B is P's effective key and key A is released.
	const postBoundaryEpochStartSlot = 60
	got, err := store.GetPoolByVrfKeyHash(keyB, postBoundaryEpochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolP, got.PoolKeyHash)

	got, err = store.GetPoolByVrfKeyHash(keyA, postBoundaryEpochStartSlot, nil)
	require.NoError(t, err)
	require.Nil(t, got)

	// A different pool's first-ever registration claiming the now-free
	// key A takes effect immediately, even mid-epoch: cardano-ledger's
	// POOL rule inserts a first registration directly into psStakePools
	// and defers only a re-registration.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolQ, VrfKeyHash: keyA},
		&models.PoolRegistration{
			PoolKeyHash: poolQ,
			VrfKeyHash:  keyA,
			AddedSlot:   65,
		},
		nil,
	))
	got, err = store.GetPoolByVrfKeyHash(keyA, postBoundaryEpochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolQ, got.PoolKeyHash)
}

// TestGetPoolByVrfKeyHashClaimsSupersededSameEpochFutureKey is the
// regression test for the PV11+ follow-up to #4352: after pool P cycles
// A -> B -> C within one epoch, a later reuse of B (the superseded, no
// longer pending value) must still be rejected, because psVRFKeyHashes
// retains every key placed in psFutureStakePoolParams during the epoch, not
// only the current one. The in-transaction owner check in gouroboros's
// validatePoolRegistration only rejects a reuse when IsVrfKeyInUse reports
// this same pool as the claimant and PoolCurrentState disagrees with the
// requested key, so this method must report P as claiming B even though B
// is neither P's effective (pre-boundary) key nor its current pending one.
func TestGetPoolByVrfKeyHashClaimsSupersededSameEpochFutureKey(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)

	poolKey := make([]byte, 28)
	poolKey[0] = 1
	keyA := make([]byte, 32)
	keyA[0] = 0xA
	keyB := make([]byte, 32)
	keyB[0] = 0xB
	keyC := make([]byte, 32)
	keyC[0] = 0xC

	// P registers with A before the current epoch begins.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: keyA},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  keyA,
			AddedSlot:   10,
		},
		nil,
	))
	// P: A -> B, mid-epoch.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: keyB},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  keyB,
			AddedSlot:   50,
		},
		nil,
	))
	// P: B -> C, same epoch. B is now superseded -- it is neither P's
	// effective key (still A) nor its current pending key (now C) -- but
	// it must still be claimed by P for the rest of the epoch.
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKey, VrfKeyHash: keyC},
		&models.PoolRegistration{
			PoolKeyHash: poolKey,
			VrfKeyHash:  keyC,
			AddedSlot:   70,
		},
		nil,
	))

	const epochStartSlot = 30

	// B must be reported as claimed by P, not free, even though it is not
	// P's effective key (A) or current pending key (C). This is the
	// signal gouroboros's caller needs: it will compare against
	// PoolCurrentState (C) and reject a P -> B reuse because C != B.
	got, err := store.GetPoolByVrfKeyHash(keyB, epochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		got,
		"a superseded same-epoch future key must still be claimed, "+
			"not reported free",
	)
	require.Equal(t, poolKey, got.PoolKeyHash)

	// Sanity check on the mechanism the caller relies on: P's current
	// (latest) registration is genuinely C, not B, which is exactly what
	// makes the reuse of B distinguishable from a legitimate revert to an
	// unchanged key. This mirrors how ledger.LedgerView.PoolCurrentState
	// picks the latest registration by AddedSlot.
	fullPool, err := store.GetPool(
		lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKey)),
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, fullPool.Registration)
	latest := fullPool.Registration[0]
	for _, reg := range fullPool.Registration[1:] {
		if reg.AddedSlot > latest.AddedSlot {
			latest = reg
		}
	}
	require.Equal(t, keyC, latest.VrfKeyHash)

	// A and C themselves are unaffected: A remains P's active key, and C
	// is P's own pending key, so reverting to either must not be treated
	// as a conflict against a different owner.
	got, err = store.GetPoolByVrfKeyHash(keyA, epochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolKey, got.PoolKeyHash)

	got, err = store.GetPoolByVrfKeyHash(keyC, epochStartSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, poolKey, got.PoolKeyHash)
}

func TestCommitTimestamp(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	timestamp, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Zero(t, timestamp)

	transaction := store.Transaction(t.Context())
	require.NoError(t, store.SetCommitTimestamp(1234, transaction))
	require.NoError(t, transaction.Commit())
	timestamp, err = store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, int64(1234), timestamp)
}

func TestNodeSettingsAreImmutableWithNetworkBackfill(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	settings, err := store.GetNodeSettings()
	require.NoError(t, err)
	require.Nil(t, settings)

	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
	}))
	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     "preview",
	}))
	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeAPI,
		Network:     "mainnet",
	}))
	settings, err = store.GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, &types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     "preview",
	}, settings)
}
