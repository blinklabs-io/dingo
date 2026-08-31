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

package forging

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSyncStateStore struct {
	values map[string]string
	getErr error
	setErr error
}

func newMockSyncStateStore() *mockSyncStateStore {
	return &mockSyncStateStore{values: make(map[string]string)}
}

func (m *mockSyncStateStore) GetSyncState(
	key string,
	_ types.Txn,
) (string, error) {
	if m.getErr != nil {
		return "", m.getErr
	}
	return m.values[key], nil
}

func (m *mockSyncStateStore) SetSyncState(
	key string,
	value string,
	_ types.Txn,
) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.values[key] = value
	return nil
}

func storeTestPoolID(seed string) lcommon.PoolKeyHash {
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], seed)
	return poolID
}

func TestSyncStateForgeFenceStoreRoundTrip(t *testing.T) {
	poolID := storeTestPoolID("testpool1234567890123456789")
	backing := newMockSyncStateStore()
	store := NewSyncStateForgeFenceStore(backing, poolID)
	require.NotNil(t, store)

	slot, ok, err := store.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.False(t, ok, "an unwritten fence must report no record")
	assert.Equal(t, uint64(0), slot)

	require.NoError(t, store.StoreLastForgedSlot(12345))

	slot, ok, err = store.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(12345), slot)
}

// TestSyncStateForgeFenceStoreIsPerPool keeps one pool's fence from
// gating another's: a node re-keyed to a different pool has not signed
// anything with the new credentials.
func TestSyncStateForgeFenceStoreIsPerPool(t *testing.T) {
	backing := newMockSyncStateStore()
	first := NewSyncStateForgeFenceStore(backing, storeTestPoolID("poolA"))
	second := NewSyncStateForgeFenceStore(backing, storeTestPoolID("poolB"))

	require.NoError(t, first.StoreLastForgedSlot(500))

	slot, ok, err := second.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), slot)

	slot, ok, err = first.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(500), slot)
}

func TestSyncStateForgeFenceStoreKeyIncludesPool(t *testing.T) {
	poolID := storeTestPoolID("testpool")
	backing := newMockSyncStateStore()
	store := NewSyncStateForgeFenceStore(backing, poolID)
	require.NoError(t, store.StoreLastForgedSlot(7))

	_, found := backing.values[syncStateForgeFencePrefix+":"+
		hex.EncodeToString(poolID[:])]
	assert.True(t, found, "fence key must be namespaced by pool id")
}

// TestSyncStateForgeFenceStoreNeverLowersFence keeps a stale or
// out-of-order write from weakening protection already recorded.
func TestSyncStateForgeFenceStoreNeverLowersFence(t *testing.T) {
	backing := newMockSyncStateStore()
	store := NewSyncStateForgeFenceStore(backing, storeTestPoolID("pool"))

	require.NoError(t, store.StoreLastForgedSlot(100))
	require.NoError(t, store.StoreLastForgedSlot(50))

	slot, ok, err := store.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(100), slot)
}

func TestSyncStateForgeFenceStoreLoadError(t *testing.T) {
	backing := newMockSyncStateStore()
	backing.getErr = errors.New("metadata unavailable")
	store := NewSyncStateForgeFenceStore(backing, storeTestPoolID("pool"))

	_, _, err := store.LoadLastForgedSlot()
	require.ErrorContains(t, err, "metadata unavailable")
}

func TestSyncStateForgeFenceStoreSaveError(t *testing.T) {
	backing := newMockSyncStateStore()
	backing.setErr = errors.New("disk full")
	store := NewSyncStateForgeFenceStore(backing, storeTestPoolID("pool"))

	require.ErrorContains(t, store.StoreLastForgedSlot(1), "disk full")
}

// TestSyncStateForgeFenceStoreRejectsCorruptRecord fails closed: an
// unreadable fence must surface as an error, not as "no fence recorded",
// which would silently allow a slot to be signed twice.
func TestSyncStateForgeFenceStoreRejectsCorruptRecord(t *testing.T) {
	poolID := storeTestPoolID("pool")
	backing := newMockSyncStateStore()
	backing.values[syncStateForgeFencePrefix+":"+
		hex.EncodeToString(poolID[:])] = "{not json"
	store := NewSyncStateForgeFenceStore(backing, poolID)

	_, _, err := store.LoadLastForgedSlot()
	require.Error(t, err)
	require.ErrorContains(t, err, "decode forge fence")
}

// TestSyncStateForgeFenceStoreRejectsForeignPoolRecord catches a record
// written under this key by different credentials.
func TestSyncStateForgeFenceStoreRejectsForeignPoolRecord(t *testing.T) {
	poolID := storeTestPoolID("poolA")
	backing := newMockSyncStateStore()
	other := NewSyncStateForgeFenceStore(backing, poolID)
	require.NoError(t, other.StoreLastForgedSlot(9))

	// Re-point the same key at different credentials.
	otherPoolID := storeTestPoolID("poolB")
	backing.values[syncStateForgeFencePrefix+":"+
		hex.EncodeToString(otherPoolID[:])] =
		backing.values[syncStateForgeFencePrefix+":"+
			hex.EncodeToString(poolID[:])]
	store := NewSyncStateForgeFenceStore(backing, otherPoolID)

	_, _, err := store.LoadLastForgedSlot()
	require.ErrorContains(t, err, "pool mismatch")
}

func TestNewSyncStateForgeFenceStoreNilBacking(t *testing.T) {
	assert.Nil(t, NewSyncStateForgeFenceStore(nil, storeTestPoolID("pool")))
}

// TestSyncStateForgeFenceStoreRealDatabase exercises the fence against a
// real metadata store, so the sync_state round trip is proven rather than
// assumed from the mock. It also covers the restart path the fence exists
// for: a second store over the same database reads back the fence.
func TestSyncStateForgeFenceStoreRealDatabase(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	poolID := storeTestPoolID("realdbpool")
	store := NewSyncStateForgeFenceStore(db.Metadata(), poolID)
	require.NotNil(t, store)

	_, ok, err := store.LoadLastForgedSlot()
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, store.StoreLastForgedSlot(98765))

	// A fresh store over the same database is what a restart sees.
	reopened := NewSyncStateForgeFenceStore(db.Metadata(), poolID)
	slot, ok, err := reopened.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(98765), slot)

	// The fence only moves forward.
	require.NoError(t, store.StoreLastForgedSlot(1))
	slot, _, err = reopened.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.Equal(t, uint64(98765), slot)
}
