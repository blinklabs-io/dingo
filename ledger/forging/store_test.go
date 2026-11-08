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
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSyncStateStore struct {
	mu     sync.Mutex
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
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
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

// TestSyncStateForgeFenceStoreIsPerPool keeps one pool's fence from
// gating another's: a node re-keyed to a different pool has not signed
// anything with the new credentials.
func TestSyncStateForgeFenceStoreIsPerPool(t *testing.T) {
	backing := newMockSyncStateStore()
	first := NewSyncStateForgeFenceStore(backing, storeTestPoolID("poolA"))
	second := NewSyncStateForgeFenceStore(backing, storeTestPoolID("poolB"))

	// NewSyncStateForgeFenceStore returns nil only for a nil store
	// (ledger/forging/store.go), and newMockSyncStateStore never returns
	// nil, so the fence stores built above are non-nil.
	//nolint:nilaway // newMockSyncStateStore returns a non-nil store
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

// TestSyncStateForgeFenceStoreRejectsMissingSlot fails closed on a record
// that decodes but carries no slot. Treating the absent field's zero value
// as a real fence would install an inert zero-slot fence that permits
// every positive slot, which is the opposite of what the record is for.
func TestSyncStateForgeFenceStoreRejectsMissingSlot(t *testing.T) {
	poolID := storeTestPoolID("pool")
	backing := newMockSyncStateStore()
	backing.values[syncStateForgeFenceKey(poolID)] = fmt.Sprintf(
		`{"format_version":%d,"pool_id":%q}`,
		forgeFenceFormatVersion,
		hex.EncodeToString(poolID[:]),
	)
	store := NewSyncStateForgeFenceStore(backing, poolID)

	_, _, err := store.LoadLastForgedSlot()
	require.ErrorContains(t, err, "missing last_forged_slot")
}

// TestSyncStateForgeFenceStoreAcceptsExplicitZeroSlot keeps the presence
// check from rejecting a genuine slot-0 fence.
func TestSyncStateForgeFenceStoreAcceptsExplicitZeroSlot(t *testing.T) {
	poolID := storeTestPoolID("pool")
	backing := newMockSyncStateStore()
	backing.values[syncStateForgeFenceKey(poolID)] = fmt.Sprintf(
		`{"format_version":%d,"pool_id":%q,"last_forged_slot":0}`,
		forgeFenceFormatVersion,
		hex.EncodeToString(poolID[:]),
	)
	store := NewSyncStateForgeFenceStore(backing, poolID)

	slot, ok, err := store.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(0), slot)
}

// TestSyncStateForgeFenceStoreConcurrentStoresKeepHighest makes the
// store's forward-only guarantee a property of the type rather than of
// its caller. The forger drives it from one goroutine today, but a
// read-modify-write on an exported type should not depend on that.
func TestSyncStateForgeFenceStoreConcurrentStoresKeepHighest(t *testing.T) {
	backing := newMockSyncStateStore()
	store := NewSyncStateForgeFenceStore(backing, storeTestPoolID("pool"))

	const highest = 500
	var wg sync.WaitGroup
	for slot := 1; slot <= highest; slot++ {
		wg.Go(func() {
			assert.NoError(t, store.StoreLastForgedSlot(uint64(slot)))
		})
	}
	wg.Wait()

	slot, ok, err := store.LoadLastForgedSlot()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(highest), slot)
}
