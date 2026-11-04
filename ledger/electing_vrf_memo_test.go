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
	"context"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingVrfKeyHashStore struct {
	metadata.MetadataStore
	cutoffHash   []byte
	earliestHash []byte
	cutoff       atomic.Int64
	earliest     atomic.Int64
	missCut      bool
	// onCutoff runs inside the cutoff lookup, after the lookup has captured
	// its memo generation and before it stores the result.
	onCutoff func()
}

func (s *countingVrfKeyHashStore) GetPoolVrfKeyHashAtSlot(
	_ []byte,
	_ uint64,
	_ types.Txn,
) ([]byte, bool, error) {
	s.cutoff.Add(1)
	if s.onCutoff != nil {
		s.onCutoff()
	}
	if s.missCut {
		return nil, false, nil
	}
	return s.cutoffHash, true, nil
}

func (s *countingVrfKeyHashStore) GetPoolEarliestVrfKeyHashAtSlot(
	_ []byte,
	_ uint64,
	_ types.Txn,
) ([]byte, bool, error) {
	s.earliest.Add(1)
	return s.earliestHash, true, nil
}

func newElectingVrfMemoLedger(
	t *testing.T,
	store *countingVrfKeyHashStore,
	tipSlot uint64,
) (*LedgerState, []models.Epoch, electingSnapshot) {
	t.Helper()
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: t.TempDir()}},
		func(inner metadata.MetadataStore) metadata.MetadataStore {
			store.MetadataStore = inner
			return store
		},
	)
	require.NoError(t, err)
	ls := newEligibilityTestLedgerOnDB(t, nil, db)
	epochCache := []models.Epoch{{
		EpochId:       5,
		StartSlot:     1_000,
		LengthInSlots: 1_000_000,
		SlotLength:    1,
		EraId:         eras.ShelleyEraDesc.Id,
	}}
	ls.epochCache = epochCache
	ls.currentTip = ochainsync.Tip{Point: ocommon.Point{Slot: tipSlot}}
	ls.publishSnapshotsLocked()
	return ls, epochCache, electingSnapshot{
		epoch:      5,
		kind:       models.PoolStakeSnapshotTypeMark,
		epochCache: epochCache,
		row:        &models.PoolStakeSnapshot{CapturedSlot: 1_500},
	}
}

func lookupElectingVrf(
	t *testing.T,
	ls *LedgerState,
	epochCache []models.Epoch,
	snap electingSnapshot,
) lcommon.Blake2b256 {
	t.Helper()
	hash, ok, err := ls.electingVrfKeyHashFromSnapshotWithContext(
		context.Background(),
		lcommon.PoolKeyHash{1},
		epochCache,
		snap,
	)
	require.NoError(t, err)
	require.True(t, ok)
	return hash
}

func TestElectingVrfKeyHashMemoizedBelowStableBound(t *testing.T) {
	t.Parallel()
	want := lcommon.Blake2b256{7}
	store := &countingVrfKeyHashStore{
		cutoffHash:   want[:],
		earliestHash: []byte{0x70, 31: 0x70},
	}
	ls, epochCache, snap := newElectingVrfMemoLedger(t, store, 1_000_000)

	first := lookupElectingVrf(t, ls, epochCache, snap)
	second := lookupElectingVrf(t, ls, epochCache, snap)

	assert.Equal(t, want, first)
	assert.Equal(t, first, second)
	assert.Equal(t, int64(1), store.cutoff.Load(), "repeat lookup hit the store")
	assert.Equal(t, int64(0), store.earliest.Load(), "cutoff hit used the earliest lookup")
}

func TestElectingVrfKeyHashEarliestFallbackMemoized(t *testing.T) {
	t.Parallel()
	want := lcommon.Blake2b256{8}
	store := &countingVrfKeyHashStore{earliestHash: want[:], missCut: true}
	ls, epochCache, snap := newElectingVrfMemoLedger(t, store, 1_000_000)

	assert.Equal(t, want, lookupElectingVrf(t, ls, epochCache, snap))
	assert.Equal(t, want, lookupElectingVrf(t, ls, epochCache, snap))
	assert.Equal(t, int64(1), store.cutoff.Load())
	assert.Equal(t, int64(1), store.earliest.Load())
}

func TestElectingVrfKeyHashNotMemoizedWithinStabilityWindow(t *testing.T) {
	t.Parallel()
	want := lcommon.Blake2b256{9}
	store := &countingVrfKeyHashStore{cutoffHash: want[:]}
	// Tip is only 100 slots past the capture slot, inside the window.
	ls, epochCache, snap := newElectingVrfMemoLedger(t, store, 1_600)

	assert.Equal(t, want, lookupElectingVrf(t, ls, epochCache, snap))
	assert.Equal(t, want, lookupElectingVrf(t, ls, epochCache, snap))
	assert.Equal(t, int64(2), store.cutoff.Load())
}

// A rollback that lands while a lookup is between reading the store and
// storing its answer must not let that pre-rollback answer into the memo.
func TestElectingVrfKeyHashLookupInFlightAcrossClearNotMemoized(t *testing.T) {
	t.Parallel()
	want := lcommon.Blake2b256{10}
	store := &countingVrfKeyHashStore{cutoffHash: want[:]}
	ls, epochCache, snap := newElectingVrfMemoLedger(t, store, 1_000_000)
	var cleared atomic.Bool
	store.onCutoff = func() {
		if cleared.CompareAndSwap(false, true) {
			ls.electingVrfMemo.clear()
		}
	}

	assert.Equal(t, want, lookupElectingVrf(t, ls, epochCache, snap))
	assert.Equal(t, want, lookupElectingVrf(t, ls, epochCache, snap))
	assert.Equal(
		t,
		int64(2),
		store.cutoff.Load(),
		"a lookup that straddled a clear repopulated the memo",
	)
}

func seedElectingVrfMemo(ls *LedgerState) electingVrfMemoKey {
	key := electingVrfMemoKey{pool: lcommon.PoolKeyHash{1}, cutoffSlot: 1}
	ls.electingVrfMemo.put(
		ls.electingVrfMemo.currentGeneration(),
		key,
		lcommon.Blake2b256{11},
	)
	return key
}

// Fork resolution and recovery rewinds roll the ledger back without a peer
// rollback event, so invalidation must happen in the ledger rollback itself.
func TestElectingVrfMemoClearedByLedgerRollback(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	key := seedElectingVrfMemo(fixture.ls)

	require.NoError(
		t,
		fixture.ls.rollback(context.Background(), fixture.ancestorTip.Point),
	)

	_, hit := fixture.ls.electingVrfMemo.get(key)
	assert.False(t, hit, "memo entry survived a ledger rollback")
}

func TestElectingVrfMemoKeptByNoOpRollback(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	key := seedElectingVrfMemo(fixture.ls)

	require.NoError(
		t,
		fixture.ls.rollback(context.Background(), fixture.ls.currentTip.Point),
	)

	_, hit := fixture.ls.electingVrfMemo.get(key)
	assert.True(t, hit, "a rollback to the current tip cleared the memo")
}
