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

package kupo

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// metadataChainSlots are the canonical slots seeded by newMetadataChainDB.
// The gaps between them are the empty slots a Kupo client may ask about.
var metadataChainSlots = []uint64{10, 20, 30}

func newMetadataChainDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	for i, slot := range metadataChainSlots {
		id := uint64(i) + database.BlockInitialIndex
		require.NoError(t, db.BlockCreate(models.Block{
			ID:     id,
			Slot:   slot,
			Hash:   bytes.Repeat([]byte{byte(slot)}, 32),
			Cbor:   []byte{0x80},
			Number: id,
			Type:   1,
		}, nil))
	}
	return db
}

// TestMetadataBlockAtOrAfterSlotSelectsTheFollowingBlock pins the Kupo
// contract for a metadata request at a slot that holds no block.
//
// Kupo resolves the latest checkpoint strictly below the slot and hands that
// point to a fetch-block client whose documented contract is the block
// immediately following the given point, and it records one checkpoint per
// rolled-forward block. So for an empty slot the answer is the next block on
// chain; the previous block carries different metadata under a different
// header hash.
func TestMetadataBlockAtOrAfterSlotSelectsTheFollowingBlock(t *testing.T) {
	t.Parallel()

	db := newMetadataChainDB(t)

	for name, tc := range map[string]struct {
		slot uint64
		want uint64
	}{
		"empty slot takes the next block": {slot: 15, want: 20},
		"slot holding a block takes it":   {slot: 20, want: 20},
		"slot below the first block":      {slot: 1, want: 10},
		"empty slot just below a block":   {slot: 29, want: 30},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			txn := db.BlobTxn(false)
			defer txn.Release()
			block, err := metadataBlockAtOrAfterSlot(t.Context(), txn, tc.slot)
			require.NoError(t, err)
			require.Equal(t, tc.want, block.Slot)
			require.Equal(
				t,
				bytes.Repeat([]byte{byte(tc.want)}, 32),
				block.Hash,
			)
		})
	}
}

// TestMetadataBlockAtOrAfterSlotReportsMissingTarget pins that a slot past the
// newest indexed block has no answer, rather than falling back to the tip.
func TestMetadataBlockAtOrAfterSlotReportsMissingTarget(t *testing.T) {
	t.Parallel()

	db := newMetadataChainDB(t)
	txn := db.BlobTxn(false)
	defer txn.Release()

	_, err := metadataBlockAtOrAfterSlot(t.Context(), txn, 31)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.True(
		t,
		isMetadataBlockUnavailable(err),
		"the handler maps this to the request-level error",
	)
}

// expiredBlockStore reports every block read as locally expired, which is what
// a slot below the history-retention floor looks like.
type expiredBlockStore struct {
	blob.BlobStore
}

func (s *expiredBlockStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	// A history-expiry tombstone keeps the block metadata and drops only the
	// CBOR, which is why the point index still resolves an expired block.
	_, metadata, err := s.BlobStore.GetBlock(txn, slot, hash)
	if err != nil {
		return nil, metadata, err
	}
	return nil, metadata, types.ErrHistoryExpired
}

// TestMetadataBlockAtOrAfterSlotReportsExpiredTarget pins that an expired
// target is reported as unavailable rather than answered from a neighbouring
// block, and that no ancestor walk runs looking for one.
func TestMetadataBlockAtOrAfterSlotReportsExpiredTarget(t *testing.T) {
	t.Parallel()

	db := newMetadataChainDB(t)
	db.SetBlobStore(&expiredBlockStore{BlobStore: db.Blob()})
	txn := db.BlobTxn(false)
	defer txn.Release()

	_, err := metadataBlockAtOrAfterSlot(t.Context(), txn, 15)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
	require.True(
		t,
		isMetadataBlockUnavailable(err),
		"the handler maps this to the request-level error",
	)
}
