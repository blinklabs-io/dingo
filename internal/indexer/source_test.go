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

package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/stretchr/testify/require"
)

func TestDatabaseSourceWalksCanonicalIndex(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	chain, err := testfixtures.GenerateConwayChainAt(1, 0, 6)
	require.NoError(t, err)
	var stored []models.Block
	for _, b := range chain {
		blk := models.Block{
			Slot: b.SlotNumber(), Hash: b.Hash().Bytes(),
			PrevHash: b.PrevHash().Bytes(), Number: b.BlockNumber(),
			Cbor: b.Cbor(), Type: uint(b.Type()),
		}
		require.NoError(t, db.BlockCreate(blk, nil))
		stored = append(stored, blk)
	}
	src := NewDatabaseSource(db)
	ctx := context.Background()
	tipOf := func(b models.Block) indexerdb.Tip {
		return indexerdb.Tip{Slot: b.Slot, BlockNumber: b.Number, Hash: b.Hash}
	}

	all, err := src.BlocksAfter(ctx, indexerdb.Tip{}, stored[5].Slot, 100)
	require.NoError(t, err)
	require.Len(t, all, 6)
	for i, b := range all {
		require.Equal(t, stored[i].Hash, b.Hash)
		require.Equal(t, stored[i].PrevHash, b.PrevHash)
		require.Equal(t, stored[i].Number, b.Number)
		require.Equal(t, stored[i].Cbor, b.Cbor)
	}

	rest, err := src.BlocksAfter(ctx, tipOf(stored[1]), stored[5].Slot, 100)
	require.NoError(t, err)
	require.Len(t, rest, 4)
	require.Equal(t, stored[2].Hash, rest[0].Hash)

	bounded, err := src.BlocksAfter(ctx, tipOf(stored[1]), stored[3].Slot, 100)
	require.NoError(t, err)
	require.Len(t, bounded, 2)

	limited, err := src.BlocksAfter(ctx, indexerdb.Tip{}, stored[5].Slot, 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)

	canonical, err := src.IsCanonical(ctx, stored[3].Hash)
	require.NoError(t, err)
	require.True(t, canonical)
	// A block the ledger no longer stores (a rollback deleted it) is simply
	// not canonical.
	canonical, err = src.IsCanonical(
		ctx,
		[]byte("unknown-hash-unknown-hash-unkno!"),
	)
	require.NoError(t, err)
	require.False(t, canonical)
}

// indexReadFails fails reads of block-index entries.
type indexReadFails struct {
	blob.BlobStore
	err error
}

func (s *indexReadFails) Get(txn types.Txn, key []byte) ([]byte, error) {
	if len(key) > 0 && string(key[:2]) == types.BlockBlobIndexKeyPrefix {
		return nil, s.err
	}
	return s.BlobStore.Get(txn, key)
}

func TestDatabaseSourceIsCanonicalPropagatesIndexReadErrors(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	chain, err := testfixtures.GenerateConwayChainAt(1, 0, 2)
	require.NoError(t, err)
	for _, b := range chain {
		require.NoError(t, db.BlockCreate(models.Block{
			Slot: b.SlotNumber(), Hash: b.Hash().Bytes(),
			PrevHash: b.PrevHash().Bytes(), Number: b.BlockNumber(),
			Cbor: b.Cbor(), Type: uint(b.Type()),
		}, nil))
	}
	boom := errors.New("index read failed")
	db.SetBlobStore(&indexReadFails{BlobStore: db.Blob(), err: boom})

	canonical, err := NewDatabaseSource(db).IsCanonical(
		context.Background(), chain[1].Hash().Bytes(),
	)
	require.ErrorIs(t, err, boom)
	require.False(t, canonical)
}
