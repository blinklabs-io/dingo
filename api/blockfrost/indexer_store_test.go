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

package blockfrost

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

type nopTipState struct{}

func (nopTipState) GetSyncState(
	string,
	*database.Txn,
) (string, error) {
	return "", nil
}

func (nopTipState) SetSyncState(
	string,
	string,
	*database.Txn,
) error {
	return nil
}

func (nopTipState) DeleteSyncState(
	string,
	*database.Txn,
) error {
	return nil
}

func (nopTipState) ListSyncStateKeysByPrefix(
	string,
	*database.Txn,
) ([]string, error) {
	return nil, nil
}

func TestAdapterReadsFromIndexerStore(t *testing.T) {
	t.Parallel()
	idxDB := &database.Database{}
	store, err := indexerdb.NewStore(idxDB, nopTipState{}, 10)
	require.NoError(t, err)

	a := &NodeAdapter{}
	a.indexer = store
	require.Same(t, idxDB, a.db())
}

type fixedTipState struct {
	nopTipState
	value string
}

func (s fixedTipState) GetSyncState(string, *database.Txn) (string, error) {
	return s.value, nil
}

func TestDataTipIsTheIndexerTipOnSplit(t *testing.T) {
	t.Parallel()
	hash := bytes.Repeat([]byte{0xab}, 32)
	store, err := indexerdb.NewStore(
		&database.Database{},
		fixedTipState{value: fmt.Sprintf("42:7:%x", hash)},
		10,
	)
	require.NoError(t, err)

	a := &NodeAdapter{}
	a.indexer = store
	tip, err := a.dataTip()
	require.NoError(t, err)
	require.Equal(t, uint64(42), tip.Point.Slot)
	require.Equal(t, uint64(7), tip.BlockNumber)
	require.Equal(t, hash, tip.Point.Hash)
}

// On the split layout the block store holds blocks the IndexerDB has not
// indexed. A slot lookup answers for a block at or below the IndexerDB tip and
// reports not found above it, as a lookup by hash or height does.
func TestBlockAtSlotHidesBlocksAboveTheIndexerTip(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	indexed := models.Block{
		ID: 1, Slot: 40, Hash: bytes.Repeat([]byte{0x01}, 32),
		Cbor: []byte{0x80}, Number: 1, Type: 1,
	}
	ahead := models.Block{
		ID: 2, Slot: 50, Hash: bytes.Repeat([]byte{0x02}, 32),
		Cbor: []byte{0x80}, Number: 2, Type: 1,
	}
	require.NoError(t, db.BlockCreate(indexed, nil))
	require.NoError(t, db.BlockCreate(ahead, nil))
	store, err := indexerdb.NewStore(
		db,
		fixedTipState{value: fmt.Sprintf("%d:%d:%x", indexed.Slot, indexed.Number, indexed.Hash)},
		10,
	)
	require.NoError(t, err)
	a := &NodeAdapter{}
	a.indexer = store

	got, err := a.blockAtSlot(t.Context(), indexed.Slot)
	require.NoError(t, err)
	require.Equal(t, indexed.Hash, got.Hash)

	_, err = a.blockAtSlot(t.Context(), ahead.Slot)
	require.ErrorIs(t, err, ErrBlockNotFound)
}
