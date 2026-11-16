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

package server

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// chainDB is blocks 0..9 at slot 10*number.
type chainDB struct{ MidnightDatabase }

func chainBlock(n uint64) models.Block {
	return models.Block{Number: n, Slot: n * 10, Hash: []byte{byte(n)}}
}

func (chainDB) BlockByHash(_ context.Context, h []byte) (models.Block, error) {
	if len(h) == 1 && h[0] < 10 {
		return chainBlock(uint64(h[0])), nil
	}
	return models.Block{}, models.ErrBlockNotFound
}

func (chainDB) BlockByNumber(
	_ context.Context,
	n uint64,
) (models.Block, error) {
	if n < 10 {
		return chainBlock(n), nil
	}
	return models.Block{}, models.ErrBlockNotFound
}

func (chainDB) BlocksRecent(context.Context, int) ([]models.Block, error) {
	return []models.Block{chainBlock(9)}, nil
}

func (chainDB) BlockBeforeSlot(
	_ context.Context,
	slot uint64,
) (models.Block, error) {
	last := min(9, (slot+9)/10)
	if slot == 0 {
		return models.Block{}, models.ErrBlockNotFound
	}
	for last > 0 && last*10 >= slot {
		last--
	}
	return chainBlock(last), nil
}

func TestBoundedDatabaseStopsAtTheTip(t *testing.T) {
	t.Parallel()
	db := NewBoundedDatabase(
		chainDB{},
		func(context.Context) (uint64, uint64, bool, error) {
			return 50, 5, true, nil
		},
	)
	ctx := context.Background()

	recent, err := db.BlocksRecent(ctx, 1)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	require.Equal(t, uint64(5), recent[0].Number)

	recent, err = db.BlocksRecent(ctx, 3)
	require.NoError(t, err)
	require.Len(t, recent, 3)
	require.Equal(t, uint64(3), recent[2].Number)

	_, err = db.BlockByNumber(ctx, 6)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	block, err := db.BlockByNumber(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, uint64(5), block.Number)

	_, err = db.BlockByHash(ctx, []byte{7})
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	_, err = db.BlockByHash(ctx, []byte{4})
	require.NoError(t, err)

	block, err = db.BlockBeforeSlot(ctx, 1000)
	require.NoError(t, err)
	require.Equal(t, uint64(5), block.Number, "as-of lookups clamp to the tip")
}

func TestBoundedDatabaseWithNothingIndexed(t *testing.T) {
	t.Parallel()
	db := NewBoundedDatabase(
		chainDB{},
		func(context.Context) (uint64, uint64, bool, error) {
			return 0, 0, false, nil
		},
	)
	recent, err := db.BlocksRecent(context.Background(), 1)
	require.NoError(t, err)
	require.Empty(t, recent)
}
