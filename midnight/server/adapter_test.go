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

package server_test

import (
	"context"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/midnight/server"
	"github.com/stretchr/testify/require"
)

func TestBlockByNumber_OverflowIsRejected(t *testing.T) {
	t.Parallel()
	db := newTestDatabase(t)
	adapted := server.NewDatabase(db)

	_, err := adapted.BlockByNumber(context.Background(), math.MaxUint64)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

func TestBlockByNumber_ResolvesInsertedBlock(t *testing.T) {
	t.Parallel()
	db := newTestDatabase(t)
	blk := insertPlaceholderBlock(t, db, 5, 5, 0x05)
	adapted := server.NewDatabase(db)

	got, err := adapted.BlockByNumber(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, blk.Hash, got.Hash)
}

func TestMidnightBlockAdapterHonorsCancelledContext(t *testing.T) {
	t.Parallel()
	db := newTestDatabase(t)
	adapter := server.NewDatabase(db)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := adapter.BlockByHash(ctx, make([]byte, 32))
	require.ErrorIs(t, err, context.Canceled)
	_, err = adapter.BlockByNumber(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = adapter.BlocksRecent(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = adapter.BlockBeforeSlot(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBlockByNumberWithBoundaryBlockStorageOffset(t *testing.T) {
	t.Parallel()
	db := newTestDatabase(t)
	for i, number := range []uint64{0, 0, 1, 2} {
		hash := make([]byte, 32)
		hash[0] = byte(i + 1)
		require.NoError(
			t,
			db.BlockCreate(
				models.Block{
					ID:     uint64(i) + 1,
					Number: number,
					Slot:   uint64(i) * 20,
					Hash:   hash,
					Cbor:   []byte{byte(i)},
				},
				nil,
			),
		)
	}
	block, err := server.NewDatabase(db).BlockByNumber(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.Number)
	require.Equal(t, byte(4), block.Hash[0])
}
