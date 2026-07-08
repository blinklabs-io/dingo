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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// hashFor returns a deterministic 32-byte hash so unique-index columns
// (tx_hash, utxo_tx_hash) never collide across fixture rows.
func hashFor(b byte) []byte {
	h := make([]byte, 32)
	h[31] = b
	return h
}

func TestFindMidnightPaginated_EmptyDatabase(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	creates, err := store.FindMidnightAssetCreatesFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Empty(t, creates)

	spends, err := store.FindMidnightAssetSpendsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Empty(t, spends)

	registrations, err := store.FindMidnightRegistrationsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Empty(t, registrations)

	deregistrations, err := store.FindMidnightDeregistrationsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Empty(t, deregistrations)
}

// TestFindMidnightAssetCreatesFrom_CursorPagination inserts rows spanning
// several (block_number, tx_index) pairs, including multiple rows in the
// same block, and pages through them with a small page size. It asserts the
// pages cover every row exactly once, in (block_number, tx_index) order,
// with no duplicates and no gaps.
func TestFindMidnightAssetCreatesFrom_CursorPagination(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	type pos struct {
		block uint64
		tx    uint32
	}
	fixture := []pos{{1, 0}, {1, 1}, {2, 0}, {2, 1}, {3, 0}}
	for i, p := range fixture {
		require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
			Address:     []byte{0x01},
			Quantity:    uint64(i + 1),
			TxHash:      hashFor(byte(i + 1)),
			OutputIndex: 0,
			BlockNumber: p.block,
			BlockHash:   hashFor(byte(p.block)),
			TxIndex:     p.tx,
		}))
	}

	const pageSize = 2
	var startBlock uint64
	var startTxIndex uint32
	var got []pos
	for range len(fixture) + 1 { // +1 guards against an infinite loop on a bug
		page, err := store.FindMidnightAssetCreatesFrom(startBlock, startTxIndex, pageSize, nil)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		require.LessOrEqual(t, len(page), pageSize)
		for _, row := range page {
			got = append(got, pos{row.BlockNumber, row.TxIndex})
		}
		last := page[len(page)-1]
		startBlock, startTxIndex = last.BlockNumber, last.TxIndex
	}

	require.Equal(t, fixture, got, "pages must cover every row exactly once, in order, with no gaps")
}

func TestFindMidnightAssetSpendsFrom_CursorPagination(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for i, p := range []struct {
		block uint64
		tx    uint32
	}{{1, 0}, {2, 0}, {2, 1}} {
		require.NoError(t, store.CreateMidnightAssetSpend(nil, &models.MidnightAssetSpend{
			Address:        []byte{0x01},
			Quantity:       uint64(i + 1),
			SpendingTxHash: hashFor(byte(i + 1)),
			UtxoTxHash:     hashFor(byte(i + 10)),
			UtxoIndex:      0,
			BlockNumber:    p.block,
			BlockHash:      hashFor(byte(p.block)),
			TxIndex:        p.tx,
		}))
	}

	page1, err := store.FindMidnightAssetSpendsFrom(0, 0, 2, nil)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.Equal(t, uint64(1), page1[0].BlockNumber)
	require.Equal(t, uint64(2), page1[1].BlockNumber)
	require.Equal(t, uint32(0), page1[1].TxIndex)

	last := page1[len(page1)-1]
	page2, err := store.FindMidnightAssetSpendsFrom(last.BlockNumber, last.TxIndex, 2, nil)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Equal(t, uint64(2), page2[0].BlockNumber)
	require.Equal(t, uint32(1), page2[0].TxIndex)

	page3, err := store.FindMidnightAssetSpendsFrom(
		page2[0].BlockNumber,
		page2[0].TxIndex,
		2,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, page3)
}

func TestFindMidnightRegistrationsFrom_CursorPagination(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for i, p := range []struct {
		block uint64
		tx    uint32
	}{{1, 0}, {2, 0}, {2, 1}} {
		require.NoError(t, store.CreateMidnightRegistration(nil, &models.MidnightRegistration{
			FullDatum:   []byte{byte(i + 1)},
			TxHash:      hashFor(byte(i + 1)),
			OutputIndex: 0,
			BlockNumber: p.block,
			BlockHash:   hashFor(byte(p.block)),
			TxIndex:     p.tx,
		}))
	}

	page1, err := store.FindMidnightRegistrationsFrom(0, 0, 2, nil)
	require.NoError(t, err)
	require.Len(t, page1, 2)

	last := page1[len(page1)-1]
	page2, err := store.FindMidnightRegistrationsFrom(last.BlockNumber, last.TxIndex, 2, nil)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Equal(t, uint64(2), page2[0].BlockNumber)
	require.Equal(t, uint32(1), page2[0].TxIndex)
}

func TestFindMidnightDeregistrationsFrom_CursorPagination(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for i, p := range []struct {
		block uint64
		tx    uint32
	}{{1, 0}, {2, 0}, {2, 1}} {
		require.NoError(t, store.CreateMidnightDeregistration(nil, &models.MidnightDeregistration{
			FullDatum:   []byte{byte(i + 1)},
			TxHash:      hashFor(byte(i + 1)),
			UtxoTxHash:  hashFor(byte(i + 10)),
			UtxoIndex:   0,
			BlockNumber: p.block,
			BlockHash:   hashFor(byte(p.block)),
			TxIndex:     p.tx,
		}))
	}

	page1, err := store.FindMidnightDeregistrationsFrom(0, 0, 2, nil)
	require.NoError(t, err)
	require.Len(t, page1, 2)

	last := page1[len(page1)-1]
	page2, err := store.FindMidnightDeregistrationsFrom(last.BlockNumber, last.TxIndex, 2, nil)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Equal(t, uint64(2), page2[0].BlockNumber)
	require.Equal(t, uint32(1), page2[0].TxIndex)
}

// TestFindMidnightAssetCreatesFrom_NoLimit verifies limit <= 0 returns every
// matching row without a SQL LIMIT applied.
func TestFindMidnightAssetCreatesFrom_NoLimit(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for i := range 5 {
		require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
			Address:     []byte{0x01},
			Quantity:    1,
			TxHash:      hashFor(byte(i + 1)),
			OutputIndex: 0,
			BlockNumber: uint64(i + 1),
			BlockHash:   hashFor(byte(i + 1)),
			TxIndex:     0,
		}))
	}

	rows, err := store.FindMidnightAssetCreatesFrom(0, 0, 0, nil)
	require.NoError(t, err)
	require.Len(t, rows, 5)
}
