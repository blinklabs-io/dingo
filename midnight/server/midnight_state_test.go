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
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/midnight"
	"github.com/blinklabs-io/dingo/midnight/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setupTestStore creates an in-memory sqlite metadata store with the schema
// migrated, so tests can seed midnight_* tables directly.
func setupTestStore(t *testing.T) *sqlite.MetadataStoreSqlite {
	t.Helper()
	store, err := sqlite.New("", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	require.NoError(t, store.DB().AutoMigrate(models.MigrateModels...))
	t.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

// startTestServerWithMetadata starts a server backed by md (with no block
// resolver configured) and returns its dial address. The server is stopped
// on test cleanup.
func startTestServerWithMetadata(t *testing.T, md *sqlite.MetadataStoreSqlite) string {
	t.Helper()
	return startTestServerConfig(t, server.Config{Metadata: md})
}

func hashForByte(b byte) []byte {
	h := make([]byte, 32)
	h[31] = b
	return h
}

func TestGetAssetCreates_EmptyDatabase(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.GetAssetCreates(ctx, &midnight.AssetCreatesRequest{UtxoCapacity: 10})
	require.NoError(t, err)
	require.Empty(t, resp.GetCreates())
}

// TestGetAssetCreates_CursorPagination seeds rows across three blocks and
// pages through them with a page size smaller than the total row count,
// asserting every row is returned exactly once, in order, with no gaps.
func TestGetAssetCreates_CursorPagination(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for i, p := range []struct {
		block uint64
		tx    uint32
	}{{1, 0}, {1, 1}, {2, 0}, {3, 0}} {
		require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
			Address:     []byte{0x01},
			Quantity:    uint64(i + 1),
			TxHash:      hashForByte(byte(i + 1)),
			OutputIndex: 0,
			BlockNumber: p.block,
			BlockHash:   hashForByte(byte(p.block)),
			TxIndex:     p.tx,
		}))
	}

	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var startBlock, startTxIndex uint32
	type pos struct {
		block uint64
		tx    uint32
	}
	var got []pos
	for range 5 {
		resp, err := client.GetAssetCreates(ctx, &midnight.AssetCreatesRequest{
			StartBlock:   startBlock,
			StartTxIndex: startTxIndex,
			UtxoCapacity: 2,
		})
		require.NoError(t, err)
		if len(resp.GetCreates()) == 0 {
			break
		}
		require.LessOrEqual(t, len(resp.GetCreates()), 2)
		for _, c := range resp.GetCreates() {
			got = append(got, pos{c.GetBlockNumber(), c.GetTxIndex()})
		}
		last := resp.GetCreates()[len(resp.GetCreates())-1]
		startBlock, startTxIndex = uint32(last.GetBlockNumber()), last.GetTxIndex()
	}

	require.Equal(t, []pos{{1, 0}, {1, 1}, {2, 0}, {3, 0}}, got)
}

func TestGetAssetSpends_EmptyDatabase(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.GetAssetSpends(ctx, &midnight.AssetSpendsRequest{UtxoCapacity: 10})
	require.NoError(t, err)
	require.Empty(t, resp.GetSpends())
}

func TestGetRegistrations_EmptyDatabase(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.GetRegistrations(ctx, &midnight.RegistrationsRequest{UtxoCapacity: 10})
	require.NoError(t, err)
	require.Empty(t, resp.GetRegistrations())
}

func TestGetDeregistrations_EmptyDatabase(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.GetDeregistrations(ctx, &midnight.DeregistrationsRequest{UtxoCapacity: 10})
	require.NoError(t, err)
	require.Empty(t, resp.GetDeregistrations())
}

func TestGetUtxoEvents_EmptyDatabase(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{TxCapacity: 10})
	require.NoError(t, err)
	require.Empty(t, resp.GetEvents())
	require.Nil(t, resp.GetNextPosition())
}

// TestGetUtxoEvents_KindOrderTieBreak seeds one row of each of the four
// event types at the same (block_number, tx_index) and asserts GetUtxoEvents
// merges them in kind_order: create, spend, registration, deregistration.
func TestGetUtxoEvents_KindOrderTieBreak(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	const block, tx = uint64(5), uint32(2)
	require.NoError(t, store.CreateMidnightDeregistration(nil, &models.MidnightDeregistration{
		FullDatum:   []byte{0xd0},
		TxHash:      hashForByte(1),
		UtxoTxHash:  hashForByte(2),
		UtxoIndex:   0,
		BlockNumber: block,
		BlockHash:   hashForByte(byte(block)),
		TxIndex:     tx,
	}))
	require.NoError(t, store.CreateMidnightRegistration(nil, &models.MidnightRegistration{
		FullDatum:   []byte{0xd1},
		TxHash:      hashForByte(3),
		OutputIndex: 0,
		BlockNumber: block,
		BlockHash:   hashForByte(byte(block)),
		TxIndex:     tx,
	}))
	require.NoError(t, store.CreateMidnightAssetSpend(nil, &models.MidnightAssetSpend{
		Address:        []byte{0x01},
		Quantity:       1,
		SpendingTxHash: hashForByte(4),
		UtxoTxHash:     hashForByte(5),
		UtxoIndex:      0,
		BlockNumber:    block,
		BlockHash:      hashForByte(byte(block)),
		TxIndex:        tx,
	}))
	require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
		Address:     []byte{0x01},
		Quantity:    1,
		TxHash:      hashForByte(6),
		OutputIndex: 0,
		BlockNumber: block,
		BlockHash:   hashForByte(byte(block)),
		TxIndex:     tx,
	}))

	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{TxCapacity: 10})
	require.NoError(t, err)
	require.Len(t, resp.GetEvents(), 4)

	require.NotNil(t, resp.GetEvents()[0].GetAssetCreate())
	require.NotNil(t, resp.GetEvents()[1].GetAssetSpend())
	require.NotNil(t, resp.GetEvents()[2].GetRegistration())
	require.NotNil(t, resp.GetEvents()[3].GetDeregistration())

	next := resp.GetNextPosition()
	require.NotNil(t, next)
	require.Equal(t, uint32(block), next.GetBlockNumber())
	require.Equal(t, tx, next.GetTxIndex())
}

// TestGetUtxoEvents_CursorPagination verifies the next_position cursor
// returned from one page correctly resumes the merged stream on the next
// call, with no duplicates and no gaps across event kinds.
func TestGetUtxoEvents_CursorPagination(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
		Address:     []byte{0x01},
		Quantity:    1,
		TxHash:      hashForByte(1),
		OutputIndex: 0,
		BlockNumber: 1,
		BlockHash:   hashForByte(1),
		TxIndex:     0,
	}))
	require.NoError(t, store.CreateMidnightRegistration(nil, &models.MidnightRegistration{
		FullDatum:   []byte{0xd1},
		TxHash:      hashForByte(2),
		OutputIndex: 0,
		BlockNumber: 1,
		BlockHash:   hashForByte(1),
		TxIndex:     1,
	}))
	require.NoError(t, store.CreateMidnightAssetSpend(nil, &models.MidnightAssetSpend{
		Address:        []byte{0x01},
		Quantity:       1,
		SpendingTxHash: hashForByte(3),
		UtxoTxHash:     hashForByte(4),
		UtxoIndex:      0,
		BlockNumber:    2,
		BlockHash:      hashForByte(2),
		TxIndex:        0,
	}))

	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	page1, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{TxCapacity: 2})
	require.NoError(t, err)
	require.Len(t, page1.GetEvents(), 2)
	require.NotNil(t, page1.GetEvents()[0].GetAssetCreate())
	require.NotNil(t, page1.GetEvents()[1].GetRegistration())
	require.NotNil(t, page1.GetNextPosition())

	page2, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{
		TxCapacity:    2,
		StartPosition: page1.GetNextPosition(),
	})
	require.NoError(t, err)
	require.Len(t, page2.GetEvents(), 1)
	require.NotNil(t, page2.GetEvents()[0].GetAssetSpend())

	page3, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{
		TxCapacity:    2,
		StartPosition: page2.GetNextPosition(),
	})
	require.NoError(t, err)
	require.Empty(t, page3.GetEvents())
}

// TestGetUtxoEvents_TxCapacityDoesNotSplitTxGroup verifies that when
// tx_capacity would otherwise cut through events sharing the same
// (block_number, tx_index) but different kinds (e.g. a cNIGHT create and a
// mapping-validator registration written by the same tx), the page is
// extended to include the whole group rather than dropping the remainder.
func TestGetUtxoEvents_TxCapacityDoesNotSplitTxGroup(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
		Address:     []byte{0x01},
		Quantity:    1,
		TxHash:      hashForByte(1),
		OutputIndex: 0,
		BlockNumber: 1,
		BlockHash:   hashForByte(1),
		TxIndex:     0,
	}))
	require.NoError(t, store.CreateMidnightRegistration(nil, &models.MidnightRegistration{
		FullDatum:   []byte{0xd1},
		TxHash:      hashForByte(2),
		OutputIndex: 0,
		BlockNumber: 1,
		BlockHash:   hashForByte(1),
		TxIndex:     0,
	}))
	require.NoError(t, store.CreateMidnightAssetSpend(nil, &models.MidnightAssetSpend{
		Address:        []byte{0x01},
		Quantity:       1,
		SpendingTxHash: hashForByte(3),
		UtxoTxHash:     hashForByte(4),
		UtxoIndex:      0,
		BlockNumber:    2,
		BlockHash:      hashForByte(2),
		TxIndex:        0,
	}))

	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// tx_capacity=1 would naturally cut after just the create, splitting
	// the (1,0) group; the handler must extend to include the registration
	// too, without pulling in the unrelated spend at (2,0).
	resp, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{TxCapacity: 1})
	require.NoError(t, err)
	require.Len(t, resp.GetEvents(), 2, "must extend past tx_capacity to include the full (1,0) group")
	require.NotNil(t, resp.GetEvents()[0].GetAssetCreate())
	require.NotNil(t, resp.GetEvents()[1].GetRegistration())

	next := resp.GetNextPosition()
	require.NotNil(t, next)
	require.Equal(t, uint32(1), next.GetBlockNumber())
	require.Equal(t, uint32(0), next.GetTxIndex())

	page2, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{
		TxCapacity:    1,
		StartPosition: next,
	})
	require.NoError(t, err)
	require.Len(t, page2.GetEvents(), 1, "resuming must find exactly the spend, with no duplicates")
	require.NotNil(t, page2.GetEvents()[0].GetAssetSpend())
}

// TestGetUtxoEvents_EndBlockHashRequiresResolver verifies end_block_hash
// fails clearly when no block resolver is configured, instead of silently
// ignoring the boundary.
func TestGetUtxoEvents_EndBlockHashRequiresResolver(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	client := midnight.NewMidnightStateClient(dial(t, startTestServerWithMetadata(t, store)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{
		TxCapacity:   10,
		EndBlockHash: hashForByte(1),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestGetUtxoEvents_EndBlockHashStopsAtBlockWithNoEvents verifies the
// end_block_hash boundary is honored even when the target block itself
// carries no Midnight events — resolved via the configured block resolver,
// not by scanning the fetched event rows (which would never find a hash
// belonging to an event-less block).
func TestGetUtxoEvents_EndBlockHashStopsAtBlockWithNoEvents(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
		Address:     []byte{0x01},
		Quantity:    1,
		TxHash:      hashForByte(1),
		OutputIndex: 0,
		BlockNumber: 1,
		BlockHash:   hashForByte(1),
		TxIndex:     0,
	}))
	require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
		Address:     []byte{0x01},
		Quantity:    1,
		TxHash:      hashForByte(2),
		OutputIndex: 0,
		BlockNumber: 3,
		BlockHash:   hashForByte(3),
		TxIndex:     0,
	}))

	// Block 2 is resolvable on-chain but has zero Midnight events of its
	// own.
	blockNumbers := map[string]uint64{
		string(hashForByte(1)): 1,
		string(hashForByte(2)): 2,
		string(hashForByte(3)): 3,
	}
	addr := startTestServerConfig(t, server.Config{
		Metadata: store,
		BlockNumberByHash: func(hash []byte) (uint64, bool, error) {
			n, ok := blockNumbers[string(hash)]
			return n, ok, nil
		},
	})
	client := midnight.NewMidnightStateClient(dial(t, addr))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{
		TxCapacity:   10,
		EndBlockHash: hashForByte(2),
	})
	require.NoError(t, err)
	require.Len(t, resp.GetEvents(), 1, "must include block 1's create but exclude block 3's, even though the boundary block 2 has no events of its own")
	require.Equal(t, uint64(1), resp.GetEvents()[0].GetAssetCreate().GetBlockNumber())
}

// spyEventStore wraps a real store and records the txn passed to
// ReadTransaction and to each Find*From call, so a test can assert
// GetUtxoEvents shares exactly one read transaction across all four tables
// instead of passing nil (a separate, independently-timed read) to each.
type spyEventStore struct {
	*sqlite.MetadataStoreSqlite
	readTxnCalls int
	seenTxns     []types.Txn
}

func (s *spyEventStore) ReadTransaction() types.Txn {
	s.readTxnCalls++
	return s.MetadataStoreSqlite.ReadTransaction()
}

func (s *spyEventStore) FindMidnightAssetCreatesFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightAssetCreate, error) {
	s.seenTxns = append(s.seenTxns, txn)
	return s.MetadataStoreSqlite.FindMidnightAssetCreatesFrom(startBlock, startTxIndex, limit, txn)
}

func (s *spyEventStore) FindMidnightAssetSpendsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightAssetSpend, error) {
	s.seenTxns = append(s.seenTxns, txn)
	return s.MetadataStoreSqlite.FindMidnightAssetSpendsFrom(startBlock, startTxIndex, limit, txn)
}

func (s *spyEventStore) FindMidnightRegistrationsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightRegistration, error) {
	s.seenTxns = append(s.seenTxns, txn)
	return s.MetadataStoreSqlite.FindMidnightRegistrationsFrom(startBlock, startTxIndex, limit, txn)
}

func (s *spyEventStore) FindMidnightDeregistrationsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightDeregistration, error) {
	s.seenTxns = append(s.seenTxns, txn)
	return s.MetadataStoreSqlite.FindMidnightDeregistrationsFrom(startBlock, startTxIndex, limit, txn)
}

// TestGetUtxoEvents_SharesOneReadTransactionAcrossTables verifies GetUtxoEvents
// opens exactly one read transaction and passes that SAME transaction to all
// four Find*From calls, rather than four independent (nil-txn) reads that
// could each observe a different point in time while the live indexer is
// mid-block.
func TestGetUtxoEvents_SharesOneReadTransactionAcrossTables(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	spy := &spyEventStore{MetadataStoreSqlite: store}

	client := midnight.NewMidnightStateClient(dial(t, startTestServerConfig(t, server.Config{Metadata: spy})))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.GetUtxoEvents(ctx, &midnight.UtxoEventsRequest{TxCapacity: 10})
	require.NoError(t, err)

	require.Equal(t, 1, spy.readTxnCalls, "GetUtxoEvents must open exactly one read transaction")
	require.Len(t, spy.seenTxns, 4, "all four Find*From calls must have run")
	for i, txn := range spy.seenTxns {
		require.NotNil(t, txn, "call %d must receive the shared transaction, not nil", i)
		require.True(t, txn == spy.seenTxns[0], "call %d must receive the SAME transaction object as the others", i)
	}
}
