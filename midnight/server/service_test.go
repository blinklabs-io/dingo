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
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/midnight"
	"github.com/blinklabs-io/dingo/midnight/server"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeSlotTimer is a linear slot/time mapping used so tests don't depend on
// real Cardano genesis configuration. Slot 0 maps to fakeGenesis; each slot
// is one second.
type fakeSlotTimer struct{}

const fakeGenesisUnix = int64(1_600_000_000)

func (fakeSlotTimer) SlotToTime(slot uint64) (time.Time, error) {
	return time.Unix(fakeGenesisUnix, 0).Add(time.Duration(slot) * time.Second), nil
}

func (fakeSlotTimer) TimeToSlot(t time.Time) (uint64, error) {
	delta := max(t.Unix()-fakeGenesisUnix, 0)
	return uint64(delta), nil
}

// newTestDatabase returns an in-memory Database with a wide-open epoch row
// (epoch 0 covers every slot used by these tests) plus fakeSlotTimer as the
// server's SlotTimer.
func newTestDatabase(t *testing.T) *database.Database {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.SetEpoch(
		0, 0,
		[]byte("epoch-0-nonce"), nil, nil, nil,
		0, 1000, math.MaxInt32,
		nil,
	))
	return db
}

// insertPlaceholderBlock inserts a block whose CBOR is never decoded by the
// handler under test (GetLatestBlock/GetStableBlock/GetLatestStableBlock
// only read Number/Hash/Slot). ID is set explicitly to Number +
// database.BlockInitialIndex, the same invariant a real contiguously-synced
// chain maintains, so BlockByIndex-based lookups (GetLatestStableBlock) work
// without inserting every intermediate block number.
func insertPlaceholderBlock(
	t *testing.T,
	db *database.Database,
	number, slot uint64,
	hashByte byte,
) models.Block {
	t.Helper()
	hash := make([]byte, 32)
	hash[0] = hashByte
	blk := models.Block{
		ID:     number + database.BlockInitialIndex,
		Number: number,
		Slot:   slot,
		Hash:   hash,
		Cbor:   []byte{0x01},
		Type:   1,
	}
	require.NoError(t, db.BlockCreate(blk, nil))
	return blk
}

// loadRealTestBlock loads one decodable block from the shared conformance
// fixtures, for the one test (GetBlockByHash) that needs a real TxCount.
func loadRealTestBlock(t *testing.T) models.Block {
	t.Helper()
	imm, err := immutable.New("../../database/immutable/testdata")
	require.NoError(t, err)
	it, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer it.Close()
	b, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, b)
	lb, err := ledger.NewBlockFromCbor(b.Type, b.Cbor)
	require.NoError(t, err)
	return models.Block{
		Hash:     lb.Hash().Bytes(),
		PrevHash: lb.PrevHash().Bytes(),
		Cbor:     b.Cbor,
		Slot:     lb.SlotNumber(),
		Number:   lb.BlockNumber(),
		Type:     uint(lb.Type()),
	}
}

func dialClient(t *testing.T, addr string) midnight.MidnightStateClient {
	t.Helper()
	return midnight.NewMidnightStateClient(dial(t, addr))
}

func callCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestGetTechnicalCommitteeDatum_AtOrBefore seeds three historical rows and
// verifies the "latest at or before the requested block" query picks the
// correct row, including the not-found case before the first row.
func TestGetTechnicalCommitteeDatum_AtOrBefore(t *testing.T) {
	db := newTestDatabase(t)
	for i, blockNumber := range []uint64{10, 20, 30} {
		require.NoError(t, db.InsertMidnightGovernanceDatum(&models.MidnightGovernanceDatum{
			DatumType:   models.MidnightGovernanceDatumTypeTechnicalCommittee,
			TxHash:      []byte{byte(i), 1, 2, 3},
			OutputIndex: 0,
			Datum:       []byte{byte('a' + i)},
			BlockNumber: blockNumber,
		}))
	}
	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	_, err := client.GetTechnicalCommitteeDatum(callCtx(t), &midnight.TechnicalCommitteeDatumRequest{BlockNumber: 5})
	require.Equal(t, codes.NotFound, status.Code(err), "before the first row must be NotFound")

	resp, err := client.GetTechnicalCommitteeDatum(callCtx(t), &midnight.TechnicalCommitteeDatumRequest{BlockNumber: 25})
	require.NoError(t, err)
	require.Equal(t, uint64(20), resp.GetSourceBlockNumber())
	require.Equal(t, []byte{'b'}, resp.GetDatum())

	resp, err = client.GetTechnicalCommitteeDatum(callCtx(t), &midnight.TechnicalCommitteeDatumRequest{BlockNumber: 1000})
	require.NoError(t, err)
	require.Equal(t, uint64(30), resp.GetSourceBlockNumber())
	require.Equal(t, []byte{'c'}, resp.GetDatum())
}

// TestGetCouncilDatum_DistinctFromTechnicalCommittee verifies the two
// governance datum RPCs are scoped by datum_type and don't cross-contaminate.
func TestGetCouncilDatum_DistinctFromTechnicalCommittee(t *testing.T) {
	db := newTestDatabase(t)
	require.NoError(t, db.InsertMidnightGovernanceDatum(&models.MidnightGovernanceDatum{
		DatumType:   models.MidnightGovernanceDatumTypeTechnicalCommittee,
		TxHash:      []byte{1, 2, 3, 4},
		OutputIndex: 0,
		Datum:       []byte("tc"),
		BlockNumber: 10,
	}))
	require.NoError(t, db.InsertMidnightGovernanceDatum(&models.MidnightGovernanceDatum{
		DatumType:   models.MidnightGovernanceDatumTypeCouncil,
		TxHash:      []byte{5, 6, 7, 8},
		OutputIndex: 0,
		Datum:       []byte("council"),
		BlockNumber: 10,
	}))
	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	resp, err := client.GetCouncilDatum(callCtx(t), &midnight.CouncilDatumRequest{BlockNumber: 100})
	require.NoError(t, err)
	require.Equal(t, []byte("council"), resp.GetDatum())
}

// TestGetAriadneParameters_AtOrBefore mirrors the governance-datum at-or-
// before semantics but against midnight_ariadne_params, keyed by epoch.
func TestGetAriadneParameters_AtOrBefore(t *testing.T) {
	db := newTestDatabase(t)
	for i, epoch := range []uint64{1, 2, 3} {
		require.NoError(t, db.UpsertMidnightAriadneParams(&models.MidnightAriadneParams{
			Epoch: epoch,
			Datum: []byte{byte('a' + i)},
		}))
	}
	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	_, err := client.GetAriadneParameters(callCtx(t), &midnight.AriadneParametersRequest{Epoch: 0})
	require.Equal(t, codes.NotFound, status.Code(err))

	resp, err := client.GetAriadneParameters(callCtx(t), &midnight.AriadneParametersRequest{Epoch: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.GetSourceEpoch())
	require.Equal(t, []byte{'b'}, resp.GetDatum())

	resp, err = client.GetAriadneParameters(callCtx(t), &midnight.AriadneParametersRequest{Epoch: 50})
	require.NoError(t, err)
	require.Equal(t, uint64(3), resp.GetSourceEpoch())
}

func TestGetEpochNonce(t *testing.T) {
	db := newTestDatabase(t)
	require.NoError(t, db.SetEpoch(100, 1, []byte("epoch-1-nonce"), nil, nil, nil, 0, 1000, 100, nil))
	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	resp, err := client.GetEpochNonce(callCtx(t), &midnight.EpochNonceRequest{Epoch: 1})
	require.NoError(t, err)
	require.Equal(t, []byte("epoch-1-nonce"), resp.GetNonce())

	_, err = client.GetEpochNonce(callCtx(t), &midnight.EpochNonceRequest{Epoch: 999})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// candidateEntryCbor mirrors midnight/indexer.CandidateEntry's CBOR shape so
// this test can seed a snapshot without importing the indexer package.
type candidateEntryCbor struct {
	TxHash      []byte `cbor:"1,keyasint"`
	OutputIndex uint32 `cbor:"2,keyasint"`
	Datum       []byte `cbor:"3,keyasint"`
}

func TestGetEpochCandidates_CandidatesAndStakeDistribution(t *testing.T) {
	db := newTestDatabase(t)
	entries := []candidateEntryCbor{
		{TxHash: []byte{0xAA}, OutputIndex: 0, Datum: []byte("candidate-a")},
		{TxHash: []byte{0xBB}, OutputIndex: 1, Datum: []byte("candidate-b")},
	}
	blob, err := cbor.Marshal(entries)
	require.NoError(t, err)
	require.NoError(t, db.UpsertMidnightEpochCandidates(&models.MidnightEpochCandidates{
		Epoch:          7,
		BlockNumber:    700,
		CandidatesCbor: blob,
	}))
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots([]*models.PoolStakeSnapshot{
		{Epoch: 7, SnapshotType: models.PoolStakeSnapshotTypeMark, PoolKeyHash: []byte{0x02}, TotalStake: 200, DelegatorCount: 1, CapturedSlot: 1},
		{Epoch: 7, SnapshotType: models.PoolStakeSnapshotTypeMark, PoolKeyHash: []byte{0x01}, TotalStake: 100, DelegatorCount: 1, CapturedSlot: 1},
	}, nil))

	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	resp, err := client.GetEpochCandidates(callCtx(t), &midnight.EpochCandidatesRequest{Epoch: 7})
	require.NoError(t, err)
	require.Len(t, resp.GetCandidates(), 2)
	require.Equal(t, []byte("candidate-a"), resp.GetCandidates()[0].GetFullDatum())
	require.Equal(t, uint64(700), resp.GetCandidates()[0].GetBlockNumber())
	require.Equal(t, uint64(7), resp.GetCandidates()[0].GetEpochNumber())

	require.Len(t, resp.GetStakeDistribution(), 2)
	// Sorted by pool key hash ascending: 0x01 before 0x02.
	require.Equal(t, []byte{0x01}, resp.GetStakeDistribution()[0].GetPoolHash())
	require.Equal(t, uint64(100), resp.GetStakeDistribution()[0].GetStake())
	require.Equal(t, []byte{0x02}, resp.GetStakeDistribution()[1].GetPoolHash())

	_, err = client.GetEpochCandidates(callCtx(t), &midnight.EpochCandidatesRequest{Epoch: 999})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetLatestBlock(t *testing.T) {
	db := newTestDatabase(t)
	insertPlaceholderBlock(t, db, 1, 1, 0x01)
	tip := insertPlaceholderBlock(t, db, 2, 2, 0x02)

	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	resp, err := client.GetLatestBlock(callCtx(t), &midnight.LatestBlockRequest{})
	require.NoError(t, err)
	require.Equal(t, uint32(tip.Number), resp.GetBlock().GetBlockNumber())
	require.Equal(t, tip.Hash, resp.GetBlock().GetBlockHash())
	require.Equal(t, tip.Slot, resp.GetBlock().GetSlotNumber())
}

func TestGetBlockByHash(t *testing.T) {
	db := newTestDatabase(t)
	blk := loadRealTestBlock(t)
	require.NoError(t, db.BlockCreate(blk, nil))
	decoded, err := blk.Decode()
	require.NoError(t, err)
	wantTxCount := len(decoded.Transactions())

	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	resp, err := client.GetBlockByHash(callCtx(t), &midnight.BlockByHashRequest{BlockHash: blk.Hash})
	require.NoError(t, err)
	require.Equal(t, uint32(blk.Number), resp.GetBlockNumber())
	require.Equal(t, blk.Slot, resp.GetSlotNumber())
	require.Equal(t, uint32(wantTxCount), resp.GetTxCount())
	require.Equal(t, fakeGenesisUnix+int64(blk.Slot), resp.GetBlockTimestampUnix())
	require.Equal(t, uint32(0), resp.GetEpochNumber())

	_, err = client.GetBlockByHash(callCtx(t), &midnight.BlockByHashRequest{BlockHash: []byte("unknown-hash-000000000000000000")})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// praosStabilityOffset returns ceil(3k/f), the Ouroboros Praos randomness/
// settlement stability window, for a given security parameter k and active
// slot coefficient f.
func praosStabilityOffset(k uint64, f float64) uint64 {
	return uint64(math.Ceil(3 * float64(k) / f))
}

// TestStability_MatchesConfiguredKAndF verifies GetStableBlock's boundary
// behaviour (tip - block >= offset) against offsets derived from the 3k/f
// Praos stability-window formula for both a small DevNet-style profile and
// preview testnet's real k/f.
func TestStability_MatchesConfiguredKAndF(t *testing.T) {
	cases := []struct {
		name string
		k    uint64
		f    float64
	}{
		{name: "devnet", k: 10, f: 0.5},
		{name: "preview", k: 432, f: 0.05},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			offset := praosStabilityOffset(tc.k, tc.f)
			db := newTestDatabase(t)
			target := insertPlaceholderBlock(t, db, 1000, 1000, 0xAA)
			tip := insertPlaceholderBlock(t, db, 1000+offset, 1000+offset, 0xBB)

			addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
			client := dialClient(t, addr)

			// Exactly at the offset: stable.
			resp, err := client.GetStableBlock(callCtx(t), &midnight.StableBlockRequest{
				BlockHash:       target.Hash,
				StabilityOffset: uint32(offset), //nolint:gosec // test-only, small values
			})
			require.NoError(t, err)
			require.NotNil(t, resp.GetBlock(), "block exactly %d behind tip must be stable", offset)
			require.Equal(t, uint32(target.Number), resp.GetBlock().GetBlockNumber())

			// One more than the offset: not yet stable.
			resp, err = client.GetStableBlock(callCtx(t), &midnight.StableBlockRequest{
				BlockHash:       target.Hash,
				StabilityOffset: uint32(offset + 1), //nolint:gosec // test-only, small values
			})
			require.NoError(t, err)
			require.Nil(t, resp.GetBlock(), "block one short of the offset must not be stable")

			// GetLatestStableBlock at the same offset must resolve back to target.
			latestResp, err := client.GetLatestStableBlock(callCtx(t), &midnight.LatestStableBlockRequest{
				StabilityOffset: uint32(offset), //nolint:gosec // test-only, small values
			})
			require.NoError(t, err)
			require.NotNil(t, latestResp.GetBlock())
			require.Equal(t, uint32(target.Number), latestResp.GetBlock().GetBlockNumber())

			// No block is stable yet when the offset exceeds the whole chain height.
			latestResp, err = client.GetLatestStableBlock(callCtx(t), &midnight.LatestStableBlockRequest{
				StabilityOffset: uint32(tip.Number + 1), //nolint:gosec // test-only, small values
			})
			require.NoError(t, err)
			require.Nil(t, latestResp.GetBlock())
		})
	}
}

// TestGetStableBlock_AsOfTimestamp verifies that AsOfTimestampUnixMillis, when
// set, resolves the "chain tip" to the latest block at or before that
// wall-clock time rather than the live tip.
func TestGetStableBlock_AsOfTimestamp(t *testing.T) {
	db := newTestDatabase(t)
	older := insertPlaceholderBlock(t, db, 10, 10, 0x01)
	insertPlaceholderBlock(t, db, 20, 20, 0x02) // newer than the as-of time

	addr := startTestServerWithConfig(t, server.Config{Database: server.NewDatabase(db), SlotTimer: fakeSlotTimer{}})
	client := dialClient(t, addr)

	asOfMillis := uint64(time.Unix(fakeGenesisUnix+15, 0).UnixMilli())
	resp, err := client.GetStableBlock(callCtx(t), &midnight.StableBlockRequest{
		BlockHash:               older.Hash,
		StabilityOffset:         0,
		AsOfTimestampUnixMillis: asOfMillis,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetBlock(), "older block must be stable relative to the as-of tip")
	require.Equal(t, uint32(older.Number), resp.GetBlock().GetBlockNumber())
}
