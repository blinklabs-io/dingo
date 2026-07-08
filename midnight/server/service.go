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
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/midnight"
	midnightindexer "github.com/blinklabs-io/dingo/midnight/indexer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The service struct is declared in server.go, where its db/slotTimer fields
// (used by the handlers below) sit alongside the metadata/blockNumberByHash
// fields used by the UTxO-event query handlers in midnight_state.go.

// GetTechnicalCommitteeDatum returns the newest Technical Committee datum at
// or before the requested block number.
func (s *service) GetTechnicalCommitteeDatum(
	_ context.Context,
	req *midnight.TechnicalCommitteeDatumRequest,
) (*midnight.TechnicalCommitteeDatumResponse, error) {
	datum, err := s.db.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeTechnicalCommittee,
		req.GetBlockNumber(),
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"get technical committee datum: %v",
			err,
		)
	}
	if datum == nil {
		return nil, status.Error(
			codes.NotFound,
			"no technical committee datum at or before the requested block",
		)
	}
	return &midnight.TechnicalCommitteeDatumResponse{
		SourceBlockNumber: datum.BlockNumber,
		Datum:             datum.Datum,
	}, nil
}

// GetCouncilDatum returns the newest Council datum at or before the
// requested block number.
func (s *service) GetCouncilDatum(
	_ context.Context,
	req *midnight.CouncilDatumRequest,
) (*midnight.CouncilDatumResponse, error) {
	datum, err := s.db.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeCouncil,
		req.GetBlockNumber(),
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"get council datum: %v",
			err,
		)
	}
	if datum == nil {
		return nil, status.Error(
			codes.NotFound,
			"no council datum at or before the requested block",
		)
	}
	return &midnight.CouncilDatumResponse{
		SourceBlockNumber: datum.BlockNumber,
		Datum:             datum.Datum,
	}, nil
}

// GetAriadneParameters returns the newest Ariadne parameters at or before
// the requested epoch.
func (s *service) GetAriadneParameters(
	_ context.Context,
	req *midnight.AriadneParametersRequest,
) (*midnight.AriadneParametersResponse, error) {
	params, err := s.db.GetMidnightAriadneParamsAtOrBeforeEpoch(req.GetEpoch())
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"get ariadne parameters: %v",
			err,
		)
	}
	if params == nil {
		return nil, status.Error(
			codes.NotFound,
			"no ariadne parameters at or before the requested epoch",
		)
	}
	return &midnight.AriadneParametersResponse{
		SourceEpoch: params.Epoch,
		Datum:       params.Datum,
	}, nil
}

// GetEpochNonce returns the stored epoch nonce for the requested epoch.
func (s *service) GetEpochNonce(
	_ context.Context,
	req *midnight.EpochNonceRequest,
) (*midnight.EpochNonceResponse, error) {
	epoch, err := s.db.GetEpoch(req.GetEpoch(), nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get epoch: %v", err)
	}
	if epoch == nil {
		return nil, status.Error(codes.NotFound, "epoch not found")
	}
	return &midnight.EpochNonceResponse{Nonce: epoch.Nonce}, nil
}

// GetEpochCandidates returns the committee-candidate snapshot for the
// requested epoch along with the pool stake distribution captured at the
// same epoch boundary.
func (s *service) GetEpochCandidates(
	_ context.Context,
	req *midnight.EpochCandidatesRequest,
) (*midnight.EpochCandidatesResponse, error) {
	epoch := req.GetEpoch()
	snapshot, err := s.db.GetMidnightEpochCandidatesByEpoch(epoch)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"get epoch candidates: %v",
			err,
		)
	}
	if snapshot == nil {
		return nil, status.Error(
			codes.NotFound,
			"no candidate snapshot for the requested epoch",
		)
	}
	entries, err := midnightindexer.DecodeEpochCandidatesCbor(
		snapshot.CandidatesCbor,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"decode candidate snapshot: %v",
			err,
		)
	}
	candidates := make([]*midnight.EpochCandidate, len(entries))
	for i, entry := range entries {
		candidates[i] = &midnight.EpochCandidate{
			FullDatum:   entry.Datum,
			UtxoTxHash:  entry.TxHash,
			UtxoIndex:   entry.OutputIndex,
			EpochNumber: epoch,
			BlockNumber: snapshot.BlockNumber,
		}
	}

	stakeSnapshots, err := s.db.GetPoolStakeSnapshotsByEpoch(
		epoch,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"get pool stake snapshots: %v",
			err,
		)
	}
	// GetPoolStakeSnapshotsByEpoch does not guarantee row order; sort by pool
	// key hash so the response is deterministic across calls and backends.
	sort.Slice(stakeSnapshots, func(i, j int) bool {
		return string(stakeSnapshots[i].PoolKeyHash) <
			string(stakeSnapshots[j].PoolKeyHash)
	})
	stakeDistribution := make([]*midnight.StakePoolEntry, len(stakeSnapshots))
	for i, snap := range stakeSnapshots {
		stakeDistribution[i] = &midnight.StakePoolEntry{
			PoolHash: snap.PoolKeyHash,
			Stake:    uint64(snap.TotalStake),
		}
	}

	return &midnight.EpochCandidatesResponse{
		Candidates:        candidates,
		StakeDistribution: stakeDistribution,
	}, nil
}

// GetBlockByHash returns metadata for the block with the requested hash.
func (s *service) GetBlockByHash(
	_ context.Context,
	req *midnight.BlockByHashRequest,
) (*midnight.BlockByHashResponse, error) {
	blk, err := database.BlockByHash(s.db, req.GetBlockHash())
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return nil, status.Errorf(codes.Internal, "get block by hash: %v", err)
	}
	decoded, err := blk.Decode()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode block: %v", err)
	}
	epoch, err := s.epochForSlot(blk.Slot)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	ts, err := s.slotTimer.SlotToTime(blk.Slot)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"resolve block timestamp: %v",
			err,
		)
	}
	return &midnight.BlockByHashResponse{
		BlockNumber:        checkedUint32(blk.Number),
		TxCount:            checkedUint32(uint64(len(decoded.Transactions()))),
		BlockTimestampUnix: ts.Unix(),
		EpochNumber:        checkedUint32(epoch.EpochId),
		SlotNumber:         blk.Slot,
	}, nil
}

// GetLatestBlock returns the current chain tip.
func (s *service) GetLatestBlock(
	_ context.Context,
	_ *midnight.LatestBlockRequest,
) (*midnight.LatestBlockResponse, error) {
	blocks, err := database.BlocksRecent(s.db, 1)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get latest block: %v", err)
	}
	if len(blocks) == 0 {
		return nil, status.Error(codes.NotFound, "no blocks in chain yet")
	}
	pb, err := s.buildBlock(blocks[0])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &midnight.LatestBlockResponse{Block: pb}, nil
}

// GetStableBlock returns the requested block, or an empty response when the
// block is not yet at or beyond the requested stability offset behind the
// chain tip (or the tip as of AsOfTimestampUnixMillis, when set).
func (s *service) GetStableBlock(
	_ context.Context,
	req *midnight.StableBlockRequest,
) (*midnight.StableBlockResponse, error) {
	blk, err := database.BlockByHash(s.db, req.GetBlockHash())
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return nil, status.Errorf(codes.Internal, "get block by hash: %v", err)
	}
	tip, err := s.resolveTipBlock(req.GetAsOfTimestampUnixMillis())
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return &midnight.StableBlockResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "resolve chain tip: %v", err)
	}
	if tip.Number < blk.Number ||
		tip.Number-blk.Number < uint64(req.GetStabilityOffset()) {
		return &midnight.StableBlockResponse{}, nil
	}
	pb, err := s.buildBlock(blk)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &midnight.StableBlockResponse{Block: pb}, nil
}

// GetLatestStableBlock returns the newest block that is at least
// StabilityOffset blocks behind the chain tip (or the tip as of
// AsOfTimestampUnixMillis, when set), or an empty response when no block is
// stable yet.
func (s *service) GetLatestStableBlock(
	_ context.Context,
	req *midnight.LatestStableBlockRequest,
) (*midnight.LatestStableBlockResponse, error) {
	tip, err := s.resolveTipBlock(req.GetAsOfTimestampUnixMillis())
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return &midnight.LatestStableBlockResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "resolve chain tip: %v", err)
	}
	offset := uint64(req.GetStabilityOffset())
	if tip.Number < offset {
		return &midnight.LatestStableBlockResponse{}, nil
	}
	// BlockByIndex looks up by the blob store's internal sequential index,
	// which is 1-based (database.BlockInitialIndex) while Cardano block
	// numbers are 0-based; translate as api/blockfrost's block-by-height
	// lookup does.
	blk, err := s.db.BlockByIndex(
		tip.Number-offset+database.BlockInitialIndex,
		nil,
	)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return &midnight.LatestStableBlockResponse{}, nil
		}
		return nil, status.Errorf(
			codes.Internal,
			"get block by number: %v",
			err,
		)
	}
	pb, err := s.buildBlock(blk)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &midnight.LatestStableBlockResponse{Block: pb}, nil
}

// resolveTipBlock returns the current chain tip, or, when asOfMillis is
// non-zero, the latest block at or before that wall-clock time.
func (s *service) resolveTipBlock(asOfMillis uint64) (models.Block, error) {
	if asOfMillis == 0 {
		blocks, err := database.BlocksRecent(s.db, 1)
		if err != nil {
			return models.Block{}, err
		}
		if len(blocks) == 0 {
			return models.Block{}, models.ErrBlockNotFound
		}
		return blocks[0], nil
	}
	// #nosec G115 -- millisecond unix timestamps fit in int64 until year 292277026596
	asOf := time.UnixMilli(int64(asOfMillis))
	slot, err := s.slotTimer.TimeToSlot(asOf)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve slot for as-of timestamp: %w",
			err,
		)
	}
	return database.BlockBeforeSlot(s.db, slot+1)
}

// buildBlock converts a stored block into the shared MidnightState Block
// message, resolving its epoch number and wall-clock timestamp.
func (s *service) buildBlock(blk models.Block) (*midnight.Block, error) {
	epoch, err := s.epochForSlot(blk.Slot)
	if err != nil {
		return nil, err
	}
	ts, err := s.slotTimer.SlotToTime(blk.Slot)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve timestamp for slot %d: %w",
			blk.Slot,
			err,
		)
	}
	return &midnight.Block{
		BlockNumber:        checkedUint32(blk.Number),
		BlockHash:          blk.Hash,
		EpochNumber:        checkedUint32(epoch.EpochId),
		SlotNumber:         blk.Slot,
		BlockTimestampUnix: checkedUint64(ts.Unix()),
	}, nil
}

// epochForSlot resolves the epoch containing slot from the stored epoch
// cache.
func (s *service) epochForSlot(slot uint64) (*models.Epoch, error) {
	epoch, err := s.db.GetEpochBySlot(slot, nil)
	if err != nil {
		return nil, fmt.Errorf("resolve epoch for slot %d: %w", slot, err)
	}
	if epoch == nil {
		return nil, fmt.Errorf("no epoch known for slot %d", slot)
	}
	return epoch, nil
}

// checkedUint32 truncates a uint64 to uint32. Block, epoch, and transaction
// counts stay well under 2^32 for any real Cardano chain.
func checkedUint32(v uint64) uint32 {
	return uint32(v) // #nosec G115
}

// checkedUint64 converts a unix-seconds timestamp (always non-negative for
// any post-genesis block) to uint64.
func checkedUint64(v int64) uint64 {
	return uint64(v) // #nosec G115
}
