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

//nolint:gosec // SQL INTEGER mappings preserve the existing unsigned domain API.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) SaveRewardAdaPots(
	pots *models.RewardAdaPots,
	txn types.Txn,
) error {
	if pots == nil {
		return errors.New("save reward ADA pots: pots are nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	epoch, err := checkedInt64(pots.Epoch)
	if err != nil {
		return err
	}
	capturedSlot, err := checkedInt64(pots.CapturedSlot)
	if err != nil {
		return err
	}
	id, err := queries.SaveRewardAdaPots(
		ctx,
		sqlitequery.SaveRewardAdaPotsParams{
			Epoch:        epoch,
			Treasury:     decimalUint64(pots.Treasury),
			Reserves:     decimalUint64(pots.Reserves),
			Fees:         decimalUint64(pots.Fees),
			Rewards:      decimalUint64(pots.Rewards),
			CapturedSlot: capturedSlot,
		},
	)
	if err != nil {
		return fmt.Errorf("save reward ADA pots: %w", err)
	}
	pots.ID = uint(id)
	return nil
}

func (s *Store) GetRewardAdaPots(
	epoch uint64,
	txn types.Txn,
) (*models.RewardAdaPots, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetRewardAdaPots(ctx, sqlEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get reward ADA pots: %w", err)
	}
	treasury, err := parseUint64("treasury", row.Treasury)
	if err != nil {
		return nil, err
	}
	reserves, err := parseUint64("reserves", row.Reserves)
	if err != nil {
		return nil, err
	}
	fees, err := parseUint64("fees", row.Fees)
	if err != nil {
		return nil, err
	}
	rewards, err := parseUint64("rewards", row.Rewards)
	if err != nil {
		return nil, err
	}
	return &models.RewardAdaPots{
		ID:           uint(row.ID),
		Epoch:        uint64(row.Epoch),
		Treasury:     types.Uint64(treasury),
		Reserves:     types.Uint64(reserves),
		Fees:         types.Uint64(fees),
		Rewards:      types.Uint64(rewards),
		CapturedSlot: uint64(row.CapturedSlot),
	}, nil
}

func (s *Store) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) error {
	if snapshot == nil {
		return errors.New("save reward snapshot: snapshot is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	params, err := rewardSnapshotParams(snapshot)
	if err != nil {
		return err
	}
	id, err := queries.SaveRewardSnapshot(
		ctx,
		sqlitequery.SaveRewardSnapshotParams(params),
	)
	if err != nil {
		return fmt.Errorf("save reward snapshot: %w", err)
	}
	snapshot.ID = uint(id)
	return nil
}

func (s *Store) SaveRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	reason string,
	capturedSlot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	sqlSlot, err := checkedInt64(capturedSlot)
	if err != nil {
		return err
	}
	if err := s.operationalQueries(db).SaveRewardSeedFailure(
		ctx,
		sqlitequery.SaveRewardSeedFailureParams{
			Epoch:         sqlEpoch,
			SnapshotType:  snapshotType,
			FailureReason: reason,
			CapturedSlot:  sqlSlot,
		},
	); err != nil {
		return fmt.Errorf("save reward seed failure: %w", err)
	}
	return nil
}

// SaveImportedPoolBlockCounts records the per-pool block counts a bootstrap
// snapshot carries for one epoch. The rows are the node's only source of pool
// performance for an epoch that ended below its trust anchor.
func (s *Store) SaveImportedPoolBlockCounts(
	counts []models.ImportedPoolBlockCount,
	txn types.Txn,
) error {
	if len(counts) == 0 {
		return nil
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	for _, count := range counts {
		sqlEpoch, err := checkedInt64(count.Epoch)
		if err != nil {
			return err
		}
		sqlBlocks, err := checkedInt64(count.BlocksProduced)
		if err != nil {
			return err
		}
		sqlSlot, err := checkedInt64(count.CapturedSlot)
		if err != nil {
			return err
		}
		if err := queries.SaveImportedPoolBlockCount(
			ctx,
			sqlitequery.SaveImportedPoolBlockCountParams{
				Epoch:          sqlEpoch,
				PoolKeyHash:    count.PoolKeyHash,
				BlocksProduced: sqlBlocks,
				CapturedSlot:   sqlSlot,
			},
		); err != nil {
			return fmt.Errorf("save imported pool block count: %w", err)
		}
	}
	return nil
}

// SaveImportedEpochBlockTotal records that an epoch's block counts came from a
// bootstrap snapshot, and the total the per-pool rows sum to. The row is what
// distinguishes a certified zero-block epoch from an epoch nothing was
// imported for; the per-pool rows alone cannot, because a BlocksMade map with
// no entries writes none.
func (s *Store) SaveImportedEpochBlockTotal(
	epoch uint64,
	totalBlocks uint64,
	capturedSlot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	sqlTotal, err := checkedInt64(totalBlocks)
	if err != nil {
		return err
	}
	sqlSlot, err := checkedInt64(capturedSlot)
	if err != nil {
		return err
	}
	if err := s.operationalQueries(db).SaveImportedEpochBlockTotal(
		ctx,
		sqlitequery.SaveImportedEpochBlockTotalParams{
			Epoch:        sqlEpoch,
			TotalBlocks:  sqlTotal,
			CapturedSlot: sqlSlot,
		},
	); err != nil {
		return fmt.Errorf("save imported epoch block total: %w", err)
	}
	return nil
}

// GetImportedPoolBlockCounts returns an epoch's imported per-pool block counts
// keyed by pool key hash, and the epoch total they sum to. The bool reports
// whether block counts were imported for the epoch at all; false means the
// counts are unknown, which is not the same answer as a zero-block epoch.
//
// The stored total is compared against the rows rather than derived from them.
// A per-pool set truncated by a partial write would otherwise present as a
// smaller but self-consistent epoch, which raises every surviving pool's share
// of the blocks and over-credits its rewards.
func (s *Store) GetImportedPoolBlockCounts(
	epoch uint64,
	txn types.Txn,
) (map[string]uint64, uint64, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, false, err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, 0, false, err
	}
	queries := s.operationalQueries(db)
	storedTotal, err := queries.GetImportedEpochBlockTotal(ctx, sqlEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, fmt.Errorf(
			"get imported epoch block total: %w",
			err,
		)
	}
	if storedTotal < 0 {
		return nil, 0, false, fmt.Errorf(
			"imported block total for epoch %d is negative: %d",
			epoch,
			storedTotal,
		)
	}
	rows, err := queries.GetImportedPoolBlockCounts(ctx, sqlEpoch)
	if err != nil {
		return nil, 0, false, fmt.Errorf(
			"get imported pool block counts: %w",
			err,
		)
	}
	ret := make(map[string]uint64, len(rows))
	var rowTotal uint64
	for _, row := range rows {
		if row.BlocksProduced < 0 {
			return nil, 0, false, fmt.Errorf(
				"imported pool block count for epoch %d pool %x is negative: %d",
				epoch,
				row.PoolKeyHash,
				row.BlocksProduced,
			)
		}
		ret[string(row.PoolKeyHash)] = uint64(row.BlocksProduced)
		rowTotal += uint64(row.BlocksProduced)
	}
	if rowTotal != uint64(storedTotal) {
		return nil, 0, false, fmt.Errorf(
			"imported pool block counts for epoch %d sum to %d, recorded total is %d",
			epoch,
			rowTotal,
			storedTotal,
		)
	}
	return ret, uint64(storedTotal), true, nil
}

// DeleteImportedPoolBlockCountsForEpoch removes an epoch's imported counts, so
// a re-import replaces them rather than merging into a stale set.
func (s *Store) DeleteImportedPoolBlockCountsForEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	if err := queries.DeleteImportedPoolBlockCountsForEpoch(
		ctx,
		sqlEpoch,
	); err != nil {
		return fmt.Errorf("delete imported pool block counts: %w", err)
	}
	if err := queries.DeleteImportedEpochBlockTotalForEpoch(
		ctx,
		sqlEpoch,
	); err != nil {
		return fmt.Errorf("delete imported epoch block total: %w", err)
	}
	return nil
}

func (s *Store) GetRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (string, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return "", err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return "", err
	}
	reason, err := s.operationalQueries(db).GetRewardSeedFailure(
		ctx,
		sqlitequery.GetRewardSeedFailureParams{
			Epoch:        sqlEpoch,
			SnapshotType: snapshotType,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get reward seed failure: %w", err)
	}
	return reason, nil
}

func (s *Store) DeleteRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	if err := s.operationalQueries(db).DeleteRewardSeedFailure(
		ctx,
		sqlitequery.DeleteRewardSeedFailureParams{
			Epoch:        sqlEpoch,
			SnapshotType: snapshotType,
		},
	); err != nil {
		return fmt.Errorf("delete reward seed failure: %w", err)
	}
	return nil
}

func (s *Store) DeleteProvisionalRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) error {
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	err = s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			return s.operationalQueries(db).DeleteProvisionalRewardSnapshot(
				ctx,
				sqlitequery.DeleteProvisionalRewardSnapshotParams{
					Epoch:        sqlEpoch,
					SnapshotType: snapshotType,
				},
			)
		},
	)
	if err != nil {
		return fmt.Errorf("delete provisional reward snapshot: %w", err)
	}
	return nil
}

func (s *Store) ClaimFallbackRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) (bool, error) {
	if snapshot == nil {
		return false, errors.New(
			"claim fallback reward snapshot: snapshot is nil",
		)
	}
	snapshot.Authoritative = false
	params, err := rewardSnapshotParams(snapshot)
	if err != nil {
		return false, err
	}
	proceed := false
	err = s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			queries := s.operationalQueries(db)
			id, err := queries.InsertRewardSnapshot(
				ctx,
				sqlitequery.InsertRewardSnapshotParams(params),
			)
			if err == nil {
				snapshot.ID = uint(id)
				proceed = true
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			existing, err := queries.GetRewardSnapshot(
				ctx,
				sqlitequery.GetRewardSnapshotParams{
					Epoch:        params.Epoch,
					SnapshotType: params.SnapshotType,
				},
			)
			if err != nil {
				return err
			}
			if existing.Authoritative {
				return nil
			}
			updated, err := queries.UpdateFallbackRewardSnapshot(
				ctx,
				sqlitequery.UpdateFallbackRewardSnapshotParams{
					TotalActiveStake:   params.TotalActiveStake,
					TotalPoolCount:     params.TotalPoolCount,
					TotalDelegators:    params.TotalDelegators,
					CapturedSlot:       params.CapturedSlot,
					BoundarySlot:       params.BoundarySlot,
					EpochNonce:         params.EpochNonce,
					ProtocolVersion:    params.ProtocolVersion,
					CalculationVersion: params.CalculationVersion,
					Epoch:              params.Epoch,
					SnapshotType:       params.SnapshotType,
				},
			)
			if err == nil && updated == 1 {
				snapshot.ID = uint(existing.ID)
				proceed = true
			}
			return err
		},
	)
	if err != nil {
		return false, fmt.Errorf("claim fallback reward snapshot: %w", err)
	}
	return proceed, nil
}

func (s *Store) ClaimFallbackRewardSnapshotGuard(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (bool, uint, error) {
	if txn == nil {
		return false, 0, errors.New(
			"ClaimFallbackRewardSnapshotGuard: transaction is required",
		)
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return false, 0, err
	}
	queries := s.operationalQueries(db)
	guard := &models.RewardSnapshot{
		Epoch:        epoch,
		SnapshotType: snapshotType,
	}
	params, err := rewardSnapshotParams(guard)
	if err != nil {
		return false, 0, err
	}
	id, err := queries.InsertRewardSnapshot(
		ctx,
		sqlitequery.InsertRewardSnapshotParams(params),
	)
	if err == nil {
		return true, uint(id), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, 0, fmt.Errorf(
			"claim fallback reward snapshot guard: %w",
			err,
		)
	}
	existing, err := queries.GetRewardSnapshot(
		ctx,
		sqlitequery.GetRewardSnapshotParams{
			Epoch:        params.Epoch,
			SnapshotType: params.SnapshotType,
		},
	)
	if err != nil {
		return false, 0, fmt.Errorf(
			"claim fallback reward snapshot guard: %w",
			err,
		)
	}
	if existing.Authoritative {
		return false, 0, nil
	}
	return true, 0, nil
}

func (s *Store) ReleaseFallbackRewardSnapshotGuard(
	guardID uint,
	txn types.Txn,
) error {
	if txn == nil {
		return errors.New(
			"ReleaseFallbackRewardSnapshotGuard: transaction is required",
		)
	}
	if guardID == 0 {
		return nil
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	rows, err := queries.ReleaseFallbackRewardSnapshotGuard(
		ctx,
		int64(guardID),
	)
	if err != nil {
		return fmt.Errorf("release fallback reward snapshot guard: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf(
			"release fallback reward snapshot guard: expected 1 row, deleted %d",
			rows,
		)
	}
	return nil
}

func (s *Store) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (*models.RewardSnapshot, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetRewardSnapshot(
		ctx,
		sqlitequery.GetRewardSnapshotParams{
			Epoch:        sqlEpoch,
			SnapshotType: snapshotType,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get reward snapshot: %w", err)
	}
	return rewardSnapshotFromSQLite(row)
}

func (s *Store) SaveRewardPoolInputs(
	inputs []*models.RewardPoolInput,
	txn types.Txn,
) error {
	return s.saveRewardRows(
		"pool inputs",
		len(inputs),
		txn,
		func(queries *sqlitequery.Queries, ctx context.Context, index int) error {
			input := inputs[index]
			if input == nil {
				return errors.New("input is nil")
			}
			params, err := rewardPoolInputParams(input)
			if err != nil {
				return err
			}
			id, err := queries.SaveRewardPoolInput(
				ctx,
				params,
			)
			if err != nil {
				return err
			}
			input.ID = uint(id)
			return nil
		},
	)
}

func (s *Store) GetRewardPoolInputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardPoolInput, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetRewardPoolInputs(ctx, sqlEpoch)
	if err != nil {
		return nil, fmt.Errorf("get reward pool inputs: %w", err)
	}
	ret := make([]*models.RewardPoolInput, 0, len(rows))
	for _, row := range rows {
		input, err := rewardPoolInputFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, input)
	}
	return ret, nil
}

func (s *Store) SaveRewardStakeInputs(
	inputs []*models.RewardStakeInput,
	txn types.Txn,
) error {
	return s.saveRewardRows(
		"stake inputs",
		len(inputs),
		txn,
		func(queries *sqlitequery.Queries, ctx context.Context, index int) error {
			input := inputs[index]
			if input == nil {
				return errors.New("input is nil")
			}
			params, err := rewardStakeInputParams(input)
			if err != nil {
				return err
			}
			id, err := queries.SaveRewardStakeInput(
				ctx,
				params,
			)
			if err != nil {
				return err
			}
			input.ID = uint(id)
			return nil
		},
	)
}

func (s *Store) GetRewardStakeInputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetRewardStakeInputs(ctx, sqlEpoch)
	if err != nil {
		return nil, fmt.Errorf("get reward stake inputs: %w", err)
	}
	ret := make([]*models.RewardStakeInput, 0, len(rows))
	for _, row := range rows {
		input, err := rewardStakeInputFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, input)
	}
	return ret, nil
}

func (s *Store) DeleteRewardInputsForEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteRewardPair(
		"inputs for epoch",
		epoch,
		txn,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			if err := q.DeleteRewardPoolInputsForEpoch(
				ctx,
				value,
			); err != nil {
				return err
			}
			return q.DeleteRewardStakeInputsForEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) DeleteRewardOutputsForEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteRewardPair(
		"outputs for epoch",
		epoch,
		txn,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			if err := q.DeleteRewardPoolOutputsForEpoch(
				ctx,
				value,
			); err != nil {
				return err
			}
			return q.DeleteRewardAccountOutputsForEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) SaveRewardPoolOutputs(
	outputs []*models.RewardPoolOutput,
	txn types.Txn,
) error {
	return s.saveRewardRows(
		"pool outputs",
		len(outputs),
		txn,
		func(queries *sqlitequery.Queries, ctx context.Context, index int) error {
			output := outputs[index]
			if output == nil {
				return errors.New("output is nil")
			}
			params, err := rewardPoolOutputParams(output)
			if err != nil {
				return err
			}
			id, err := queries.SaveRewardPoolOutput(
				ctx,
				params,
			)
			if err != nil {
				return err
			}
			output.ID = uint(id)
			return nil
		},
	)
}

func (s *Store) GetRewardPoolOutputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardPoolOutput, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetRewardPoolOutputs(ctx, sqlEpoch)
	if err != nil {
		return nil, fmt.Errorf("get reward pool outputs: %w", err)
	}
	ret := make([]*models.RewardPoolOutput, 0, len(rows))
	for _, row := range rows {
		output, err := rewardPoolOutputFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, output)
	}
	return ret, nil
}

func (s *Store) SaveRewardAccountOutputs(
	outputs []*models.RewardAccountOutput,
	txn types.Txn,
) error {
	if len(outputs) == 0 {
		return nil
	}
	params := make([]sqlitequery.SaveRewardAccountOutputParams, len(outputs))
	for index, output := range outputs {
		if output == nil {
			return errors.New("save reward account outputs: output is nil")
		}
		value, err := rewardAccountOutputParams(output)
		if err != nil {
			return fmt.Errorf("save reward account output %d: %w", index, err)
		}
		params[index] = value
	}
	// PostgreSQL rejects a multi-row upsert when two input rows target the
	// same unique key. Row-at-a-time behavior historically accepted that shape
	// (the last row won), so collapse duplicates while retaining the latest
	// value and restore the resulting ID onto every original model below.
	uniqueParams := make(
		[]sqlitequery.SaveRewardAccountOutputParams,
		0,
		len(params),
	)
	latestIndex := make(map[rewardAccountOutputKey]int, len(params))
	for _, value := range params {
		key := rewardAccountOutputKey{
			value.Epoch,
			value.CredentialTag,
			string(value.StakingKey),
			string(value.PoolKeyHash),
			value.RewardType,
		}
		if index, ok := latestIndex[key]; ok {
			uniqueParams[index] = value
			continue
		}
		latestIndex[key] = len(uniqueParams)
		uniqueParams = append(uniqueParams, value)
	}
	resolvedIDs := make(map[rewardAccountOutputKey]uint, len(uniqueParams))
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			// SQLite's parameter limit is the binding constraint in the default
			// deployment. Keep each statement bounded while reducing the million-row
			// reward import from one round trip per row to one per chunk.
			chunkSize := min(1000, max(1, s.dialect.ParameterLimit()/10))
			for start := 0; start < len(uniqueParams); start += chunkSize {
				end := min(start+chunkSize, len(uniqueParams))
				ids, err := s.saveRewardAccountOutputChunk(
					ctx,
					db,
					uniqueParams[start:end],
				)
				if err != nil {
					return err
				}
				for index, id := range ids {
					value := uniqueParams[start+index]
					resolvedIDs[rewardAccountOutputKey{value.Epoch, value.CredentialTag, string(value.StakingKey), string(value.PoolKeyHash), value.RewardType}] = uint(
						id,
					)
				}
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("save reward account outputs: %w", err)
	}
	for index, value := range params {
		id, ok := resolvedIDs[rewardAccountOutputKey{value.Epoch, value.CredentialTag, string(value.StakingKey), string(value.PoolKeyHash), value.RewardType}]
		if !ok {
			return fmt.Errorf(
				"save reward account outputs: missing ID at index %d",
				index,
			)
		}
		outputs[index].ID = id
	}
	return nil
}

type rewardAccountOutputKey struct {
	epoch         int64
	credentialTag int64
	stakingKey    string
	poolKeyHash   string
	rewardType    string
}

func (s *Store) saveRewardAccountOutputChunk(
	ctx context.Context,
	db queryer,
	params []sqlitequery.SaveRewardAccountOutputParams,
) ([]int64, error) {
	if len(params) == 0 {
		return nil, nil
	}
	const columns = "staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount, spendable, guarded, captured_slot, boundary_slot"
	values := make([]string, len(params))
	args := make([]any, 0, len(params)*10)
	for index, value := range params {
		values[index] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(
			args,
			value.StakingKey,
			value.PoolKeyHash,
			value.RewardType,
			value.Epoch,
			value.CredentialTag,
			value.Amount,
			value.Spendable,
			value.Guarded,
			value.CapturedSlot,
			value.BoundarySlot,
		)
	}
	query := `INSERT INTO reward_account_output (` + columns + `)
VALUES ` + strings.Join(values, ", ") + `
ON CONFLICT (epoch, credential_tag, staking_key, pool_key_hash, reward_type)
DO UPDATE SET amount = excluded.amount, spendable = excluded.spendable,
guarded = excluded.guarded, captured_slot = excluded.captured_slot,
boundary_slot = excluded.boundary_slot`
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("insert reward account output batch: %w", err)
	}

	// Multi-row upserts do not expose all generated IDs portably. Resolve them
	// by their natural key in one bounded query and assign IDs only after the
	// write succeeds.
	//
	// Looked up via a join against a derived table of literal
	// (epoch, credential_tag, staking_key, pool_key_hash, reward_type) rows,
	// not an OR-chain of five-way equality predicates -- the same
	// GetAccountsByCredential planner limitation applies here:
	// idx_reward_account_output_epoch_cred_pool_type exists on exactly these
	// five columns, but a long OR-chain over it is not reliably compiled into
	// per-term index seeks, and this runs on every epoch boundary for every
	// account earning a reward.
	rowSelectTemplate := "SELECT ? AS epoch, ? AS credential_tag, ? AS staking_key, ? AS pool_key_hash, ? AS reward_type"
	if s.dialect.Name() == "postgres" {
		// See GetAccountsByCredential's identical cast for why: an otherwise
		// untyped derived-table parameter resolves to text on Postgres rather
		// than being inferred from the joined columns, which fails the join
		// once compared against epoch/credential_tag (BIGINT) or
		// staking_key/pool_key_hash (BYTEA). reward_type (VARCHAR) needs no
		// cast: Postgres's text default already matches it.
		rowSelectTemplate = "SELECT CAST(? AS BIGINT) AS epoch, CAST(? AS BIGINT) AS credential_tag, CAST(? AS BYTEA) AS staking_key, CAST(? AS BYTEA) AS pool_key_hash, ? AS reward_type"
	}
	rowSelects := make([]string, len(params))
	lookupArgs := make([]any, 0, len(params)*5)
	for index, value := range params {
		rowSelects[index] = rowSelectTemplate
		lookupArgs = append(
			lookupArgs,
			value.Epoch,
			value.CredentialTag,
			value.StakingKey,
			value.PoolKeyHash,
			value.RewardType,
		)
	}
	lookupQuery := s.dialect.Rebind(
		`SELECT o.id, o.epoch, o.credential_tag, o.staking_key, o.pool_key_hash, o.reward_type
FROM reward_account_output o JOIN (` + strings.Join(rowSelects, " UNION ALL ") + `) v
ON o.epoch = v.epoch AND o.credential_tag = v.credential_tag AND
   o.staking_key = v.staking_key AND o.pool_key_hash = v.pool_key_hash AND
   o.reward_type = v.reward_type`,
	)
	rows, err := db.QueryContext(ctx, lookupQuery, lookupArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup reward account output batch IDs: %w",
			err,
		)
	}
	defer rows.Close()
	ids := make(map[rewardAccountOutputKey]int64, len(params))
	for rows.Next() {
		var id, epoch, credentialTag int64
		var stakingKey, poolKeyHash []byte
		var rewardType string
		if err := rows.Scan(&id, &epoch, &credentialTag, &stakingKey, &poolKeyHash, &rewardType); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf(
				"scan reward account output batch IDs: %w",
				err,
			)
		}
		ids[rewardAccountOutputKey{epoch, credentialTag, string(stakingKey), string(poolKeyHash), rewardType}] = id
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf(
			"iterate reward account output batch IDs: %w",
			err,
		)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close reward account output batch IDs: %w", err)
	}
	result := make([]int64, len(params))
	for index, value := range params {
		id, ok := ids[rewardAccountOutputKey{value.Epoch, value.CredentialTag, string(value.StakingKey), string(value.PoolKeyHash), value.RewardType}]
		if !ok {
			return nil, fmt.Errorf(
				"reward account output batch missing key at index %d",
				index,
			)
		}
		result[index] = id
	}
	return result, nil
}

func (s *Store) GetRewardAccountOutputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetRewardAccountOutputs(
		ctx,
		sqlEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("get reward account outputs: %w", err)
	}
	ret := make([]*models.RewardAccountOutput, 0, len(rows))
	for _, row := range rows {
		output, err := rewardAccountOutputFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, output)
	}
	return ret, nil
}

func (s *Store) GetRewardAccountOutputsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	ret := make([]*models.RewardAccountOutput, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query := `
SELECT staking_key, pool_key_hash, reward_type, id, epoch, credential_tag,
       amount, spendable, guarded, captured_slot, boundary_slot
FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ?
  AND spendable = TRUE AND guarded = FALSE`
	if strings.EqualFold(order, "desc") {
		query += " ORDER BY epoch DESC, pool_key_hash ASC, reward_type ASC"
	} else {
		query += " ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC"
	}
	args := []any{credentialTag, stakingKey}
	query, args = addLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward account outputs by credential: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			output       models.RewardAccountOutput
			amount       string
			id           int64
			epoch        int64
			sqlTag       int64
			capturedSlot int64
			boundarySlot int64
		)
		if err := rows.Scan(
			&output.StakingKey,
			&output.PoolKeyHash,
			&output.RewardType,
			&id,
			&epoch,
			&sqlTag,
			&amount,
			&output.Spendable,
			&output.Guarded,
			&capturedSlot,
			&boundarySlot,
		); err != nil {
			return nil, err
		}
		value, err := parseUint64("reward account amount", amount)
		if err != nil {
			return nil, err
		}
		output.ID = uint(id)
		output.Epoch = uint64(epoch)
		output.CredentialTag = uint8(sqlTag)
		output.Amount = types.Uint64(value)
		output.CapturedSlot = uint64(capturedSlot)
		output.BoundarySlot = uint64(boundarySlot)
		ret = append(ret, &output)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) CountRewardAccountOutputsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int
	err = db.QueryRowContext(
		ctx,
		s.dialect.Rebind(`
SELECT COUNT(*)
FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ?
  AND spendable = TRUE AND guarded = FALSE`),
		credentialTag,
		stakingKey,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(
			"count reward account outputs by credential: %w",
			err,
		)
	}
	return count, nil
}

func (s *Store) DeleteRewardStateAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	err = s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			q := s.operationalQueries(db)
			if err := q.DeleteRewardAdaPotsAfterSlot(
				ctx,
				sqlSlot,
			); err != nil {
				return err
			}
			pair := sqlitequery.DeleteRewardSnapshotsAfterSlotParams{
				CapturedSlot: sqlSlot,
				BoundarySlot: sqlSlot,
			}
			if err := q.DeleteRewardSnapshotsAfterSlot(
				ctx,
				pair,
			); err != nil {
				return err
			}
			if err := q.DeleteRewardSeedFailuresAfterSlot(
				ctx,
				sqlSlot,
			); err != nil {
				return err
			}
			if err := q.DeleteImportedPoolBlockCountsAfterSlot(
				ctx,
				sqlSlot,
			); err != nil {
				return err
			}
			if err := q.DeleteImportedEpochBlockTotalsAfterSlot(
				ctx,
				sqlSlot,
			); err != nil {
				return err
			}
			if err := q.DeleteRewardPoolInputsAfterSlot(
				ctx,
				sqlitequery.DeleteRewardPoolInputsAfterSlotParams(pair),
			); err != nil {
				return err
			}
			if err := q.DeleteRewardStakeInputsAfterSlot(
				ctx,
				sqlitequery.DeleteRewardStakeInputsAfterSlotParams(pair),
			); err != nil {
				return err
			}
			if err := q.DeleteRewardPoolOutputsAfterSlot(
				ctx,
				sqlitequery.DeleteRewardPoolOutputsAfterSlotParams(pair),
			); err != nil {
				return err
			}
			return q.DeleteRewardAccountOutputsAfterSlot(
				ctx,
				sqlitequery.DeleteRewardAccountOutputsAfterSlotParams(pair),
			)
		},
	)
	if err != nil {
		return fmt.Errorf("delete reward state after slot: %w", err)
	}
	return nil
}

func (s *Store) DeleteRewardStateBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteRewardPair(
		"state before epoch",
		epoch,
		txn,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			if err := q.DeleteRewardStakeInputsBeforeEpoch(
				ctx,
				value,
			); err != nil {
				return err
			}
			return q.DeleteRewardAccountOutputsBeforeEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) DeleteRewardStakeInputBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteRewardPair(
		"stake input before epoch",
		epoch,
		txn,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteRewardStakeInputsBeforeEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) saveRewardRows(
	description string,
	count int,
	txn types.Txn,
	save func(*sqlitequery.Queries, context.Context, int) error,
) error {
	if count == 0 {
		return nil
	}
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			queries := s.operationalQueries(db)
			for index := range count {
				if err := save(queries, ctx, index); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("save reward %s: %w", description, err)
	}
	return nil
}

func (s *Store) deleteRewardPair(
	description string,
	value uint64,
	txn types.Txn,
	deleteFn func(*sqlitequery.Queries, context.Context, int64) error,
) error {
	sqlValue, err := checkedInt64(value)
	if err != nil {
		return err
	}
	err = s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			queries := s.operationalQueries(db)
			return deleteFn(queries, ctx, sqlValue)
		},
	)
	if err != nil {
		return fmt.Errorf("delete reward %s: %w", description, err)
	}
	return nil
}

type rewardSnapshotQueryParams struct {
	Epoch              int64
	SnapshotType       string
	TotalActiveStake   string
	TotalPoolCount     int64
	TotalDelegators    int64
	CapturedSlot       int64
	BoundarySlot       int64
	EpochNonce         []byte
	ProtocolVersion    int64
	Authoritative      bool
	CalculationVersion int64
}

func rewardSnapshotParams(
	snapshot *models.RewardSnapshot,
) (rewardSnapshotQueryParams, error) {
	epoch, err := checkedInt64(snapshot.Epoch)
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	totalPoolCount, err := checkedInt64(snapshot.TotalPoolCount)
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	totalDelegators, err := checkedInt64(snapshot.TotalDelegators)
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	capturedSlot, err := checkedInt64(snapshot.CapturedSlot)
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	boundarySlot, err := checkedInt64(snapshot.BoundarySlot)
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	protocolVersion, err := checkedInt64(uint64(snapshot.ProtocolVersion))
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	calculationVersion, err := checkedInt64(uint64(snapshot.CalculationVersion))
	if err != nil {
		return rewardSnapshotQueryParams{}, err
	}
	return rewardSnapshotQueryParams{
		Epoch:              epoch,
		SnapshotType:       snapshot.SnapshotType,
		TotalActiveStake:   decimalUint64(snapshot.TotalActiveStake),
		TotalPoolCount:     totalPoolCount,
		TotalDelegators:    totalDelegators,
		CapturedSlot:       capturedSlot,
		BoundarySlot:       boundarySlot,
		EpochNonce:         snapshot.EpochNonce,
		ProtocolVersion:    protocolVersion,
		Authoritative:      snapshot.Authoritative,
		CalculationVersion: calculationVersion,
	}, nil
}

func rewardSnapshotFromSQLite(
	row sqlitequery.RewardSnapshot,
) (*models.RewardSnapshot, error) {
	totalActiveStake, err := parseUint64(
		"reward total active stake",
		row.TotalActiveStake,
	)
	if err != nil {
		return nil, err
	}
	return &models.RewardSnapshot{
		ID:                 uint(row.ID),
		Epoch:              uint64(row.Epoch),
		SnapshotType:       row.SnapshotType,
		TotalActiveStake:   types.Uint64(totalActiveStake),
		TotalPoolCount:     uint64(row.TotalPoolCount),
		TotalDelegators:    uint64(row.TotalDelegators),
		CapturedSlot:       uint64(row.CapturedSlot),
		BoundarySlot:       uint64(row.BoundarySlot),
		EpochNonce:         row.EpochNonce,
		ProtocolVersion:    uint(row.ProtocolVersion),
		Authoritative:      row.Authoritative,
		CalculationVersion: uint(row.CalculationVersion),
	}, nil
}

func rewardPoolInputParams(
	input *models.RewardPoolInput,
) (sqlitequery.SaveRewardPoolInputParams, error) {
	epoch, err := checkedInt64(input.Epoch)
	if err != nil {
		return sqlitequery.SaveRewardPoolInputParams{}, err
	}
	delegatorCount, err := checkedInt64(input.DelegatorCount)
	if err != nil {
		return sqlitequery.SaveRewardPoolInputParams{}, err
	}
	capturedSlot, err := checkedInt64(input.CapturedSlot)
	if err != nil {
		return sqlitequery.SaveRewardPoolInputParams{}, err
	}
	boundarySlot, err := checkedInt64(input.BoundarySlot)
	if err != nil {
		return sqlitequery.SaveRewardPoolInputParams{}, err
	}
	blocksProduced, err := nullableUint64(input.BlocksProduced)
	if err != nil {
		return sqlitequery.SaveRewardPoolInputParams{}, err
	}
	totalBlocks, err := nullableUint64(input.TotalBlocksInEpoch)
	if err != nil {
		return sqlitequery.SaveRewardPoolInputParams{}, err
	}
	return sqlitequery.SaveRewardPoolInputParams{
		Margin:                     nullableRat(input.Margin),
		PoolKeyHash:                input.PoolKeyHash,
		RewardAccount:              input.RewardAccount,
		BlocksProduced:             blocksProduced,
		TotalBlocksInEpoch:         totalBlocks,
		Epoch:                      epoch,
		Pledge:                     decimalUint64(input.Pledge),
		DelegatedStake:             decimalUint64(input.DelegatedStake),
		OwnerStake:                 decimalUint64(input.OwnerStake),
		Cost:                       decimalUint64(input.Cost),
		DelegatorCount:             delegatorCount,
		RewardAccountCredentialTag: int64(input.RewardAccountCredentialTag),
		CapturedSlot:               capturedSlot,
		BoundarySlot:               boundarySlot,
	}, nil
}

func rewardPoolInputFromSQLite(
	row sqlitequery.RewardPoolInput,
) (*models.RewardPoolInput, error) {
	pledge, err := parseUint64("reward pledge", row.Pledge)
	if err != nil {
		return nil, err
	}
	delegatedStake, err := parseUint64(
		"reward delegated stake",
		row.DelegatedStake,
	)
	if err != nil {
		return nil, err
	}
	ownerStake, err := parseUint64("reward owner stake", row.OwnerStake)
	if err != nil {
		return nil, err
	}
	cost, err := parseUint64("reward cost", row.Cost)
	if err != nil {
		return nil, err
	}
	margin, err := parseNullableRat(row.Margin)
	if err != nil {
		return nil, err
	}
	return &models.RewardPoolInput{
		Margin:                     margin,
		PoolKeyHash:                row.PoolKeyHash,
		RewardAccount:              row.RewardAccount,
		BlocksProduced:             uint64Pointer(row.BlocksProduced),
		TotalBlocksInEpoch:         uint64Pointer(row.TotalBlocksInEpoch),
		ID:                         uint(row.ID),
		Epoch:                      uint64(row.Epoch),
		Pledge:                     types.Uint64(pledge),
		DelegatedStake:             types.Uint64(delegatedStake),
		OwnerStake:                 types.Uint64(ownerStake),
		Cost:                       types.Uint64(cost),
		DelegatorCount:             uint64(row.DelegatorCount),
		RewardAccountCredentialTag: uint8(row.RewardAccountCredentialTag),
		CapturedSlot:               uint64(row.CapturedSlot),
		BoundarySlot:               uint64(row.BoundarySlot),
	}, nil
}

func rewardStakeInputParams(
	input *models.RewardStakeInput,
) (sqlitequery.SaveRewardStakeInputParams, error) {
	epoch, err := checkedInt64(input.Epoch)
	if err != nil {
		return sqlitequery.SaveRewardStakeInputParams{}, err
	}
	capturedSlot, err := checkedInt64(input.CapturedSlot)
	if err != nil {
		return sqlitequery.SaveRewardStakeInputParams{}, err
	}
	boundarySlot, err := checkedInt64(input.BoundarySlot)
	if err != nil {
		return sqlitequery.SaveRewardStakeInputParams{}, err
	}
	return sqlitequery.SaveRewardStakeInputParams{
		PoolKeyHash:   input.PoolKeyHash,
		StakingKey:    input.StakingKey,
		Epoch:         epoch,
		CredentialTag: int64(input.CredentialTag),
		Stake:         decimalUint64(input.Stake),
		Owner:         input.Owner,
		Registered:    input.Registered,
		CapturedSlot:  capturedSlot,
		BoundarySlot:  boundarySlot,
	}, nil
}

func rewardStakeInputFromSQLite(
	row sqlitequery.RewardStakeInput,
) (*models.RewardStakeInput, error) {
	stake, err := parseUint64("reward stake", row.Stake)
	if err != nil {
		return nil, err
	}
	return &models.RewardStakeInput{
		PoolKeyHash:   row.PoolKeyHash,
		StakingKey:    row.StakingKey,
		ID:            uint(row.ID),
		Epoch:         uint64(row.Epoch),
		CredentialTag: uint8(row.CredentialTag),
		Stake:         types.Uint64(stake),
		Owner:         row.Owner,
		Registered:    row.Registered,
		CapturedSlot:  uint64(row.CapturedSlot),
		BoundarySlot:  uint64(row.BoundarySlot),
	}, nil
}

func rewardPoolOutputParams(
	output *models.RewardPoolOutput,
) (sqlitequery.SaveRewardPoolOutputParams, error) {
	epoch, err := checkedInt64(output.Epoch)
	if err != nil {
		return sqlitequery.SaveRewardPoolOutputParams{}, err
	}
	capturedSlot, err := checkedInt64(output.CapturedSlot)
	if err != nil {
		return sqlitequery.SaveRewardPoolOutputParams{}, err
	}
	boundarySlot, err := checkedInt64(output.BoundarySlot)
	if err != nil {
		return sqlitequery.SaveRewardPoolOutputParams{}, err
	}
	return sqlitequery.SaveRewardPoolOutputParams{
		ApparentPerformance: nullableRat(output.ApparentPerformance),
		PoolKeyHash:         output.PoolKeyHash,
		Epoch:               epoch,
		OptimalReward:       decimalUint64(output.OptimalReward),
		TotalReward:         decimalUint64(output.TotalReward),
		LeaderReward:        decimalUint64(output.LeaderReward),
		MemberRewardTotal:   decimalUint64(output.MemberRewardTotal),
		OwnerStake:          decimalUint64(output.OwnerStake),
		Undistributed:       decimalUint64(output.Undistributed),
		Unspendable:         decimalUint64(output.Unspendable),
		CapturedSlot:        capturedSlot,
		BoundarySlot:        boundarySlot,
	}, nil
}

func rewardPoolOutputFromSQLite(
	row sqlitequery.RewardPoolOutput,
) (*models.RewardPoolOutput, error) {
	values, err := parseUint64Fields(
		[]string{
			row.OptimalReward,
			row.TotalReward,
			row.LeaderReward,
			row.MemberRewardTotal,
			row.OwnerStake,
			row.Undistributed,
			row.Unspendable,
		},
	)
	if err != nil {
		return nil, err
	}
	performance, err := parseNullableRat(row.ApparentPerformance)
	if err != nil {
		return nil, err
	}
	return &models.RewardPoolOutput{
		ApparentPerformance: performance,
		PoolKeyHash:         row.PoolKeyHash,
		ID:                  uint(row.ID),
		Epoch:               uint64(row.Epoch),
		OptimalReward:       types.Uint64(values[0]),
		TotalReward:         types.Uint64(values[1]),
		LeaderReward:        types.Uint64(values[2]),
		MemberRewardTotal:   types.Uint64(values[3]),
		OwnerStake:          types.Uint64(values[4]),
		Undistributed:       types.Uint64(values[5]),
		Unspendable:         types.Uint64(values[6]),
		CapturedSlot:        uint64(row.CapturedSlot),
		BoundarySlot:        uint64(row.BoundarySlot),
	}, nil
}

func rewardAccountOutputParams(
	output *models.RewardAccountOutput,
) (sqlitequery.SaveRewardAccountOutputParams, error) {
	epoch, err := checkedInt64(output.Epoch)
	if err != nil {
		return sqlitequery.SaveRewardAccountOutputParams{}, err
	}
	capturedSlot, err := checkedInt64(output.CapturedSlot)
	if err != nil {
		return sqlitequery.SaveRewardAccountOutputParams{}, err
	}
	boundarySlot, err := checkedInt64(output.BoundarySlot)
	if err != nil {
		return sqlitequery.SaveRewardAccountOutputParams{}, err
	}
	return sqlitequery.SaveRewardAccountOutputParams{
		StakingKey:    output.StakingKey,
		PoolKeyHash:   output.PoolKeyHash,
		RewardType:    output.RewardType,
		Epoch:         epoch,
		CredentialTag: int64(output.CredentialTag),
		Amount:        decimalUint64(output.Amount),
		Spendable:     output.Spendable,
		Guarded:       output.Guarded,
		CapturedSlot:  capturedSlot,
		BoundarySlot:  boundarySlot,
	}, nil
}

func rewardAccountOutputFromSQLite(
	row sqlitequery.RewardAccountOutput,
) (*models.RewardAccountOutput, error) {
	amount, err := parseUint64("reward account amount", row.Amount)
	if err != nil {
		return nil, err
	}
	return &models.RewardAccountOutput{
		StakingKey:    row.StakingKey,
		PoolKeyHash:   row.PoolKeyHash,
		RewardType:    row.RewardType,
		ID:            uint(row.ID),
		Epoch:         uint64(row.Epoch),
		CredentialTag: uint8(row.CredentialTag),
		Amount:        types.Uint64(amount),
		Spendable:     row.Spendable,
		Guarded:       row.Guarded,
		CapturedSlot:  uint64(row.CapturedSlot),
		BoundarySlot:  uint64(row.BoundarySlot),
	}, nil
}

func decimalUint64(value types.Uint64) string {
	return strconv.FormatUint(uint64(value), 10)
}

func parseUint64(description, value string) (uint64, error) {
	ret, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("decode %s: %w", description, err)
	}
	return ret, nil
}

func parseUint64Fields(values []string) ([]uint64, error) {
	ret := make([]uint64, len(values))
	for index, value := range values {
		parsed, err := parseUint64("reward value", value)
		if err != nil {
			return nil, err
		}
		ret[index] = parsed
	}
	return ret, nil
}

func nullableRat(value *types.Rat) sql.NullString {
	if value == nil || value.Rat == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: value.String(), Valid: true}
}

func parseNullableRat(value sql.NullString) (*types.Rat, error) {
	if !value.Valid {
		return nil, nil
	}
	rat, ok := new(big.Rat).SetString(value.String)
	if !ok {
		return nil, fmt.Errorf("decode rational %q", value.String)
	}
	return &types.Rat{Rat: rat}, nil
}
