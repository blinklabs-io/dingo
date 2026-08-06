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
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) SavePoolStakeSnapshot(
	snapshot *models.PoolStakeSnapshot,
	txn types.Txn,
) error {
	if snapshot == nil {
		return errors.New("save pool stake snapshot: snapshot is nil")
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	params, err := poolStakeSnapshotParams(snapshot)
	if err != nil {
		return err
	}
	id, err := queries.SavePoolStakeSnapshot(
		context.Background(),
		sqlitequery.SavePoolStakeSnapshotParams(params),
	)
	if err != nil {
		return fmt.Errorf("save pool stake snapshot: %w", err)
	}
	snapshot.ID = uint(id)
	return nil
}

func (s *Store) SavePoolStakeSnapshots(
	snapshots []*models.PoolStakeSnapshot,
	txn types.Txn,
) error {
	if len(snapshots) == 0 {
		return nil
	}
	err := s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			queries := s.operationalQueries(db)
			for _, snapshot := range snapshots {
				if snapshot == nil {
					return errors.New("pool stake snapshot is nil")
				}
				params, err := poolStakeSnapshotParams(snapshot)
				if err != nil {
					return err
				}
				id, err := queries.SavePoolStakeSnapshot(
					context.Background(),
					sqlitequery.SavePoolStakeSnapshotParams(params),
				)
				if err != nil {
					return err
				}
				snapshot.ID = uint(id)
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("save pool stake snapshots: %w", err)
	}
	return nil
}

func (s *Store) GetPoolStakeSnapshot(
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	txn types.Txn,
) (*models.PoolStakeSnapshot, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetPoolStakeSnapshot(
		context.Background(),
		sqlitequery.GetPoolStakeSnapshotParams{
			Epoch:        sqlEpoch,
			SnapshotType: snapshotType,
			PoolKeyHash:  poolKeyHash,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pool stake snapshot: %w", err)
	}
	return poolStakeSnapshotFromSQLite(row)
}

func (s *Store) GetPoolStakeSnapshotsByEpoch(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetPoolStakeSnapshotsByEpoch(
		context.Background(),
		sqlitequery.GetPoolStakeSnapshotsByEpochParams{
			Epoch:        sqlEpoch,
			SnapshotType: snapshotType,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("get pool stake snapshots: %w", err)
	}
	ret := make([]*models.PoolStakeSnapshot, 0, len(rows))
	for _, row := range rows {
		snapshot, err := poolStakeSnapshotFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, snapshot)
	}
	return ret, nil
}

// poolStakeSnapshotColumns is the projection poolStakeSnapshotFromSQLite
// expects, in the order it scans them.
const poolStakeSnapshotColumns = `id, epoch, snapshot_type, pool_key_hash,
total_stake, stake_denominator, delegator_count, captured_slot,
calculation_version, reward_account_auto_vote,
reward_account_auto_vote_resolved`

// GetPoolStakeSnapshotsForPools returns the snapshot rows for just the pools
// named, for a caller that wants a bounded subset rather than a whole epoch.
//
// A named pool with no row is simply absent from the result: the snapshot
// holds a row per pool with stake, so a pool it does not hold has no stake in
// that snapshot rather than a stake of zero.
//
// The read is chunked over the dialect's parameter limit rather than issued
// per pool, so a large filter costs a bounded number of round trips instead of
// one per pool named.
func (s *Store) GetPoolStakeSnapshotsForPools(
	epoch uint64,
	snapshotType string,
	poolKeyHashes [][]byte,
	txn types.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	ret := []*models.PoolStakeSnapshot{}
	if len(poolKeyHashes) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	// Deduplicated before chunking, which is what keeps chunking invisible to
	// the caller. A single `IN (...)` has set semantics -- naming a pool twice
	// still matches its row once -- but two mentions landing in different
	// chunks match in both, and the chunks are concatenated.
	//
	// This list is not the node's to choose: it arrives from the wire as the
	// pool filter on GetPoolDistr2, and PoolFilter hands back the client's
	// items verbatim without collapsing repeats, so a caller may name a pool
	// as many times as it likes.
	unique := make([][]byte, 0, len(poolKeyHashes))
	seen := make(map[string]struct{}, len(poolKeyHashes))
	for _, hash := range poolKeyHashes {
		if _, ok := seen[string(hash)]; ok {
			continue
		}
		seen[string(hash)] = struct{}{}
		unique = append(unique, hash)
	}
	// Two parameters are spent on the epoch and snapshot type before any pool
	// key hash is bound.
	chunkSize := max(s.dialect.ParameterLimit()-2, 1)
	for start := 0; start < len(unique); start += chunkSize {
		end := min(start+chunkSize, len(unique))
		chunk := unique[start:end]
		args := make([]any, 0, len(chunk)+2)
		args = append(args, sqlEpoch, snapshotType)
		placeholders := make([]string, 0, len(chunk))
		for _, hash := range chunk {
			placeholders = append(placeholders, "?")
			args = append(args, hash)
		}
		query := "SELECT " + poolStakeSnapshotColumns +
			" FROM pool_stake_snapshot" +
			" WHERE epoch = ? AND snapshot_type = ?" +
			" AND pool_key_hash IN (" +
			strings.Join(placeholders, ",") + ") ORDER BY id"
		chunkRows, err := s.scanPoolStakeSnapshots(db, query, args)
		if err != nil {
			return nil, err
		}
		ret = append(ret, chunkRows...)
	}
	return ret, nil
}

func (s *Store) scanPoolStakeSnapshots(
	db queryer,
	query string,
	args []any,
) ([]*models.PoolStakeSnapshot, error) {
	rows, err := db.QueryContext(
		context.Background(),
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("get pool stake snapshots for pools: %w", err)
	}
	defer rows.Close()
	ret := []*models.PoolStakeSnapshot{}
	for rows.Next() {
		var row sqlitequery.PoolStakeSnapshot
		if err := rows.Scan(
			&row.ID,
			&row.Epoch,
			&row.SnapshotType,
			&row.PoolKeyHash,
			&row.TotalStake,
			&row.StakeDenominator,
			&row.DelegatorCount,
			&row.CapturedSlot,
			&row.CalculationVersion,
			&row.RewardAccountAutoVote,
			&row.RewardAccountAutoVoteResolved,
		); err != nil {
			return nil, err
		}
		snapshot, err := poolStakeSnapshotFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, snapshot)
	}
	return ret, rows.Err()
}

func (s *Store) GetTotalActiveStake(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (uint64, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return 0, err
	}
	if snapshotType == models.PoolStakeSnapshotTypeMark {
		summary, err := queries.GetEpochSummary(
			context.Background(),
			sqlEpoch,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("get total active stake: %w", err)
		}
		if err == nil && summary.SnapshotReady {
			value, err := strconv.ParseUint(
				summary.TotalActiveStake,
				10,
				64,
			)
			if err != nil {
				return 0, fmt.Errorf("get total active stake: %w", err)
			}
			return value, nil
		}
	}
	value, err := sumUint64Rows(db, s.dialect.Rebind(`
SELECT total_stake FROM pool_stake_snapshot
WHERE epoch = ? AND snapshot_type = ?`), sqlEpoch, snapshotType)
	if err != nil {
		return 0, fmt.Errorf("get total active stake: %w", err)
	}
	return value, nil
}

func (s *Store) SaveEpochSummary(
	summary *models.EpochSummary,
	txn types.Txn,
) error {
	if summary == nil {
		return errors.New("save epoch summary: summary is nil")
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	epoch, err := checkedInt64(summary.Epoch)
	if err != nil {
		return err
	}
	totalPoolCount, err := checkedInt64(summary.TotalPoolCount)
	if err != nil {
		return err
	}
	totalDelegators, err := checkedInt64(summary.TotalDelegators)
	if err != nil {
		return err
	}
	boundarySlot, err := checkedInt64(summary.BoundarySlot)
	if err != nil {
		return err
	}
	id, err := queries.SaveEpochSummary(
		context.Background(),
		sqlitequery.SaveEpochSummaryParams{
			Epoch: epoch,
			TotalActiveStake: strconv.FormatUint(
				uint64(summary.TotalActiveStake),
				10,
			),
			TotalPoolCount:  totalPoolCount,
			TotalDelegators: totalDelegators,
			EpochNonce:      summary.EpochNonce,
			BoundarySlot:    boundarySlot,
			SnapshotReady:   summary.SnapshotReady,
		},
	)
	if err != nil {
		return fmt.Errorf("save epoch summary: %w", err)
	}
	summary.ID = uint(id)
	return nil
}

func (s *Store) GetEpochSummary(
	epoch uint64,
	txn types.Txn,
) (*models.EpochSummary, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetEpochSummary(context.Background(), sqlEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get epoch summary: %w", err)
	}
	return epochSummaryFromSQLite(row)
}

func (s *Store) GetLatestEpochSummary(
	txn types.Txn,
) (*models.EpochSummary, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	row, err := queries.GetLatestEpochSummary(context.Background())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest epoch summary: %w", err)
	}
	return epochSummaryFromSQLite(row)
}

func (s *Store) DeletePoolStakeSnapshotsForEpoch(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	err = queries.DeletePoolStakeSnapshotsForEpoch(
		context.Background(),
		sqlitequery.DeletePoolStakeSnapshotsForEpochParams{
			Epoch:        sqlEpoch,
			SnapshotType: snapshotType,
		},
	)
	if err != nil {
		return fmt.Errorf("delete pool stake snapshots for epoch: %w", err)
	}
	return nil
}

func (s *Store) DeletePoolStakeSnapshotsAfterEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteSnapshotsByEpoch(
		"after",
		epoch,
		txn,
		func(q *sqlitequery.Queries, value int64) error {
			return q.DeletePoolStakeSnapshotsAfterEpoch(
				context.Background(),
				value,
			)
		},
	)
}

func (s *Store) DeletePoolStakeSnapshotsBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteSnapshotsByEpoch(
		"before",
		epoch,
		txn,
		func(q *sqlitequery.Queries, value int64) error {
			return q.DeletePoolStakeSnapshotsBeforeEpoch(
				context.Background(),
				value,
			)
		},
	)
}

func (s *Store) DeleteEpochSummariesAfterEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	return s.deleteSnapshotsByEpoch(
		"summaries after",
		epoch,
		txn,
		func(q *sqlitequery.Queries, value int64) error {
			return q.DeleteEpochSummariesAfterEpoch(
				context.Background(),
				value,
			)
		},
	)
}

func (s *Store) deleteSnapshotsByEpoch(
	description string,
	epoch uint64,
	txn types.Txn,
	deleteFn func(*sqlitequery.Queries, int64) error,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	if err := deleteFn(queries, sqlEpoch); err != nil {
		return fmt.Errorf(
			"delete pool stake snapshot %s epoch: %w",
			description,
			err,
		)
	}
	return nil
}

type poolStakeSnapshotQueryParams struct {
	Epoch                         int64
	SnapshotType                  string
	PoolKeyHash                   []byte
	TotalStake                    string
	StakeDenominator              string
	DelegatorCount                int64
	CapturedSlot                  int64
	CalculationVersion            int64
	RewardAccountAutoVote         int64
	RewardAccountAutoVoteResolved bool
}

func poolStakeSnapshotParams(
	snapshot *models.PoolStakeSnapshot,
) (poolStakeSnapshotQueryParams, error) {
	epoch, err := checkedInt64(snapshot.Epoch)
	if err != nil {
		return poolStakeSnapshotQueryParams{}, err
	}
	delegatorCount, err := checkedInt64(snapshot.DelegatorCount)
	if err != nil {
		return poolStakeSnapshotQueryParams{}, err
	}
	capturedSlot, err := checkedInt64(snapshot.CapturedSlot)
	if err != nil {
		return poolStakeSnapshotQueryParams{}, err
	}
	calculationVersion, err := checkedInt64(uint64(snapshot.CalculationVersion))
	if err != nil {
		return poolStakeSnapshotQueryParams{}, err
	}
	return poolStakeSnapshotQueryParams{
		Epoch:        epoch,
		SnapshotType: snapshot.SnapshotType,
		PoolKeyHash:  snapshot.PoolKeyHash,
		TotalStake:   strconv.FormatUint(uint64(snapshot.TotalStake), 10),
		StakeDenominator: strconv.FormatUint(
			uint64(snapshot.StakeDenominator),
			10,
		),
		DelegatorCount:     delegatorCount,
		CapturedSlot:       capturedSlot,
		CalculationVersion: calculationVersion,
		RewardAccountAutoVote: int64(
			snapshot.RewardAccountAutoVote,
		),
		RewardAccountAutoVoteResolved: snapshot.RewardAccountAutoVoteResolved,
	}, nil
}

func poolStakeSnapshotFromSQLite(
	row sqlitequery.PoolStakeSnapshot,
) (*models.PoolStakeSnapshot, error) {
	totalStake, err := strconv.ParseUint(row.TotalStake, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("decode pool total stake: %w", err)
	}
	stakeDenominator, err := strconv.ParseUint(
		row.StakeDenominator,
		10,
		64,
	)
	if err != nil {
		return nil, fmt.Errorf("decode pool stake denominator: %w", err)
	}
	return &models.PoolStakeSnapshot{
		ID:               uint(row.ID),
		Epoch:            uint64(row.Epoch),
		SnapshotType:     row.SnapshotType,
		PoolKeyHash:      row.PoolKeyHash,
		TotalStake:       types.Uint64(totalStake),
		StakeDenominator: types.Uint64(stakeDenominator),
		DelegatorCount:   uint64(row.DelegatorCount),
		CapturedSlot:     uint64(row.CapturedSlot),
		CalculationVersion: uint(
			row.CalculationVersion,
		),
		RewardAccountAutoVote: uint8(
			row.RewardAccountAutoVote,
		),
		RewardAccountAutoVoteResolved: row.RewardAccountAutoVoteResolved,
	}, nil
}

func epochSummaryFromSQLite(
	row sqlitequery.EpochSummary,
) (*models.EpochSummary, error) {
	totalActiveStake, err := strconv.ParseUint(
		row.TotalActiveStake,
		10,
		64,
	)
	if err != nil {
		return nil, fmt.Errorf("decode total active stake: %w", err)
	}
	return &models.EpochSummary{
		ID:               uint(row.ID),
		Epoch:            uint64(row.Epoch),
		TotalActiveStake: types.Uint64(totalActiveStake),
		TotalPoolCount:   uint64(row.TotalPoolCount),
		TotalDelegators:  uint64(row.TotalDelegators),
		EpochNonce:       row.EpochNonce,
		BoundarySlot:     uint64(row.BoundarySlot),
		SnapshotReady:    row.SnapshotReady,
	}, nil
}
