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

//nolint:gosec,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
)

func (s *Store) FindMidnightAssetCreatesFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightAssetCreate, error) {
	return findMidnightPage(
		s,
		txn,
		"midnight_asset_creates",
		"id, address, quantity, tx_hash, output_index, block_number, "+
			"block_hash, tx_index, block_timestamp_ms",
		startBlock,
		startTxIndex,
		limit,
		scanMidnightAssetCreate,
	)
}

func (s *Store) FindMidnightAssetSpendsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightAssetSpend, error) {
	return findMidnightPage(
		s,
		txn,
		"midnight_asset_spends",
		"id, address, quantity, spending_tx_hash, utxo_tx_hash, "+
			"utxo_index, block_number, block_hash, tx_index, "+
			"block_timestamp_ms",
		startBlock,
		startTxIndex,
		limit,
		scanMidnightAssetSpend,
	)
}

func (s *Store) FindMidnightRegistrationsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightRegistration, error) {
	return findMidnightPage(
		s,
		txn,
		"midnight_registrations",
		"id, full_datum, tx_hash, output_index, block_number, block_hash, "+
			"tx_index, block_timestamp_ms",
		startBlock,
		startTxIndex,
		limit,
		scanMidnightRegistration,
	)
}

func (s *Store) FindMidnightDeregistrationsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightDeregistration, error) {
	return findMidnightPage(
		s,
		txn,
		"midnight_deregistrations",
		"id, full_datum, tx_hash, utxo_tx_hash, utxo_index, block_number, "+
			"block_hash, tx_index, block_timestamp_ms",
		startBlock,
		startTxIndex,
		limit,
		scanMidnightDeregistration,
	)
}

func (s *Store) GetMidnightCandidates(
	address ledger.Address,
	txn types.Txn,
) ([]models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	predicates := []string{}
	args := []any{}
	if err := models.AppendUtxoAddressOrBranch(
		&predicates,
		&args,
		address,
	); err != nil {
		return nil, err
	}
	if len(predicates) == 0 {
		return nil, nil
	}
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(`
SELECT utxo.tx_id, utxo.output_idx, datum.raw_datum
FROM utxo
LEFT JOIN datum ON datum.hash = utxo.datum_hash
WHERE utxo.deleted_slot = 0 AND `+predicates[0]),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []models.Utxo{}
	for rows.Next() {
		var utxo models.Utxo
		if err := rows.Scan(
			&utxo.TxId,
			&utxo.OutputIdx,
			&utxo.Datum,
		); err != nil {
			return nil, err
		}
		ret = append(ret, utxo)
	}
	return ret, rows.Err()
}

func (s *Store) CreateMidnightAssetCreate(
	txn types.Txn,
	row *models.MidnightAssetCreate,
) error {
	if row == nil {
		return errors.New("create Midnight asset: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	params, err := midnightAssetCreateParams(row)
	if err != nil {
		return err
	}
	id, err := q.CreateMidnightAssetCreate(ctx, params)
	return applyIgnoredInsertID(&row.ID, id, err)
}

func (s *Store) CreateMidnightAssetSpend(
	txn types.Txn,
	row *models.MidnightAssetSpend,
) error {
	if row == nil {
		return errors.New("create Midnight asset spend: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	params, err := midnightAssetSpendParams(row)
	if err != nil {
		return err
	}
	id, err := q.CreateMidnightAssetSpend(ctx, params)
	return applyIgnoredInsertID(&row.ID, id, err)
}

func (s *Store) CreateMidnightRegistration(
	txn types.Txn,
	row *models.MidnightRegistration,
) error {
	if row == nil {
		return errors.New("create Midnight registration: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	params, err := midnightRegistrationParams(row)
	if err != nil {
		return err
	}
	id, err := q.CreateMidnightRegistration(ctx, params)
	return applyIgnoredInsertID(&row.ID, id, err)
}

func (s *Store) CreateMidnightDeregistration(
	txn types.Txn,
	row *models.MidnightDeregistration,
) error {
	if row == nil {
		return errors.New("create Midnight deregistration: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	params, err := midnightDeregistrationParams(row)
	if err != nil {
		return err
	}
	id, err := q.CreateMidnightDeregistration(ctx, params)
	return applyIgnoredInsertID(&row.ID, id, err)
}

func (s *Store) FindUnspentMidnightAssetCreates() (
	[]models.MidnightAssetCreate,
	error,
) {
	q := s.operationalQueries(s.readDB)
	// No txn parameter on this method, so no caller-managed ctx is
	// available -- same accepted autocommit-path gap as dbFromTxn/
	// readDBFromTxn document for a nil txn.
	rows, err := q.FindUnspentMidnightAssetCreates(context.Background())
	if err != nil {
		return nil, err
	}
	return mapMidnightAssetCreates(rows), nil
}

func (s *Store) FindUnspentMidnightRegistrations() (
	[]models.MidnightRegistration,
	error,
) {
	q := s.operationalQueries(s.readDB)
	// See FindUnspentMidnightAssetCreates: no txn parameter, no ctx to use.
	rows, err := q.FindUnspentMidnightRegistrations(context.Background())
	if err != nil {
		return nil, err
	}
	return mapMidnightRegistrations(rows), nil
}

func (s *Store) DeleteMidnightAssetCreatesByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightAssetCreate, error) {
	db, ctx, sqlBlock, err := s.midnightWriteDB(txn, blockNumber)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	rows, err := q.GetMidnightAssetCreatesByBlock(
		ctx,
		sqlBlock,
	)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if err := q.DeleteMidnightAssetCreatesByBlock(
		ctx,
		sqlBlock,
	); err != nil {
		return nil, err
	}
	return mapMidnightAssetCreates(rows), nil
}

func (s *Store) DeleteMidnightAssetSpendsByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightAssetSpend, error) {
	db, ctx, sqlBlock, err := s.midnightWriteDB(txn, blockNumber)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	rows, err := q.GetMidnightAssetSpendsByBlock(
		ctx,
		sqlBlock,
	)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if err := q.DeleteMidnightAssetSpendsByBlock(
		ctx,
		sqlBlock,
	); err != nil {
		return nil, err
	}
	return mapMidnightAssetSpends(rows), nil
}

func (s *Store) DeleteMidnightRegistrationsByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightRegistration, error) {
	db, ctx, sqlBlock, err := s.midnightWriteDB(txn, blockNumber)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	rows, err := q.GetMidnightRegistrationsByBlock(
		ctx,
		sqlBlock,
	)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if err := q.DeleteMidnightRegistrationsByBlock(
		ctx,
		sqlBlock,
	); err != nil {
		return nil, err
	}
	return mapMidnightRegistrations(rows), nil
}

func (s *Store) DeleteMidnightDeregistrationsByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightDeregistration, error) {
	db, ctx, sqlBlock, err := s.midnightWriteDB(txn, blockNumber)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	rows, err := q.GetMidnightDeregistrationsByBlock(
		ctx,
		sqlBlock,
	)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if err := q.DeleteMidnightDeregistrationsByBlock(
		ctx,
		sqlBlock,
	); err != nil {
		return nil, err
	}
	return mapMidnightDeregistrations(rows), nil
}

func (s *Store) InsertMidnightGovernanceDatum(
	txn types.Txn,
	datum *models.MidnightGovernanceDatum,
) error {
	if datum == nil {
		return errors.New("insert Midnight governance datum: datum is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	outputIndex, err := checkedInt64(uint64(datum.OutputIndex))
	if err != nil {
		return err
	}
	blockNumber, err := checkedInt64(datum.BlockNumber)
	if err != nil {
		return err
	}
	id, err := q.InsertMidnightGovernanceDatum(
		ctx,
		sqlitequery.InsertMidnightGovernanceDatumParams{
			DatumType:   datum.DatumType,
			TxHash:      datum.TxHash,
			OutputIndex: outputIndex,
			Datum:       datum.Datum,
			BlockNumber: blockNumber,
		},
	)
	return applyIgnoredInsertID(&datum.ID, id, err)
}

func (s *Store) DeleteMidnightGovernanceDatumsByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	return s.deleteMidnightByUint64(
		txn,
		blockNumber,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteMidnightGovernanceDatumsByBlock(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) GetLatestMidnightGovernanceDatum(
	datumType string,
	blockNumber uint64,
	txn types.Txn,
) (*models.MidnightGovernanceDatum, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	sqlBlock, err := checkedInt64(blockNumber)
	if err != nil {
		return nil, err
	}
	row, err := q.GetLatestMidnightGovernanceDatum(
		ctx,
		sqlitequery.GetLatestMidnightGovernanceDatumParams{
			DatumType:   datumType,
			BlockNumber: sqlBlock,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ret := midnightGovernanceDatumFromSQLite(row)
	return &ret, nil
}

func (s *Store) GetLatestMidnightAriadneParams(
	txn types.Txn,
) (*models.MidnightAriadneParams, error) {
	return s.getMidnightAriadneParams(
		txn,
		func(
			q *sqlitequery.Queries,
			ctx context.Context,
		) (sqlitequery.MidnightAriadneParam, error) {
			return q.GetLatestMidnightAriadneParams(ctx)
		},
	)
}

func (s *Store) GetMidnightAriadneParamsByEpoch(
	epoch uint64,
	txn types.Txn,
) (*models.MidnightAriadneParams, error) {
	return s.getMidnightAriadneParamsByEpoch(
		epoch,
		txn,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) (
			sqlitequery.MidnightAriadneParam,
			error,
		) {
			return q.GetMidnightAriadneParamsByEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) GetMidnightAriadneParamsAtOrBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) (*models.MidnightAriadneParams, error) {
	return s.getMidnightAriadneParamsByEpoch(
		epoch,
		txn,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) (
			sqlitequery.MidnightAriadneParam,
			error,
		) {
			return q.GetMidnightAriadneParamsAtOrBeforeEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) UpsertMidnightAriadneParams(
	txn types.Txn,
	params *models.MidnightAriadneParams,
) error {
	if params == nil {
		return errors.New("upsert Midnight Ariadne params: params are nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	epoch, err := checkedInt64(params.Epoch)
	if err != nil {
		return err
	}
	id, err := q.UpsertMidnightAriadneParams(
		ctx,
		sqlitequery.UpsertMidnightAriadneParamsParams{
			Epoch: epoch,
			Datum: params.Datum,
		},
	)
	if err == nil {
		params.ID = uint(id)
	}
	return err
}

func (s *Store) DeleteMidnightAriadneParamsByEpoch(
	txn types.Txn,
	epoch uint64,
) error {
	return s.deleteMidnightByUint64(
		txn,
		epoch,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteMidnightAriadneParamsByEpoch(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) CreateMidnightAriadneRollback(
	txn types.Txn,
	rollback *models.MidnightAriadneRollback,
) error {
	if rollback == nil {
		return errors.New("create Midnight Ariadne rollback: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	blockNumber, err := checkedInt64(rollback.BlockNumber)
	if err != nil {
		return err
	}
	epoch, err := checkedInt64(rollback.Epoch)
	if err != nil {
		return err
	}
	id, err := q.CreateMidnightAriadneRollback(
		ctx,
		sqlitequery.CreateMidnightAriadneRollbackParams{
			BlockNumber:    blockNumber,
			Epoch:          epoch,
			PreviousExists: rollback.PreviousExists,
			PreviousDatum:  rollback.PreviousDatum,
		},
	)
	return applyIgnoredInsertID(&rollback.ID, id, err)
}

func (s *Store) FindMidnightAriadneRollbacksByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightAriadneRollback, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	sqlBlock, err := checkedInt64(blockNumber)
	if err != nil {
		return nil, err
	}
	rows, err := q.FindMidnightAriadneRollbacksByBlock(
		ctx,
		sqlBlock,
	)
	if err != nil {
		return nil, err
	}
	ret := make([]models.MidnightAriadneRollback, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, models.MidnightAriadneRollback{
			ID:             uint(row.ID),
			BlockNumber:    uint64(row.BlockNumber),
			Epoch:          uint64(row.Epoch),
			PreviousExists: row.PreviousExists,
			PreviousDatum:  row.PreviousDatum,
		})
	}
	return ret, nil
}

func (s *Store) DeleteMidnightAriadneRollbacksByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	return s.deleteMidnightByUint64(
		txn,
		blockNumber,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteMidnightAriadneRollbacksByBlock(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) DeleteMidnightAriadneRollbacksBeforeBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	return s.deleteMidnightByUint64(
		txn,
		blockNumber,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteMidnightAriadneRollbacksBeforeBlock(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) UpsertMidnightEpochCandidates(
	txn types.Txn,
	epochCandidates *models.MidnightEpochCandidates,
) error {
	if epochCandidates == nil {
		return errors.New("upsert Midnight epoch candidates: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	epoch, err := checkedInt64(epochCandidates.Epoch)
	if err != nil {
		return err
	}
	blockNumber, err := checkedInt64(epochCandidates.BlockNumber)
	if err != nil {
		return err
	}
	id, err := q.UpsertMidnightEpochCandidates(
		ctx,
		sqlitequery.UpsertMidnightEpochCandidatesParams{
			Epoch:          epoch,
			BlockNumber:    blockNumber,
			CandidatesCbor: epochCandidates.CandidatesCbor,
		},
	)
	if err == nil {
		epochCandidates.ID = uint(id)
	}
	return err
}

func (s *Store) DeleteMidnightEpochCandidatesByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	return s.deleteMidnightByUint64(
		txn,
		blockNumber,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteMidnightEpochCandidatesByBlock(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) GetMidnightEpochCandidatesByEpoch(
	epoch uint64,
	txn types.Txn,
) (*models.MidnightEpochCandidates, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	row, err := q.GetMidnightEpochCandidatesByEpoch(
		ctx,
		sqlEpoch,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &models.MidnightEpochCandidates{
		ID:             uint(row.ID),
		Epoch:          uint64(row.Epoch),
		BlockNumber:    uint64(row.BlockNumber),
		CandidatesCbor: row.CandidatesCbor,
	}, nil
}

func (s *Store) InsertMidnightCommitteeCandidateRegistration(
	txn types.Txn,
	row *models.MidnightCommitteeCandidateRegistration,
) error {
	if row == nil {
		return errors.New("insert Midnight candidate registration: row is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	outputIndex, err := checkedInt64(uint64(row.OutputIndex))
	if err != nil {
		return err
	}
	blockNumber, err := checkedInt64(row.BlockNumber)
	if err != nil {
		return err
	}
	slotNumber, err := checkedInt64(row.SlotNumber)
	if err != nil {
		return err
	}
	txIndex, err := checkedInt64(uint64(row.TxIndex))
	if err != nil {
		return err
	}
	id, err := q.InsertMidnightCommitteeCandidateRegistration(
		ctx,
		sqlitequery.InsertMidnightCommitteeCandidateRegistrationParams{
			TxHash:       row.TxHash,
			OutputIndex:  outputIndex,
			BlockNumber:  blockNumber,
			SlotNumber:   slotNumber,
			TxIndex:      txIndex,
			TxInputsCbor: row.TxInputsCbor,
		},
	)
	return applyIgnoredInsertID(&row.ID, id, err)
}

func (s *Store) DeleteMidnightCommitteeCandidateRegistrationsByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	return s.deleteMidnightByUint64(
		txn,
		blockNumber,
		func(q *sqlitequery.Queries, ctx context.Context, value int64) error {
			return q.DeleteMidnightCommitteeCandidateRegistrationsByBlock(
				ctx,
				value,
			)
		},
	)
}

func (s *Store) GetMidnightCommitteeCandidateRegistrationsByTxHashes(
	txHashes [][]byte,
	txn types.Txn,
) ([]models.MidnightCommitteeCandidateRegistration, error) {
	if len(txHashes) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	ret := make([]models.MidnightCommitteeCandidateRegistration, 0)
	chunkSize := s.dialect.ParameterLimit()
	for start := 0; start < len(txHashes); start += chunkSize {
		end := min(start+chunkSize, len(txHashes))
		chunk := txHashes[start:end]
		args := make([]any, len(chunk))
		for i := range chunk {
			args[i] = chunk[i]
		}
		query := "SELECT id, tx_hash, output_index, block_number, " +
			"slot_number, tx_index, tx_inputs_cbor " +
			"FROM midnight_committee_candidate_registrations " +
			"WHERE tx_hash IN (" + bindPlaceholders(len(chunk)) + ") " +
			"ORDER BY id"
		rows, err := db.QueryContext(
			ctx,
			s.dialect.Rebind(query),
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var row models.MidnightCommitteeCandidateRegistration
			if err := rows.Scan(
				&row.ID,
				&row.TxHash,
				&row.OutputIndex,
				&row.BlockNumber,
				&row.SlotNumber,
				&row.TxIndex,
				&row.TxInputsCbor,
			); err != nil {
				rows.Close()
				return nil, err
			}
			ret = append(ret, row)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (s *Store) midnightWriteDB(
	txn types.Txn,
	value uint64,
) (queryer, context.Context, int64, error) {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return nil, nil, 0, err
	}
	sqlValue, err := checkedInt64(value)
	return db, ctx, sqlValue, err
}

func (s *Store) deleteMidnightByUint64(
	txn types.Txn,
	value uint64,
	deleteFn func(*sqlitequery.Queries, context.Context, int64) error,
) error {
	db, ctx, sqlValue, err := s.midnightWriteDB(txn, value)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	return deleteFn(q, ctx, sqlValue)
}

func (s *Store) getMidnightAriadneParams(
	txn types.Txn,
	get func(
		*sqlitequery.Queries,
		context.Context,
	) (sqlitequery.MidnightAriadneParam, error),
) (*models.MidnightAriadneParams, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	row, err := get(q, ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &models.MidnightAriadneParams{
		ID:    uint(row.ID),
		Epoch: uint64(row.Epoch),
		Datum: row.Datum,
	}, nil
}

func (s *Store) getMidnightAriadneParamsByEpoch(
	epoch uint64,
	txn types.Txn,
	get func(
		*sqlitequery.Queries,
		context.Context,
		int64,
	) (sqlitequery.MidnightAriadneParam, error),
) (*models.MidnightAriadneParams, error) {
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	return s.getMidnightAriadneParams(
		txn,
		func(
			q *sqlitequery.Queries,
			ctx context.Context,
		) (sqlitequery.MidnightAriadneParam, error) {
			return get(q, ctx, sqlEpoch)
		},
	)
}

func applyIgnoredInsertID(destination *uint, id int64, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	*destination = uint(id)
	return nil
}

func midnightAssetCreateParams(
	row *models.MidnightAssetCreate,
) (sqlitequery.CreateMidnightAssetCreateParams, error) {
	quantity, err := checkedInt64(row.Quantity)
	if err != nil {
		return sqlitequery.CreateMidnightAssetCreateParams{}, err
	}
	outputIndex, err := checkedInt64(uint64(row.OutputIndex))
	if err != nil {
		return sqlitequery.CreateMidnightAssetCreateParams{}, err
	}
	blockNumber, err := checkedInt64(row.BlockNumber)
	if err != nil {
		return sqlitequery.CreateMidnightAssetCreateParams{}, err
	}
	txIndex, err := checkedInt64(uint64(row.TxIndex))
	if err != nil {
		return sqlitequery.CreateMidnightAssetCreateParams{}, err
	}
	timestamp, err := checkedInt64(row.BlockTimestampMs)
	if err != nil {
		return sqlitequery.CreateMidnightAssetCreateParams{}, err
	}
	return sqlitequery.CreateMidnightAssetCreateParams{
		Address:          row.Address,
		Quantity:         quantity,
		TxHash:           row.TxHash,
		OutputIndex:      outputIndex,
		BlockNumber:      blockNumber,
		BlockHash:        row.BlockHash,
		TxIndex:          txIndex,
		BlockTimestampMs: timestamp,
	}, nil
}

func midnightAssetSpendParams(
	row *models.MidnightAssetSpend,
) (sqlitequery.CreateMidnightAssetSpendParams, error) {
	quantity, err := checkedInt64(row.Quantity)
	if err != nil {
		return sqlitequery.CreateMidnightAssetSpendParams{}, err
	}
	utxoIndex, err := checkedInt64(uint64(row.UtxoIndex))
	if err != nil {
		return sqlitequery.CreateMidnightAssetSpendParams{}, err
	}
	blockNumber, err := checkedInt64(row.BlockNumber)
	if err != nil {
		return sqlitequery.CreateMidnightAssetSpendParams{}, err
	}
	txIndex, err := checkedInt64(uint64(row.TxIndex))
	if err != nil {
		return sqlitequery.CreateMidnightAssetSpendParams{}, err
	}
	timestamp, err := checkedInt64(row.BlockTimestampMs)
	if err != nil {
		return sqlitequery.CreateMidnightAssetSpendParams{}, err
	}
	return sqlitequery.CreateMidnightAssetSpendParams{
		Address:          row.Address,
		Quantity:         quantity,
		SpendingTxHash:   row.SpendingTxHash,
		UtxoTxHash:       row.UtxoTxHash,
		UtxoIndex:        utxoIndex,
		BlockNumber:      blockNumber,
		BlockHash:        row.BlockHash,
		TxIndex:          txIndex,
		BlockTimestampMs: timestamp,
	}, nil
}

func midnightRegistrationParams(
	row *models.MidnightRegistration,
) (sqlitequery.CreateMidnightRegistrationParams, error) {
	outputIndex, err := checkedInt64(uint64(row.OutputIndex))
	if err != nil {
		return sqlitequery.CreateMidnightRegistrationParams{}, err
	}
	blockNumber, err := checkedInt64(row.BlockNumber)
	if err != nil {
		return sqlitequery.CreateMidnightRegistrationParams{}, err
	}
	txIndex, err := checkedInt64(uint64(row.TxIndex))
	if err != nil {
		return sqlitequery.CreateMidnightRegistrationParams{}, err
	}
	timestamp, err := checkedInt64(row.BlockTimestampMs)
	if err != nil {
		return sqlitequery.CreateMidnightRegistrationParams{}, err
	}
	return sqlitequery.CreateMidnightRegistrationParams{
		FullDatum:        row.FullDatum,
		TxHash:           row.TxHash,
		OutputIndex:      outputIndex,
		BlockNumber:      blockNumber,
		BlockHash:        row.BlockHash,
		TxIndex:          txIndex,
		BlockTimestampMs: timestamp,
	}, nil
}

func midnightDeregistrationParams(
	row *models.MidnightDeregistration,
) (sqlitequery.CreateMidnightDeregistrationParams, error) {
	utxoIndex, err := checkedInt64(uint64(row.UtxoIndex))
	if err != nil {
		return sqlitequery.CreateMidnightDeregistrationParams{}, err
	}
	blockNumber, err := checkedInt64(row.BlockNumber)
	if err != nil {
		return sqlitequery.CreateMidnightDeregistrationParams{}, err
	}
	txIndex, err := checkedInt64(uint64(row.TxIndex))
	if err != nil {
		return sqlitequery.CreateMidnightDeregistrationParams{}, err
	}
	timestamp, err := checkedInt64(row.BlockTimestampMs)
	if err != nil {
		return sqlitequery.CreateMidnightDeregistrationParams{}, err
	}
	return sqlitequery.CreateMidnightDeregistrationParams{
		FullDatum:        row.FullDatum,
		TxHash:           row.TxHash,
		UtxoTxHash:       row.UtxoTxHash,
		UtxoIndex:        utxoIndex,
		BlockNumber:      blockNumber,
		BlockHash:        row.BlockHash,
		TxIndex:          txIndex,
		BlockTimestampMs: timestamp,
	}, nil
}

func mapMidnightAssetCreates(
	rows []sqlitequery.MidnightAssetCreate,
) []models.MidnightAssetCreate {
	ret := make([]models.MidnightAssetCreate, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, models.MidnightAssetCreate{
			ID:               uint(row.ID),
			Address:          row.Address,
			Quantity:         uint64(row.Quantity),
			TxHash:           row.TxHash,
			OutputIndex:      uint32(row.OutputIndex),
			BlockNumber:      uint64(row.BlockNumber),
			BlockHash:        row.BlockHash,
			TxIndex:          uint32(row.TxIndex),
			BlockTimestampMs: uint64(row.BlockTimestampMs),
		})
	}
	return ret
}

func mapMidnightAssetSpends(
	rows []sqlitequery.MidnightAssetSpend,
) []models.MidnightAssetSpend {
	ret := make([]models.MidnightAssetSpend, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, models.MidnightAssetSpend{
			ID:               uint(row.ID),
			Address:          row.Address,
			Quantity:         uint64(row.Quantity),
			SpendingTxHash:   row.SpendingTxHash,
			UtxoTxHash:       row.UtxoTxHash,
			UtxoIndex:        uint32(row.UtxoIndex),
			BlockNumber:      uint64(row.BlockNumber),
			BlockHash:        row.BlockHash,
			TxIndex:          uint32(row.TxIndex),
			BlockTimestampMs: uint64(row.BlockTimestampMs),
		})
	}
	return ret
}

func mapMidnightRegistrations(
	rows []sqlitequery.MidnightRegistration,
) []models.MidnightRegistration {
	ret := make([]models.MidnightRegistration, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, models.MidnightRegistration{
			ID:               uint(row.ID),
			FullDatum:        row.FullDatum,
			TxHash:           row.TxHash,
			OutputIndex:      uint32(row.OutputIndex),
			BlockNumber:      uint64(row.BlockNumber),
			BlockHash:        row.BlockHash,
			TxIndex:          uint32(row.TxIndex),
			BlockTimestampMs: uint64(row.BlockTimestampMs),
		})
	}
	return ret
}

func mapMidnightDeregistrations(
	rows []sqlitequery.MidnightDeregistration,
) []models.MidnightDeregistration {
	ret := make([]models.MidnightDeregistration, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, models.MidnightDeregistration{
			ID:               uint(row.ID),
			FullDatum:        row.FullDatum,
			TxHash:           row.TxHash,
			UtxoTxHash:       row.UtxoTxHash,
			UtxoIndex:        uint32(row.UtxoIndex),
			BlockNumber:      uint64(row.BlockNumber),
			BlockHash:        row.BlockHash,
			TxIndex:          uint32(row.TxIndex),
			BlockTimestampMs: uint64(row.BlockTimestampMs),
		})
	}
	return ret
}

func midnightGovernanceDatumFromSQLite(
	row sqlitequery.MidnightGovernanceDatum,
) models.MidnightGovernanceDatum {
	return models.MidnightGovernanceDatum{
		ID:          uint(row.ID),
		DatumType:   row.DatumType,
		TxHash:      row.TxHash,
		OutputIndex: uint32(row.OutputIndex),
		Datum:       row.Datum,
		BlockNumber: uint64(row.BlockNumber),
	}
}

type midnightPageRow interface {
	models.MidnightAssetCreate |
		models.MidnightAssetSpend |
		models.MidnightRegistration |
		models.MidnightDeregistration
}

type midnightRowScanner[T midnightPageRow] func(*sql.Rows) (T, error)

func findMidnightPage[T midnightPageRow](
	store *Store,
	txn types.Txn,
	table, columns string,
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	scan midnightRowScanner[T],
) ([]T, error) {
	db, ctx, err := store.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	sqlBlock, err := checkedInt64(startBlock)
	if err != nil {
		return nil, err
	}
	sqlTxIndex := int64(startTxIndex)
	query := "SELECT " + columns + " FROM " + table +
		" WHERE (block_number > ?) OR " +
		"(block_number = ? AND tx_index > ?)" +
		" ORDER BY block_number ASC, tx_index ASC, id ASC"
	args := []any{sqlBlock, sqlBlock, sqlTxIndex}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	ret, err := queryMidnightRows(
		ctx,
		db,
		store.dialect.Rebind(query),
		args,
		scan,
	)
	if err != nil || limit <= 0 || len(ret) < limit {
		return ret, err
	}
	lastBlock, lastTxIndex := midnightPosition(ret[len(ret)-1])
	lastID := midnightID(ret[len(ret)-1])
	extraQuery := "SELECT " + columns + " FROM " + table +
		" WHERE block_number = ? AND tx_index = ? AND id > ?" +
		" ORDER BY id ASC"
	extra, err := queryMidnightRows(
		ctx,
		db,
		store.dialect.Rebind(extraQuery),
		[]any{lastBlock, lastTxIndex, lastID},
		scan,
	)
	if err != nil {
		return nil, err
	}
	return append(ret, extra...), nil
}

func queryMidnightRows[T midnightPageRow](
	ctx context.Context,
	db queryer,
	query string,
	args []any,
	scan midnightRowScanner[T],
) ([]T, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := make([]T, 0)
	for rows.Next() {
		row, err := scan(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func scanMidnightAssetCreate(
	rows *sql.Rows,
) (models.MidnightAssetCreate, error) {
	var row models.MidnightAssetCreate
	err := rows.Scan(
		&row.ID,
		&row.Address,
		&row.Quantity,
		&row.TxHash,
		&row.OutputIndex,
		&row.BlockNumber,
		&row.BlockHash,
		&row.TxIndex,
		&row.BlockTimestampMs,
	)
	return row, err
}

func scanMidnightAssetSpend(
	rows *sql.Rows,
) (models.MidnightAssetSpend, error) {
	var row models.MidnightAssetSpend
	err := rows.Scan(
		&row.ID,
		&row.Address,
		&row.Quantity,
		&row.SpendingTxHash,
		&row.UtxoTxHash,
		&row.UtxoIndex,
		&row.BlockNumber,
		&row.BlockHash,
		&row.TxIndex,
		&row.BlockTimestampMs,
	)
	return row, err
}

func scanMidnightRegistration(
	rows *sql.Rows,
) (models.MidnightRegistration, error) {
	var row models.MidnightRegistration
	err := rows.Scan(
		&row.ID,
		&row.FullDatum,
		&row.TxHash,
		&row.OutputIndex,
		&row.BlockNumber,
		&row.BlockHash,
		&row.TxIndex,
		&row.BlockTimestampMs,
	)
	return row, err
}

func scanMidnightDeregistration(
	rows *sql.Rows,
) (models.MidnightDeregistration, error) {
	var row models.MidnightDeregistration
	err := rows.Scan(
		&row.ID,
		&row.FullDatum,
		&row.TxHash,
		&row.UtxoTxHash,
		&row.UtxoIndex,
		&row.BlockNumber,
		&row.BlockHash,
		&row.TxIndex,
		&row.BlockTimestampMs,
	)
	return row, err
}

func midnightPosition[T midnightPageRow](row T) (uint64, uint32) {
	switch value := any(row).(type) {
	case models.MidnightAssetCreate:
		return value.BlockNumber, value.TxIndex
	case models.MidnightAssetSpend:
		return value.BlockNumber, value.TxIndex
	case models.MidnightRegistration:
		return value.BlockNumber, value.TxIndex
	case models.MidnightDeregistration:
		return value.BlockNumber, value.TxIndex
	default:
		panic("unreachable Midnight row type")
	}
}

func midnightID[T midnightPageRow](row T) uint {
	switch value := any(row).(type) {
	case models.MidnightAssetCreate:
		return value.ID
	case models.MidnightAssetSpend:
		return value.ID
	case models.MidnightRegistration:
		return value.ID
	case models.MidnightDeregistration:
		return value.ID
	default:
		panic("unreachable Midnight row type")
	}
}

func bindPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}
