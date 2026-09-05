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
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (s *Store) operationalQueries(db queryer) *sqlitequery.Queries {
	return sqlitequery.New(newDialectQueryer(db, s.dialect.Name()))
}

func (s *Store) GetTip(txn types.Txn) (ochainsync.Tip, error) {
	var tip ochainsync.Tip
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return tip, err
	}
	queries := s.operationalQueries(db)
	row, err := queries.GetTip(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return tip, nil
	}
	if err != nil {
		return tip, fmt.Errorf("get tip: %w", err)
	}
	slot, err := checkedUint64(row.Slot.Int64)
	if err != nil {
		return tip, fmt.Errorf("get tip slot: %w", err)
	}
	blockNumber, err := checkedUint64(row.BlockNumber.Int64)
	if err != nil {
		return tip, fmt.Errorf("get tip block number: %w", err)
	}
	tip.Point.Slot = slot
	tip.Point.Hash = row.Hash
	tip.BlockNumber = blockNumber
	return tip, nil
}

func (s *Store) SetTip(tip ochainsync.Tip, txn types.Txn) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(tip.Point.Slot)
	if err != nil {
		return fmt.Errorf("set tip slot: %w", err)
	}
	blockNumber, err := checkedInt64(tip.BlockNumber)
	if err != nil {
		return fmt.Errorf("set tip block number: %w", err)
	}
	if err := queries.SetTip(
		ctx,
		sqlitequery.SetTipParams{
			Hash: tip.Point.Hash,
			Slot: sql.NullInt64{Int64: slot, Valid: true},
			BlockNumber: sql.NullInt64{
				Int64: blockNumber,
				Valid: true,
			},
		},
	); err != nil {
		return fmt.Errorf("set tip: %w", err)
	}
	return nil
}

func (s *Store) SetNetworkState(
	treasury, reserves uint64,
	slot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set network state: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("set network state: %w", err)
	}
	err = queries.SetNetworkState(
		ctx,
		sqlitequery.SetNetworkStateParams{
			Treasury: strconv.FormatUint(treasury, 10),
			Reserves: strconv.FormatUint(reserves, 10),
			Slot:     sqlSlot,
		},
	)
	if err != nil {
		return fmt.Errorf("set network state: %w", err)
	}
	return nil
}

func (s *Store) DeleteNetworkStateAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete network state after slot: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	if err := queries.DeleteNetworkStateAfterSlot(
		ctx,
		sqlSlot,
	); err != nil {
		return fmt.Errorf("delete network state after slot %d: %w", slot, err)
	}
	return nil
}

func (s *Store) GetNetworkState(
	txn types.Txn,
) (*models.NetworkState, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get network state: %w", err)
	}
	queries := s.operationalQueries(db)
	row, err := queries.GetLatestNetworkState(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get network state: %w", err)
	}
	treasury, err := strconv.ParseUint(row.Treasury, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("get network state treasury: %w", err)
	}
	reserves, err := strconv.ParseUint(row.Reserves, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("get network state reserves: %w", err)
	}
	slot, err := checkedUint64(row.Slot)
	if err != nil {
		return nil, fmt.Errorf("get network state slot: %w", err)
	}
	return &models.NetworkState{
		ID:       uint(row.ID),
		Treasury: types.Uint64(treasury),
		Reserves: types.Uint64(reserves),
		Slot:     slot,
	}, nil
}

func (s *Store) GetSyncState(key string, txn types.Txn) (string, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return "", fmt.Errorf("get sync state: %w", err)
	}
	queries := s.operationalQueries(db)
	value, err := queries.GetSyncState(ctx, key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get sync state: %w", err)
	}
	return value, nil
}

func (s *Store) SetSyncState(
	key, value string,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set sync state: %w", err)
	}
	queries := s.operationalQueries(db)
	if err := queries.SetSyncState(
		ctx,
		sqlitequery.SetSyncStateParams{SyncKey: key, Value: value},
	); err != nil {
		return fmt.Errorf("set sync state: %w", err)
	}
	return nil
}

func (s *Store) DeleteSyncState(key string, txn types.Txn) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete sync state: %w", err)
	}
	queries := s.operationalQueries(db)
	if err := queries.DeleteSyncState(ctx, key); err != nil {
		return fmt.Errorf("delete sync state: %w", err)
	}
	return nil
}

func (s *Store) ClearSyncState(txn types.Txn) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("clear sync state: %w", err)
	}
	queries := s.operationalQueries(db)
	if err := queries.ClearSyncState(ctx); err != nil {
		return fmt.Errorf("clear sync state: %w", err)
	}
	return nil
}

func (s *Store) GetEpoch(
	epochID uint64,
	txn types.Txn,
) (*models.Epoch, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get epoch: %w", err)
	}
	queries := s.operationalQueries(db)
	id, err := checkedInt64(epochID)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetEpoch(
		ctx,
		sql.NullInt64{Int64: id, Valid: true},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get epoch: %w", err)
	}
	epoch, err := epochFromValues(
		row.ID,
		row.EpochID,
		row.StartSlot,
		row.Nonce,
		row.EvolvingNonce,
		row.CandidateNonce,
		row.LastEpochBlockNonce,
		row.EraID,
		row.SlotLength,
		row.LengthInSlots,
	)
	if err != nil {
		return nil, fmt.Errorf("get epoch: %w", err)
	}
	return epoch, nil
}

func (s *Store) GetEpochsByEra(
	eraID uint,
	txn types.Txn,
) ([]models.Epoch, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get epochs by era: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlEraID, err := checkedInt64(uint64(eraID))
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetEpochsByEra(
		ctx,
		sql.NullInt64{Int64: sqlEraID, Valid: true},
	)
	if err != nil {
		return nil, fmt.Errorf("get epochs by era: %w", err)
	}
	ret := make([]models.Epoch, 0, len(rows))
	for _, row := range rows {
		epoch, err := epochFromValues(
			row.ID,
			row.EpochID,
			row.StartSlot,
			row.Nonce,
			row.EvolvingNonce,
			row.CandidateNonce,
			row.LastEpochBlockNonce,
			row.EraID,
			row.SlotLength,
			row.LengthInSlots,
		)
		if err != nil {
			return nil, fmt.Errorf("get epochs by era: %w", err)
		}
		ret = append(ret, *epoch)
	}
	return ret, nil
}

func (s *Store) GetEpochs(txn types.Txn) ([]models.Epoch, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get epochs: %w", err)
	}
	queries := s.operationalQueries(db)
	rows, err := queries.GetEpochs(ctx)
	if err != nil {
		return nil, fmt.Errorf("get epochs: %w", err)
	}
	ret := make([]models.Epoch, 0, len(rows))
	for _, row := range rows {
		epoch, err := epochFromValues(
			row.ID,
			row.EpochID,
			row.StartSlot,
			row.Nonce,
			row.EvolvingNonce,
			row.CandidateNonce,
			row.LastEpochBlockNonce,
			row.EraID,
			row.SlotLength,
			row.LengthInSlots,
		)
		if err != nil {
			return nil, fmt.Errorf("get epochs: %w", err)
		}
		ret = append(ret, *epoch)
	}
	return ret, nil
}

func (s *Store) GetEpochBySlot(
	slot uint64,
	txn types.Txn,
) (*models.Epoch, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get epoch by slot: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetEpochBySlot(
		ctx,
		sqlitequery.GetEpochBySlotParams{
			StartSlot: sql.NullInt64{Int64: sqlSlot, Valid: true},
			StartSlot_2: sql.NullInt64{
				Int64: sqlSlot,
				Valid: true,
			},
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get epoch by slot: %w", err)
	}
	epoch, err := epochFromValues(
		row.ID,
		row.EpochID,
		row.StartSlot,
		row.Nonce,
		row.EvolvingNonce,
		row.CandidateNonce,
		row.LastEpochBlockNonce,
		row.EraID,
		row.SlotLength,
		row.LengthInSlots,
	)
	if err != nil {
		return nil, fmt.Errorf("get epoch by slot: %w", err)
	}
	return epoch, nil
}

func (s *Store) DeleteEpochsAfterSlot(slot uint64, txn types.Txn) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete epochs after slot: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	if err := queries.DeleteEpochsAfterSlot(
		ctx,
		sql.NullInt64{Int64: sqlSlot, Valid: true},
	); err != nil {
		return fmt.Errorf("delete epochs after slot: %w", err)
	}
	return nil
}

func (s *Store) SetEpoch(
	slot, epoch uint64,
	nonce, evolvingNonce, candidateNonce, lastEpochBlockNonce []byte,
	era, slotLength, lengthInSlots uint,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set epoch: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	sqlEra, err := checkedInt64(uint64(era))
	if err != nil {
		return err
	}
	sqlSlotLength, err := checkedInt64(uint64(slotLength))
	if err != nil {
		return err
	}
	sqlLengthInSlots, err := checkedInt64(uint64(lengthInSlots))
	if err != nil {
		return err
	}
	if err := queries.SetEpoch(
		ctx,
		sqlitequery.SetEpochParams{
			EpochID: sql.NullInt64{Int64: sqlEpoch, Valid: true},
			StartSlot: sql.NullInt64{
				Int64: sqlSlot,
				Valid: true,
			},
			Nonce:               nonce,
			EvolvingNonce:       evolvingNonce,
			CandidateNonce:      candidateNonce,
			LastEpochBlockNonce: lastEpochBlockNonce,
			EraID: sql.NullInt64{
				Int64: sqlEra,
				Valid: true,
			},
			SlotLength: sql.NullInt64{
				Int64: sqlSlotLength,
				Valid: true,
			},
			LengthInSlots: sql.NullInt64{
				Int64: sqlLengthInSlots,
				Valid: true,
			},
		},
	); err != nil {
		return fmt.Errorf("set epoch: %w", err)
	}
	return nil
}

func (s *Store) SetBlockNonce(
	blockHash []byte,
	slotNumber uint64,
	nonce []byte,
	isCheckpoint bool,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(slotNumber)
	if err != nil {
		return err
	}
	return queries.SetBlockNonce(
		ctx,
		sqlitequery.SetBlockNonceParams{
			Hash:  blockHash,
			Slot:  sql.NullInt64{Int64: slot, Valid: true},
			Nonce: nonce,
			IsCheckpoint: sql.NullBool{
				Bool:  isCheckpoint,
				Valid: true,
			},
		},
	)
}

func (s *Store) GetBlockNonce(
	point ocommon.Point,
	txn types.Txn,
) ([]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(point.Slot)
	if err != nil {
		return nil, err
	}
	nonce, err := queries.GetBlockNonce(
		ctx,
		sqlitequery.GetBlockNonceParams{
			Hash: point.Hash,
			Slot: sql.NullInt64{Int64: slot, Valid: true},
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return nonce, err
}

func (s *Store) GetBlockNoncesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) ([]models.BlockNonce, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	start, err := checkedInt64(startSlot)
	if err != nil {
		return nil, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetBlockNoncesInSlotRange(
		ctx,
		sqlitequery.GetBlockNoncesInSlotRangeParams{
			Slot:   sql.NullInt64{Int64: start, Valid: true},
			Slot_2: sql.NullInt64{Int64: end, Valid: true},
		},
	)
	if err != nil {
		return nil, err
	}
	ret := make([]models.BlockNonce, 0, len(rows))
	for _, row := range rows {
		slot, err := checkedUint64(row.Slot.Int64)
		if err != nil {
			return nil, fmt.Errorf("get block nonces in slot range: %w", err)
		}
		ret = append(ret, models.BlockNonce{
			Hash:         row.Hash,
			Nonce:        row.Nonce,
			ID:           uint(row.ID),
			Slot:         slot,
			IsCheckpoint: row.IsCheckpoint.Bool,
		})
	}
	return ret, nil
}

func (s *Store) GetLastBlockNonceInRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) ([]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	start, err := checkedInt64(startSlot)
	if err != nil {
		return nil, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return nil, err
	}
	nonce, err := queries.GetLastBlockNonceInRange(
		ctx,
		sqlitequery.GetLastBlockNonceInRangeParams{
			Slot:   sql.NullInt64{Int64: start, Valid: true},
			Slot_2: sql.NullInt64{Int64: end, Valid: true},
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return nonce, err
}

// GetLatestBlockNonce returns the highest-slot nonce row. The nonce is
// written in the same metadata transaction as the corresponding ledger
// effects, so this row is the durable ledger-state high-water mark.
func (s *Store) GetLatestBlockNonce(
	txn types.Txn,
) (models.BlockNonce, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return models.BlockNonce{}, false, err
	}
	row := db.QueryRowContext(ctx,
		`SELECT hash, nonce, id, slot, is_checkpoint
		 FROM block_nonce
		 ORDER BY slot DESC, hash DESC
		 LIMIT 1`,
	)
	var ret models.BlockNonce
	var slot sql.NullInt64
	var checkpoint sql.NullBool
	if err := row.Scan(
		&ret.Hash, &ret.Nonce, &ret.ID, &slot, &checkpoint,
	); errors.Is(err, sql.ErrNoRows) {
		return models.BlockNonce{}, false, nil
	} else if err != nil {
		return models.BlockNonce{}, false, err
	}
	if slot.Valid {
		s, err := checkedUint64(slot.Int64)
		if err != nil {
			return models.BlockNonce{}, false, fmt.Errorf(
				"get latest block nonce: %w",
				err,
			)
		}
		ret.Slot = s
	}
	if checkpoint.Valid {
		ret.IsCheckpoint = checkpoint.Bool
	}
	return ret, true, nil
}

func (s *Store) DeleteBlockNoncesBeforeSlot(
	slotNumber uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(slotNumber)
	if err != nil {
		return err
	}
	return queries.DeleteBlockNoncesBeforeSlot(
		ctx,
		sql.NullInt64{Int64: slot, Valid: true},
	)
}

func (s *Store) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
	slotNumber uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(slotNumber)
	if err != nil {
		return err
	}
	return queries.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		ctx,
		sql.NullInt64{Int64: slot, Valid: true},
	)
}

func (s *Store) DeleteBlockNoncesAfterPoint(
	point ocommon.Point,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(point.Slot)
	if err != nil {
		return err
	}
	if len(point.Hash) == 0 {
		return queries.DeleteBlockNoncesAfterOrigin(
			ctx,
			sql.NullInt64{Int64: slot, Valid: true},
		)
	}
	return queries.DeleteBlockNoncesAfterPoint(
		ctx,
		sqlitequery.DeleteBlockNoncesAfterPointParams{
			Slot:   sql.NullInt64{Int64: slot, Valid: true},
			Slot_2: sql.NullInt64{Int64: slot, Valid: true},
			Hash:   point.Hash,
		},
	)
}

func (s *Store) GetDatum(
	hash lcommon.Blake2b256,
	txn types.Txn,
) (*models.Datum, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	row, err := queries.GetDatum(ctx, hash[:])
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	addedSlot, err := checkedUint64(row.AddedSlot)
	if err != nil {
		return nil, fmt.Errorf("get datum: %w", err)
	}
	return &models.Datum{
		Hash:      row.Hash,
		RawDatum:  row.RawDatum,
		ID:        uint(row.ID),
		AddedSlot: addedSlot,
	}, nil
}

func (s *Store) SetDatum(
	hash lcommon.Blake2b256,
	rawDatum []byte,
	addedSlot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	slot, err := checkedInt64(addedSlot)
	if err != nil {
		return err
	}
	if err := queries.SetDatum(
		ctx,
		sqlitequery.SetDatumParams{
			Hash:      hash[:],
			RawDatum:  rawDatum,
			AddedSlot: slot,
		},
	); err != nil {
		return fmt.Errorf("create datum: %w", err)
	}
	return nil
}

func (s *Store) GetScript(
	hash lcommon.ScriptHash,
	txn types.Txn,
) (*models.Script, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	row, err := queries.GetScript(ctx, hash[:])
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	createdSlot, err := checkedUint64(row.CreatedSlot.Int64)
	if err != nil {
		return nil, fmt.Errorf("get script: %w", err)
	}
	scriptType, err := checkedUint8(row.Type.Int64)
	if err != nil {
		return nil, fmt.Errorf("get script: %w", err)
	}
	return &models.Script{
		Hash:        row.Hash,
		Content:     row.Content,
		ID:          uint(row.ID),
		CreatedSlot: createdSlot,
		Type:        scriptType,
	}, nil
}

func (s *Store) GetPParams(
	epoch uint64,
	eraID uint,
	txn types.Txn,
) ([]models.PParams, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	sqlEraID, err := checkedInt64(uint64(eraID))
	if err != nil {
		return nil, err
	}
	rows, err := queries.GetPParams(
		ctx,
		sqlitequery.GetPParamsParams{
			Epoch: sql.NullInt64{Int64: sqlEpoch, Valid: true},
			EraID: sql.NullInt64{Int64: sqlEraID, Valid: true},
		},
	)
	if err != nil {
		return nil, err
	}
	ret := make([]models.PParams, 0, len(rows))
	for _, row := range rows {
		addedSlot, err := checkedUint64(row.AddedSlot.Int64)
		if err != nil {
			return nil, fmt.Errorf("get pparams: %w", err)
		}
		epoch, err := checkedUint64(row.Epoch.Int64)
		if err != nil {
			return nil, fmt.Errorf("get pparams: %w", err)
		}
		eraId, err := checkedUint(row.EraID.Int64)
		if err != nil {
			return nil, fmt.Errorf("get pparams: %w", err)
		}
		ret = append(ret, models.PParams{
			Cbor:      row.Cbor,
			ID:        uint(row.ID),
			AddedSlot: addedSlot,
			Epoch:     epoch,
			EraId:     eraId,
		})
	}
	return ret, nil
}

func (s *Store) GetPParamUpdates(
	epoch uint64,
	txn types.Txn,
) ([]models.PParamUpdate, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	queries := s.operationalQueries(db)
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	previousEpoch := sqlEpoch
	if epoch > 0 {
		previousEpoch--
	}
	rows, err := queries.GetPParamUpdates(
		ctx,
		sqlitequery.GetPParamUpdatesParams{
			Epoch: sql.NullInt64{
				Int64: sqlEpoch,
				Valid: true,
			},
			Epoch_2: sql.NullInt64{
				Int64: previousEpoch,
				Valid: true,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	ret := make([]models.PParamUpdate, 0, len(rows))
	for _, row := range rows {
		addedSlot, err := checkedUint64(row.AddedSlot.Int64)
		if err != nil {
			return nil, fmt.Errorf("get pparam updates: %w", err)
		}
		epoch, err := checkedUint64(row.Epoch.Int64)
		if err != nil {
			return nil, fmt.Errorf("get pparam updates: %w", err)
		}
		ret = append(ret, models.PParamUpdate{
			GenesisHash: row.GenesisHash,
			Cbor:        row.Cbor,
			ID:          uint(row.ID),
			AddedSlot:   addedSlot,
			Epoch:       epoch,
		})
	}
	return ret, nil
}

func (s *Store) SetPParams(
	params []byte,
	slot, epoch uint64,
	eraID uint,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	return queries.SetPParams(
		ctx,
		sqlitequery.SetPParamsParams{
			Cbor: params,
			AddedSlot: sql.NullInt64{
				Int64: sqlSlot,
				Valid: true,
			},
			Epoch: sql.NullInt64{
				Int64: sqlEpoch,
				Valid: true,
			},
			EraID: sql.NullInt64{
				Int64: int64(eraID),
				Valid: true,
			},
		},
	)
}

func (s *Store) SetPParamUpdate(
	genesis, update []byte,
	slot, epoch uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	return queries.SetPParamUpdate(
		ctx,
		sqlitequery.SetPParamUpdateParams{
			GenesisHash: genesis,
			Cbor:        update,
			AddedSlot: sql.NullInt64{
				Int64: sqlSlot,
				Valid: true,
			},
			Epoch: sql.NullInt64{
				Int64: sqlEpoch,
				Valid: true,
			},
		},
	)
}

func (s *Store) DeletePParamsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return queries.DeletePParamsAfterSlot(
		ctx,
		sql.NullInt64{Int64: sqlSlot, Valid: true},
	)
}

func (s *Store) DeletePParamUpdatesAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return queries.DeletePParamUpdatesAfterSlot(
		ctx,
		sql.NullInt64{Int64: sqlSlot, Valid: true},
	)
}

func (s *Store) AddNetworkDonation(
	slot, epoch, amount uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("add network donation: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	sqlAmount, err := checkedInt64(amount)
	if err != nil {
		return err
	}
	if err := queries.AddNetworkDonation(
		ctx,
		sqlitequery.AddNetworkDonationParams{
			Slot:   sqlSlot,
			Epoch:  sqlEpoch,
			Amount: sqlAmount,
		},
	); err != nil {
		return fmt.Errorf("add network donation: %w", err)
	}
	return nil
}

func (s *Store) SumNetworkDonationsForEpoch(
	epoch uint64,
	txn types.Txn,
) (uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, fmt.Errorf("sum network donations: %w", err)
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return 0, err
	}
	total, err := sumUint64Rows(ctx, db, s.dialect.Rebind(`
SELECT amount FROM network_donation WHERE epoch = ?`), sqlEpoch)
	if err != nil {
		return 0, fmt.Errorf(
			"sum network donations for epoch %d: %w",
			epoch,
			err,
		)
	}
	return total, nil
}

func (s *Store) DeleteNetworkDonationsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete network donations after slot: %w", err)
	}
	queries := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	if err := queries.DeleteNetworkDonationsAfterSlot(
		ctx,
		sqlSlot,
	); err != nil {
		return fmt.Errorf(
			"delete network donations after slot %d: %w",
			slot,
			err,
		)
	}
	return nil
}

func (s *Store) GetImportCheckpoint(
	importKey string,
	txn types.Txn,
) (*models.ImportCheckpoint, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get import checkpoint: %w", err)
	}
	queries := s.operationalQueries(db)
	row, err := queries.GetImportCheckpoint(
		ctx,
		importKey,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get import checkpoint: %w", err)
	}
	return &models.ImportCheckpoint{
		ID:        uint(row.ID),
		ImportKey: row.ImportKey,
		Phase:     row.Phase,
	}, nil
}

func (s *Store) SetImportCheckpoint(
	checkpoint *models.ImportCheckpoint,
	txn types.Txn,
) error {
	if checkpoint == nil {
		return errors.New("set import checkpoint: checkpoint is nil")
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set import checkpoint: %w", err)
	}
	queries := s.operationalQueries(db)
	if err := queries.SetImportCheckpoint(
		ctx,
		sqlitequery.SetImportCheckpointParams{
			ImportKey: checkpoint.ImportKey,
			Phase:     checkpoint.Phase,
		},
	); err != nil {
		return fmt.Errorf("set import checkpoint: %w", err)
	}
	return nil
}

func epochFromValues(
	id int64,
	epochID, startSlot sql.NullInt64,
	nonce, evolvingNonce, candidateNonce, lastEpochBlockNonce []byte,
	eraID, slotLength, lengthInSlots sql.NullInt64,
) (*models.Epoch, error) {
	epochId, err := checkedUint64(epochID.Int64)
	if err != nil {
		return nil, fmt.Errorf("epoch id: %w", err)
	}
	startSlotVal, err := checkedUint64(startSlot.Int64)
	if err != nil {
		return nil, fmt.Errorf("epoch start slot: %w", err)
	}
	eraIdVal, err := checkedUint(eraID.Int64)
	if err != nil {
		return nil, fmt.Errorf("epoch era id: %w", err)
	}
	slotLengthVal, err := checkedUint(slotLength.Int64)
	if err != nil {
		return nil, fmt.Errorf("epoch slot length: %w", err)
	}
	lengthInSlotsVal, err := checkedUint(lengthInSlots.Int64)
	if err != nil {
		return nil, fmt.Errorf("epoch length in slots: %w", err)
	}
	return &models.Epoch{
		Nonce:               nonce,
		EvolvingNonce:       evolvingNonce,
		CandidateNonce:      candidateNonce,
		LastEpochBlockNonce: lastEpochBlockNonce,
		ID:                  uint(id),
		EpochId:             epochId,
		StartSlot:           startSlotVal,
		EraId:               eraIdVal,
		SlotLength:          slotLengthVal,
		LengthInSlots:       lengthInSlotsVal,
	}, nil
}

func checkedInt64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("unsigned SQL value %d exceeds int64", value)
	}
	return int64(value), nil
}

// checkedUint64 is the reverse of checkedInt64: SQLite's INTEGER columns
// are signed, so a row corrupted or tampered with outside normal writes can
// surface a negative value here. Converting that directly to uint64 would
// silently produce a near-MaxUint64 chain point instead of failing.
func checkedUint64(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("signed SQL value %d is negative", value)
	}
	return uint64(value), nil
}

// checkedUint8 narrows a signed SQL column to uint8. script.type is an
// unconstrained SQLite INTEGER, so a row corrupted or tampered with
// outside normal writes can hold a value outside [0, 255]; converting
// that directly would silently wrap instead of failing.
func checkedUint8(value int64) (uint8, error) {
	if value < 0 || value > math.MaxUint8 {
		return 0, fmt.Errorf("signed SQL value %d does not fit in uint8", value)
	}
	return uint8(value), nil
}

// checkedUint narrows a signed SQL column to uint. Several columns (era
// id, slot length, length in slots) are unconstrained SQLite INTEGERs
// read into a plain uint: a negative stored value would otherwise
// reinterpret its bit pattern as unsigned (e.g. -1 silently becoming
// MaxUint) instead of failing, and on a 32-bit build uint is only 32 bits
// wide, so the upper bound is checked against this platform's actual
// uint width (via ^uint(0), not a hardcoded 64-bit assumption).
func checkedUint(value int64) (uint, error) {
	if value < 0 {
		return 0, fmt.Errorf("signed SQL value %d is negative", value)
	}
	if uint64(value) > uint64(^uint(0)) {
		return 0, fmt.Errorf("signed SQL value %d exceeds uint width", value)
	}
	return uint(value), nil
}
