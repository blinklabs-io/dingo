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

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetBackfillCheckpoint(
	phase string,
	txn types.Txn,
) (*models.BackfillCheckpoint, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get backfill checkpoint: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetBackfillCheckpoint(context.Background(), phase)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get backfill checkpoint: %w", err)
	}
	return &models.BackfillCheckpoint{
		ID:         uint(row.ID),
		Phase:      row.Phase,
		LastSlot:   uint64(row.LastSlot.Int64),
		TotalSlots: uint64(row.TotalSlots.Int64),
		StartedAt:  row.StartedAt.Time,
		UpdatedAt:  row.UpdatedAt.Time,
		Completed:  row.Completed.Bool,
	}, nil
}

func (s *Store) SetBackfillCheckpoint(
	checkpoint *models.BackfillCheckpoint,
	txn types.Txn,
) error {
	if checkpoint == nil {
		return errors.New("set backfill checkpoint: checkpoint is nil")
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set backfill checkpoint: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	lastSlot, err := checkedInt64(checkpoint.LastSlot)
	if err != nil {
		return fmt.Errorf("set backfill checkpoint last slot: %w", err)
	}
	totalSlots, err := checkedInt64(checkpoint.TotalSlots)
	if err != nil {
		return fmt.Errorf("set backfill checkpoint total slots: %w", err)
	}
	id, err := queries.SetBackfillCheckpoint(
		context.Background(),
		sqlitequery.SetBackfillCheckpointParams{
			Phase:      checkpoint.Phase,
			LastSlot:   sql.NullInt64{Int64: lastSlot, Valid: true},
			TotalSlots: sql.NullInt64{Int64: totalSlots, Valid: true},
			StartedAt: sql.NullTime{
				Time:  checkpoint.StartedAt,
				Valid: true,
			},
			UpdatedAt: sql.NullTime{
				Time:  checkpoint.UpdatedAt,
				Valid: true,
			},
			Completed: sql.NullBool{
				Bool:  checkpoint.Completed,
				Valid: true,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("set backfill checkpoint: %w", err)
	}
	checkpoint.ID = uint(id)
	return nil
}

func (s *Store) GetConstitution(
	txn types.Txn,
) (*models.Constitution, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get constitution: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	row, err := queries.GetConstitution(context.Background())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get constitution: %w", err)
	}
	return constitutionFromSQLite(row), nil
}

func (s *Store) SetConstitution(
	constitution *models.Constitution,
	txn types.Txn,
) error {
	if constitution == nil {
		return errors.New("set constitution: constitution is nil")
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set constitution: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	addedSlot, err := checkedInt64(constitution.AddedSlot)
	if err != nil {
		return fmt.Errorf("set constitution added slot: %w", err)
	}
	deletedSlot, err := nullableUint64(constitution.DeletedSlot)
	if err != nil {
		return fmt.Errorf("set constitution deleted slot: %w", err)
	}
	id, err := queries.SetConstitution(
		context.Background(),
		sqlitequery.SetConstitutionParams{
			AnchorUrl:   constitution.AnchorURL,
			AnchorHash:  constitution.AnchorHash,
			PolicyHash:  constitution.PolicyHash,
			AddedSlot:   addedSlot,
			DeletedSlot: deletedSlot,
		},
	)
	if err != nil {
		return fmt.Errorf("set constitution: %w", err)
	}
	constitution.ID = uint(id)
	return nil
}

func (s *Store) DeleteConstitutionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("delete constitutions after slot: %w", err)
	}
	err = s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			queries, err := s.sqliteQueries(db)
			if err != nil {
				return err
			}
			if err := queries.DeleteConstitutionsAddedAfterSlot(
				context.Background(),
				sqlSlot,
			); err != nil {
				return err
			}
			return queries.RestoreConstitutionsDeletedAfterSlot(
				context.Background(),
				sql.NullInt64{Int64: sqlSlot, Valid: true},
			)
		},
	)
	if err != nil {
		return fmt.Errorf("delete constitutions after slot: %w", err)
	}
	return nil
}

func (s *Store) SetCommitteeMembers(
	members []*models.CommitteeMember,
	txn types.Txn,
) error {
	if len(members) == 0 {
		return nil
	}
	err := s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			queries, err := s.sqliteQueries(db)
			if err != nil {
				return err
			}
			for _, member := range members {
				if member == nil {
					return errors.New("committee member is nil")
				}
				expiresEpoch, err := checkedInt64(member.ExpiresEpoch)
				if err != nil {
					return err
				}
				addedSlot, err := checkedInt64(member.AddedSlot)
				if err != nil {
					return err
				}
				deletedSlot, err := nullableUint64(member.DeletedSlot)
				if err != nil {
					return err
				}
				id, err := queries.SetCommitteeMember(
					context.Background(),
					sqlitequery.SetCommitteeMemberParams{
						ColdCredHash: member.ColdCredHash,
						ExpiresEpoch: expiresEpoch,
						AddedSlot:    addedSlot,
						DeletedSlot:  deletedSlot,
					},
				)
				if err != nil {
					return err
				}
				member.ID = uint(id)
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("set committee members: %w", err)
	}
	return nil
}

func (s *Store) SetCommitteeQuorum(
	quorum *types.Rat,
	slot uint64,
	txn types.Txn,
) error {
	if quorum == nil || quorum.Rat == nil {
		return errors.New("committee quorum cannot be nil")
	}
	return s.setCommitteeQuorum(quorum.String(), slot, txn)
}

func (s *Store) ClearCommitteeQuorum(
	slot uint64,
	txn types.Txn,
) error {
	return s.setCommitteeQuorum("0", slot, txn)
}

func (s *Store) setCommitteeQuorum(
	quorum string,
	slot uint64,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set committee quorum: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("set committee quorum: %w", err)
	}
	if err := queries.SetCommitteeQuorum(
		context.Background(),
		sqlitequery.SetCommitteeQuorumParams{
			Quorum:    sql.NullString{String: quorum, Valid: true},
			AddedSlot: sqlSlot,
		},
	); err != nil {
		return fmt.Errorf("set committee quorum: %w", err)
	}
	return nil
}

func (s *Store) GetCommitteeQuorum(
	txn types.Txn,
) (*types.Rat, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get committee quorum: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	value, err := queries.GetCommitteeQuorum(context.Background())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get committee quorum: %w", err)
	}
	if !value.Valid {
		return nil, nil
	}
	rat, ok := new(big.Rat).SetString(value.String)
	if !ok {
		return nil, fmt.Errorf(
			"get committee quorum: invalid rational %q",
			value.String,
		)
	}
	if rat.Sign() <= 0 {
		return nil, nil
	}
	return &types.Rat{Rat: rat}, nil
}

func (s *Store) GetCommitteeMembers(
	txn types.Txn,
) ([]*models.CommitteeMember, error) {
	return s.getCommitteeMembers(txn, false)
}

func (s *Store) GetCommitteeMembersIncludeDeleted(
	txn types.Txn,
) ([]*models.CommitteeMember, error) {
	return s.getCommitteeMembers(txn, true)
}

func (s *Store) getCommitteeMembers(
	txn types.Txn,
	includeDeleted bool,
) ([]*models.CommitteeMember, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("get committee members: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	var rows []sqlitequery.CommitteeMember
	if includeDeleted {
		rows, err = queries.GetCommitteeMembersIncludeDeleted(
			context.Background(),
		)
	} else {
		rows, err = queries.GetCommitteeMembers(context.Background())
	}
	if err != nil {
		return nil, fmt.Errorf("get committee members: %w", err)
	}
	ret := make([]*models.CommitteeMember, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, committeeMemberFromSQLite(row))
	}
	return ret, nil
}

func (s *Store) SoftDeleteCommitteeMembers(
	coldCredHashes [][]byte,
	slot uint64,
	txn types.Txn,
) error {
	if len(coldCredHashes) == 0 {
		return nil
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("soft delete committee members: %w", err)
	}
	err = s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			queries, err := s.sqliteQueries(db)
			if err != nil {
				return err
			}
			for _, hash := range coldCredHashes {
				if err := queries.SoftDeleteCommitteeMember(
					context.Background(),
					sqlitequery.SoftDeleteCommitteeMemberParams{
						DeletedSlot: sql.NullInt64{
							Int64: sqlSlot,
							Valid: true,
						},
						ColdCredHash: hash,
					},
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("soft delete committee members: %w", err)
	}
	return nil
}

func (s *Store) SoftDeleteAllCommitteeMembers(
	slot uint64,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("soft delete all committee members: %w", err)
	}
	queries, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("soft delete all committee members: %w", err)
	}
	if err := queries.SoftDeleteAllCommitteeMembers(
		context.Background(),
		sql.NullInt64{Int64: sqlSlot, Valid: true},
	); err != nil {
		return fmt.Errorf("soft delete all committee members: %w", err)
	}
	return nil
}

func (s *Store) DeleteCommitteeMembersAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return fmt.Errorf("delete committee members after slot: %w", err)
	}
	err = s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			queries, err := s.sqliteQueries(db)
			if err != nil {
				return err
			}
			if err := queries.DeleteCommitteeMembersAddedAfterSlot(
				context.Background(),
				sqlSlot,
			); err != nil {
				return err
			}
			if err := queries.DeleteCommitteeQuorumsAfterSlot(
				context.Background(),
				sqlSlot,
			); err != nil {
				return err
			}
			return queries.RestoreCommitteeMembersDeletedAfterSlot(
				context.Background(),
				sql.NullInt64{Int64: sqlSlot, Valid: true},
			)
		},
	)
	if err != nil {
		return fmt.Errorf("delete committee members after slot: %w", err)
	}
	return nil
}

func constitutionFromSQLite(
	row sqlitequery.Constitution,
) *models.Constitution {
	return &models.Constitution{
		ID:          uint(row.ID),
		AnchorURL:   row.AnchorUrl,
		AnchorHash:  row.AnchorHash,
		PolicyHash:  row.PolicyHash,
		AddedSlot:   uint64(row.AddedSlot),
		DeletedSlot: uint64Pointer(row.DeletedSlot),
	}
}

func committeeMemberFromSQLite(
	row sqlitequery.CommitteeMember,
) *models.CommitteeMember {
	return &models.CommitteeMember{
		ID:           uint(row.ID),
		ColdCredHash: row.ColdCredHash,
		ExpiresEpoch: uint64(row.ExpiresEpoch),
		AddedSlot:    uint64(row.AddedSlot),
		DeletedSlot:  uint64Pointer(row.DeletedSlot),
	}
}

func nullableUint64(value *uint64) (sql.NullInt64, error) {
	if value == nil {
		return sql.NullInt64{}, nil
	}
	converted, err := checkedInt64(*value)
	if err != nil {
		return sql.NullInt64{}, err
	}
	return sql.NullInt64{Int64: converted, Valid: true}, nil
}

func uint64Pointer(value sql.NullInt64) *uint64 {
	if !value.Valid {
		return nil
	}
	ret := uint64(value.Int64)
	return &ret
}
