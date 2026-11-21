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

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) SaveGovernanceBoundarySnapshot(
	snapshot *models.GovernanceBoundarySnapshot,
	txn types.Txn,
) error {
	if snapshot == nil {
		return errors.New("save governance boundary snapshot: snapshot is nil")
	}
	epoch, err := checkedInt64(snapshot.Epoch)
	if err != nil {
		return err
	}
	boundarySlot, err := checkedInt64(snapshot.BoundarySlot)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode governance boundary snapshot: %w", err)
	}
	err = s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(
				ctx,
				s.dialect.Rebind(
					"DELETE FROM governance_boundary_snapshot WHERE epoch = ?",
				),
				epoch,
			); err != nil {
				return err
			}
			_, err := db.ExecContext(
				ctx,
				s.dialect.Rebind(
					"INSERT INTO governance_boundary_snapshot "+
						"(epoch, boundary_slot, snapshot) VALUES (?, ?, ?)",
				),
				epoch,
				boundarySlot,
				payload,
			)
			return err
		},
	)
	if err != nil {
		return fmt.Errorf("save governance boundary snapshot: %w", err)
	}
	return nil
}

func (s *Store) GetGovernanceBoundarySnapshotAtSlot(
	slot uint64,
	txn types.Txn,
) (*models.GovernanceBoundarySnapshot, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = db.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			"SELECT snapshot FROM governance_boundary_snapshot "+
				"WHERE boundary_slot <= ? ORDER BY boundary_slot DESC, epoch DESC LIMIT 1",
		),
		sqlSlot,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get governance boundary snapshot: %w", err)
	}
	var snapshot models.GovernanceBoundarySnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode governance boundary snapshot: %w", err)
	}
	return &snapshot, nil
}

func (s *Store) DeleteGovernanceBoundarySnapshotsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(
		ctx,
		s.dialect.Rebind(
			"DELETE FROM governance_boundary_snapshot WHERE boundary_slot > ?",
		),
		sqlSlot,
	); err != nil {
		return fmt.Errorf("delete governance boundary snapshots: %w", err)
	}
	return nil
}
