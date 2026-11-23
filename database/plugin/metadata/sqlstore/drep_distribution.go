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

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) SaveDRepDistributionSnapshot(
	snapshot *models.DRepDistributionSnapshot,
	txn types.Txn,
) error {
	if snapshot == nil {
		return errors.New("save DRep distribution snapshot: snapshot is nil")
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			epoch, err := checkedInt64(snapshot.Epoch)
			if err != nil {
				return err
			}
			boundarySlot, err := checkedInt64(snapshot.BoundarySlot)
			if err != nil {
				return err
			}
			id, err := s.operationalQueries(db).SaveDRepDistributionSnapshot(
				ctx,
				sqlitequery.SaveDRepDistributionSnapshotParams{
					Epoch:        epoch,
					Distribution: snapshot.Distribution,
					BoundarySlot: boundarySlot,
				},
			)
			if err != nil {
				return fmt.Errorf("save DRep distribution snapshot: %w", err)
			}
			snapshot.ID = uint(id)
			return nil
		},
	)
}

func (s *Store) GetDRepDistributionSnapshot(
	epoch uint64,
	txn types.Txn,
) (*models.DRepDistributionSnapshot, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	row, err := s.operationalQueries(db).
		GetDRepDistributionSnapshot(ctx, sqlEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get DRep distribution snapshot: %w", err)
	}
	return &models.DRepDistributionSnapshot{
		ID:           uint(row.ID),
		Epoch:        uint64(row.Epoch),
		Distribution: row.Distribution,
		BoundarySlot: uint64(row.BoundarySlot),
	}, nil
}
