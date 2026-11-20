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
	"errors"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// PrefetchBackfillInputs reads upcoming SQLite input rows through independent
// read-pool snapshots. The caller overlaps this with the ordered writer; the
// returned rows are deliberately discarded because the writer remains the
// only authority for transaction visibility and spend ordering.
func (s *Store) PrefetchBackfillInputs(
	ctx context.Context,
	refs []models.UtxoId,
) error {
	if s.dialect.Name() != "sqlite" || s.readDB == s.writeDB || len(refs) == 0 {
		return nil
	}
	refs = dedupeUtxoIDs(refs)
	workerCount := min(len(refs), s.ReadSnapshotLimit())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	for worker := range workerCount {
		start := len(refs) * worker / workerCount
		end := len(refs) * (worker + 1) / workerCount
		wg.Add(1)
		go func(batch []models.UtxoId) {
			defer wg.Done()
			txn := s.ReadTransaction(ctx)
			err := s.prefetchBackfillInputRows(ctx, txn, batch)
			rollbackErr := txn.Rollback()
			if err == nil {
				err = rollbackErr
			}
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}(refs[start:end])
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Store) prefetchBackfillInputRows(
	ctx context.Context,
	txn types.Txn,
	refs []models.UtxoId,
) error {
	db, _, err := s.readDBFromTxn(txn)
	if err != nil {
		return err
	}
	for start := 0; start < len(refs); start += overlayPrefetchChunk {
		end := min(start+overlayPrefetchChunk, len(refs))
		batch := refs[start:end]
		args := make([]any, 0, 2*len(batch))
		for _, ref := range batch {
			args = append(args, ref.Hash, ref.Idx)
		}
		rows, err := db.QueryContext(
			ctx,
			overlayPrefetchQuery(len(batch)),
			args...,
		)
		if err != nil {
			return err
		}
		err = scanBackfillPrefetchRows(rows)
		if err != nil {
			return err
		}
	}
	return nil
}

func scanBackfillPrefetchRows(rows *sql.Rows) error {
	defer rows.Close()
	for rows.Next() {
		var (
			id          int64
			txHash      []byte
			outputIdx   int64
			credential  int64
			stakingKey  []byte
			amount      sql.NullString
			deletedSlot sql.NullInt64
			spentAt     sql.NullInt64
		)
		if err := rows.Scan(
			&id,
			&txHash,
			&outputIdx,
			&credential,
			&stakingKey,
			&amount,
			&deletedSlot,
			&spentAt,
		); err != nil {
			return err
		}
	}
	return rows.Err()
}
