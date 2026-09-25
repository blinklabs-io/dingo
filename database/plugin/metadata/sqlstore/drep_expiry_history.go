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
	"fmt"
	"math"
	"strings"

	"github.com/blinklabs-io/dingo/database/types"
)

func recordDrepExpiryHistory(
	ctx context.Context,
	db queryer,
	tag uint8,
	credential []byte,
	slot uint64,
	dialect string,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	insert := `INSERT INTO drep_expiry_history (
    credential_tag, credential, added_slot,
    previous_expiry_epoch, previous_last_activity_epoch
)
SELECT credential_tag, credential, ?, expiry_epoch, last_activity_epoch
FROM drep
WHERE credential_tag = ? AND credential = ?`
	if dialect == "mysql" {
		insert = strings.Replace(insert, "INSERT INTO", "INSERT IGNORE INTO", 1)
	} else {
		insert += "\nON CONFLICT (credential_tag, credential, added_slot) DO NOTHING"
	}
	_, err = db.ExecContext(ctx, insert,
		slotValue,
		tag,
		credential,
	)
	return err
}

func (s *Store) BumpDormantDRepExpiries(
	slot uint64,
	txn types.Txn,
) (int, error) {
	var affected int
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			slotValue, err := checkedInt64(slot)
			if err != nil {
				return err
			}
			markerQuery := `INSERT INTO drep_expiry_epoch_event (added_slot) VALUES (?)`
			if s.dialect.Name() == "mysql" {
				markerQuery = strings.Replace(markerQuery, "INSERT INTO", "INSERT IGNORE INTO", 1)
			} else {
				markerQuery += " ON CONFLICT (added_slot) DO NOTHING"
			}
			marker, err := db.ExecContext(ctx, markerQuery, slotValue)
			if err != nil {
				return fmt.Errorf("record dormant DRep expiry epoch: %w", err)
			}
			inserted, err := marker.RowsAffected()
			if err != nil {
				return err
			}
			if inserted == 0 {
				return nil
			}
			if err := s.incrementDormantDRepEpochs(db, ctx, slotValue); err != nil {
				return err
			}

			var overflows bool
			if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM drep
    WHERE active = TRUE AND expiry_epoch >= ?
)`, int64(math.MaxInt64)).Scan(&overflows); err != nil {
				return fmt.Errorf("check DRep expiry overflow: %w", err)
			}
			if overflows {
				return fmt.Errorf("dormant DRep expiry exceeds storage range at slot %d", slot)
			}

			historyQuery := `INSERT INTO drep_expiry_history (
    credential_tag, credential, added_slot,
    previous_expiry_epoch, previous_last_activity_epoch
)
SELECT credential_tag, credential, ?, expiry_epoch, last_activity_epoch
FROM drep
WHERE active = TRUE AND expiry_epoch > 0`
			if s.dialect.Name() == "mysql" {
				historyQuery = strings.Replace(historyQuery, "INSERT INTO", "INSERT IGNORE INTO", 1)
			} else {
				historyQuery += "\nON CONFLICT (credential_tag, credential, added_slot) DO NOTHING"
			}
			if _, err := db.ExecContext(ctx, historyQuery,
				slotValue,
			); err != nil {
				return fmt.Errorf("record dormant DRep expiries: %w", err)
			}
			result, err := db.ExecContext(ctx, `
UPDATE drep
SET expiry_epoch = expiry_epoch + 1
WHERE active = TRUE AND expiry_epoch > 0`)
			if err != nil {
				return fmt.Errorf("bump dormant DRep expiries: %w", err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			affected = int(rows)
			return nil
		},
	)
	return affected, err
}

func (s *Store) GetDormantDRepEpochs(txn types.Txn) (uint64, error) {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return 0, err
	}
	if err := s.ensureDormantDRepState(db, ctx); err != nil {
		return 0, err
	}
	var dormant int64
	if err := db.QueryRowContext(ctx, `
SELECT dormant_epochs
FROM drep_dormancy_state
WHERE id = 1`).Scan(&dormant); err != nil {
		return 0, fmt.Errorf("get dormant DRep epoch count: %w", err)
	}
	if dormant < 0 {
		return 0, fmt.Errorf("invalid negative dormant DRep epoch count: %d", dormant)
	}
	return uint64(dormant), nil
}

func (s *Store) ResetDormantDRepEpochs(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		if err := s.ensureDormantDRepState(db, ctx); err != nil {
			return err
		}
		slotValue, err := checkedInt64(slot)
		if err != nil {
			return err
		}
		var dormant int64
		if err := db.QueryRowContext(ctx, `
SELECT dormant_epochs
FROM drep_dormancy_state
WHERE id = 1`).Scan(&dormant); err != nil {
			return fmt.Errorf("read dormant DRep epoch count: %w", err)
		}
		if dormant == 0 {
			return nil
		}
		if err := s.recordDormantDRepEpochHistory(db, ctx, slotValue, dormant); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `
UPDATE drep_dormancy_state
SET dormant_epochs = 0
WHERE id = 1`); err != nil {
			return fmt.Errorf("reset dormant DRep epoch count: %w", err)
		}
		return nil
	})
}

func (s *Store) SetImportedDormantDRepEpochs(
	dormantEpochs uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(txn, func(db queryer, ctx context.Context) error {
		if err := s.ensureDormantDRepState(db, ctx); err != nil {
			return err
		}
		dormant, err := checkedInt64(dormantEpochs)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `
UPDATE drep_dormancy_state
SET dormant_epochs = ?
WHERE id = 1`, dormant); err != nil {
			return fmt.Errorf("set imported dormant DRep epoch count: %w", err)
		}
		return nil
	})
}

func (s *Store) incrementDormantDRepEpochs(
	db queryer,
	ctx context.Context,
	slot int64,
) error {
	if err := s.ensureDormantDRepState(db, ctx); err != nil {
		return err
	}
	var dormant int64
	if err := db.QueryRowContext(ctx, `
SELECT dormant_epochs
FROM drep_dormancy_state
WHERE id = 1`).Scan(&dormant); err != nil {
		return fmt.Errorf("read dormant DRep epoch count: %w", err)
	}
	if dormant == math.MaxInt64 {
		return fmt.Errorf("dormant DRep epoch count exceeds storage range at slot %d", slot)
	}
	if err := s.recordDormantDRepEpochHistory(db, ctx, slot, dormant); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
UPDATE drep_dormancy_state
SET dormant_epochs = dormant_epochs + 1
WHERE id = 1`); err != nil {
		return fmt.Errorf("increment dormant DRep epoch count: %w", err)
	}
	return nil
}

func (s *Store) ensureDormantDRepState(db queryer, ctx context.Context) error {
	query := `INSERT INTO drep_dormancy_state (id, dormant_epochs) VALUES (1, 0)`
	if s.dialect.Name() == "mysql" {
		query = strings.Replace(query, "INSERT INTO", "INSERT IGNORE INTO", 1)
	} else {
		query += " ON CONFLICT (id) DO NOTHING"
	}
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("ensure dormant DRep state row: %w", err)
	}
	return nil
}

func (s *Store) recordDormantDRepEpochHistory(
	db queryer,
	ctx context.Context,
	slot int64,
	dormant int64,
) error {
	if _, err := db.ExecContext(ctx, `
INSERT INTO drep_dormancy_history (added_slot, previous_dormant_epochs)
VALUES (?, ?)`, slot, dormant); err != nil {
		return fmt.Errorf("record dormant DRep epoch history: %w", err)
	}
	return nil
}

func (s *Store) restoreDrepExpiryHistory(
	db queryer,
	ctx context.Context,
	slot uint64,
) error {
	rows, err := db.QueryContext(ctx, `
SELECT credential_tag, credential, previous_expiry_epoch,
       previous_last_activity_epoch
FROM drep_expiry_history
WHERE added_slot > ?
ORDER BY added_slot DESC`, slot)
	if err != nil {
		return err
	}
	defer rows.Close()
	type historyRow struct {
		tag          uint8
		credential   []byte
		expiry       uint64
		lastActivity uint64
	}
	var items []historyRow
	for rows.Next() {
		var item historyRow
		if err := rows.Scan(
			&item.tag,
			&item.credential,
			&item.expiry,
			&item.lastActivity,
		); err != nil {
			return err
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range items {
		if _, err := db.ExecContext(ctx, `
UPDATE drep
SET expiry_epoch = ?, last_activity_epoch = ?
WHERE credential_tag = ? AND credential = ?`,
			item.expiry,
			item.lastActivity,
			item.tag,
			item.credential,
		); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM drep_expiry_history WHERE added_slot > ?`, slot); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM drep_expiry_epoch_event WHERE added_slot > ?`, slot); err != nil {
		return err
	}
	if err := s.restoreDormantDRepEpochHistory(db, ctx, slot); err != nil {
		return err
	}
	return nil
}

func (s *Store) restoreDormantDRepEpochHistory(
	db queryer,
	ctx context.Context,
	slot uint64,
) error {
	rows, err := db.QueryContext(ctx, `
SELECT id, previous_dormant_epochs
FROM drep_dormancy_history
WHERE added_slot > ?
ORDER BY id DESC`, slot)
	if err != nil {
		return err
	}
	type historyRow struct {
		id      int64
		dormant int64
	}
	var items []historyRow
	for rows.Next() {
		var item historyRow
		if err := rows.Scan(&item.id, &item.dormant); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range items {
		if _, err := db.ExecContext(ctx, `
UPDATE drep_dormancy_state
SET dormant_epochs = ?
WHERE id = 1`, item.dormant); err != nil {
			return fmt.Errorf("restore dormant DRep epoch count from history %d: %w", item.id, err)
		}
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM drep_dormancy_history WHERE added_slot > ?`, slot); err != nil {
		return err
	}
	return nil
}
