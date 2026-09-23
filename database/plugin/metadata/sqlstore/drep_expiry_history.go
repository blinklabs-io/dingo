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
	return nil
}
