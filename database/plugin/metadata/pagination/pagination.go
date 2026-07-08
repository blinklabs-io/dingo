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

// Package pagination holds cursor-pagination helpers shared by the
// metadata plugin backends (sqlite, postgres, mysql). It lives outside the
// metadata package itself because that package blank-imports the backends
// for plugin registration; a backend importing metadata back would cycle.
package pagination

import "gorm.io/gorm"

// BlockTxPositioned is implemented by row models that carry a
// (block_number, tx_index) cursor position, e.g. the midnight_* UTxO-event
// tables.
type BlockTxPositioned interface {
	BlockTxPosition() (blockNumber uint64, txIndex uint32)
}

// ExtendPageToFullTxGroup re-queries db for every row sharing the last row's
// (block_number, tx_index) key and appends any rows from that group not
// already present in rows.
//
// (block_number, tx_index) is not a unique key in the midnight_* tables: a
// single transaction can write more than one row to the same table (for
// example several cNIGHT outputs created in one tx). If a page's SQL LIMIT
// lands in the middle of such a group, a cursor that resumes strictly after
// (block_number, tx_index) would silently skip the remaining rows in that
// group forever. Extending the page to the end of the group closes that
// gap, at the cost of an occasional page slightly larger than the
// requested limit.
//
// rows must already be ordered ascending by (block_number, tx_index) and
// contain exactly limit rows (the function is a no-op otherwise, since a
// short page cannot have been cut mid-group). db must carry no lingering
// WHERE/ORDER conditions from the caller's own query — GORM chain calls
// clone rather than mutate, so reusing the handle that ran that query is
// safe.
//
// The replacement group is ordered by id ASC, so repeated calls for the
// same page return the tied rows in the same order — every current caller's
// row model has an auto-increment "id" primary key column.
func ExtendPageToFullTxGroup[T BlockTxPositioned](
	db *gorm.DB,
	rows []T,
	limit int,
) ([]T, error) {
	if limit <= 0 || len(rows) != limit {
		return rows, nil
	}
	n := len(rows)
	lastBlock, lastTx := rows[n-1].BlockTxPosition()
	tail := 1
	for tail < n {
		b, tx := rows[n-1-tail].BlockTxPosition()
		if b != lastBlock || tx != lastTx {
			break
		}
		tail++
	}
	var group []T
	if err := db.Where("block_number = ? AND tx_index = ?", lastBlock, lastTx).
		Order("id ASC").
		Find(&group).Error; err != nil {
		return nil, err
	}
	if len(group) <= tail {
		return rows, nil
	}
	extended := make([]T, 0, n-tail+len(group))
	extended = append(extended, rows[:n-tail]...)
	extended = append(extended, group...)
	return extended, nil
}
