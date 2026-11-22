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

package migrations

import (
	"context"
	"fmt"
)

// swapRebuiltTable replaces table with its rebuilt copy and can be rerun from
// any point an earlier call stopped at.
//
// SQLite and PostgreSQL run the swap in the batch's transaction, so it is
// atomic. MySQL commits every DDL statement implicitly, so a crash can leave
// the swap half done. The rename of both tables is one atomic statement, which
// keeps the original in place until the copy takes its name; the displaced
// table is dropped afterwards, and a rerun drops one left behind. When the
// original is already gone and only the copy remains, the copy is renamed
// without a reconciliation, which has no original to count.
//
// verify runs only when the original and the copy both exist, before anything
// is replaced. A nil verify skips the check.
func swapRebuiltTable(
	ctx context.Context,
	batch Batch,
	dialect, table, rebuilt string,
	verify func() error,
) error {
	hasRebuilt, err := tableExists(ctx, batch, dialect, rebuilt)
	if err != nil {
		return err
	}
	if hasRebuilt {
		hasOriginal, err := tableExists(ctx, batch, dialect, table)
		if err != nil {
			return err
		}
		if hasOriginal && verify != nil {
			if err := verify(); err != nil {
				return err
			}
		}
		statements := replaceStatements(dialect, table, rebuilt, hasOriginal)
		for _, statement := range statements {
			if _, err := batch.Tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("swap in %s: %s: %w", table, statement, err)
			}
		}
	}
	return dropDisplacedTable(ctx, batch, dialect, table)
}

// completeSwap finishes a swap that an earlier run left half done, and does
// nothing when none is pending. It never replaces a table that exists.
func completeSwap(
	ctx context.Context,
	batch Batch,
	dialect, table, rebuilt string,
) error {
	hasRebuilt, err := tableExists(ctx, batch, dialect, rebuilt)
	if err != nil {
		return err
	}
	if hasRebuilt {
		hasOriginal, err := tableExists(ctx, batch, dialect, table)
		if err != nil {
			return err
		}
		if !hasOriginal {
			return swapRebuiltTable(ctx, batch, dialect, table, rebuilt, nil)
		}
	}
	return dropDisplacedTable(ctx, batch, dialect, table)
}

func replaceStatements(
	dialect, table, rebuilt string,
	hasOriginal bool,
) []string {
	if dialect == "mysql" {
		if hasOriginal {
			return []string{
				"RENAME TABLE `" + table + "` TO `" + table + "_old`, `" +
					rebuilt + "` TO `" + table + "`",
			}
		}
		return []string{"RENAME TABLE `" + rebuilt + "` TO `" + table + "`"}
	}
	var statements []string
	if hasOriginal {
		statements = append(statements, "DROP TABLE "+table)
	}
	return append(statements, "ALTER TABLE "+rebuilt+" RENAME TO "+table)
}

// dropDisplacedTable drops the original that a MySQL swap renamed aside.
func dropDisplacedTable(
	ctx context.Context,
	batch Batch,
	dialect, table string,
) error {
	if dialect != "mysql" {
		return nil
	}
	if _, err := batch.Tx.ExecContext(
		ctx, "DROP TABLE IF EXISTS `"+table+"_old`",
	); err != nil {
		return fmt.Errorf("drop replaced %s: %w", table, err)
	}
	return nil
}
