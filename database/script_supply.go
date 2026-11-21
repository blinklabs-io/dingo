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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
)

// PruneScriptLockedSupplyBefore drops the script-locked supply rows older
// than the newest row at or below floor, which stays as the base for the
// slots above it. The caller passes the lowest slot a rollback of this
// database can still reach; a rollback below the base rebuilds the aggregate
// by a scan instead of reading a trimmed row. txn must be a read-write
// metadata transaction of this database, so the trim commits with the writes
// it follows.
func (d *Database) PruneScriptLockedSupplyBefore(
	floor uint64,
	txn *Txn,
) error {
	if txn == nil || txn.db != d || txn.Metadata() == nil ||
		!txn.IsReadWrite() {
		return fmt.Errorf(
			"prune script-locked supply: %w",
			types.ErrTxnWrongType,
		)
	}
	return d.utxoStore().PruneScriptLockedSupplyBefore(floor, txn.Metadata())
}
