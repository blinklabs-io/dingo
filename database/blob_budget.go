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
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
)

// stagedBlobDeleteReserve is the transaction headroom, in entries, that a
// bulk blob delete staged in a caller's transaction leaves behind for the
// rest of that transaction.
//
// A delete set is never the last thing to enter the transaction. Txn.Commit
// writes a commit timestamp into the same blob transaction before committing
// either store, and TruncateAfterSlot stages the UTxO and transaction delete
// sets back to back. A loop that stages until the budget is gone therefore
// does not produce a partial blob cleanup, which callers tolerate -- it
// produces a transaction that can no longer accept even a 35-byte timestamp,
// so the whole rollback commit fails. On the startup rollback path that
// leaves a node failing identically on every start
// (blinklabs-io/dingo#4657). One entry would cover the timestamp; the margin
// is larger so that ordinary growth in what a caller stages after its
// deletes cannot silently consume it.
const stagedBlobDeleteReserve = 1024

// stagedBlobDeleteLimit reports how many of a want-sized delete set, whose
// keys cost entryBytes each, may be staged into blobTxn while leaving
// stagedBlobDeleteReserve entries of headroom behind.
//
// A store that reports no budget (blob.TxnBudget) has none to exhaust, so the
// whole set is staged -- the behavior every store but badger keeps, since the
// cloud plugins stage mutations in memory and apply them in Commit.
func stagedBlobDeleteLimit(
	store blob.BlobStore,
	blobTxn types.Txn,
	entryBytes int,
	want int,
) int {
	budget, ok := store.(blob.TxnBudget)
	if !ok {
		return want
	}
	remaining, ok := budget.RemainingTxnEntries(blobTxn, entryBytes)
	if !ok {
		return want
	}
	remaining -= stagedBlobDeleteReserve
	if remaining <= 0 {
		return 0
	}
	return min(remaining, want)
}
