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

package ledger

import (
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
)

// recomputeAccountExpirationsAfterRollback restores the CIP-0163
// expiration_epoch of the reward accounts affected by a chain rollback.
// It is a thin wrapper around database.RecomputeAccountExpirationsAfterTruncate
// — the shared implementation database/lifecycle.Truncate also calls, so
// live ledger rollback and offline/live CIP-0135 truncate apply the exact
// same CIP-0163 bookkeeping. See that function's doc comment for the full
// explanation of the recompute and activation-floor logic.
//
// It is a no-op when the delegator-inactivity gate is disabled, and must run
// inside the rollback's write transaction, after RestoreAccountStateAtSlot has
// restored the remaining account fields.
func (ls *LedgerState) recomputeAccountExpirationsAfterRollback(
	txn *database.Txn,
	rollbackSlot uint64,
	affectedRefs []models.StakeCredentialRef,
) error {
	return database.RecomputeAccountExpirationsAfterTruncate(
		ls.db,
		txn,
		ls.config.DelegatorInactivityEnabled,
		ls.config.DelegatorInactivity,
		rollbackSlot,
		affectedRefs,
	)
}
