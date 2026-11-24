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

package node

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dledger "github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// witnessedAccounts is the CIP-0163 witness set of the blocks one IndexBlocks
// call indexes: each account's greatest witnessing epoch. The ledger renews an
// account at every block that witnesses it with the block's epoch plus the
// window, which only moves forward, so the last renewal of a call is the one
// that survives, and applying it once after the call's rows are flushed leaves
// the same expiration. A witness always follows the registration that creates
// the account's row, so the row exists by then.
type witnessedAccounts struct {
	epoch map[string]uint64
	refs  map[string]models.StakeCredentialRef
}

// noteWitnessed records the accounts the valid transaction levels witness at
// epoch. It is a no-op outside IndexBlocks or with the gate off.
func (b *Backfill) noteWitnessed(
	epoch uint64,
	levels []lcommon.Transaction,
) {
	if b.witnessed == nil {
		return
	}
	for _, tx := range levels {
		if tx == nil || !tx.IsValid() {
			continue
		}
		for _, ref := range dledger.WitnessedRewardCredentials(tx) {
			key := ref.MapKey()
			b.witnessed.refs[key] = ref
			b.witnessed.epoch[key] = max(b.witnessed.epoch[key], epoch)
		}
	}
}

// renewWitnessed applies the recorded witnesses: every account is renewed to
// the epoch it was last witnessed in plus the window.
func (b *Backfill) renewWitnessed(
	ctx context.Context,
	txn *database.Txn,
) error {
	w := b.witnessed
	byExpiration := map[uint64][]models.StakeCredentialRef{}
	for key, epoch := range w.epoch {
		expiration := epoch + b.delegatorInactivity
		byExpiration[expiration] = append(
			byExpiration[expiration], w.refs[key],
		)
	}
	for expiration, refs := range byExpiration {
		if err := b.db.RenewAccountExpirations(
			ctx, refs, expiration, txn,
		); err != nil {
			return fmt.Errorf("renew account expirations: %w", err)
		}
	}
	return nil
}
