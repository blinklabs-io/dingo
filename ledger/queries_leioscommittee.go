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
	"context"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// queryShelleyLeiosCommittee answers GetLeiosCommittee (Shelley sub-query 41),
// the Leios voting committees of the current and the next epoch, which
// `cardano-cli query leios-committee` sends so an operator can check whether
// the pool holds a seat and whether its BLS key is voting.
//
// The current committee is required: failing to build it fails the query. The
// next committee is optional in the reply (the ledger's StrictMaybe), because
// the stake snapshot it is drawn from may not exist yet; when it cannot be
// built the reply leaves it out rather than failing.
func (ls *LedgerState) queryShelleyLeiosCommittee(
	ctx context.Context,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	epoch, err := ls.shelleyEpochNo(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	current, err := ls.config.LeiosCommittee(epoch)
	if err != nil {
		return nil, fmt.Errorf(
			"leios committee for epoch %d: %w",
			epoch,
			err,
		)
	}
	result := olocalstatequery.LeiosCommitteeResult{Current: current}
	if epoch < math.MaxUint64 {
		next, err := ls.config.LeiosCommittee(epoch + 1)
		if err != nil {
			ls.config.Logger.Debug(
				"leios committee for next epoch unavailable",
				"component", "ledger",
				"epoch", epoch+1,
				"error", err,
			)
		} else {
			result.Next = next
		}
	}
	return result, nil
}
