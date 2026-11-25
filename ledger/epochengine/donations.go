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

package epochengine

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
)

// ApplyEpochDonations moves the treasury donations accumulated during the
// ended epoch into the treasury at the epoch boundary. The donation rows are
// left in place (keyed by slot) so a rollback past the boundary drops both the
// donation rows and the boundary NetworkState row, and re-applying the
// boundary re-derives the same total. It writes the updated treasury at the
// boundary slot, the same slot governance treasury-withdrawal enactment uses,
// so the row reflects withdrawals first and donations second.
func (e *Engine) ApplyEpochDonations(
	txn ledgerdb.Txn,
	endedEpoch uint64,
	boundarySlot uint64,
) error {
	donations, err := e.ledgerDB().SumNetworkDonationsForEpoch(
		endedEpoch, txn,
	)
	if err != nil {
		return fmt.Errorf(
			"sum donations for epoch %d: %w", endedEpoch, err,
		)
	}
	if donations == 0 {
		return nil
	}
	state, err := e.ledgerDB().GetNetworkState(txn)
	if err != nil {
		return fmt.Errorf("get network state: %w", err)
	}
	var treasury, reserves uint64
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	if err := e.ledgerDB().SetNetworkState(
		treasury+donations,
		reserves,
		boundarySlot,
		txn,
	); err != nil {
		return fmt.Errorf("set network state with donations: %w", err)
	}
	return nil
}
