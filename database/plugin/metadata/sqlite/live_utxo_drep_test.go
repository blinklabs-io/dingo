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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestDRepVotingPowerByTypeRefusesIncompleteLiveStake requires a store that
// keeps no utxo rows to refuse a by-type total when a delegator has no
// reward_live_stake row, rather than answer from its empty utxo table.
func TestDRepVotingPowerByTypeRefusesIncompleteLiveStake(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("abstain-staking-key-28-bytes"),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true,
		Reward:     10,
	}))
	// Creating the account may add its stake row; the case is a delegator
	// whose row is missing.
	_, err := writeDB.Exec(`DELETE FROM reward_live_stake`)
	require.NoError(t, err)
	store.SetLiveUtxoSource(emptyLiveSource{})
	t.Cleanup(func() { store.SetLiveUtxoSource(nil) })

	_, err = store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain}, 0, nil,
	)
	require.ErrorContains(t, err, "reward_live_stake is incomplete")
}
