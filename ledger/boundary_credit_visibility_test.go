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
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// boundaryCreditPostSnapshot returns the AccountRewardDelta.PostSnapshot flag of
// the single credit journaled for a credential.
func boundaryCreditPostSnapshot(
	t *testing.T,
	db *sql.DB,
	credential []byte,
) bool {
	t.Helper()
	var postSnapshot bool
	require.NoError(t, db.QueryRow(`
SELECT post_snapshot FROM account_reward_delta
WHERE credential_tag = ? AND staking_key = ? AND withdrawal = FALSE`,
		0, credential).Scan(&postSnapshot))
	return postSnapshot
}

func insertBoundaryAccount(t *testing.T, db *sql.DB, credential []byte) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO account (staking_key, active, reward)
VALUES (?, TRUE, '0')`, credential)
	require.NoError(t, err)
}

// TestBoundaryCreditVisibility_MIRIsIncludedInSnapshot pins MIR credits as
// pre-SNAP.
//
// cardano-ledger's Shelley NEWEPOCH rule runs applyRUpd, then the MIR rule, then
// the EPOCH rule whose first sub-rule is SNAP. MIR credits are therefore part of
// the mark snapshot, so their journal rows must NOT be stamped PostSnapshot —
// an epoch-boundary reconstruction has to retain them exactly like the delayed
// reward update.
func TestBoundaryCreditVisibility_MIRIsIncludedInSnapshot(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(100)
		boundarySlot   = uint64(200)
		amount         = uint64(70)
	)
	credential := mirCred28(0x51)
	insertBoundaryAccount(t, gdb, credential)
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 1, nil))
	seedMIRDistribution(
		t,
		gdb,
		0,
		epochStartSlot+1,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: credential, Amount: types.Uint64(amount)},
		},
	)

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	require.False(
		t,
		boundaryCreditPostSnapshot(t, gdb, credential),
		"MIR runs before SNAP in cardano-ledger, so its credit belongs in the mark snapshot",
	)
}

// TestBoundaryCreditVisibility_PoolReapIsExcludedFromSnapshot pins POOLREAP
// deposit refunds as post-SNAP: cardano-ledger's EPOCH rule runs SNAP before
// POOLREAP, so the refund is not part of the mark snapshot.
func TestBoundaryCreditVisibility_PoolReapIsExcludedFromSnapshot(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(2000)
	)
	rewardAccount := reapCred28(0x52)
	insertBoundaryAccount(t, gdb, rewardAccount)
	seedRetiringPool(
		t, gdb, reapCred28(0xB2), rewardAccount, deposit, 10, newEpoch, 20,
	)

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	require.True(
		t,
		boundaryCreditPostSnapshot(t, gdb, rewardAccount),
		"POOLREAP runs after SNAP, so its refund must be excluded from the mark snapshot",
	)
}

// TestBoundaryCreditVisibility_StakeRewardIsIncludedInSnapshot pins the delayed
// reward update as pre-SNAP by exercising the crediting primitive it uses
// (Database.AddAccountRewardByCredential), which must leave the journal row
// unstamped.
func TestBoundaryCreditVisibility_StakeRewardIsIncludedInSnapshot(
	t *testing.T,
) {
	_, db, gdb := newPoolreapTestLedger(t)

	credential := reapCred28(0x53)
	insertBoundaryAccount(t, gdb, credential)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.AddAccountRewardByCredential(
			0, credential, 40, 200, make([]byte, 32), txn,
		)
	}))

	require.False(
		t,
		boundaryCreditPostSnapshot(t, gdb, credential),
		"the delayed reward update precedes SNAP and belongs in the mark snapshot",
	)
}
