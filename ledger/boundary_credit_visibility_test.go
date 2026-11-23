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
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// boundaryCreditPostSnapshot reports whether the ledger recorded a credit to
// credential at slot as applied after the boundary's SNAP point, and requires
// the credit itself to have reached the account.
func boundaryCreditPostSnapshot(
	t *testing.T,
	ls *LedgerState,
	credential []byte,
	slot, amount uint64,
) bool {
	t.Helper()
	account, err := ls.ledgerDB().GetAccountByCredential(
		context.Background(), 0, credential, false, nil,
	)
	require.NoError(t, err)
	require.Equal(t, amount, uint64(account.Reward))
	r := ls.rewardOutputReader(nil)
	require.NotNil(t, r)
	credits, err := ls.postSnapshotCreditRows(r, nil, slot)
	require.NoError(t, err)
	for _, c := range credits {
		if c.Ref.Tag == 0 && bytes.Equal(c.Ref.Key, credential) {
			require.Equal(t, amount, c.Amount)
			return true
		}
	}
	return false
}

func insertBoundaryAccount(
	t *testing.T,
	db *database.Database,
	credential []byte,
) {
	t.Helper()
	require.NoError(t, seedCreateAccount(
		t, db, context.Background(), nil,
		&models.Account{StakingKey: credential, Active: true},
	))
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
	t.Parallel()

	ls, db, _ := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(100)
		boundarySlot   = uint64(200)
		amount         = uint64(70)
	)
	credential := mirCred28(0x51)
	insertBoundaryAccount(t, db, credential)
	require.NoError(t, seedDB(t, db).SetNetworkState(0, 1_000, 1, nil))
	seedMIRDistribution(
		t,
		db,
		0,
		epochStartSlot+1,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: credential, Amount: new(big.Int).SetUint64(amount)},
		},
	)

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	require.False(
		t,
		boundaryCreditPostSnapshot(t, ls, credential, boundarySlot, amount),
		"MIR runs before SNAP in cardano-ledger, so its credit belongs in the mark snapshot",
	)
}

// TestBoundaryCreditVisibility_PoolReapIsExcludedFromSnapshot pins POOLREAP
// deposit refunds as post-SNAP: cardano-ledger's EPOCH rule runs SNAP before
// POOLREAP, so the refund is not part of the mark snapshot.
func TestBoundaryCreditVisibility_PoolReapIsExcludedFromSnapshot(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(2000)
	)
	rewardAccount := reapCred28(0x52)
	insertBoundaryAccount(t, db, rewardAccount)
	fxRetiringPool(
		t, db, reapCred28(0xB2), rewardAccount, deposit, 10, newEpoch, 20,
	)

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	require.True(
		t,
		boundaryCreditPostSnapshot(t, ls, rewardAccount, boundarySlot, deposit),
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
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	credential := reapCred28(0x53)
	insertBoundaryAccount(t, db, credential)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return seedDB(t, db).AddAccountRewardByCredential(context.Background(),
			0, credential, 40, 200, make([]byte, 32), txn,
		)
	}))

	require.False(
		t,
		boundaryCreditPostSnapshot(t, ls, credential, 200, 40),
		"the delayed reward update precedes SNAP and belongs in the mark snapshot",
	)
}
