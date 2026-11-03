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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// uncachedPendingInstantaneousRewards is the per-call scan the view's cache
// replaces.
func uncachedPendingInstantaneousRewards(
	t *testing.T,
	ls *LedgerState,
	txn *database.Txn,
	epochStartSlot uint64,
	source uint,
	cred lcommon.Credential,
) *big.Int {
	t.Helper()
	effects, err := ls.db.GetMIRCertsInSlotRange(
		context.Background(), epochStartSlot, math.MaxInt64, txn,
	)
	require.NoError(t, err)
	var total *big.Int
	for _, effect := range effects {
		if effect.Pot != source || effect.OtherPot > 0 {
			continue
		}
		for _, reward := range effect.Rewards {
			if reward.Amount == nil ||
				uint(reward.CredentialTag) != cred.CredType ||
				!bytes.Equal(reward.Credential, cred.Credential[:]) {
				continue
			}
			if total == nil {
				total = new(big.Int)
			}
			total.Add(total, reward.Amount)
		}
	}
	return total
}

func mirTestCredential(credType uint, seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType:   credType,
		Credential: lcommon.NewBlake2b224(mirCred28(seed)),
	}
}

func TestLedgerViewPendingInstantaneousRewardsReadsOncePerView(t *testing.T) {
	t.Parallel()
	ls, db, gdb := newMIRTestLedger(t)
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(5)},
			{Credential: mirCred28(2), Amount: big.NewInt(6)},
		})
	loads := 0
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{
			ls: ls, txn: txn, epochStartSlot: 100,
			onMIRCertsLoad: func() { loads++ },
		}
		for i := range 50 {
			_, err := lv.PendingInstantaneousRewards(
				mirPotReserves, mirTestCredential(0, byte(1+i%3)),
			)
			require.NoError(t, err)
		}
		return nil
	}))
	require.Equal(t, 1, loads, "MIR certs must be read once per view")
}

func TestLedgerViewPendingInstantaneousRewardsMatchesUncached(t *testing.T) {
	t.Parallel()
	ls, db, gdb := newMIRTestLedger(t)
	seedMIRDistribution(t, gdb, mirPotReserves, 50,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(1_000)},
		})
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(10)},
			{Credential: mirCred28(2), Amount: big.NewInt(20)},
			{
				Credential: mirCred28(1), CredentialTag: 1,
				Amount: big.NewInt(30),
			},
		})
	seedMIRDistribution(t, gdb, mirPotReserves, 160,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(7)},
		})
	seedMIRDistribution(t, gdb, mirPotTreasury, 170,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(100)},
		})
	seedMIRPotTransfer(t, gdb, mirPotTreasury, 999, 180)
	creds := []lcommon.Credential{
		mirTestCredential(0, 1),
		mirTestCredential(0, 2),
		mirTestCredential(1, 1),
		mirTestCredential(1, 2),
		mirTestCredential(0, 9),
	}
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}
		for _, pot := range []uint{mirPotReserves, mirPotTreasury, 2} {
			for _, cred := range creds {
				want := uncachedPendingInstantaneousRewards(
					t, ls, txn, 100, pot, cred,
				)
				got, err := lv.PendingInstantaneousRewards(pot, cred)
				require.NoError(t, err)
				if want == nil {
					require.Nil(t, got, "pot %d cred %v", pot, cred)
					continue
				}
				require.NotNil(t, got, "pot %d cred %v", pot, cred)
				require.Zero(t, want.Cmp(got), "pot %d cred %v", pot, cred)
				// Callers own the result; mutating it must not corrupt
				// later answers.
				got.Add(got, big.NewInt(1))
				again, err := lv.PendingInstantaneousRewards(pot, cred)
				require.NoError(t, err)
				require.Zero(t, want.Cmp(again))
			}
		}
		return nil
	}))
}

// A MIR certificate stored between two views is visible to the later one.
// Validation builds one view per transaction and stores a transaction's
// certificates only after it validates, so no view spans a MIR write.
func TestLedgerViewPendingInstantaneousRewardsSeesLaterStoredCert(t *testing.T) {
	t.Parallel()
	ls, db, gdb := newMIRTestLedger(t)
	cred := mirTestCredential(0, 1)
	read := func() *big.Int {
		var got *big.Int
		txn := db.Transaction(context.Background(), false)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}
			var err error
			got, err = lv.PendingInstantaneousRewards(mirPotReserves, cred)
			return err
		}))
		return got
	}
	require.Nil(t, read())
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(10)},
		})
	require.Zero(t, big.NewInt(10).Cmp(read()))
	seedMIRDistribution(t, gdb, mirPotReserves, 160,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(5)},
		})
	require.Zero(t, big.NewInt(15).Cmp(read()))
}
