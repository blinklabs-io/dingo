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

package governance

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// requireSoleCreditPostSnapshot asserts the single credit journaled for a
// credential carries the expected PostSnapshot flag.
func requireSoleCreditPostSnapshot(
	t *testing.T,
	gdb *gorm.DB,
	credential []byte,
	want bool,
	msg string,
) {
	t.Helper()
	var deltas []models.AccountRewardDelta
	require.NoError(t, gdb.Where(
		"credential_tag = ? AND staking_key = ? AND withdrawal = ?",
		0, credential, false,
	).Find(&deltas).Error)
	require.Len(t, deltas, 1, "expected exactly one boundary credit")
	require.Equal(t, want, deltas[0].PostSnapshot, msg)
}

// TestBoundaryCreditVisibility_TreasuryWithdrawalIsExcludedFromSnapshot pins an
// enacted treasury withdrawal as post-SNAP: cardano-ledger's EPOCH rule runs
// SNAP (and POOLREAP) before ratification/enactment, so a withdrawal credited at
// the boundary is not part of that boundary's mark snapshot.
func TestBoundaryCreditVisibility_TreasuryWithdrawalIsExcludedFromSnapshot(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x61)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	require.NoError(t, applyTreasuryWithdrawal(
		&EnactmentContext{DB: db, Slot: 200},
		&lcommon.TreasuryWithdrawalGovAction{
			Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
		},
		&models.GovernanceProposal{TxHash: testBytes(32, 0x62)},
	))

	requireSoleCreditPostSnapshot(t, store.DB(), stakeCred, true,
		"enactment runs after SNAP, so a treasury withdrawal must be excluded from the mark snapshot")
}

// TestBoundaryCreditVisibility_ProposalRefundIsExcludedFromSnapshot pins a
// governance proposal-deposit refund as post-SNAP, for the same reason.
func TestBoundaryCreditVisibility_ProposalRefundIsExcludedFromSnapshot(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x63)
	rewardAddrBytes := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	require.NoError(t, refundProposalDeposit(db, nil, &models.GovernanceProposal{
		TxHash:        testBytes(32, 0x64),
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 200))

	requireSoleCreditPostSnapshot(t, store.DB(), stakeCred, true,
		"a proposal-deposit refund is enacted after SNAP and must be excluded from the mark snapshot")
}
