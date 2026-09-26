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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// depositReturnAddress builds reward-account address bytes for stakeCred,
// suitable for a GovernanceProposal's ReturnAddress.
func depositReturnAddress(t *testing.T, stakeCred []byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return addrBytes
}

// TestPredefinedDRepAmountIncludesActiveProposalDeposit proves the
// Blockfrost single-DRep endpoint reports the same CIP-1694
// deposit-inclusive voting power ledger/governance.LoadDRepVotingState uses
// for real ratification (blinklabs-io/dingo#4355), not the plain
// GetDRepVotingPowerByType figure alone, for the AlwaysNoConfidence
// predefined DRep.
//
// The credential-backed single-DRep endpoint (drepByCredentialTag) merges
// deposit power through the identical map lookup this test and
// TestDRepsListAmountsIncludeActiveProposalDeposit already exercise, but
// additionally resolves a registration epoch via the ledger's genesis
// hard-fork summary -- machinery a fresh, block-less test ledger state
// cannot produce -- so it is not separately exercised end-to-end here.
func TestPredefinedDRepAmountIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)
	returnStakeCred := []byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: returnStakeCred,
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        []byte("proposal-tx-hash-32-bytes-long2"),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
		Deposit:       75,
		ReturnAddress: depositReturnAddress(t, returnStakeCred),
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    []byte("anchor-hash-32-bytes-long-valu2"),
		AddedSlot:     1,
	}, nil))

	drepType := models.DrepTypeAlwaysNoConfidence
	info, err := adapter.DRep(DRepCredential{
		ID:         "drep_always_no_confidence",
		Predefined: &drepType,
	})
	require.NoError(t, err)
	assert.Equal(t, "75", info.Amount)
}

// TestDRepsListAmountsIncludeActiveProposalDeposit is the batch-listing
// counterpart: the /governance/dreps page must report the same
// deposit-inclusive amount as the single-DRep endpoint.
func TestDRepsListAmountsIncludeActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)
	drepCred := []byte{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}
	returnStakeCred := []byte{6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}

	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
		AddedSlot:  1,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: returnStakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        []byte("proposal-tx-hash-32-bytes-long3"),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
		Deposit:       30,
		ReturnAddress: depositReturnAddress(t, returnStakeCred),
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    []byte("anchor-hash-32-bytes-long-valu3"),
		AddedSlot:     1,
	}, nil))

	items, total, err := adapter.DReps(DRepListParams{
		Pagination: PaginationParams{
			Count: 100,
			Page:  1,
			Order: PaginationOrderAsc,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, items, 1)
	assert.Equal(t, "30", items[0].Amount)
}
