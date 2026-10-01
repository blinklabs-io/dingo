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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedProposalReturnAccount creates a registered account and returns the
// reward-account address bytes for it, for use as a GovernanceProposal's
// ReturnAddress.
func seedProposalReturnAccount(
	t *testing.T,
	store *tallyTestStore,
	stakeCred []byte,
	account *models.Account,
) []byte {
	return seedProposalReturnAccountWithTag(
		t, store, stakeCred, 0, account,
	)
}

func seedProposalReturnAccountWithTag(
	t *testing.T,
	store *tallyTestStore,
	stakeCred []byte,
	credentialTag uint8,
	account *models.Account,
) []byte {
	t.Helper()
	account.StakingKey = stakeCred
	account.CredentialTag = credentialTag
	require.NoError(t, store.CreateAccount(nil, account))
	addressType := uint8(lcommon.AddressTypeNoneKey)
	if credentialTag == 1 {
		addressType = uint8(lcommon.AddressTypeNoneScript)
	}
	addr, err := lcommon.NewAddressFromParts(
		addressType,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return addrBytes
}

func TestLoadDRepVotingStateIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	drepStakeCred := testBytes(28, 2)
	returnStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, drepStakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 9),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  5,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAA),
		AddedSlot:     1,
	}, nil))

	state, err := loadDRepVotingState(db, nil, 6, 5, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}
	assert.Equal(t, uint64(150), state.Powers[ref.MapKey()])
}

func TestSPOVotingPowerIncludesProposalDepositsWithoutChangingMarkStake(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	poolWithDeposit := testBytes(28, 60)
	poolWithoutDeposit := testBytes(28, 61)
	seedPoolWithStake(
		t, store, poolWithDeposit, testBytes(28, 62), 100, 6,
	)
	seedPoolWithStake(
		t, store, poolWithoutDeposit, testBytes(28, 63), 100, 6,
	)

	returnStakeCred := testBytes(28, 64)
	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Pool:      poolWithDeposit,
			DrepType:  models.DrepTypeAlwaysNoConfidence,
			AddedSlot: 1,
			Active:    true,
		},
	)
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 65),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: 5,
		ExpiresEpoch:  5,
		Deposit:       100,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/spo-deposit",
		AnchorHash:    testBytes(32, 0xA1),
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))

	state, err := LoadSPOVotingState(db, nil, 6)
	require.NoError(t, err)
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeUpdateCommittee),
		DRepYesStake:   100,
		DRepTotalStake: 100,
	}
	proposalEpoch := uint64(5)
	require.NoError(t, tallySPOVotes(
		&TallyContext{
			DB:                  db,
			StakeEpoch:          6,
			CurrentEpoch:        6,
			ActiveProposalEpoch: &proposalEpoch,
			SPOState:            state,
		},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeSPO,
			VoterCredential: poolWithDeposit,
			Vote:            models.VoteYes,
		}},
		tally,
	))

	assert.Equal(t, uint64(200), tally.SPOYesStake)
	assert.Equal(t, uint64(300), tally.SPOTotalStake)
	assert.Equal(t, big.NewRat(2, 3), tally.SPOYesRatio())
	replayedTally := &ProposalTally{ActionType: tally.ActionType}
	require.NoError(t, tallySPOVotes(
		&TallyContext{
			DB:           db,
			StakeEpoch:   6,
			CurrentEpoch: 6,
			SPOState:     state,
		},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeSPO,
			VoterCredential: poolWithDeposit,
			Vote:            models.VoteYes,
		}},
		replayedTally,
	))
	assert.Equal(t, uint64(200), replayedTally.SPOYesStake)
	assert.Equal(t, uint64(300), replayedTally.SPOTotalStake)

	pparams := conwayPParamsFixture(10)
	pparams.DRepVotingThresholds.CommitteeNoConfidence = newRat(60, 100)
	pparams.PoolVotingThresholds.CommitteeNoConfidence = newRat(60, 100)
	decision := ShouldRatify(RatifyInputs{
		Tally:   tally,
		PParams: pparams,
		GovAction: &lcommon.UpdateCommitteeGovAction{
			Type:       uint(lcommon.GovActionTypeUpdateCommittee),
			CredEpochs: map[*lcommon.Credential]uint64{},
		},
		CurrentEpoch:    6,
		CommitteeAbsent: true,
		MajorVersion:    10,
	})
	assert.True(t, decision.SPOApproved)
	assert.True(t, decision.Ratified)

	markRows, err := db.GetPoolStakeSnapshotsByEpoch(6, "mark", nil)
	require.NoError(t, err)
	var persistedPoolStake uint64
	for _, row := range markRows {
		if string(row.PoolKeyHash) == string(poolWithDeposit) {
			persistedPoolStake = uint64(row.TotalStake)
		}
	}
	assert.Equal(t, uint64(100), persistedPoolStake)
}

func TestProcessEpochRatifiesWithMultipleKeyAndScriptProposalDeposits(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	const epoch = uint64(6)
	drepYes := testBytes(28, 70)
	drepNo := testBytes(28, 71)
	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepYes,
		Active:     true,
	}))
	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepNo,
		Active:     true,
	}))
	seedDRepStake(
		t, store, testBytes(28, 72), drepYes,
		models.DrepTypeAddrKeyHash, 100, 73,
	)
	seedDRepStake(
		t, store, testBytes(28, 74), drepNo,
		models.DrepTypeAddrKeyHash, 300, 75,
	)

	poolYes := testBytes(28, 76)
	poolNo := testBytes(28, 77)
	seedPoolWithStake(t, store, poolYes, testBytes(28, 78), 100, epoch)
	seedPoolWithStake(t, store, poolNo, testBytes(28, 79), 300, epoch)

	returnHash := testBytes(28, 80)
	keyReturnAddress := seedProposalReturnAccountWithTag(
		t, store, returnHash, 0, &models.Account{
			Pool:      poolYes,
			Drep:      drepYes,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	scriptReturnAddress := seedProposalReturnAccountWithTag(
		t, store, returnHash, 1, &models.Account{
			Pool:      poolYes,
			Drep:      drepYes,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	infoAction, err := cbor.Encode(&lcommon.InfoGovAction{
		Type: uint(lcommon.GovActionTypeInfo),
	})
	require.NoError(t, err)
	for i, proposal := range []struct {
		seed       byte
		deposit    uint64
		returnAddr []byte
	}{
		{0x81, 30, keyReturnAddress},
		{0x82, 40, keyReturnAddress},
		{0x83, 50, scriptReturnAddress},
	} {
		require.NoError(t, db.SetGovernanceProposal(
			&models.GovernanceProposal{
				TxHash:        testBytes(32, proposal.seed),
				ActionIndex:   uint32(i),
				ActionType:    uint8(lcommon.GovActionTypeInfo),
				ProposedEpoch: epoch - 1,
				ExpiresEpoch:  epoch + 4,
				Deposit:       proposal.deposit,
				ReturnAddress: proposal.returnAddr,
				AnchorURL:     "https://example.invalid/deposit",
				AnchorHash:    testBytes(32, proposal.seed),
				GovActionCbor: infoAction,
				AddedSlot:     1,
			}, nil,
		))
	}

	committeeAction, err := cbor.Encode(&lcommon.UpdateCommitteeGovAction{
		Type:        uint(lcommon.GovActionTypeUpdateCommittee),
		Credentials: []lcommon.Credential{},
		CredEpochs:  map[*lcommon.Credential]uint64{},
		Quorum:      newRat(2, 3),
	})
	require.NoError(t, err)
	target := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0x84),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: epoch - 1,
		ExpiresEpoch:  epoch + 4,
		AnchorURL:     "https://example.invalid/committee",
		AnchorHash:    testBytes(32, 0x85),
		GovActionCbor: committeeAction,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(target, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      target.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: drepYes,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      target.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: poolYes,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	pparams := conwayPParamsFixture(10)
	pparams.DRepVotingThresholds.CommitteeNoConfidence = newRat(40, 100)
	pparams.PoolVotingThresholds.CommitteeNoConfidence = newRat(40, 100)
	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    epoch - 1,
		NewEpoch:     epoch,
		BoundarySlot: epoch * 100,
		PParams:      pparams,
		UpdateFn: func(
			p lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.RatifiedCount)
	stored, err := db.GetGovernanceProposal(
		target.TxHash,
		target.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored.RatifiedEpoch)
	assert.Equal(t, epoch, *stored.RatifiedEpoch)

	markRows, err := db.GetPoolStakeSnapshotsByEpoch(epoch, "mark", nil)
	require.NoError(t, err)
	for _, row := range markRows {
		switch string(row.PoolKeyHash) {
		case string(poolYes):
			assert.Equal(t, uint64(100), uint64(row.TotalStake))
		case string(poolNo):
			assert.Equal(t, uint64(300), uint64(row.TotalStake))
		}
	}

	// Rolling back the proposal transactions removes their deposits from
	// both voting distributions without changing the persistent mark snapshot.
	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(0, nil))
	drepState, err := LoadDRepVotingState(db, nil, epoch, false)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint64(100),
		drepState.Powers[(models.StakeCredentialRef{
			Tag: 0,
			Key: drepYes,
		}).MapKey()],
	)
	spoState, err := LoadSPOVotingState(db, nil, epoch)
	require.NoError(t, err)
	rolledBackTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeUpdateCommittee),
	}
	require.NoError(t, tallySPOVotes(
		&TallyContext{
			DB:           db,
			StakeEpoch:   epoch,
			CurrentEpoch: epoch,
			SPOState:     spoState,
		},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeSPO,
			VoterCredential: poolYes,
			Vote:            models.VoteYes,
		}},
		rolledBackTally,
	))
	assert.Equal(t, uint64(100), rolledBackTally.SPOYesStake)
	assert.Equal(t, uint64(400), rolledBackTally.SPOTotalStake)
}

func TestProcessEpochUpdateCommitteeThresholdUsesCommitteePresence(
	t *testing.T,
) {
	t.Parallel()

	const epoch = uint64(6)
	tests := []struct {
		name             string
		committeeExpires uint64
		wantRatify       bool
	}{
		{
			name:       "absent committee uses no-confidence threshold",
			wantRatify: true,
		},
		{
			name:             "all-expired elected committee uses normal threshold",
			committeeExpires: epoch - 1,
			wantRatify:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, store := newTallyTestDB(t)
			drepYes := testBytes(28, 90)
			drepNo := testBytes(28, 91)
			require.NoError(t, store.CreateDrep(nil, &models.Drep{
				Credential: drepYes,
				Active:     true,
			}))
			require.NoError(t, store.CreateDrep(nil, &models.Drep{
				Credential: drepNo,
				Active:     true,
			}))
			seedDRepStake(
				t, store, testBytes(28, 92), drepYes,
				models.DrepTypeAddrKeyHash, 60, 93,
			)
			seedDRepStake(
				t, store, testBytes(28, 94), drepNo,
				models.DrepTypeAddrKeyHash, 40, 95,
			)

			poolYes := testBytes(28, 96)
			poolNo := testBytes(28, 97)
			seedPoolWithStake(t, store, poolYes, testBytes(28, 98), 60, epoch)
			seedPoolWithStake(t, store, poolNo, testBytes(28, 99), 40, epoch)

			if tt.committeeExpires != 0 {
				require.NoError(t, db.SetCommitteeMembers(
					[]*models.CommitteeMember{{
						ColdCredHash: testBytes(28, 100),
						ExpiresEpoch: tt.committeeExpires,
					}}, nil,
				))
			}

			actionCbor, err := cbor.Encode(&lcommon.UpdateCommitteeGovAction{
				Type:        uint(lcommon.GovActionTypeUpdateCommittee),
				Credentials: []lcommon.Credential{},
				CredEpochs:  map[*lcommon.Credential]uint64{},
				Quorum:      newRat(2, 3),
			})
			require.NoError(t, err)
			proposal := &models.GovernanceProposal{
				TxHash:        testBytes(32, 101),
				ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
				ProposedEpoch: epoch - 1,
				ExpiresEpoch:  epoch + 4,
				GovActionCbor: actionCbor,
				AddedSlot:     1,
			}
			require.NoError(t, db.SetGovernanceProposal(proposal, nil))
			for _, vote := range []*models.GovernanceVote{
				{
					ProposalID:      proposal.ID,
					VoterType:       models.VoterTypeDRep,
					VoterCredential: drepYes,
					Vote:            models.VoteYes,
					AddedSlot:       2,
				},
				{
					ProposalID:      proposal.ID,
					VoterType:       models.VoterTypeSPO,
					VoterCredential: poolYes,
					Vote:            models.VoteYes,
					AddedSlot:       2,
				},
			} {
				require.NoError(t, db.SetGovernanceVote(vote, nil))
			}

			pparams := conwayPParamsFixture(10)
			pparams.DRepVotingThresholds.CommitteeNormal = newRat(70, 100)
			pparams.DRepVotingThresholds.CommitteeNoConfidence = newRat(50, 100)
			pparams.PoolVotingThresholds.CommitteeNormal = newRat(70, 100)
			pparams.PoolVotingThresholds.CommitteeNoConfidence = newRat(50, 100)
			txn := db.MetadataTxn(true)
			defer txn.Release()
			out, err := ProcessEpoch(&EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    epoch - 1,
				NewEpoch:     epoch,
				BoundarySlot: epoch * 100,
				PParams:      pparams,
				UpdateFn: func(
					p lcommon.ProtocolParameters,
					_ any,
				) (lcommon.ProtocolParameters, error) {
					return p, nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())

			stored, err := db.GetGovernanceProposal(
				proposal.TxHash, proposal.ActionIndex, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, stored)
			if tt.wantRatify {
				assert.Equal(t, 1, out.RatifiedCount)
				require.NotNil(t, stored.RatifiedEpoch)
				assert.Equal(t, epoch, *stored.RatifiedEpoch)
			} else {
				assert.Equal(t, 0, out.RatifiedCount)
				assert.Nil(t, stored.RatifiedEpoch)
			}
		})
	}
}

func TestLoadDRepVotingStateAddsProposalDepositToAlwaysNoConfidence(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	returnStakeCred := testBytes(28, 1)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			DrepType:  models.DrepTypeAlwaysNoConfidence,
			AddedSlot: 1,
			Active:    true,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 10),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       75,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAB),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(75), state.NoConfidencePower)
}

func TestLoadDRepVotingStateExcludesAlwaysAbstainProposalDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	returnStakeCred := testBytes(28, 1)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			DrepType:  models.DrepTypeAlwaysAbstain,
			AddedSlot: 1,
			Active:    true,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 11),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       75,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAC),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), state.AbstainPower)
	assert.Equal(t, uint64(0), state.NoConfidencePower)
}

func TestLoadDRepVotingStateExcludesExpiredProposalDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	returnStakeCred := testBytes(28, 2)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	// Expired as of epoch 6: expires_epoch (5) < currentEpoch (6), so
	// GetActiveGovernanceProposals must not return it.
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 12),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 1,
		ExpiresEpoch:  5,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAD),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}
	assert.Equal(t, uint64(0), state.Powers[ref.MapKey()])
}

func TestLoadDRepVotingStateExcludesDeregisteredReturnAccountDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	drepStakeCred := testBytes(28, 2)
	returnStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, drepStakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    false,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 13),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAE),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}
	assert.Equal(t, uint64(100), state.Powers[ref.MapKey()])
}

func TestLoadDRepVotingStateHonorsDelegatorInactivityGateOnDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	drepStakeCred := testBytes(28, 2)
	returnStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, drepStakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	// The return account's own CIP-0163 activity stamp is stale relative to
	// currentEpoch (6): ExpirationEpoch (1) < expiryEpoch (6).
	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:            drepCred,
			DrepType:        models.DrepTypeAddrKeyHash,
			AddedSlot:       1,
			Active:          true,
			ExpirationEpoch: 1,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 14),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAF),
		AddedSlot:     1,
	}, nil))

	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}

	// Gate off: byte-identical to pre-CIP-0163 behavior, deposit still
	// counts regardless of the stale ExpirationEpoch.
	offState, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(150), offState.Powers[ref.MapKey()])

	// Gate on: the return account is inactive by the same rule
	// VotingPowerBatchSQL applies to its ordinary stake, so its deposit is
	// excluded too.
	onState, err := LoadDRepVotingState(db, nil, 6, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), onState.Powers[ref.MapKey()])
}

// TestLoadDRepVotingStateProposalDepositSumOverflow drives the per-return-
// account deposit accumulation past the uint64 boundary using three active
// proposals that all return to the same account: two deposits of MaxInt64
// (each safely representable in the sqlite INTEGER deposit column) plus one
// of 2 sum to MaxUint64+1. This targets the deposit-summation guard itself
// (ActiveProposalDepositDRepPower's `deposits[key], err = addUint64(...)`),
// not the later drep-power merge -- a single deposit anywhere near MaxUint64
// cannot round-trip through the database, since the sqlite INTEGER type is
// signed 64-bit.
func TestLoadDRepVotingStateProposalDepositSumOverflow(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	returnStakeCred := testBytes(28, 2)
	const maxInt64 = uint64(1<<63 - 1)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	for i, deposit := range []uint64{maxInt64, maxInt64, 2} {
		require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
			TxHash:        testBytes(32, byte(0xC0+i)),
			ActionIndex:   0,
			ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
			ProposedEpoch: 5,
			ExpiresEpoch:  10,
			Deposit:       deposit,
			ReturnAddress: returnAddrBytes,
			AnchorURL:     "https://example.invalid/deposit",
			AnchorHash:    testBytes(32, byte(0xD0+i)),
			AddedSlot:     1,
		}, nil))
	}

	_, err := LoadDRepVotingState(db, nil, 6, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sum active proposal deposits")
}
