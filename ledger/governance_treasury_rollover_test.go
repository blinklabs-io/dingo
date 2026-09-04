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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const treasuryRolloverGenesisHash = "0101010101010101010101010101010101010101010101010101010101010101"

type treasuryRolloverFixture struct {
	ls             *LedgerState
	db             *database.Database
	currentEpoch   models.Epoch
	currentPParams *conway.ConwayProtocolParameters
	hotCredential  []byte
}

func newTreasuryRolloverFixture(
	t *testing.T,
	treasury uint64,
) *treasuryRolloverFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	currentEpoch := newTestEpoch(5, 500, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(treasury, 1_000, 499, nil))

	pparams := donationTestConwayPParams(10)
	pparams.MinCommitteeSize = 1
	pparams.DRepVotingThresholds.TreasuryWithdrawal = cbor.Rat{
		Rat: big.NewRat(0, 1),
	}

	coldCredential := repeatByte(28, 0xc1)
	hotCredential := repeatByte(28, 0xc2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: currentEpoch.EpochId + 20,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(slog.NewJSONHandler(
				io.Discard,
				nil,
			)),
		},
	}
	return &treasuryRolloverFixture{
		ls:             ls,
		db:             db,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		hotCredential:  hotCredential,
	}
}

func (f *treasuryRolloverFixture) rewardAddress(
	t *testing.T,
	marker byte,
) (*lcommon.Address, []byte, []byte) {
	t.Helper()
	stakeCredential := repeatByte(28, marker)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCredential,
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)
	require.NoError(t, f.db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCredential,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	return &address, addressBytes, stakeCredential
}

func (f *treasuryRolloverFixture) addProposal(
	t *testing.T,
	marker byte,
	addedSlot uint64,
	withdrawals map[*lcommon.Address]uint64,
	returnAddress []byte,
	deposit uint64,
	ratified bool,
) *models.GovernanceProposal {
	t.Helper()
	actionCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        2,
		Withdrawals: withdrawals,
	})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, marker),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: f.currentEpoch.EpochId,
		ExpiresEpoch:  f.currentEpoch.EpochId + 20,
		AnchorURL:     "https://example.invalid/treasury-withdrawal",
		AnchorHash:    repeatByte(32, marker+1),
		Deposit:       deposit,
		ReturnAddress: returnAddress,
		GovActionCbor: actionCbor,
		AddedSlot:     addedSlot,
	}
	if ratified {
		ratifiedEpoch := f.currentEpoch.EpochId
		ratifiedSlot := f.currentEpoch.StartSlot + 50
		proposal.RatifiedEpoch = &ratifiedEpoch
		proposal.RatifiedSlot = &ratifiedSlot
	}
	require.NoError(t, f.db.SetGovernanceProposal(proposal, nil))
	require.NoError(t, f.db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: f.hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       addedSlot + 1,
	}, nil))
	return proposal
}

func (f *treasuryRolloverFixture) rollover(
	t *testing.T,
	currentEpoch models.Epoch,
	currentPParams lcommon.ProtocolParameters,
) *EpochRolloverResult {
	t.Helper()
	var result *EpochRolloverResult
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = f.ls.processEpochRollover(
			txn,
			currentEpoch,
			eras.ConwayEraDesc,
			currentPParams,
			false,
		)
		return rolloverErr
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	return result
}

func (f *treasuryRolloverFixture) proposal(
	t *testing.T,
	proposal *models.GovernanceProposal,
) *models.GovernanceProposal {
	t.Helper()
	loaded, err := f.db.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

func (f *treasuryRolloverFixture) accountReward(
	t *testing.T,
	stakeCredential []byte,
) uint64 {
	t.Helper()
	account, err := f.db.GetAccountByCredential(
		0,
		stakeCredential,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	return uint64(account.Reward)
}

func TestProcessEpochRolloverTreasuryRatificationUsesRunningBudget(
	t *testing.T,
) {
	f := newTreasuryRolloverFixture(t, 100)
	firstAddress, firstReturn, firstCredential := f.rewardAddress(t, 0x11)
	secondAddress, secondReturn, secondCredential := f.rewardAddress(t, 0x21)
	thirdAddress, thirdReturn, _ := f.rewardAddress(t, 0x31)
	fourthAddress, fourthReturn, fourthCredential := f.rewardAddress(t, 0x41)
	fifthAddress, _, _ := f.rewardAddress(t, 0x51)

	first := f.addProposal(
		t, 0x61, 501,
		map[*lcommon.Address]uint64{firstAddress: 70},
		firstReturn, 0, false,
	)
	second := f.addProposal(
		t, 0x62, 503,
		map[*lcommon.Address]uint64{secondAddress: 40},
		secondReturn, 0, false,
	)
	overflow := f.addProposal(
		t, 0x63, 505,
		map[*lcommon.Address]uint64{
			thirdAddress: ^uint64(0),
			fifthAddress: 1,
		},
		thirdReturn, 0, false,
	)
	fourth := f.addProposal(
		t, 0x64, 507,
		map[*lcommon.Address]uint64{fourthAddress: 30},
		fourthReturn, 0, false,
	)

	firstRollover := f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+1,
		firstRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, first).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, second).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, overflow).RatifiedEpoch)
	assert.NotNil(t, f.proposal(t, fourth).RatifiedEpoch)

	secondRollover := f.rollover(
		t,
		firstRollover.NewCurrentEpoch,
		firstRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+2,
		secondRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, first).EnactedEpoch)
	assert.Nil(t, f.proposal(t, second).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, overflow).RatifiedEpoch)
	assert.NotNil(t, f.proposal(t, fourth).EnactedEpoch)
	assert.Equal(t, uint64(70), f.accountReward(t, firstCredential))
	assert.Equal(t, uint64(0), f.accountReward(t, secondCredential))
	assert.Equal(t, uint64(30), f.accountReward(t, fourthCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Zero(t, treasury)
}

func TestProcessEpochRolloverEnactmentFailureRollsBackAndRetries(
	t *testing.T,
) {
	f := newTreasuryRolloverFixture(t, 100)
	failingAddress, failingReturn, failingCredential := f.rewardAddress(t, 0x71)
	succeedingAddress, succeedingReturn, succeedingCredential := f.rewardAddress(
		t,
		0x72,
	)
	failing := f.addProposal(
		t, 0x73, 501,
		map[*lcommon.Address]uint64{failingAddress: 60},
		[]byte{0xff}, 1, true,
	)
	succeeding := f.addProposal(
		t, 0x74, 503,
		map[*lcommon.Address]uint64{succeedingAddress: 50},
		succeedingReturn, 0, true,
	)
	firstRollover := f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+1,
		firstRollover.NewCurrentEpoch.EpochId,
	)
	failedAfterRollover := f.proposal(t, failing)
	assert.Nil(t, failedAfterRollover.EnactedEpoch)
	assert.Nil(t, failedAfterRollover.RatifiedEpoch)
	assert.Nil(t, failedAfterRollover.ExpiredEpoch)
	assert.Nil(t, failedAfterRollover.DeletedSlot)
	assert.NotNil(t, f.proposal(t, succeeding).EnactedEpoch)
	assert.Zero(t, f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, reserves, _ := networkState(t, f.db)
	assert.Equal(t, uint64(50), treasury)

	retry := f.proposal(t, failing)
	retry.ReturnAddress = failingReturn
	require.NoError(t, f.db.SetGovernanceProposal(retry, nil))
	require.NoError(t, f.db.Metadata().SetNetworkState(
		70,
		reserves,
		firstRollover.NewCurrentEpoch.StartSlot+50,
		nil,
	))

	secondRollover := f.rollover(
		t,
		firstRollover.NewCurrentEpoch,
		firstRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+2,
		secondRollover.NewCurrentEpoch.EpochId,
	)
	assert.Nil(t, f.proposal(t, failing).EnactedEpoch)
	assert.NotNil(t, f.proposal(t, failing).RatifiedEpoch)
	assert.Zero(t, f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, _, _ = networkState(t, f.db)
	assert.Equal(t, uint64(70), treasury)

	thirdRollover := f.rollover(
		t,
		secondRollover.NewCurrentEpoch,
		secondRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+3,
		thirdRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, failing).EnactedEpoch)
	assert.Equal(t, uint64(60), f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, _, _ = networkState(t, f.db)
	assert.Equal(t, uint64(10), treasury)
}

func TestProcessEpochRolloverReplayEnactmentFailureRemainsFatal(
	t *testing.T,
) {
	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, _, stakeCredential := f.rewardAddress(t, 0x81)
	proposal := f.addProposal(
		t, 0x82, 501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		[]byte{0xff}, 1, true,
	)
	enactedEpoch := f.currentEpoch.EpochId + 1
	enactedSlot := f.currentEpoch.StartSlot +
		uint64(f.currentEpoch.LengthInSlots)
	proposal.EnactedEpoch = &enactedEpoch
	proposal.EnactedSlot = &enactedSlot
	require.NoError(t, f.db.SetGovernanceProposal(proposal, nil))

	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		_, rolloverErr := f.ls.processEpochRollover(
			txn,
			f.currentEpoch,
			eras.ConwayEraDesc,
			f.currentPParams,
			false,
		)
		return rolloverErr
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replay enacted proposal")
	assert.Zero(t, f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Equal(t, uint64(100), treasury)
	newEpoch, err := f.db.Metadata().GetEpoch(enactedEpoch, nil)
	require.NoError(t, err)
	assert.Nil(t, newEpoch)
}
