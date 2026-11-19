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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeGovStateReply encodes the handler's reply and decodes it as a client
// does: a one-element array around the governance state.
func decodeGovStateReply(
	t *testing.T,
	reply any,
) olocalstatequery.GovStateResult {
	t.Helper()
	encoded, err := cbor.Encode(reply)
	require.NoError(t, err)
	var decoded struct {
		cbor.StructAsArray
		State olocalstatequery.GovStateResult
	}
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	return decoded.State
}

func TestQueryShelleyGovStateAssemblesLedgerGovernanceState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 0}
	ls.publishSnapshotsLocked()

	policyHash := repeatedBytes(28, 0x61)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.com/constitution.txt",
		AnchorHash: repeatedBytes(32, 0x62),
		PolicyHash: policyHash,
		AddedSlot:  1,
	}, nil))
	coldCredential := repeatedBytes(28, 0x63)
	require.NoError(t, db.SetCommitteeMembers(ctx, []*models.CommitteeMember{{
		ColdCredentialTag: 0,
		ColdCredHash:      coldCredential,
		ExpiresEpoch:      42,
		AddedSlot:         1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(ctx, big.NewRat(2, 3), 1, nil))

	govAction, err := cbor.Encode([]any{uint64(lcommon.GovActionTypeInfo)})
	require.NoError(t, err)
	returnAddress := append([]byte{0xe0}, repeatedBytes(28, 0x64)...)
	enactedEpoch, enactedSlot := uint64(0), uint64(50)
	enacted := &models.GovernanceProposal{
		TxHash:        repeatedBytes(32, 0x65),
		ActionIndex:   2,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ExpiresEpoch:  9,
		AnchorURL:     "https://example.com/enacted.json",
		AnchorHash:    repeatedBytes(32, 0x66),
		ReturnAddress: returnAddress,
		GovActionCbor: govAction,
		EnactedEpoch:  &enactedEpoch,
		EnactedSlot:   &enactedSlot,
		AddedSlot:     10,
	}
	require.NoError(t, db.SetGovernanceProposal(ctx, enacted, nil))
	pending := &models.GovernanceProposal{
		TxHash:        repeatedBytes(32, 0x67),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ExpiresEpoch:  10,
		AnchorURL:     "https://example.com/pending.json",
		AnchorHash:    repeatedBytes(32, 0x68),
		Deposit:       100_000_000,
		ReturnAddress: returnAddress,
		GovActionCbor: govAction,
		AddedSlot:     100,
	}
	require.NoError(t, db.SetGovernanceProposal(ctx, pending, nil))
	require.NoError(t, db.SetGovernanceVote(ctx, &models.GovernanceVote{
		ProposalID:         pending.ID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    stakeCred28(0x69),
		Vote:               models.VoteYes,
		AddedSlot:          101,
	}, nil))

	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)

	require.NotNil(t, state.ProposalRoots.ParameterChange)
	assert.Equal(
		t,
		enacted.TxHash,
		state.ProposalRoots.ParameterChange.TransactionId[:],
	)
	assert.Equal(
		t,
		enacted.ActionIndex,
		state.ProposalRoots.ParameterChange.GovActionIdx,
	)
	assert.Nil(t, state.ProposalRoots.HardFork)
	assert.Nil(t, state.ProposalRoots.Committee)
	assert.Nil(t, state.ProposalRoots.Constitution)

	var pendingIDs [][]byte
	for _, proposal := range state.Proposals {
		pendingIDs = append(pendingIDs, proposal.Id.TransactionId[:])
	}
	assert.Contains(t, pendingIDs, pending.TxHash)

	committee, err := state.DecodeCommittee()
	require.NoError(t, err)
	require.NotNil(t, committee)
	require.Len(t, committee.Members, 1)
	for credential, expiry := range committee.Members {
		assert.Equal(t, coldCredential, credential.Bytes[:])
		assert.Equal(t, uint64(42), expiry)
	}
	assert.Zero(t, committee.Threshold.Cmp(big.NewRat(2, 3)))

	assert.Equal(
		t,
		"https://example.com/constitution.txt",
		state.Constitution.Anchor.Url,
	)
	assert.Equal(t, policyHash, state.Constitution.ScriptHash)

	future, err := state.DecodeFuturePParams()
	require.NoError(t, err)
	assert.Equal(t, olocalstatequery.FuturePParamsNone, future.Kind)

	pulsing, err := state.DecodeDRepPulsingState()
	require.NoError(t, err)
	assert.Len(t, pulsing.Snapshot.Proposals, len(state.Proposals))
	assert.Equal(
		t,
		state.Constitution,
		pulsing.RatifyState.EnactState.Constitution,
	)
	assert.Empty(t, pulsing.RatifyState.Enacted)
	assert.Empty(t, pulsing.RatifyState.Expired)
	assert.False(t, pulsing.RatifyState.Delayed)
}

func TestQueryShelleyGovStateNoConfidenceReportsNoCommittee(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 0}
	ls.publishSnapshotsLocked()
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.com/constitution.txt",
		AnchorHash: repeatedBytes(32, 0x62),
		AddedSlot:  1,
	}, nil))

	reply, err := ls.queryShelleyLeaf(
		ctx, &olocalstatequery.ShelleyGovStateQuery{}, QueryPoint{}, nil, 13,
	)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)
	committee, err := state.DecodeCommittee()
	require.NoError(t, err)
	assert.Nil(t, committee)
	assert.Nil(t, state.Constitution.ScriptHash)
}

func TestQueryShelleyGovStateRefusesPinnedPoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	_, err := ls.queryShelleyGovState(
		context.Background(),
		QueryPoint{Slot: 10, Hash: repeatedBytes(32, 0x01)},
		nil,
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

func TestQueryShelleyGovStateCarriesDRepAndPoolStake(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newTestDB(t)
	pool := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x11), repeatedBytes(32, 0xaa), 3_000_000, 0,
	)
	drepCredential := stakeCred28(0xc1)
	require.NoError(t, db.CreateDrep(ctx, nil, &models.Drep{
		Credential:    drepCredential,
		CredentialTag: 0,
		ExpiryEpoch:   22,
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey:    stakeCred28(0xd2),
		CredentialTag: 0,
		Drep:          drepCredential,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        5_000_000,
		Active:        true,
		AddedSlot:     100,
	}))
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey:    stakeCred28(0xd3),
		CredentialTag: 0,
		DrepType:      models.DrepTypeAlwaysAbstain,
		Reward:        2_000_000,
		Active:        true,
		AddedSlot:     101,
	}))
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.com/constitution.txt",
		AnchorHash: repeatedBytes(32, 0x62),
		AddedSlot:  1,
	}, nil))
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 0}
	ls.publishSnapshotsLocked()

	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)
	pulsing, err := state.DecodeDRepPulsingState()
	require.NoError(t, err)

	stakeByDRep := make(map[int]uint64)
	for _, entry := range pulsing.Snapshot.DRepDistr {
		stakeByDRep[entry.Drep.Type] += entry.Stake
	}
	assert.Equal(t, uint64(5_000_000), stakeByDRep[int(models.DrepTypeAddrKeyHash)])
	assert.Equal(t, uint64(2_000_000), stakeByDRep[int(models.DrepTypeAlwaysAbstain)])
	assert.NotContains(
		t, stakeByDRep, int(models.DrepTypeAlwaysNoConfidence),
		"an option holding no stake is not in the distribution",
	)

	require.Len(t, pulsing.Snapshot.DRepState, 1)
	assert.Equal(
		t,
		uint64(3_000_000),
		pulsing.Snapshot.PoolDistr[ledger.Blake2b224(pool)],
	)
}

func TestQueryShelleyGovStateSeatsGenesisCommitteeWithoutEnactedQuorum(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetCommitteeMembers(ctx, []*models.CommitteeMember{{
		ColdCredentialTag: 0,
		ColdCredHash:      repeatedBytes(28, 0x63),
		ExpiresEpoch:      42,
		AddedSlot:         1,
	}}, nil))

	committee, err := ls.govStateCommittee(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, committee)
	assert.Len(t, committee.Members, 1)
	assert.NotNil(t, committee.Threshold.Rat)
}
