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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
	recordGovStateBoundary(t, ls)
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

	assert.Empty(t, state.Proposals)

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

	recordGovStateBoundary(t, ls)
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

	recordGovStateBoundary(t, ls)
	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)
	pulsing, err := state.DecodeDRepPulsingState()
	require.NoError(t, err)

	stakeByDRep := make(map[int]uint64)
	for _, entry := range pulsing.Snapshot.DRepDistr {
		stakeByDRep[entry.Drep.Type] += entry.Stake
	}
	assert.Equal(
		t,
		uint64(5_000_000),
		stakeByDRep[int(models.DrepTypeAddrKeyHash)],
	)
	assert.Equal(
		t,
		uint64(2_000_000),
		stakeByDRep[int(models.DrepTypeAlwaysAbstain)],
	)
	assert.NotContains(
		t, stakeByDRep, int(models.DrepTypeAlwaysNoConfidence),
		"an option holding no stake is not in the distribution",
	)

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

// recordGovStateBoundary records the DRep distribution of the boundary that
// opened the ledger's current epoch, as the rollover does.
func recordGovStateBoundary(t *testing.T, ls *LedgerState) {
	t.Helper()
	epoch := ls.currentEpoch
	require.NoError(t, ls.captureDRepDistributionSnapshot(
		context.Background(), nil, epoch.EpochId, epoch.StartSlot,
	))
}

// newGovStateLedger returns a Conway ledger at the given epoch with a
// constitution recorded, the minimum GetGovState needs.
func newGovStateLedger(
	t *testing.T,
	epoch models.Epoch,
) (*LedgerState, *database.Database) {
	t.Helper()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = epoch
	ls.publishSnapshotsLocked()
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.com/constitution.txt",
		AnchorHash: repeatedBytes(32, 0x62),
		AddedSlot:  1,
	}, nil))
	return ls, db
}

func TestQueryShelleyGovStateAnswersPinnedPointAtTheTip(t *testing.T) {
	t.Parallel()

	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 0})
	tip := ocommon.NewPoint(350, repeatedBytes(32, 0x0b))
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))

	recordGovStateBoundary(t, ls)
	_, err := ls.queryShelleyGovState(
		context.Background(),
		QueryPoint{Slot: tip.Slot, Hash: tip.Hash},
		nil,
	)
	require.NoError(t, err)

	_, err = ls.queryShelleyGovState(
		context.Background(),
		QueryPoint{Slot: tip.Slot - 1, Hash: tip.Hash},
		nil,
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// seedGovStateProposal records an Info proposal added at slot, optionally
// enacted at enactedSlot.
func seedGovStateProposal(
	t *testing.T,
	db *database.Database,
	hash byte,
	slot uint64,
	enactedSlot *uint64,
) {
	t.Helper()
	govAction, err := cbor.Encode([]any{uint64(lcommon.GovActionTypeInfo)})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        repeatedBytes(32, hash),
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ExpiresEpoch:  10,
		AnchorURL:     "https://example.com/proposal.json",
		AnchorHash:    repeatedBytes(32, 0x68),
		ReturnAddress: append([]byte{0xe0}, repeatedBytes(28, 0x64)...),
		GovActionCbor: govAction,
		AddedSlot:     slot,
		EnactedSlot:   enactedSlot,
	}
	if enactedSlot != nil {
		epoch := uint64(0)
		proposal.EnactedEpoch = &epoch
	}
	require.NoError(
		t,
		db.SetGovernanceProposal(context.Background(), proposal, nil),
	)
}

func TestQueryShelleyGovStateRefusesWhileAnActionIsPending(t *testing.T) {
	t.Parallel()

	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 1, StartSlot: 100})
	seedGovStateProposal(t, db, 0x72, 150, nil)

	_, err := ls.queryShelleyGovState(context.Background(), QueryPoint{}, nil)
	require.ErrorIs(t, err, ErrQueryNotAnswered)
}

func TestQueryShelleyGovStateRefusesWhenAnActionStoodAtTheBoundary(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 1, StartSlot: 100})
	// Proposed before the epoch began and enacted after, so no action is
	// pending now but the epoch-start snapshot held it.
	enactedSlot := uint64(150)
	seedGovStateProposal(t, db, 0x71, 50, &enactedSlot)

	_, err := ls.queryShelleyGovState(context.Background(), QueryPoint{}, nil)
	require.ErrorIs(t, err, ErrQueryNotAnswered)
}

func TestQueryShelleyGovStateSnapshotHoldsOnlyBoundaryDReps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 1, StartSlot: 100})
	for _, drep := range []struct {
		key  byte
		slot uint64
	}{{0xc1, 50}, {0xc2, 150}} {
		require.NoError(t, db.CreateDrep(ctx, nil, &models.Drep{
			Credential:    stakeCred28(drep.key),
			CredentialTag: 0,
			Active:        true,
			AddedSlot:     drep.slot,
		}))
	}

	recordGovStateBoundary(t, ls)
	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)
	pulsing, err := state.DecodeDRepPulsingState()
	require.NoError(t, err)

	assert.Empty(t, state.Proposals)
	assert.Empty(t, pulsing.Snapshot.Proposals)
	require.Len(t, pulsing.Snapshot.DRepState, 1)
	for credential := range pulsing.Snapshot.DRepState {
		assert.Equal(t, stakeCred28(0xc1), credential.Bytes[:])
	}
}

func TestQueryShelleyGovStatePreviousParametersFollowTheViewEpoch(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	// The live epoch is 21 while the database tip, which is what a view
	// acquired before the boundary sees, is in epoch 20.
	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 21, StartSlot: 2100})
	conwayEraID := uint(eras.ConwayEraDesc.Id)
	for startSlot, epoch := range map[uint64]uint64{
		1900: 19, 2000: 20, 2100: 21,
	} {
		require.NoError(t, db.SetEpoch(
			startSlot, epoch, nil, nil, nil, nil, conwayEraID, 1, 100, nil,
		))
	}
	for epoch, costs := range map[uint64][]int64{
		19: {1, 1, 1},
		20: {2, 2, 2},
	} {
		encoded, err := cbor.Encode(conwayPParamsWithCostModels(
			map[uint][]int64{0: costs},
		))
		require.NoError(t, err)
		require.NoError(t, db.SetPParams(
			encoded, 1900+(epoch-19)*100, epoch, conwayEraID, nil,
		))
	}
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(2050, repeatedBytes(32, 0x0b)),
	}, nil))

	require.NoError(t, ls.captureDRepDistributionSnapshot(
		ctx, nil, 20, 2000,
	))
	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)

	var current, previous conway.ConwayProtocolParameters
	_, err = cbor.Decode(state.CurrentPParams, &current)
	require.NoError(t, err)
	_, err = cbor.Decode(state.PrevPParams, &previous)
	require.NoError(t, err)
	assert.Equal(t, []int64{2, 2, 2}, current.CostModels[0])
	assert.Equal(t, []int64{1, 1, 1}, previous.CostModels[0])
}

func TestQueryShelleyGovStateConvertsPreviousParametersToTheCurrentEra(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	conwayParams := conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	dijkstraParams, err := eras.HardForkDijkstra(nil, conwayParams)
	require.NoError(t, err)
	ls, db := newGovStateLedger(t, models.Epoch{
		EpochId: 20, StartSlot: 2000, EraId: uint(eras.DijkstraEraDesc.Id),
	})
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = dijkstraParams
	ls.publishSnapshotsLocked()
	require.NoError(t, db.SetEpoch(
		1900, 19, nil, nil, nil, nil, uint(eras.ConwayEraDesc.Id), 1, 100, nil,
	))
	require.NoError(t, db.SetEpoch(
		2000,
		20,
		nil,
		nil,
		nil,
		nil,
		uint(eras.DijkstraEraDesc.Id),
		1,
		100,
		nil,
	))
	previous, err := cbor.Encode(conwayParams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		previous, 1900, 19, uint(eras.ConwayEraDesc.Id), nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(2050, repeatedBytes(32, 0x0b)),
	}, nil))

	recordGovStateBoundary(t, ls)
	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)

	want, err := cbor.Encode(dijkstraParams)
	require.NoError(t, err)
	assert.Equal(t, want, []byte(state.PrevPParams),
		"the previous parameters take the current era's representation")
	assert.NotEqual(t, previous, []byte(state.PrevPParams))
}
