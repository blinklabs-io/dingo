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
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func proposalSetTestPParams() *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = lcommon.ProtocolVersionConway + 1
	p.MinCommitteeSize = 3
	p.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    rat(67, 100),
		CommitteeNormal:       rat(67, 100),
		CommitteeNoConfidence: rat(60, 100),
		UpdateToConstitution:  rat(75, 100),
		HardForkInitiation:    rat(60, 100),
		PpNetworkGroup:        rat(67, 100),
		PpEconomicGroup:       rat(67, 100),
		PpTechnicalGroup:      rat(67, 100),
		PpGovGroup:            rat(75, 100),
		TreasuryWithdrawal:    rat(67, 100),
	}
	p.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    rat(51, 100),
		CommitteeNormal:       rat(51, 100),
		CommitteeNoConfidence: rat(51, 100),
		HardForkInitiation:    rat(51, 100),
		PpSecurityGroup:       rat(51, 100),
	}
	return p
}

func runProposalSetBoundary(
	t *testing.T,
	db *database.Database,
	pparams lcommon.ProtocolParameters,
	newEpoch uint64,
	boundarySlot uint64,
) *governance.EpochOutput {
	t.Helper()
	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := governance.ProcessEpoch(&governance.EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: boundarySlot,
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
	return out
}

// Conway GOV resolves votes and parent references against the proposals set
// (Rules/Gov.hs: curGovActionIds = proposalsActionsMap proposals, and
// proposalsAddAction's pGraph membership test). EPOCH removes an expired
// action and its subtree only when it applies the pulser that classified it
// (Rules/Epoch.hs proposalsApplyEnactment), one boundary after RATIFY's
// `gasExpiresAfter < reCurrentEpoch` (Rules/Ratify.hs). In the epoch between,
// the action and its descendants are still members: a child may name it, a
// descendant may still be voted on, and a vote on the action itself fails
// only VotingOnExpiredGovAction (`curEpoch <= gasExpiresAfter`).
func TestProposalSetKeepsExpiredActionUntilItIsDropped(t *testing.T) {
	t.Parallel()

	pparams := proposalSetTestPParams()
	lv, db := governanceTestView(t, pparams)
	for epoch := uint64(10); epoch <= 12; epoch++ {
		require.NoError(t, db.SetEpoch(
			epoch*100, epoch, nil, nil, nil, nil, 0, 1, 100, nil,
		))
	}
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	parentID := governanceTestID(0x41, 0)
	childID := governanceTestID(0x42, 0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        parentID.TransactionId[:],
		ActionIndex:   parentID.GovActionIdx,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 10,
		ExpiresEpoch:  10,
		Deposit:       5,
		ReturnAddress: returnAddrBytes,
		AddedSlot:     1_010,
	}, hardForkGovernanceTestAction(t, nil, 11, 0))
	parentIdx := parentID.GovActionIdx
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          childID.TransactionId[:],
		ActionIndex:     childID.GovActionIdx,
		ActionType:      uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch:   10,
		ExpiresEpoch:    16,
		ParentTxHash:    parentID.TransactionId[:],
		ParentActionIdx: &parentIdx,
		Deposit:         5,
		ReturnAddress:   returnAddrBytes,
		AddedSlot:       1_020,
	}, hardForkGovernanceTestAction(t, &parentID, 11, 1))

	// The parent's last voting epoch is 10. RATIFY at the boundary into 11
	// classifies it expired; it stays in the proposals set throughout 11.
	out := runProposalSetBoundary(t, db, pparams, 11, 1_100)
	require.Equal(t, 1, out.ExpiredCount)
	const slotInEleven = 1_150

	parentState, err := lv.GovActionById(parentID)
	require.NoError(t, err)
	require.NotNil(
		t,
		parentState,
		"expired parent left the proposals set a boundary early",
	)
	require.True(t, lv.GovActionExists(parentID))
	var expiryErr conway.VotingOnExpiredGovActionError
	require.ErrorAs(t, conway.UtxoValidateVotingOnExpiredGovAction(
		governanceVoteTestTx(parentID, lcommon.VoterTypeDRepKeyHash),
		slotInEleven, lv, pparams,
	), &expiryErr)

	grandchild := governanceProposalTestTx(
		t,
		hardForkGovernanceTestAction(t, &parentID, 11, 2),
	)
	require.NoError(t, conway.UtxoValidateProposalAncestry(
		grandchild, slotInEleven, lv, pparams,
	), "a child naming a member of the proposals set was rejected")

	require.True(
		t,
		lv.GovActionExists(childID),
		"descendant of an expired action left the proposals set a boundary early",
	)
	childVote := governanceVoteTestTx(childID, lcommon.VoterTypeDRepKeyHash)
	require.NoError(t, conway.UtxoValidateUnknownGovActionIds(
		childVote, slotInEleven, lv, pparams,
	))
	require.NoError(t, conway.UtxoValidateVotingOnExpiredGovAction(
		childVote, slotInEleven, lv, pparams,
	))

	// A child proposed during 11 under the expired parent joins its subtree
	// and leaves with it at the boundary into 12.
	lateChildID := governanceTestID(0x43, 0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          lateChildID.TransactionId[:],
		ActionIndex:     lateChildID.GovActionIdx,
		ActionType:      uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch:   11,
		ExpiresEpoch:    17,
		ParentTxHash:    parentID.TransactionId[:],
		ParentActionIdx: &parentIdx,
		Deposit:         5,
		ReturnAddress:   returnAddrBytes,
		AddedSlot:       slotInEleven,
	}, hardForkGovernanceTestAction(t, &parentID, 11, 2))

	runProposalSetBoundary(t, db, pparams, 12, 1_200)
	for _, id := range []lcommon.GovActionId{parentID, childID, lateChildID} {
		state, err := lv.GovActionById(id)
		require.NoError(t, err)
		require.Nil(t, state, "dropped subtree still resolvable")
		require.False(t, lv.GovActionExists(id))
		proposal, err := db.GetGovernanceProposal(
			id.TransactionId[:], id.GovActionIdx, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, proposal.DroppedEpoch)
		require.Equal(t, uint64(12), *proposal.DroppedEpoch)
	}
	var unknownErr conway.UnknownGovActionIdError
	require.ErrorAs(t, conway.UtxoValidateUnknownGovActionIds(
		childVote, 1_250, lv, pparams,
	), &unknownErr)
}

// preprodVoteOnExpiredParentsChild is preprod transaction
// d7d663ce95ec482418e5678d3ac1b6dfb748cdb5a093ec44385b9050f9351904, in block
// de223e7f0b0628fe158e472a83fef45d6028ff8d637d31d507ae23e9dc3f7163 (height
// 5,152,095, slot 133,175,865, epoch 312). It carries a DRep vote on
// ParameterChange 4f2b214e...7b15#0 (proposed in 311, gasExpiresAfter 317),
// whose parent 78a9aafe...0db3#0 (proposed in 305, gasExpiresAfter 311) RATIFY
// classified expired at the boundary into 312. Both left the proposals set at
// the boundary into 313.
const preprodVoteOnExpiredParentsChild = "" +
	"84a500d90102818258203d5f13a8d355d53eb7e99ce447fe890e6ecdc6c014e3" +
	"da4deef32f268608748100018182583930fe12059162068d4302ae76efd38515" +
	"3a08ee82aed46a0a6f4238ce6ae867f87c9bddadcae49ce60d430d5f65876c72" +
	"a0ab6beab2d8434c0b1b000000011bb1ce7f021a0002e481075820bdaa99eb15" +
	"8414dea0a91d6c727e2268574b23efe6e08ab3b841abe8059a030c13a1820358" +
	"1cfe12059162068d4302ae76efd385153a08ee82aed46a0a6f4238ce6aa18258" +
	"204f2b214e38732ed27cef1006470a7a161577dcb5ca620b594a5626155fc07b" +
	"1500820182785d68747470733a2f2f676174657761792e70696e6174612e636c" +
	"6f75642f697066732f6261666b726569676a62676478736c72356166656c6f78" +
	"666e6d737a34376a33336662636e66377565746f76636b657833627274706477" +
	"743576755820165be521963b57ce3857736ae44f64068addb490183ffe50b4cb" +
	"e6c0b34d380ba200d90102828258204edacc8cba0ff93118666ec81f1aabe085" +
	"221f8e3ca5117371042d1820dce4d55840a911d5db111f7a1ac7bb26fb38bad9" +
	"8703d6b32aea407d9eceab555bfac129b03c0cec9be3b7b43bfa16dd7ea81d4e" +
	"bd2b793a9ed77814f0a0e6f153a0ace804825820de33b1511cac065733086ca2" +
	"9d3b2c75597dafbf3613299f90412ab5ff80526058400edbe15ec0f6cde0fbb0" +
	"6a2cd718e1dc95c806d1f3628ffa04a645e7b0837d88b5f9a7fbb5577d01cdfd" +
	"40e376a8e6fc533ce9375f50d7df50cb3cb2b978510301d90102818303028382" +
	"00581c16c1554c34114687cfe699e548e1799c4a1dc17c76b869a63236186382" +
	"00581c63083bdf144e0976e5ac4dd23cd82f5d0493d7a46365c6c8f1f9899982" +
	"00581c6e23d3de62a0f2820f0c6e9c462e366e9a1448c3d6970a1619f5460bf5" +
	"d90103a0"

func TestPreprodVoteOnDescendantOfExpiredActionIsValid(t *testing.T) {
	t.Parallel()

	const (
		epochLength   = 432_000
		boundary312   = 133_142_400
		voteSlot      = 133_175_865
		lifetime      = 6
		parentEpoch   = 305
		childEpoch    = 311
		boundaryEpoch = 312
	)
	txBytes, err := hex.DecodeString(preprodVoteOnExpiredParentsChild)
	require.NoError(t, err)
	tx, err := conway.NewConwayTransactionFromCbor(txBytes)
	require.NoError(t, err)
	require.Equal(
		t,
		"d7d663ce95ec482418e5678d3ac1b6dfb748cdb5a093ec44385b9050f9351904",
		tx.Hash().String(),
	)

	pparams := proposalSetTestPParams()
	lv, db := governanceTestView(t, pparams)
	for epoch := uint64(parentEpoch); epoch <= boundaryEpoch; epoch++ {
		start := boundary312 - (boundaryEpoch-epoch)*epochLength
		require.NoError(t, db.SetEpoch(
			start, epoch, nil, nil, nil, nil, 0, 1, epochLength, nil,
		))
	}
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	parentHash, err := hex.DecodeString(
		"78a9aafe2e4e14828efa8cd5202fec08c996a9a00c7d56b317b6a95a80510db3",
	)
	require.NoError(t, err)
	childHash, err := hex.DecodeString(
		"4f2b214e38732ed27cef1006470a7a161577dcb5ca620b594a5626155fc07b15",
	)
	require.NoError(t, err)
	var parentID lcommon.GovActionId
	copy(parentID.TransactionId[:], parentHash)
	parentIdx := uint32(0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        parentHash,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch: parentEpoch,
		ExpiresEpoch:  parentEpoch + lifetime,
		Deposit:       5,
		ReturnAddress: returnAddrBytes,
		AddedSlot:     boundary312 - 7*epochLength,
	}, &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
	})
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          childHash,
		ActionType:      uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch:   childEpoch,
		ExpiresEpoch:    childEpoch + lifetime,
		ParentTxHash:    parentHash,
		ParentActionIdx: &parentIdx,
		Deposit:         5,
		ReturnAddress:   returnAddrBytes,
		AddedSlot:       boundary312 - epochLength/2,
	}, &conway.ConwayParameterChangeGovAction{
		Type:     uint(lcommon.GovActionTypeParameterChange),
		ActionId: &parentID,
	})

	out := runProposalSetBoundary(t, db, pparams, boundaryEpoch, boundary312)
	require.Equal(t, 1, out.ExpiredCount)

	require.NoError(t, conway.UtxoValidateUnknownGovActionIds(
		tx, voteSlot, lv, pparams,
	), "preprod accepted this vote on a proposals-set member")
	require.NoError(t, conway.UtxoValidateVotingOnExpiredGovAction(
		tx, voteSlot, lv, pparams,
	))
}
