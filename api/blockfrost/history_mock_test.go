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

package blockfrost

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"strconv"
)

// pageStub is a canned answer for a paginated node method that also records
// the arguments of its last call.
type pageStub[T any] struct {
	items  []T
	total  int
	err    error
	params PaginationParams
	key    string
	number uint64
}

func (p *pageStub[T]) respond(
	number uint64,
	key string,
	params PaginationParams,
) ([]T, int, error) {
	p.number = number
	p.key = key
	p.params = params
	return p.items, p.total, p.err
}

// valueStub is a canned answer for a single-value node method.
type valueStub[T any] struct {
	value  T
	err    error
	number uint64
	key    string
}

func (v *valueStub[T]) respond(number uint64, key string) (T, error) {
	v.number = number
	v.key = key
	return v.value, v.err
}

// historyMock supplies the historical-query half of BlockfrostNode for
// mockNode.
type historyMock struct {
	poolHistoryMock
	accountHistoryMock
	governanceMock
	epochByNumber   valueStub[EpochInfo]
	epochsAfter     pageStub[EpochInfo]
	epochsBefore    pageStub[EpochInfo]
	epochStakes     pageStub[EpochStakeInfo]
	epochPoolStakes pageStub[EpochPoolStakeInfo]
	epochBlocks     pageStub[string]
	epochPoolBlocks pageStub[string]
}

func (m *historyMock) Epoch(
	_ context.Context,
	epoch uint64,
) (EpochInfo, error) {
	return m.epochByNumber.respond(epoch, "")
}

func (m *historyMock) EpochsAfter(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	return m.epochsAfter.respond(epoch, "", params)
}

func (m *historyMock) EpochsBefore(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	return m.epochsBefore.respond(epoch, "", params)
}

func (m *historyMock) EpochStakes(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochStakeInfo, int, error) {
	return m.epochStakes.respond(epoch, "", params)
}

func (m *historyMock) EpochPoolStakes(
	_ context.Context,
	epoch uint64,
	poolID string,
	params PaginationParams,
) ([]EpochPoolStakeInfo, int, error) {
	return m.epochPoolStakes.respond(epoch, poolID, params)
}

func (m *historyMock) EpochBlocks(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]string, int, error) {
	return m.epochBlocks.respond(epoch, "", params)
}

func (m *historyMock) EpochPoolBlocks(
	_ context.Context,
	epoch uint64,
	poolID string,
	params PaginationParams,
) ([]string, int, error) {
	return m.epochPoolBlocks.respond(epoch, poolID, params)
}

func jsonDecode(recorder *httptest.ResponseRecorder, into any) error {
	return json.Unmarshal(recorder.Body.Bytes(), into)
}

// poolHistoryMock supplies the pool half of historyMock.
type poolHistoryMock struct {
	poolsRetired   pageStub[PoolRetiringInfo]
	poolHistory    pageStub[PoolHistoryInfo]
	poolRelays     valueStub[[]PoolRelayInfo]
	poolDelegators pageStub[PoolDelegatorInfo]
	poolBlocks     pageStub[string]
	poolUpdates    pageStub[PoolUpdateInfo]
}

func (m *poolHistoryMock) PoolsRetired(
	_ context.Context,
	params PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return m.poolsRetired.respond(0, "", params)
}

func (m *poolHistoryMock) PoolHistory(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolHistoryInfo, int, error) {
	return m.poolHistory.respond(0, poolID, params)
}

func (m *poolHistoryMock) PoolRelays(
	_ context.Context,
	poolID string,
) ([]PoolRelayInfo, error) {
	return m.poolRelays.respond(0, poolID)
}

func (m *poolHistoryMock) PoolDelegators(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolDelegatorInfo, int, error) {
	return m.poolDelegators.respond(0, poolID, params)
}

func (m *poolHistoryMock) PoolBlocks(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]string, int, error) {
	return m.poolBlocks.respond(0, poolID, params)
}

func (m *poolHistoryMock) PoolUpdates(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolUpdateInfo, int, error) {
	return m.poolUpdates.respond(0, poolID, params)
}

// accountHistoryMock supplies the account half of historyMock.
type accountHistoryMock struct {
	accountStakeHistory pageStub[AccountStakeHistoryInfo]
	accountMIRs         pageStub[AccountMIRInfo]
	accountAssets       pageStub[AddressAmountInfo]
	accountTotal        valueStub[AccountAddressTotalInfo]
}

func (m *accountHistoryMock) AccountStakeHistory(
	_ context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AccountStakeHistoryInfo, int, error) {
	return m.accountStakeHistory.respond(0, stakeAddress, params)
}

func (m *accountHistoryMock) AccountMIRs(
	_ context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AccountMIRInfo, int, error) {
	return m.accountMIRs.respond(0, stakeAddress, params)
}

func (m *accountHistoryMock) AccountAddressAssets(
	_ context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AddressAmountInfo, int, error) {
	return m.accountAssets.respond(0, stakeAddress, params)
}

func (m *accountHistoryMock) AccountAddressTotal(
	_ context.Context,
	stakeAddress string,
) (AccountAddressTotalInfo, error) {
	return m.accountTotal.respond(0, stakeAddress)
}

func refKey(ref GovActionRef) string {
	return hex.EncodeToString(ref.TxHash) + "#" + strconv.Itoa(int(ref.Index))
}

// governanceMock supplies the governance half of historyMock.
type governanceMock struct {
	proposals           pageStub[GovernanceProposalInfo]
	proposal            valueStub[GovernanceProposalDetailInfo]
	proposalParams      valueStub[GovernanceProposalParametersInfo]
	proposalWithdrawals pageStub[GovernanceWithdrawalInfo]
	proposalVotes       pageStub[GovernanceProposalVoteInfo]
	proposalMetadata    valueStub[GovernanceProposalMetadataInfo]
	drepDelegators      pageStub[DRepDelegatorInfo]
	drepMetadata        valueStub[DRepAnchorInfo]
	drepUpdates         pageStub[DRepUpdateInfo]
	drepVotes           pageStub[DRepVoteInfo]
	poolVotes           pageStub[PoolVoteInfo]
	committee           valueStub[CommitteeInfo]
	committeeVotes      pageStub[CommitteeVoteInfo]
	// committeeCold records the cold flag of the last CommitteeVotes call.
	committeeCold bool
}

func (m *governanceMock) GovernanceProposals(
	_ context.Context,
	params PaginationParams,
) ([]GovernanceProposalInfo, int, error) {
	return m.proposals.respond(0, "", params)
}

func (m *governanceMock) GovernanceProposal(
	_ context.Context,
	ref GovActionRef,
) (GovernanceProposalDetailInfo, error) {
	return m.proposal.respond(0, refKey(ref))
}

func (m *governanceMock) GovernanceProposalParameters(
	_ context.Context,
	ref GovActionRef,
) (GovernanceProposalParametersInfo, error) {
	return m.proposalParams.respond(0, refKey(ref))
}

func (m *governanceMock) GovernanceProposalWithdrawals(
	_ context.Context,
	ref GovActionRef,
	params PaginationParams,
) ([]GovernanceWithdrawalInfo, int, error) {
	return m.proposalWithdrawals.respond(0, refKey(ref), params)
}

func (m *governanceMock) GovernanceProposalVotes(
	_ context.Context,
	ref GovActionRef,
	params PaginationParams,
) ([]GovernanceProposalVoteInfo, int, error) {
	return m.proposalVotes.respond(0, refKey(ref), params)
}

func (m *governanceMock) GovernanceProposalMetadata(
	_ context.Context,
	ref GovActionRef,
) (GovernanceProposalMetadataInfo, error) {
	return m.proposalMetadata.respond(0, refKey(ref))
}

func (m *governanceMock) DRepDelegators(
	_ context.Context,
	credential DRepCredential,
	params PaginationParams,
) ([]DRepDelegatorInfo, int, error) {
	return m.drepDelegators.respond(0, credential.ID, params)
}

func (m *governanceMock) DRepMetadata(
	_ context.Context,
	credential DRepCredential,
) (DRepAnchorInfo, error) {
	return m.drepMetadata.respond(0, credential.ID)
}

func (m *governanceMock) DRepUpdates(
	_ context.Context,
	credential DRepCredential,
	params PaginationParams,
) ([]DRepUpdateInfo, int, error) {
	return m.drepUpdates.respond(0, credential.ID, params)
}

func (m *governanceMock) DRepVotes(
	_ context.Context,
	credential DRepCredential,
	params PaginationParams,
) ([]DRepVoteInfo, int, error) {
	return m.drepVotes.respond(0, credential.ID, params)
}

func (m *governanceMock) PoolVotes(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolVoteInfo, int, error) {
	return m.poolVotes.respond(0, poolID, params)
}

func (m *governanceMock) GovernanceCommittee(
	_ context.Context,
) (CommitteeInfo, error) {
	return m.committee.respond(0, "")
}

func (m *governanceMock) CommitteeVotes(
	_ context.Context,
	credential *CommitteeCredentialRef,
	cold bool,
	params PaginationParams,
) ([]CommitteeVoteInfo, int, error) {
	key := ""
	if credential != nil {
		key = hex.EncodeToString(credential.Hash)
		if credential.HasScript {
			key += "/script"
		}
	}
	m.committeeCold = cold
	return m.committeeVotes.respond(0, key, params)
}
