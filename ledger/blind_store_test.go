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
	"errors"
	"math/big"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var errStoreReadDenied = errors.New("metadata store read denied by the test")

// blindStore is the metadata store's LedgerDB with the named reads failing
// while it is on, so a read that still returns an answer was served without
// the store.
type blindStore struct {
	ledgerdb.LedgerDB
	denied map[string]bool
	// off lets reads through, for the writes whose own logic reads the
	// store to learn what it applied.
	off  atomic.Bool
	hits atomic.Int64
}

func (b *blindStore) deny(name string) error {
	if !b.denied[name] || b.off.Load() {
		return nil
	}
	b.hits.Add(1)
	return errStoreReadDenied
}

func (b *blindStore) GetAccountByCredential(
	ctx context.Context,
	tag uint8,
	key []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Account, error) {
	if err := b.deny("GetAccountByCredential"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetAccountByCredential(ctx, tag, key, includeInactive, txn)
}

func (b *blindStore) GetAccountsByCredential(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	includeInactive bool,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	if err := b.deny("GetAccountsByCredential"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetAccountsByCredential(ctx, refs, includeInactive, txn)
}

func (b *blindStore) GetDrepByCredential(
	ctx context.Context,
	tag uint8,
	cred []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Drep, error) {
	if err := b.deny("GetDrepByCredential"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDrepByCredential(ctx, tag, cred, includeInactive, txn)
}

func (b *blindStore) GetActiveDreps(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	if err := b.deny("GetActiveDreps"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActiveDreps(ctx, txn)
}

func (b *blindStore) GetDormantDRepEpochs(
	ctx context.Context,
	txn ledgerdb.Txn,
) (uint64, error) {
	if err := b.deny("GetDormantDRepEpochs"); err != nil {
		return 0, err
	}
	return b.LedgerDB.GetDormantDRepEpochs(ctx, txn)
}

func (b *blindStore) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	if err := b.deny("GetPoolRegistrationsEffectiveForEpoch"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPoolRegistrationsEffectiveForEpoch(
		poolKeyHashes, epochStartSlot, endedEpoch, snapshotSlot, txn,
	)
}

func (b *blindStore) GetPoolStatus(
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (models.PoolStatus, error) {
	if err := b.deny("GetPoolStatus"); err != nil {
		return models.PoolStatus{}, err
	}
	return b.LedgerDB.GetPoolStatus(pkh, txn)
}

func (b *blindStore) LatestPoolOpCertSequence(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	if err := b.deny("LatestPoolOpCertSequence"); err != nil {
		return 0, false, err
	}
	return b.LedgerDB.LatestPoolOpCertSequence(ctx, pkh, txn)
}

func (b *blindStore) LatestPoolOpCertSequenceAfter(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	afterSlot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	if err := b.deny("LatestPoolOpCertSequenceAfter"); err != nil {
		return 0, false, err
	}
	return b.LedgerDB.LatestPoolOpCertSequenceAfter(ctx, pkh, afterSlot, txn)
}

func (b *blindStore) LatestPoolOpCertSequenceAtOrBefore(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	if err := b.deny("LatestPoolOpCertSequenceAtOrBefore"); err != nil {
		return 0, false, err
	}
	return b.LedgerDB.LatestPoolOpCertSequenceAtOrBefore(ctx, pkh, slot, txn)
}

func (b *blindStore) LatestPoolOpCertSequenceInRange(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	startSlot, endSlot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	if err := b.deny("LatestPoolOpCertSequenceInRange"); err != nil {
		return 0, false, err
	}
	return b.LedgerDB.LatestPoolOpCertSequenceInRange(
		ctx, pkh, startSlot, endSlot, txn,
	)
}

func (b *blindStore) LatestPoolOpCertSequences(
	ctx context.Context,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("LatestPoolOpCertSequences"); err != nil {
		return nil, err
	}
	return b.LedgerDB.LatestPoolOpCertSequences(ctx, txn)
}

func (b *blindStore) GetConstitution(
	txn ledgerdb.Txn,
) (*models.Constitution, error) {
	if err := b.deny("GetConstitution"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetConstitution(txn)
}

func (b *blindStore) GetCommitteeQuorum(
	ctx context.Context, txn ledgerdb.Txn,
) (*big.Rat, error) {
	if err := b.deny("GetCommitteeQuorum"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetCommitteeQuorum(ctx, txn)
}

func (b *blindStore) LatestCommitteeQuorum(
	ctx context.Context, txn ledgerdb.Txn,
) (*models.CommitteeQuorum, error) {
	if err := b.deny("LatestCommitteeQuorum"); err != nil {
		return nil, err
	}
	return b.LedgerDB.LatestCommitteeQuorum(ctx, txn)
}

func (b *blindStore) LatestCommitteeResignations(
	ctx context.Context, txn ledgerdb.Txn,
) ([]*models.ResignCommitteeCold, error) {
	if err := b.deny("LatestCommitteeResignations"); err != nil {
		return nil, err
	}
	return b.LedgerDB.LatestCommitteeResignations(ctx, txn)
}

func (b *blindStore) GetCommitteeMembers(
	ctx context.Context, txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	if err := b.deny("GetCommitteeMembers"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetCommitteeMembers(ctx, txn)
}

func (b *blindStore) GetCommitteeMembersIncludeDeleted(
	ctx context.Context, txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	if err := b.deny("GetCommitteeMembersIncludeDeleted"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetCommitteeMembersIncludeDeleted(ctx, txn)
}

func (b *blindStore) GetCommitteeMember(
	ctx context.Context, tag uint8, cold []byte, termStartSlot uint64, txn ledgerdb.Txn,
) (*models.AuthCommitteeHot, error) {
	if err := b.deny("GetCommitteeMember"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetCommitteeMember(ctx, tag, cold, termStartSlot, txn)
}

func (b *blindStore) IsCommitteeMemberResigned(
	ctx context.Context, tag uint8, cold []byte, termStartSlot uint64, txn ledgerdb.Txn,
) (bool, error) {
	if err := b.deny("IsCommitteeMemberResigned"); err != nil {
		return false, err
	}
	return b.LedgerDB.IsCommitteeMemberResigned(ctx, tag, cold, termStartSlot, txn)
}

func (b *blindStore) GetResignedCommitteeMembers(
	ctx context.Context, creds []models.CommitteeCredential, txn ledgerdb.Txn,
) (map[string]bool, error) {
	if err := b.deny("GetResignedCommitteeMembers"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetResignedCommitteeMembers(ctx, creds, txn)
}

func (b *blindStore) GetActiveCommitteeMembers(
	ctx context.Context, txn ledgerdb.Txn,
) ([]*models.AuthCommitteeHot, error) {
	if err := b.deny("GetActiveCommitteeMembers"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActiveCommitteeMembers(ctx, txn)
}

func (b *blindStore) GetCommitteeAuthorizedCount(
	ctx context.Context, txn ledgerdb.Txn,
) (int, error) {
	if err := b.deny("GetCommitteeAuthorizedCount"); err != nil {
		return 0, err
	}
	return b.LedgerDB.GetCommitteeAuthorizedCount(ctx, txn)
}

func (b *blindStore) GetCommitteeHotAuthorizationsSince(
	ctx context.Context, minSlot uint64, txn ledgerdb.Txn,
) ([]*models.AuthCommitteeHot, error) {
	if err := b.deny("GetCommitteeHotAuthorizationsSince"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetCommitteeHotAuthorizationsSince(ctx, minSlot, txn)
}

func (b *blindStore) GetLastEnactedGovernanceProposal(
	ctx context.Context,
	actionTypes []uint8,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	if err := b.deny("GetLastEnactedGovernanceProposal"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetLastEnactedGovernanceProposal(ctx, actionTypes, txn)
}

func (b *blindStore) GetLiveStakeInputsForPools(
	pools [][]byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := b.deny("GetLiveStakeInputsForPools"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetLiveStakeInputsForPools(pools, expiryEpoch, txn)
}

func (b *blindStore) GetDelegatedPoolKeyHashes(
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := b.deny("GetDelegatedPoolKeyHashes"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDelegatedPoolKeyHashes(txn)
}

func (b *blindStore) GetStakeByPools(
	ctx context.Context,
	pools [][]byte,
	txn ledgerdb.Txn,
) (map[string]uint64, map[string]uint64, error) {
	if err := b.deny("GetStakeByPools"); err != nil {
		return nil, nil, err
	}
	return b.LedgerDB.GetStakeByPools(ctx, pools, txn)
}

func (b *blindStore) GetDRepVotingPower(
	ctx context.Context,
	tag uint8,
	drep []byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	if err := b.deny("GetDRepVotingPower"); err != nil {
		return 0, err
	}
	return b.LedgerDB.GetDRepVotingPower(ctx, tag, drep, expiryEpoch, txn)
}

func (b *blindStore) GetDRepVotingPowerBatch(
	ctx context.Context,
	drepCredentials []models.StakeCredentialRef,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("GetDRepVotingPowerBatch"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDRepVotingPowerBatch(ctx, drepCredentials, expiryEpoch, txn)
}

func (b *blindStore) GetDRepVotingPowerByType(
	ctx context.Context,
	drepTypes []uint64,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (map[uint64]uint64, error) {
	if err := b.deny("GetDRepVotingPowerByType"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDRepVotingPowerByType(ctx, drepTypes, expiryEpoch, txn)
}

func (b *blindStore) GetGovernanceProposal(
	ctx context.Context,
	txHash []byte,
	actionIndex uint32,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	if err := b.deny("GetGovernanceProposal"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetGovernanceProposal(ctx, txHash, actionIndex, txn)
}

func (b *blindStore) GetActiveGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetActiveGovernanceProposals"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActiveGovernanceProposals(ctx, epoch, txn)
}

func (b *blindStore) GetGovernanceProposalSet(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetGovernanceProposalSet"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetGovernanceProposalSet(ctx, txn)
}

func (b *blindStore) GetGovernanceProposalSetAtSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetGovernanceProposalSetAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetGovernanceProposalSetAtSlot(ctx, slot, txn)
}

func (b *blindStore) GetExpiringGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetExpiringGovernanceProposals"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetExpiringGovernanceProposals(ctx, epoch, txn)
}

func (b *blindStore) GetExpiredGovernanceProposalsAt(
	ctx context.Context,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetExpiredGovernanceProposalsAt"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetExpiredGovernanceProposalsAt(ctx, epoch, slot, txn)
}

func (b *blindStore) GetExpiredAwaitingDropGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetExpiredAwaitingDropGovernanceProposals"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetExpiredAwaitingDropGovernanceProposals(ctx, epoch, txn)
}

func (b *blindStore) GetDroppedGovernanceProposalsAt(
	ctx context.Context,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetDroppedGovernanceProposalsAt"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDroppedGovernanceProposalsAt(ctx, epoch, slot, txn)
}

func (b *blindStore) GetEnactedGovernanceProposalsAt(
	ctx context.Context,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetEnactedGovernanceProposalsAt"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetEnactedGovernanceProposalsAt(ctx, epoch, slot, txn)
}

func (b *blindStore) GetRatifiedGovernanceProposals(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := b.deny("GetRatifiedGovernanceProposals"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRatifiedGovernanceProposals(ctx, txn)
}

func (b *blindStore) GetGovernanceVotes(
	ctx context.Context,
	proposalID uint,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	if err := b.deny("GetGovernanceVotes"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetGovernanceVotes(ctx, proposalID, txn)
}

func (b *blindStore) GetActiveAccountCredentials(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if err := b.deny("GetActiveAccountCredentials"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActiveAccountCredentials(txn)
}

func (b *blindStore) GetActiveDelegatorCountsByPool(
	pools []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("GetActiveDelegatorCountsByPool"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActiveDelegatorCountsByPool(pools, txn)
}

func (b *blindStore) GetActiveDelegatorCredentialsByPool(
	pools []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	if err := b.deny("GetActiveDelegatorCredentialsByPool"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActiveDelegatorCredentialsByPool(pools, txn)
}

func (b *blindStore) GetDRepDelegators(
	ctx context.Context,
	tag uint8,
	cred []byte,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if err := b.deny("GetDRepDelegators"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDRepDelegators(ctx, tag, cred, txn)
}

func (b *blindStore) GetPoolsRetiringAtEpoch(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetirementRefund, error) {
	if err := b.deny("GetPoolsRetiringAtEpoch"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPoolsRetiringAtEpoch(ctx, epoch, boundarySlot, txn)
}

func (b *blindStore) GetActivePoolKeyHashes(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := b.deny("GetActivePoolKeyHashes"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActivePoolKeyHashes(ctx, txn)
}

func (b *blindStore) GetActivePoolKeyHashesAtSlot(
	slot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := b.deny("GetActivePoolKeyHashesAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetActivePoolKeyHashesAtSlot(slot, txn)
}

func (b *blindStore) HasAppliedRewardCreditRound(
	epoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	if err := b.deny("HasAppliedRewardCreditRound"); err != nil {
		return false, err
	}
	return b.LedgerDB.HasAppliedRewardCreditRound(epoch, txn)
}

func (b *blindStore) GetPendingRewardCreditRounds(
	txn ledgerdb.Txn,
) ([]models.RewardCreditRound, error) {
	if err := b.deny("GetPendingRewardCreditRounds"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPendingRewardCreditRounds(txn)
}

func (b *blindStore) GetPendingRewardAccountOutputsForCredential(
	tag uint8,
	key []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	const name = "GetPendingRewardAccountOutputsForCredential"
	if err := b.deny(name); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPendingRewardAccountOutputsForCredential(tag, key, txn)
}

func (b *blindStore) HasPendingRewardCreditRounds(
	txn ledgerdb.Txn,
) (bool, error) {
	if err := b.deny("HasPendingRewardCreditRounds"); err != nil {
		return false, err
	}
	return b.LedgerDB.HasPendingRewardCreditRounds(txn)
}

func (b *blindStore) HasUnfoldedRewardCreditsThroughEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	if err := b.deny("HasUnfoldedRewardCreditsThroughEpoch"); err != nil {
		return false, err
	}
	return b.LedgerDB.HasUnfoldedRewardCreditsThroughEpoch(epoch, txn)
}

func (b *blindStore) GetPostSnapshotRewardCreditEvents(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	if err := b.deny("GetPostSnapshotRewardCreditEvents"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPostSnapshotRewardCreditEvents(slot, txn)
}

func (b *blindStore) GetPostSnapshotRewardCredits(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	if err := b.deny("GetPostSnapshotRewardCredits"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPostSnapshotRewardCredits(slot, txn)
}

func (b *blindStore) LatestPoolOpCertSequencesAtOrBefore(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("LatestPoolOpCertSequencesAtOrBefore"); err != nil {
		return nil, err
	}
	return b.LedgerDB.LatestPoolOpCertSequencesAtOrBefore(ctx, slot, txn)
}

func (b *blindStore) PoolOpCertSequencesExistAtSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	if err := b.deny("PoolOpCertSequencesExistAtSlot"); err != nil {
		return false, err
	}
	return b.LedgerDB.PoolOpCertSequencesExistAtSlot(ctx, slot, txn)
}

func (b *blindStore) GetPParamUpdates(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]models.PParamUpdate, error) {
	if err := b.deny("GetPParamUpdates"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPParamUpdates(epoch, txn)
}

func (b *blindStore) ForecastPParamUpdates(
	epoch uint64,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	cloneFunc func(lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error),
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	if err := b.deny("ForecastPParamUpdates"); err != nil {
		return nil, err
	}
	return b.LedgerDB.ForecastPParamUpdates(
		epoch, quorum, currentPParams, decodeFunc, updateFunc, cloneFunc, txn,
	)
}

func (b *blindStore) GetAccountRegistrationHistoryByCredential(
	ctx context.Context,
	tag uint8,
	key []byte,
	limit int,
	offset int,
	order string,
	txn ledgerdb.Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	const name = "GetAccountRegistrationHistoryByCredential"
	if err := b.deny(name); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetAccountRegistrationHistoryByCredential(
		ctx, tag, key, limit, offset, order, txn,
	)
}

func (b *blindStore) GetStakeCredentialsWithRegistrationEvents(
	fromSlot, toSlot uint64,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if err := b.deny("GetStakeCredentialsWithRegistrationEvents"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetStakeCredentialsWithRegistrationEvents(
		fromSlot, toSlot, txn,
	)
}

func (b *blindStore) GetLatestAccountRegistrationAtOrBefore(
	ctx context.Context,
	tag uint8,
	key []byte,
	slot uint64,
	txn ledgerdb.Txn,
) (*models.AccountRegistrationHistoryRow, error) {
	if err := b.deny("GetLatestAccountRegistrationAtOrBefore"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetLatestAccountRegistrationAtOrBefore(
		ctx, tag, key, slot, txn,
	)
}

func (b *blindStore) GetStakeRegistrationsByCredential(
	tag uint8,
	key []byte,
	txn ledgerdb.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	if err := b.deny("GetStakeRegistrationsByCredential"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetStakeRegistrationsByCredential(tag, key, txn)
}

func (b *blindStore) GetAccountsActiveAtSlot(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]struct{}, error) {
	if err := b.deny("GetAccountsActiveAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetAccountsActiveAtSlot(refs, slot, txn)
}

func (b *blindStore) GetDrepLastRegistrationDeposit(
	ctx context.Context,
	tag uint8,
	cred []byte,
	txn ledgerdb.Txn,
) (*uint64, error) {
	if err := b.deny("GetDrepLastRegistrationDeposit"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDrepLastRegistrationDeposit(ctx, tag, cred, txn)
}

func (b *blindStore) GetDrepLastRegistrationDeposits(
	ctx context.Context,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("GetDrepLastRegistrationDeposits"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDrepLastRegistrationDeposits(ctx, txn)
}

func (b *blindStore) GetDrepsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	if err := b.deny("GetDrepsAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDrepsAtSlot(ctx, refs, slot, txn)
}

func (b *blindStore) GetDRepDelegatorsAtSlot(
	ctx context.Context,
	dreps []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	if err := b.deny("GetDRepDelegatorsAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDRepDelegatorsAtSlot(ctx, dreps, slot, txn)
}

func (b *blindStore) GetDrepRegistrationDepositsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("GetDrepRegistrationDepositsAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetDrepRegistrationDepositsAtSlot(ctx, refs, slot, txn)
}

func (b *blindStore) GetGovernanceVotesAtSlot(
	ctx context.Context,
	proposalID uint,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	if err := b.deny("GetGovernanceVotesAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetGovernanceVotesAtSlot(ctx, proposalID, slot, txn)
}

func (b *blindStore) GetPoolRegistrationsAtSlot(
	pools []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	if err := b.deny("GetPoolRegistrationsAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPoolRegistrationsAtSlot(pools, slot, txn)
}

func (b *blindStore) GetRetiringPoolsAtSlot(
	currentEpoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	if err := b.deny("GetRetiringPoolsAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRetiringPoolsAtSlot(currentEpoch, slot, txn)
}

func (b *blindStore) GetPoolDepositsHeldAtSlot(
	pools []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("GetPoolDepositsHeldAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPoolDepositsHeldAtSlot(pools, slot, txn)
}

func (b *blindStore) GetAccountsByCredentialAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	if err := b.deny("GetAccountsByCredentialAtSlot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetAccountsByCredentialAtSlot(ctx, refs, slot, txn)
}

func (b *blindStore) GetEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.Epoch, error) {
	if err := b.deny("GetEpoch"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetEpoch(epoch, txn)
}

func (b *blindStore) GetEpochSummary(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.EpochSummary, error) {
	if err := b.deny("GetEpochSummary"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetEpochSummary(epoch, txn)
}

func (b *blindStore) ListPParamsForEra(
	era uint,
	txn ledgerdb.Txn,
) ([]models.PParams, error) {
	if err := b.deny("ListPParamsForEra"); err != nil {
		return nil, err
	}
	return b.LedgerDB.ListPParamsForEra(era, txn)
}

func (b *blindStore) GetEpochBoundaryActivePoolKeyHashes(
	slot, boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := b.deny("GetEpochBoundaryActivePoolKeyHashes"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetEpochBoundaryActivePoolKeyHashes(slot, boundarySlot, txn)
}

func (b *blindStore) GetEpochBoundaryDelegatedPoolKeyHashes(
	snapshotSlot, boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := b.deny("GetEpochBoundaryDelegatedPoolKeyHashes"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetEpochBoundaryDelegatedPoolKeyHashes(
		snapshotSlot, boundarySlot, txn,
	)
}

func (b *blindStore) GetEpochBoundaryStakeByPools(
	poolKeyHashes [][]byte,
	snapshotSlot, boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch, inactivityPeriod uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, map[string]uint64, error) {
	if err := b.deny("GetEpochBoundaryStakeByPools"); err != nil {
		return nil, nil, err
	}
	return b.LedgerDB.GetEpochBoundaryStakeByPools(
		poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (b *blindStore) GetEpochBoundaryRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	snapshotSlot, boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch, inactivityPeriod uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := b.deny("GetEpochBoundaryRewardStakeInputsForPools"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetEpochBoundaryRewardStakeInputsForPools(
		poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (b *blindStore) GetPointerStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot, boundarySlot, expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := b.deny("GetPointerStakeInputsForPools"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPointerStakeInputsForPools(
		poolKeyHashes, slot, boundarySlot, expiryEpoch, txn,
	)
}

func (b *blindStore) IterateAccounts(
	txn ledgerdb.Txn,
	fn func(*models.Account) error,
) error {
	if err := b.deny("IterateAccounts"); err != nil {
		return err
	}
	return b.LedgerDB.IterateAccounts(txn, fn)
}

func (b *blindStore) IterateDreps(
	txn ledgerdb.Txn,
	fn func(*models.Drep) error,
) error {
	if err := b.deny("IterateDreps"); err != nil {
		return err
	}
	return b.LedgerDB.IterateDreps(txn, fn)
}

func (b *blindStore) IterateLiveDrepDelegators(
	txn ledgerdb.Txn,
	fn func(drep, stake models.StakeCredentialRef) error,
) error {
	if err := b.deny("IterateLiveDrepDelegators"); err != nil {
		return err
	}
	return b.LedgerDB.IterateLiveDrepDelegators(txn, fn)
}

func (b *blindStore) ListPoolKeyHashes(txn ledgerdb.Txn) ([][]byte, error) {
	if err := b.deny("ListPoolKeyHashes"); err != nil {
		return nil, err
	}
	return b.LedgerDB.ListPoolKeyHashes(txn)
}

func (b *blindStore) GetPools(
	ctx context.Context,
	pkhs []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) ([]models.Pool, error) {
	if err := b.deny("GetPools"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPools(ctx, pkhs, txn)
}

func (b *blindStore) GetPoolDepositsHeld(
	pkhs []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("GetPoolDepositsHeld"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPoolDepositsHeld(pkhs, txn)
}

func (b *blindStore) AccountLastWitnessSlots(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	maxSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := b.deny("AccountLastWitnessSlots"); err != nil {
		return nil, err
	}
	return b.LedgerDB.AccountLastWitnessSlots(ctx, refs, maxSlot, txn)
}

func (b *blindStore) GetAccountImportRegistrationByCredential(
	ctx context.Context,
	tag uint8,
	key []byte,
	txn ledgerdb.Txn,
) (*models.AccountImportRegistration, error) {
	if err := b.deny("GetAccountImportRegistrationByCredential"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetAccountImportRegistrationByCredential(
		ctx,
		tag,
		key,
		txn,
	)
}

func (b *blindStore) CountPoolBlocksInSlotRange(
	poolKeyHashes []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, error) {
	if err := b.deny("CountPoolBlocksInSlotRange"); err != nil {
		return nil, 0, err
	}
	return b.LedgerDB.CountPoolBlocksInSlotRange(
		poolKeyHashes, startSlot, endSlot, txn,
	)
}

func (b *blindStore) GetPoolBlockIssuersInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolOpCertSequence, error) {
	if err := b.deny("GetPoolBlockIssuersInSlotRange"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetPoolBlockIssuersInSlotRange(startSlot, endSlot, txn)
}

func (b *blindStore) MergeImportedPoolBlockCounts(
	epoch uint64,
	epochStartSlot uint64,
	counts map[string]uint64,
	totalBlocks uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if err := b.deny("MergeImportedPoolBlockCounts"); err != nil {
		return nil, 0, false, err
	}
	return b.LedgerDB.MergeImportedPoolBlockCounts(
		epoch, epochStartSlot, counts, totalBlocks, txn,
	)
}

func (b *blindStore) CountEpochPoolBlocks(
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if err := b.deny("CountEpochPoolBlocks"); err != nil {
		return nil, 0, false, err
	}
	return b.LedgerDB.CountEpochPoolBlocks(
		epoch, poolKeys, startSlot, endSlot, txn,
	)
}

func (b *blindStore) SumTransactionFeesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	if err := b.deny("SumTransactionFeesInSlotRange"); err != nil {
		return 0, err
	}
	return b.LedgerDB.SumTransactionFeesInSlotRange(startSlot, endSlot, txn)
}

func (b *blindStore) GetRewardAdaPots(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.RewardAdaPots, error) {
	if err := b.deny("GetRewardAdaPots"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardAdaPots(epoch, txn)
}

func (b *blindStore) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (*models.RewardSnapshot, error) {
	if err := b.deny("GetRewardSnapshot"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardSnapshot(epoch, snapshotType, txn)
}

func (b *blindStore) GetRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (string, error) {
	if err := b.deny("GetRewardSeedFailure"); err != nil {
		return "", err
	}
	return b.LedgerDB.GetRewardSeedFailure(epoch, snapshotType, txn)
}

func (b *blindStore) GetRewardPoolInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolInput, error) {
	if err := b.deny("GetRewardPoolInputs"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardPoolInputs(epoch, txn)
}

func (b *blindStore) GetRewardStakeInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := b.deny("GetRewardStakeInputs"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardStakeInputs(epoch, txn)
}

func (b *blindStore) GetRewardStakeInputsInPoolKeyHashRange(
	epoch uint64,
	lo, hi []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := b.deny("GetRewardStakeInputsInPoolKeyHashRange"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardStakeInputsInPoolKeyHashRange(
		epoch, lo, hi, txn,
	)
}

func (b *blindStore) GetRewardPoolOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolOutput, error) {
	if err := b.deny("GetRewardPoolOutputs"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardPoolOutputs(epoch, txn)
}

func (b *blindStore) GetRewardAccountOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if err := b.deny("GetRewardAccountOutputs"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardAccountOutputs(epoch, txn)
}

func (b *blindStore) GetRewardAccountOutputsForEligibility(
	epochs []uint64,
	tag uint8,
	key []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if err := b.deny("GetRewardAccountOutputsForEligibility"); err != nil {
		return nil, err
	}
	return b.LedgerDB.GetRewardAccountOutputsForEligibility(
		epochs, tag, key, txn,
	)
}

// GetSyncState is denied for a precompute cursor only: the ledger reads other
// sync state through the store in every run.
func (b *blindStore) GetSyncState(
	key string,
	txn ledgerdb.Txn,
) (string, error) {
	if isRewardCursorKey(key) {
		if err := b.deny("GetSyncState"); err != nil {
			return "", err
		}
	}
	return b.LedgerDB.GetSyncState(key, txn)
}
