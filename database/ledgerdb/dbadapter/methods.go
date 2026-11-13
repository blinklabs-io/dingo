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

package dbadapter

import (
	"context"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

func (a Adapter) ConsumedUtxoPruneFloor(txn ledgerdb.Txn) (uint64, error) {
	return a.db.ConsumedUtxoPruneFloor(dbTxn(txn))
}

func (a Adapter) CountUtxosConsumedUpTo(
	ctx context.Context,
	slot uint64,
	limit int,
) (int, error) {
	return a.db.CountUtxosConsumedUpTo(ctx, slot, limit)
}

func (a Adapter) GetLiveUtxoLovelace(txn ledgerdb.Txn) (uint64, error) {
	return a.db.Metadata().GetLiveUtxoLovelace(metaTxn(txn))
}

func (a Adapter) IterateLiveUtxoRefs(
	ctx context.Context,
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return a.db.IterateLiveUtxoRefs(ctx, dbTxn(txn), fn)
}

func (a Adapter) IterateLiveUtxos(
	ctx context.Context,
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return a.db.IterateLiveUtxos(ctx, dbTxn(txn), fn)
}

func (a Adapter) IterateUtxoRefsAsOf(
	ctx context.Context,
	atSlot uint64,
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return a.db.IterateUtxoRefsAsOf(ctx, atSlot, dbTxn(txn), fn)
}

func (a Adapter) IterateUtxoRefsAsOfPaged(
	ctx context.Context,
	atSlot uint64,
	pageSize int,
	fn func(database.UtxoRef) error,
) error {
	return a.db.IterateUtxoRefsAsOfPaged(ctx, atSlot, pageSize, fn)
}

func (a Adapter) MarkUtxosDeletedAtSlot(
	ctx context.Context,
	txn ledgerdb.Txn,
	refs []types.UtxoKey,
	atSlot uint64,
) error {
	return a.db.MarkUtxosDeletedAtSlot(ctx, dbTxn(txn), refs, atSlot)
}

func (a Adapter) UtxoByRef(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn ledgerdb.Txn,
) (*models.Utxo, error) {
	return a.db.UtxoByRef(ctx, txId, outputIdx, dbTxn(txn))
}

func (a Adapter) UtxoByRefIncludingSpent(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn ledgerdb.Txn,
) (*models.Utxo, error) {
	return a.db.UtxoByRefIncludingSpent(ctx, txId, outputIdx, dbTxn(txn))
}

func (a Adapter) UtxoExists(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn ledgerdb.Txn,
) (bool, error) {
	return a.db.UtxoExists(ctx, txId, outputIdx, dbTxn(txn))
}

func (a Adapter) UtxosByAddress(
	ctx context.Context,
	addrs []ledger.Address,
	maxResults int,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByAddress(ctx, addrs, maxResults, dbTxn(txn))
}

func (a Adapter) UtxosByAddressAsOf(
	ctx context.Context,
	addrs []ledger.Address,
	atSlot uint64,
	maxResults int,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByAddressAsOf(ctx, addrs, atSlot, maxResults, dbTxn(txn))
}

func (a Adapter) UtxosByRefs(
	ctx context.Context,
	refs []models.UtxoId,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByRefs(ctx, refs, dbTxn(txn))
}

func (a Adapter) UtxosByRefsAsOf(
	ctx context.Context,
	refs []models.UtxoId,
	atSlot uint64,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByRefsAsOf(ctx, refs, atSlot, dbTxn(txn))
}

func (a Adapter) UtxosByRefsBounded(
	ctx context.Context,
	refs []models.UtxoId,
	maxBytes int64,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByRefsBounded(ctx, refs, maxBytes, dbTxn(txn))
}

func (a Adapter) UtxosDeleteConsumed(
	ctx context.Context,
	slot uint64,
	limit int,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.UtxosDeleteConsumed(ctx, slot, limit, dbTxn(txn))
}

func (a Adapter) UtxosDeleteSpentInBatch(
	ids []models.UtxoId,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.UtxosDeleteSpentInBatch(ids, slot, dbTxn(txn))
}

func (a Adapter) UtxosDropSpentInBatchBlobs(
	ids []models.UtxoId,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.UtxosDropSpentInBatchBlobs(ids, slot, dbTxn(txn))
}

func (a Adapter) AccountInactivityActivationMembership(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	txn ledgerdb.Txn,
) (map[string]struct{}, error) {
	return a.db.AccountInactivityActivationMembership(ctx, refs, dbTxn(txn))
}

func (a Adapter) AccountLastWitnessSlots(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	maxSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.AccountLastWitnessSlots(ctx, refs, maxSlot, dbTxn(txn))
}

func (a Adapter) AccountsWitnessedAfterSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	return a.db.AccountsWitnessedAfterSlot(ctx, slot, dbTxn(txn))
}

func (a Adapter) GetAccountByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Account, error) {
	return a.db.GetAccountByCredential(
		ctx,
		credentialTag,
		stakeKey,
		includeInactive,
		dbTxn(txn),
	)
}

func (a Adapter) GetAccountImportRegistrationByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	txn ledgerdb.Txn,
) (*models.AccountImportRegistration, error) {
	return a.db.GetAccountImportRegistrationByCredential(
		ctx,
		credentialTag,
		stakeKey,
		dbTxn(txn),
	)
}

func (a Adapter) GetAccountRegistrationHistoryByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	limit int,
	offset int,
	order string,
	txn ledgerdb.Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	return a.db.GetAccountRegistrationHistoryByCredential(
		ctx,
		credentialTag,
		stakeKey,
		limit,
		offset,
		order,
		dbTxn(txn),
	)
}

func (a Adapter) GetAccountsByCredential(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	includeInactive bool,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	return a.db.GetAccountsByCredential(ctx, refs, includeInactive, dbTxn(txn))
}

func (a Adapter) GetAccountsByCredentialAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	return a.db.GetAccountsByCredentialAtSlot(ctx, refs, slot, dbTxn(txn))
}

func (a Adapter) GetActiveAccountCredentials(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	return a.db.Metadata().GetActiveAccountCredentials(metaTxn(txn))
}

func (a Adapter) GetGenesisDelegationForSlot(
	genesisHash []byte,
	blockSlot uint64,
	stabilityWindow uint64,
	txn ledgerdb.Txn,
) (*models.GenesisDelegation, error) {
	return a.db.Metadata().
		GetGenesisDelegationForSlot(genesisHash, blockSlot, stabilityWindow, metaTxn(txn))
}

func (a Adapter) GetGenesisDelegationsInSlotRange(
	fromSlot uint64,
	uptoSlot uint64,
	txn ledgerdb.Txn,
) ([]models.GenesisDelegation, error) {
	return a.db.Metadata().
		GetGenesisDelegationsInSlotRange(fromSlot, uptoSlot, metaTxn(txn))
}

func (a Adapter) GetLatestAccountRegistrationAtOrBefore(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	slot uint64,
	txn ledgerdb.Txn,
) (*models.AccountRegistrationHistoryRow, error) {
	return a.db.GetLatestAccountRegistrationAtOrBefore(
		ctx,
		credentialTag,
		stakeKey,
		slot,
		dbTxn(txn),
	)
}

func (a Adapter) GetStakeCredentialsWithRegistrationEvents(
	fromSlot uint64,
	toSlot uint64,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	return a.db.Metadata().
		GetStakeCredentialsWithRegistrationEvents(fromSlot, toSlot, metaTxn(txn))
}

func (a Adapter) GetStakeRegistrationsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	return a.db.GetStakeRegistrationsByCredential(
		credentialTag,
		stakingKey,
		dbTxn(txn),
	)
}

func (a Adapter) RenewAccountExpirations(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	expirationEpoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.RenewAccountExpirations(ctx, refs, expirationEpoch, dbTxn(txn))
}

func (a Adapter) SetGenesisStakingWithDeposits(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	keyDeposit uint64,
	poolDeposit uint64,
	blockHash []byte,
	txn ledgerdb.Txn,
) error {
	return a.db.SetGenesisStakingWithDeposits(
		pools,
		stakeDelegations,
		keyDeposit,
		poolDeposit,
		blockHash,
		dbTxn(txn),
	)
}

func (a Adapter) StampAllActiveAccountExpirations(
	ctx context.Context,
	expirationEpoch uint64,
	txn ledgerdb.Txn,
) (int64, error) {
	return a.db.StampAllActiveAccountExpirations(
		ctx,
		expirationEpoch,
		dbTxn(txn),
	)
}

func (a Adapter) AddAccountRewardByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn ledgerdb.Txn,
) error {
	return a.db.AddAccountRewardByCredential(
		ctx,
		credentialTag,
		stakeKey,
		amount,
		slot,
		sourceHash,
		dbTxn(txn),
	)
}

func (a Adapter) AddAccountRewardsByCredential(
	ctx context.Context,
	credits []models.AccountRewardCredit,
	txn ledgerdb.Txn,
) error {
	return a.db.AddAccountRewardsByCredential(ctx, credits, dbTxn(txn))
}

func (a Adapter) AddAppliedRewardCreditRound(
	round models.RewardCreditRound,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().AddAppliedRewardCreditRound(round, metaTxn(txn))
}

func (a Adapter) AddPostSnapshotAccountRewardByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn ledgerdb.Txn,
) error {
	return a.db.AddPostSnapshotAccountRewardByCredential(
		ctx,
		credentialTag,
		stakeKey,
		amount,
		slot,
		sourceHash,
		dbTxn(txn),
	)
}

func (a Adapter) ClaimFallbackRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn ledgerdb.Txn,
) (bool, error) {
	return a.db.Metadata().ClaimFallbackRewardSnapshot(snapshot, metaTxn(txn))
}

func (a Adapter) ClaimFallbackRewardSnapshotGuard(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (bool, uint, error) {
	return a.db.Metadata().
		ClaimFallbackRewardSnapshotGuard(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) ClaimPendingRewardCreditsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	return a.db.Metadata().
		ClaimPendingRewardCreditsForCredential(credentialTag, stakingKey, metaTxn(txn))
}

func (a Adapter) ClaimUnfoldedRewardCredits(
	snapshotEpoch uint64,
	limit int,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	return a.db.Metadata().
		ClaimUnfoldedRewardCredits(snapshotEpoch, limit, metaTxn(txn))
}

func (a Adapter) DeleteRewardInputsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().DeleteRewardInputsForEpoch(epoch, metaTxn(txn))
}

func (a Adapter) DeleteRewardOutputsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().DeleteRewardOutputsForEpoch(epoch, metaTxn(txn))
}

func (a Adapter) DeleteRewardStakeInputBeforeEpoch(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		DeleteRewardStakeInputBeforeEpoch(ctx, epoch, metaTxn(txn))
}

func (a Adapter) DeleteRewardStateBeforeEpoch(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		DeleteRewardStateBeforeEpoch(ctx, epoch, metaTxn(txn))
}

func (a Adapter) GetEpochBoundaryRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return a.db.Metadata().
		GetEpochBoundaryRewardStakeInputsForPools(poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID, expiryEpoch, inactivityPeriod, metaTxn(txn))
}

func (a Adapter) GetMIRCertsInSlotRange(
	ctx context.Context,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) ([]models.MIREffect, error) {
	return a.db.GetMIRCertsInSlotRange(ctx, startSlot, endSlot, dbTxn(txn))
}

func (a Adapter) GetPendingRewardAccountOutputsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	return a.db.Metadata().
		GetPendingRewardAccountOutputsForCredential(credentialTag, stakingKey, metaTxn(txn))
}

func (a Adapter) GetPendingRewardCreditRounds(
	txn ledgerdb.Txn,
) ([]models.RewardCreditRound, error) {
	return a.db.Metadata().GetPendingRewardCreditRounds(metaTxn(txn))
}

func (a Adapter) GetPostSnapshotRewardCredits(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	return a.db.Metadata().GetPostSnapshotRewardCredits(slot, metaTxn(txn))
}

func (a Adapter) GetRewardAccountOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	return a.db.Metadata().GetRewardAccountOutputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardAccountOutputsForEligibility(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	return a.db.Metadata().
		GetRewardAccountOutputsForEligibility(epochs, credentialTag, stakingKey, metaTxn(txn))
}

func (a Adapter) GetRewardAdaPots(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.RewardAdaPots, error) {
	return a.db.Metadata().GetRewardAdaPots(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardPoolInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolInput, error) {
	return a.db.Metadata().GetRewardPoolInputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardPoolOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolOutput, error) {
	return a.db.Metadata().GetRewardPoolOutputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (string, error) {
	return a.db.Metadata().
		GetRewardSeedFailure(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (*models.RewardSnapshot, error) {
	return a.db.Metadata().GetRewardSnapshot(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) GetRewardStakeInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return a.db.Metadata().GetRewardStakeInputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardStakeInputsInPoolKeyHashRange(
	epoch uint64,
	poolKeyHashLo []byte,
	poolKeyHashHi []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return a.db.Metadata().
		GetRewardStakeInputsInPoolKeyHashRange(epoch, poolKeyHashLo, poolKeyHashHi, metaTxn(txn))
}

func (a Adapter) HasAppliedRewardCreditRound(
	epoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	return a.db.Metadata().HasAppliedRewardCreditRound(epoch, metaTxn(txn))
}

func (a Adapter) HasPendingRewardCreditRounds(txn ledgerdb.Txn) (bool, error) {
	return a.db.Metadata().HasPendingRewardCreditRounds(metaTxn(txn))
}

func (a Adapter) HasUnfoldedRewardCreditsThroughEpoch(
	snapshotEpoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	return a.db.Metadata().
		HasUnfoldedRewardCreditsThroughEpoch(snapshotEpoch, metaTxn(txn))
}

func (a Adapter) ReleaseFallbackRewardSnapshotGuard(
	guardID uint,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		ReleaseFallbackRewardSnapshotGuard(guardID, metaTxn(txn))
}

func (a Adapter) SaveRewardAccountOutputs(
	outputs []*models.RewardAccountOutput,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveRewardAccountOutputs(outputs, metaTxn(txn))
}

func (a Adapter) SaveRewardAdaPots(
	pots *models.RewardAdaPots,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveRewardAdaPots(pots, metaTxn(txn))
}

func (a Adapter) SaveRewardPoolInputs(
	inputs []*models.RewardPoolInput,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveRewardPoolInputs(inputs, metaTxn(txn))
}

func (a Adapter) SaveRewardPoolOutputs(
	outputs []*models.RewardPoolOutput,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveRewardPoolOutputs(outputs, metaTxn(txn))
}

func (a Adapter) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveRewardSnapshot(snapshot, metaTxn(txn))
}

func (a Adapter) SaveRewardStakeInputs(
	inputs []*models.RewardStakeInput,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveRewardStakeInputs(inputs, metaTxn(txn))
}

func (a Adapter) TakeRewardEligibilityRecheck(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	return a.db.Metadata().TakeRewardEligibilityRecheck(metaTxn(txn))
}

func (a Adapter) ClearDelegationsToRetiredPool(
	poolKeyHash []byte,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		ClearDelegationsToRetiredPool(poolKeyHash, boundarySlot, metaTxn(txn))
}

func (a Adapter) CountPoolBlocksInSlotRange(
	poolKeyHashes []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, error) {
	return a.db.Metadata().
		CountPoolBlocksInSlotRange(poolKeyHashes, startSlot, endSlot, metaTxn(txn))
}

func (a Adapter) DeletePoolStakeSnapshotsBeforeEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		DeletePoolStakeSnapshotsBeforeEpoch(epoch, metaTxn(txn))
}

func (a Adapter) DeletePoolStakeSnapshotsForEpoch(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		DeletePoolStakeSnapshotsForEpoch(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) GetActiveDelegatorCountsByPool(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.Metadata().
		GetActiveDelegatorCountsByPool(poolKeyHashes, metaTxn(txn))
}

func (a Adapter) GetActiveDelegatorCredentialsByPool(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	return a.db.Metadata().
		GetActiveDelegatorCredentialsByPool(poolKeyHashes, metaTxn(txn))
}

func (a Adapter) GetActivePoolKeyHashes(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return a.db.GetActivePoolKeyHashes(ctx, dbTxn(txn))
}

func (a Adapter) GetActivePoolKeyHashesAtSlot(
	slot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return a.db.Metadata().GetActivePoolKeyHashesAtSlot(slot, metaTxn(txn))
}

func (a Adapter) GetActivePoolRelays(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]models.PoolRegistrationRelay, error) {
	return a.db.GetActivePoolRelays(ctx, dbTxn(txn))
}

func (a Adapter) GetDelegatedPoolKeyHashes(
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return a.db.Metadata().GetDelegatedPoolKeyHashes(metaTxn(txn))
}

func (a Adapter) GetEpochBoundaryActivePoolKeyHashes(
	slot uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return a.db.Metadata().
		GetEpochBoundaryActivePoolKeyHashes(slot, boundarySlot, metaTxn(txn))
}

func (a Adapter) GetEpochBoundaryDelegatedPoolKeyHashes(
	snapshotSlot uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return a.db.Metadata().
		GetEpochBoundaryDelegatedPoolKeyHashes(snapshotSlot, boundarySlot, metaTxn(txn))
}

func (a Adapter) GetEpochBoundaryStakeByPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, map[string]uint64, error) {
	return a.db.Metadata().
		GetEpochBoundaryStakeByPools(poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID, expiryEpoch, inactivityPeriod, metaTxn(txn))
}

func (a Adapter) GetFuturePoolByLeiosKey(
	ctx context.Context,
	publicKey []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return a.db.GetFuturePoolByLeiosKey(
		ctx,
		publicKey,
		epochStartSlot,
		dbTxn(txn),
	)
}

func (a Adapter) GetFuturePoolByVrfKeyHash(
	ctx context.Context,
	vrfKeyHash []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return a.db.GetFuturePoolByVrfKeyHash(
		ctx,
		vrfKeyHash,
		epochStartSlot,
		dbTxn(txn),
	)
}

func (a Adapter) GetPool(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return a.db.GetPool(ctx, pkh, includeInactive, dbTxn(txn))
}

func (a Adapter) GetPoolBlockIssuersInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolOpCertSequence, error) {
	return a.db.Metadata().
		GetPoolBlockIssuersInSlotRange(startSlot, endSlot, metaTxn(txn))
}

func (a Adapter) GetPoolByLeiosKey(
	ctx context.Context,
	publicKey []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return a.db.GetPoolByLeiosKey(ctx, publicKey, epochStartSlot, dbTxn(txn))
}

func (a Adapter) GetPoolByVrfKeyHash(
	ctx context.Context,
	vrfKeyHash []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return a.db.GetPoolByVrfKeyHash(ctx, vrfKeyHash, epochStartSlot, dbTxn(txn))
}

func (a Adapter) GetPoolDepositsHeld(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.Metadata().GetPoolDepositsHeld(poolKeyHashes, metaTxn(txn))
}

func (a Adapter) GetPoolDepositsHeldAtSlot(
	poolKeyHashes []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.Metadata().
		GetPoolDepositsHeldAtSlot(poolKeyHashes, slot, metaTxn(txn))
}

func (a Adapter) GetPoolEarliestVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	return a.db.Metadata().
		GetPoolEarliestVrfKeyHashAtSlot(poolKeyHash, slot, metaTxn(txn))
}

func (a Adapter) GetPoolRegistrations(
	poolKeyHash lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	return a.db.GetPoolRegistrations(poolKeyHash, dbTxn(txn))
}

func (a Adapter) GetPoolRegistrationsAtSlot(
	poolKeyHashes []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	return a.db.Metadata().
		GetPoolRegistrationsAtSlot(poolKeyHashes, slot, metaTxn(txn))
}

func (a Adapter) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	return a.db.Metadata().
		GetPoolRegistrationsEffectiveForEpoch(poolKeyHashes, epochStartSlot, endedEpoch, snapshotSlot, metaTxn(txn))
}

func (a Adapter) GetPoolVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	return a.db.Metadata().
		GetPoolVrfKeyHashAtSlot(poolKeyHash, slot, metaTxn(txn))
}

func (a Adapter) GetPoolVrfState(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return a.db.GetPoolVrfState(ctx, pkh, dbTxn(txn))
}

func (a Adapter) GetPools(
	ctx context.Context,
	pkhs []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) ([]models.Pool, error) {
	return a.db.GetPools(ctx, pkhs, dbTxn(txn))
}

func (a Adapter) GetPoolsRetiringAtEpoch(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetirementRefund, error) {
	return a.db.GetPoolsRetiringAtEpoch(ctx, epoch, boundarySlot, dbTxn(txn))
}

func (a Adapter) GetRetiringPools(
	currentEpoch uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	return a.db.Metadata().GetRetiringPools(currentEpoch, metaTxn(txn))
}

func (a Adapter) GetRetiringPoolsAtSlot(
	currentEpoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	return a.db.Metadata().
		GetRetiringPoolsAtSlot(currentEpoch, slot, metaTxn(txn))
}

func (a Adapter) LatestPoolOpCertSequence(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	return a.db.LatestPoolOpCertSequence(ctx, pkh, dbTxn(txn))
}

func (a Adapter) LatestPoolOpCertSequenceAfter(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	afterSlot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	return a.db.LatestPoolOpCertSequenceAfter(ctx, pkh, afterSlot, dbTxn(txn))
}

func (a Adapter) LatestPoolOpCertSequenceAtOrBefore(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	return a.db.LatestPoolOpCertSequenceAtOrBefore(ctx, pkh, slot, dbTxn(txn))
}

func (a Adapter) LatestPoolOpCertSequenceInRange(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	return a.db.LatestPoolOpCertSequenceInRange(
		ctx,
		pkh,
		startSlot,
		endSlot,
		dbTxn(txn),
	)
}

func (a Adapter) LatestPoolOpCertSequences(
	ctx context.Context,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.LatestPoolOpCertSequences(ctx, dbTxn(txn))
}

func (a Adapter) LatestPoolOpCertSequencesAtOrBefore(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.LatestPoolOpCertSequencesAtOrBefore(ctx, slot, dbTxn(txn))
}

func (a Adapter) PoolOpCertSequencesExistAtSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	return a.db.PoolOpCertSequencesExistAtSlot(ctx, slot, dbTxn(txn))
}

func (a Adapter) UpdatePoolOpCertSequence(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.UpdatePoolOpCertSequence(ctx, pkh, sequence, slot, dbTxn(txn))
}

func (a Adapter) UpdatePoolOpCertSequenceBatched(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
) error {
	return a.db.UpdatePoolOpCertSequenceBatched(
		ctx,
		pkh,
		sequence,
		slot,
		acc,
		dbTxn(txn),
	)
}

func (a Adapter) GetLiveStakeInputsForPools(
	poolKeyHashes [][]byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return a.db.Metadata().
		GetLiveStakeInputsForPools(poolKeyHashes, expiryEpoch, metaTxn(txn))
}

func (a Adapter) GetPointerStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return a.db.Metadata().
		GetPointerStakeInputsForPools(poolKeyHashes, slot, boundarySlot, expiryEpoch, metaTxn(txn))
}

func (a Adapter) GetPoolStakeSnapshot(
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	txn ledgerdb.Txn,
) (*models.PoolStakeSnapshot, error) {
	return a.db.Metadata().
		GetPoolStakeSnapshot(epoch, snapshotType, poolKeyHash, metaTxn(txn))
}

func (a Adapter) GetPoolStakeSnapshotsByEpoch(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	return a.db.GetPoolStakeSnapshotsByEpoch(epoch, snapshotType, dbTxn(txn))
}

func (a Adapter) GetPoolStakeSnapshotsByEpochBounded(
	epoch uint64,
	snapshotType string,
	maxRows int,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	return a.db.Metadata().
		GetPoolStakeSnapshotsByEpochBounded(epoch, snapshotType, maxRows, metaTxn(txn))
}

func (a Adapter) GetPoolStakeSnapshotsForPools(
	epoch uint64,
	snapshotType string,
	poolKeyHashes [][]byte,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	return a.db.Metadata().
		GetPoolStakeSnapshotsForPools(epoch, snapshotType, poolKeyHashes, metaTxn(txn))
}

func (a Adapter) GetStakeByPools(
	ctx context.Context,
	poolKeyHashes [][]byte,
	txn ledgerdb.Txn,
) (map[string]uint64, map[string]uint64, error) {
	return a.db.GetStakeByPools(ctx, poolKeyHashes, dbTxn(txn))
}

func (a Adapter) GetTotalActiveStake(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.Metadata().
		GetTotalActiveStake(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) SavePoolStakeSnapshots(
	snapshots []*models.PoolStakeSnapshot,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SavePoolStakeSnapshots(snapshots, metaTxn(txn))
}

func (a Adapter) BumpDormantDRepExpiries(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.BumpDormantDRepExpiries(ctx, slot, dbTxn(txn))
}

func (a Adapter) ClearCommitteeQuorum(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.ClearCommitteeQuorum(ctx, slot, dbTxn(txn))
}

func (a Adapter) ClearDanglingDRepDelegations(
	ctx context.Context,
	atSlot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.ClearDanglingDRepDelegations(ctx, atSlot, dbTxn(txn))
}

func (a Adapter) ClearGovernanceProposalRatification(
	ctx context.Context,
	txHash []byte,
	actionIndex uint32,
	transitionSlot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.ClearGovernanceProposalRatification(
		ctx,
		txHash,
		actionIndex,
		transitionSlot,
		dbTxn(txn),
	)
}

func (a Adapter) DeleteGovernanceVotesForDrep(
	ctx context.Context,
	credentialTag uint8,
	credential []byte,
	epoch uint64,
	deletedSlot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.DeleteGovernanceVotesForDrep(
		ctx,
		credentialTag,
		credential,
		epoch,
		deletedSlot,
		dbTxn(txn),
	)
}

func (a Adapter) GetActiveCommitteeMembers(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.AuthCommitteeHot, error) {
	return a.db.GetActiveCommitteeMembers(ctx, dbTxn(txn))
}

func (a Adapter) GetActiveDreps(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	return a.db.GetActiveDreps(ctx, dbTxn(txn))
}

func (a Adapter) GetActiveGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetActiveGovernanceProposals(ctx, epoch, dbTxn(txn))
}

func (a Adapter) GetCommitteeAuthorizedCount(
	ctx context.Context,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.GetCommitteeAuthorizedCount(ctx, dbTxn(txn))
}

func (a Adapter) GetCommitteeHotAuthorizationsSince(
	ctx context.Context,
	minSlot uint64,
	txn ledgerdb.Txn,
) ([]*models.AuthCommitteeHot, error) {
	return a.db.GetCommitteeHotAuthorizationsSince(ctx, minSlot, dbTxn(txn))
}

func (a Adapter) GetCommitteeMember(
	ctx context.Context,
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.AuthCommitteeHot, error) {
	return a.db.GetCommitteeMember(
		ctx,
		coldCredentialTag,
		coldKey,
		termStartSlot,
		dbTxn(txn),
	)
}

func (a Adapter) GetCommitteeMembers(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	return a.db.GetCommitteeMembers(ctx, dbTxn(txn))
}

func (a Adapter) GetCommitteeMembersIncludeDeleted(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	return a.db.GetCommitteeMembersIncludeDeleted(ctx, dbTxn(txn))
}

func (a Adapter) GetCommitteeQuorum(
	ctx context.Context,
	txn ledgerdb.Txn,
) (*big.Rat, error) {
	return a.db.GetCommitteeQuorum(ctx, dbTxn(txn))
}

func (a Adapter) GetConstitution(
	txn ledgerdb.Txn,
) (*models.Constitution, error) {
	return a.db.GetConstitution(dbTxn(txn))
}

func (a Adapter) GetDRepDelegators(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	return a.db.GetDRepDelegators(
		ctx,
		credentialTag,
		drepCredential,
		dbTxn(txn),
	)
}

func (a Adapter) GetDRepDelegatorsAtSlot(
	ctx context.Context,
	dreps []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	return a.db.GetDRepDelegatorsAtSlot(ctx, dreps, slot, dbTxn(txn))
}

func (a Adapter) GetDRepVotingPower(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.GetDRepVotingPower(
		ctx,
		credentialTag,
		drepCredential,
		expiryEpoch,
		dbTxn(txn),
	)
}

func (a Adapter) GetDRepVotingPowerBatch(
	ctx context.Context,
	drepCredentials []models.StakeCredentialRef,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.GetDRepVotingPowerBatch(
		ctx,
		drepCredentials,
		expiryEpoch,
		dbTxn(txn),
	)
}

func (a Adapter) GetDRepVotingPowerByType(
	ctx context.Context,
	drepTypes []uint64,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (map[uint64]uint64, error) {
	return a.db.GetDRepVotingPowerByType(
		ctx,
		drepTypes,
		expiryEpoch,
		dbTxn(txn),
	)
}

func (a Adapter) GetDormantDRepEpochs(
	ctx context.Context,
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.GetDormantDRepEpochs(ctx, dbTxn(txn))
}

func (a Adapter) GetDrepByCredential(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Drep, error) {
	return a.db.GetDrepByCredential(
		ctx,
		credentialTag,
		cred,
		includeInactive,
		dbTxn(txn),
	)
}

func (a Adapter) GetDrepLastRegistrationDeposit(
	ctx context.Context,
	credentialTag uint8,
	credential []byte,
	txn ledgerdb.Txn,
) (*uint64, error) {
	return a.db.GetDrepLastRegistrationDeposit(
		ctx,
		credentialTag,
		credential,
		dbTxn(txn),
	)
}

func (a Adapter) GetDrepLastRegistrationDeposits(
	ctx context.Context,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.GetDrepLastRegistrationDeposits(ctx, dbTxn(txn))
}

func (a Adapter) GetDrepRegistrationDepositsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return a.db.GetDrepRegistrationDepositsAtSlot(ctx, refs, slot, dbTxn(txn))
}

func (a Adapter) GetDrepsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	return a.db.GetDrepsAtSlot(ctx, refs, slot, dbTxn(txn))
}

func (a Adapter) GetDroppedGovernanceProposalsAt(
	ctx context.Context,
	epoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetDroppedGovernanceProposalsAt(ctx, epoch, slot, dbTxn(txn))
}

func (a Adapter) GetEnactedGovernanceProposalsAt(
	ctx context.Context,
	epoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetEnactedGovernanceProposalsAt(ctx, epoch, slot, dbTxn(txn))
}

func (a Adapter) GetExpiredAwaitingDropGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetExpiredAwaitingDropGovernanceProposals(
		ctx,
		epoch,
		dbTxn(txn),
	)
}

func (a Adapter) GetExpiredDReps(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	return a.db.GetExpiredDReps(ctx, epoch, dbTxn(txn))
}

func (a Adapter) GetExpiredGovernanceProposalsAt(
	ctx context.Context,
	epoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetExpiredGovernanceProposalsAt(ctx, epoch, slot, dbTxn(txn))
}

func (a Adapter) GetExpiringGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetExpiringGovernanceProposals(ctx, epoch, dbTxn(txn))
}

func (a Adapter) GetGovernanceProposal(
	ctx context.Context,
	txHash []byte,
	actionIndex uint32,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	return a.db.GetGovernanceProposal(ctx, txHash, actionIndex, dbTxn(txn))
}

func (a Adapter) GetGovernanceProposalSet(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetGovernanceProposalSet(ctx, dbTxn(txn))
}

func (a Adapter) GetGovernanceProposalSetAtSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetGovernanceProposalSetAtSlot(ctx, slot, dbTxn(txn))
}

func (a Adapter) GetGovernanceVotes(
	ctx context.Context,
	proposalID uint,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	return a.db.GetGovernanceVotes(ctx, proposalID, dbTxn(txn))
}

func (a Adapter) GetGovernanceVotesAtSlot(
	ctx context.Context,
	proposalID uint,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	return a.db.GetGovernanceVotesAtSlot(ctx, proposalID, slot, dbTxn(txn))
}

func (a Adapter) GetLastEnactedGovernanceProposal(
	ctx context.Context,
	actionTypes []uint8,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	return a.db.GetLastEnactedGovernanceProposal(ctx, actionTypes, dbTxn(txn))
}

func (a Adapter) GetRatifiedGovernanceProposals(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetRatifiedGovernanceProposals(ctx, dbTxn(txn))
}

func (a Adapter) GetResignedCommitteeMembers(
	ctx context.Context,
	coldCredentials []models.CommitteeCredential,
	txn ledgerdb.Txn,
) (map[string]bool, error) {
	return a.db.GetResignedCommitteeMembers(ctx, coldCredentials, dbTxn(txn))
}

func (a Adapter) InsertDrepIfAbsent(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn ledgerdb.Txn,
) error {
	return a.db.InsertDrepIfAbsent(
		ctx,
		credentialTag,
		cred,
		slot,
		url,
		hash,
		active,
		dbTxn(txn),
	)
}

func (a Adapter) IsCommitteeMemberResigned(
	ctx context.Context,
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	return a.db.IsCommitteeMemberResigned(
		ctx,
		coldCredentialTag,
		coldKey,
		termStartSlot,
		dbTxn(txn),
	)
}

func (a Adapter) RecordDRepActivityEpoch(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	activityEpoch uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.RecordDRepActivityEpoch(
		ctx,
		credentialTag,
		drepCredential,
		activityEpoch,
		dbTxn(txn),
	)
}

func (a Adapter) ResetDormantDRepEpochs(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.ResetDormantDRepEpochs(ctx, slot, dbTxn(txn))
}

func (a Adapter) ResolvePoolRewardAccountAutoVotes(
	ctx context.Context,
	snapshots []*models.PoolStakeSnapshot,
	txn ledgerdb.Txn,
) error {
	return a.db.ResolvePoolRewardAccountAutoVotes(ctx, snapshots, dbTxn(txn))
}

func (a Adapter) SetCommitteeMembers(
	ctx context.Context,
	members []*models.CommitteeMember,
	txn ledgerdb.Txn,
) error {
	return a.db.SetCommitteeMembers(ctx, members, dbTxn(txn))
}

func (a Adapter) SetCommitteeQuorum(
	ctx context.Context,
	quorum *big.Rat,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.SetCommitteeQuorum(ctx, quorum, slot, dbTxn(txn))
}

func (a Adapter) SetConstitution(
	constitution *models.Constitution,
	txn ledgerdb.Txn,
) error {
	return a.db.SetConstitution(constitution, dbTxn(txn))
}

func (a Adapter) SetGenesisGovernance(
	initialDReps conway.ConwayGenesisInitialDReps,
	delegs conway.ConwayGenesisDelegs,
	blockHash []byte,
	txn ledgerdb.Txn,
) error {
	return a.db.SetGenesisGovernance(
		initialDReps,
		delegs,
		blockHash,
		dbTxn(txn),
	)
}

func (a Adapter) SetGovernanceProposal(
	ctx context.Context,
	proposal *models.GovernanceProposal,
	txn ledgerdb.Txn,
) error {
	return a.db.SetGovernanceProposal(ctx, proposal, dbTxn(txn))
}

func (a Adapter) SetGovernanceVote(
	ctx context.Context,
	vote *models.GovernanceVote,
	txn ledgerdb.Txn,
) error {
	return a.db.SetGovernanceVote(ctx, vote, dbTxn(txn))
}

func (a Adapter) SoftDeleteAllCommitteeMembers(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.SoftDeleteAllCommitteeMembers(ctx, slot, dbTxn(txn))
}

func (a Adapter) SoftDeleteCommitteeMembers(
	ctx context.Context,
	coldCredentials []models.CommitteeCredential,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.SoftDeleteCommitteeMembers(
		ctx,
		coldCredentials,
		slot,
		dbTxn(txn),
	)
}

func (a Adapter) UpdateDRepActivity(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	activityEpoch uint64,
	inactivityPeriod uint64,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.UpdateDRepActivity(
		ctx,
		credentialTag,
		drepCredential,
		activityEpoch,
		inactivityPeriod,
		slot,
		dbTxn(txn),
	)
}

func (a Adapter) AddNetworkDonation(
	slot uint64,
	epoch uint64,
	amount uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().AddNetworkDonation(slot, epoch, amount, metaTxn(txn))
}

func (a Adapter) ComputeAndApplyPParamUpdates(
	ctx context.Context,
	slot uint64,
	epoch uint64,
	era uint,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	hasPlutusV2CostModelFunc func(any) bool,
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, bool, error) {
	return a.db.ComputeAndApplyPParamUpdates(
		ctx,
		slot,
		epoch,
		era,
		quorum,
		currentPParams,
		decodeFunc,
		updateFunc,
		hasPlutusV2CostModelFunc,
		dbTxn(txn),
	)
}

func (a Adapter) DeleteBlockNoncesAfterPoint(
	point common.Point,
	txn ledgerdb.Txn,
) error {
	return a.db.DeleteBlockNoncesAfterPoint(point, dbTxn(txn))
}

func (a Adapter) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
	slotNumber uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		slotNumber,
		dbTxn(txn),
	)
}

func (a Adapter) ForecastPParamUpdates(
	epoch uint64,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	cloneFunc func(lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error),
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	return a.db.ForecastPParamUpdates(
		epoch,
		quorum,
		currentPParams,
		decodeFunc,
		updateFunc,
		cloneFunc,
		dbTxn(txn),
	)
}

func (a Adapter) GetBlockNonce(
	point common.Point,
	txn ledgerdb.Txn,
) ([]byte, error) {
	return a.db.GetBlockNonce(point, dbTxn(txn))
}

func (a Adapter) GetBlockNoncesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	return a.db.GetBlockNoncesInSlotRange(startSlot, endSlot, dbTxn(txn))
}

func (a Adapter) GetEpoch(
	epochId uint64,
	txn ledgerdb.Txn,
) (*models.Epoch, error) {
	return a.db.GetEpoch(epochId, dbTxn(txn))
}

func (a Adapter) GetEpochBySlot(
	slot uint64,
	txn ledgerdb.Txn,
) (*models.Epoch, error) {
	return a.db.GetEpochBySlot(slot, dbTxn(txn))
}

func (a Adapter) GetEpochSummary(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.EpochSummary, error) {
	return a.db.Metadata().GetEpochSummary(epoch, metaTxn(txn))
}

func (a Adapter) GetEpochs(txn ledgerdb.Txn) ([]models.Epoch, error) {
	return a.db.GetEpochs(dbTxn(txn))
}

func (a Adapter) GetEpochsByEra(
	eraId uint,
	txn ledgerdb.Txn,
) ([]models.Epoch, error) {
	return a.db.GetEpochsByEra(eraId, dbTxn(txn))
}

func (a Adapter) GetLatestBlockNonce(
	txn ledgerdb.Txn,
) (models.BlockNonce, bool, error) {
	return a.db.GetLatestBlockNonce(dbTxn(txn))
}

func (a Adapter) GetNetworkState(
	txn ledgerdb.Txn,
) (*models.NetworkState, error) {
	return a.db.Metadata().GetNetworkState(metaTxn(txn))
}

func (a Adapter) GetNetworkStateAsOfSlot(
	slot uint64,
	txn ledgerdb.Txn,
) (*models.NetworkState, error) {
	return a.db.Metadata().GetNetworkStateAsOfSlot(slot, metaTxn(txn))
}

func (a Adapter) GetPParams(
	epoch uint64,
	eraId uint,
	decodeFunc func([]byte) (lcommon.ProtocolParameters, error),
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	return a.db.GetPParams(epoch, eraId, decodeFunc, dbTxn(txn))
}

func (a Adapter) HasAnyGenesisCbor(slot uint64) (bool, error) {
	return a.db.HasAnyGenesisCbor(slot)
}

func (a Adapter) HasGenesisCbor(slot uint64, hash []byte) (bool, error) {
	return a.db.HasGenesisCbor(slot, hash)
}

func (a Adapter) ListPParamsForEra(
	eraID uint,
	txn ledgerdb.Txn,
) ([]models.PParams, error) {
	return a.db.Metadata().ListPParamsForEra(eraID, metaTxn(txn))
}

func (a Adapter) ReconcileAlonzoPParamsUnitAfterRecovery(
	ctx context.Context,
) error {
	return a.db.ReconcileAlonzoPParamsUnitAfterRecovery(ctx)
}

func (a Adapter) SaveEpochSummary(
	summary *models.EpochSummary,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().SaveEpochSummary(summary, metaTxn(txn))
}

func (a Adapter) SetBlockNonce(
	blockHash []byte,
	slotNumber uint64,
	nonce []byte,
	isCheckpoint bool,
	txn ledgerdb.Txn,
) error {
	return a.db.SetBlockNonce(
		blockHash,
		slotNumber,
		nonce,
		isCheckpoint,
		dbTxn(txn),
	)
}

func (a Adapter) SetBlockNonces(
	nonces []models.BlockNonce,
	txn ledgerdb.Txn,
) error {
	return a.db.SetBlockNonces(nonces, dbTxn(txn))
}

func (a Adapter) SetEpoch(
	slot uint64,
	epoch uint64,
	nonce []byte,
	evolvingNonce []byte,
	candidateNonce []byte,
	lastEpochBlockNonce []byte,
	era uint,
	slotLength uint,
	lengthInSlots uint,
	txn ledgerdb.Txn,
) error {
	return a.db.SetEpoch(
		slot,
		epoch,
		nonce,
		evolvingNonce,
		candidateNonce,
		lastEpochBlockNonce,
		era,
		slotLength,
		lengthInSlots,
		dbTxn(txn),
	)
}

func (a Adapter) SetGenesisCbor(
	slot uint64,
	hash []byte,
	cborData []byte,
	txn ledgerdb.Txn,
) error {
	return a.db.SetGenesisCbor(slot, hash, cborData, dbTxn(txn))
}

func (a Adapter) SetGenesisTransaction(
	ctx context.Context,
	txHash []byte,
	blockHash []byte,
	outputs []lcommon.Utxo,
	offsets map[database.UtxoRef]database.CborOffset,
	txn ledgerdb.Txn,
) error {
	return a.db.SetGenesisTransaction(
		ctx,
		txHash,
		blockHash,
		outputs,
		offsets,
		dbTxn(txn),
	)
}

func (a Adapter) SetNetworkState(
	treasury uint64,
	reserves uint64,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.Metadata().
		SetNetworkState(treasury, reserves, slot, metaTxn(txn))
}

func (a Adapter) SetPParams(
	params []byte,
	slot uint64,
	epoch uint64,
	era uint,
	txn ledgerdb.Txn,
) error {
	return a.db.SetPParams(params, slot, epoch, era, dbTxn(txn))
}

func (a Adapter) SumNetworkDonationsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.Metadata().SumNetworkDonationsForEpoch(epoch, metaTxn(txn))
}

func (a Adapter) BlockAtOrAfterIndex(
	blockIndex uint64,
	txn ledgerdb.Txn,
) (models.Block, error) {
	return a.db.BlockAtOrAfterIndex(blockIndex, dbTxn(txn))
}

func (a Adapter) BlockByIndex(
	blockIndex uint64,
	txn ledgerdb.Txn,
) (models.Block, error) {
	return a.db.BlockByIndex(blockIndex, dbTxn(txn))
}

func (a Adapter) BlockCreate(block models.Block, txn ledgerdb.Txn) error {
	return a.db.BlockCreate(block, dbTxn(txn))
}

func (a Adapter) BlockPointByIndex(
	blockIndex uint64,
	txn ledgerdb.Txn,
) (common.Point, error) {
	return a.db.BlockPointByIndex(blockIndex, dbTxn(txn))
}

func (a Adapter) DeleteDeferredHeaderMarker(key string) error {
	return a.db.DeleteDeferredHeaderMarker(key)
}

func (a Adapter) DeleteSyncState(key string, txn ledgerdb.Txn) error {
	return a.db.DeleteSyncState(key, dbTxn(txn))
}

func (a Adapter) EnsureBlockInventory() error {
	return a.db.EnsureBlockInventory()
}

func (a Adapter) GetDeferredHeaderMarkerValue(
	key string,
) ([]byte, bool, error) {
	return a.db.GetDeferredHeaderMarkerValue(key)
}

func (a Adapter) GetSyncState(key string, txn ledgerdb.Txn) (string, error) {
	return a.db.GetSyncState(key, dbTxn(txn))
}

func (a Adapter) GetSyncStateBounded(
	key string,
	maxBytes int,
	txn ledgerdb.Txn,
) (string, error) {
	return a.db.GetSyncStateBounded(key, maxBytes, dbTxn(txn))
}

func (a Adapter) GetTip(txn ledgerdb.Txn) (chainsync.Tip, error) {
	return a.db.GetTip(dbTxn(txn))
}

func (a Adapter) ListDeferredHeaderMarkerValues() ([]database.DeferredHeaderMarker, error) {
	return a.db.ListDeferredHeaderMarkerValues()
}

func (a Adapter) ListSyncStateKeysByPrefix(
	prefix string,
	txn ledgerdb.Txn,
) ([]string, error) {
	return a.db.ListSyncStateKeysByPrefix(prefix, dbTxn(txn))
}

func (a Adapter) MithrilTrustBoundarySlotStrict(
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.MithrilTrustBoundarySlotStrict(dbTxn(txn))
}

func (a Adapter) OldestRetainedSlot() (uint64, bool) {
	return a.db.OldestRetainedSlot()
}

func (a Adapter) SetDeferredHeaderMarkerWithValue(
	key string,
	value []byte,
) error {
	return a.db.SetDeferredHeaderMarkerWithValue(key, value)
}

func (a Adapter) SetMidEpochEraSwitch(
	sw database.MidEpochEraSwitch,
	txn ledgerdb.Txn,
) error {
	return a.db.SetMidEpochEraSwitch(sw, dbTxn(txn))
}

func (a Adapter) SetSyncState(
	key string,
	value string,
	txn ledgerdb.Txn,
) error {
	return a.db.SetSyncState(key, value, dbTxn(txn))
}

func (a Adapter) SetTip(tip chainsync.Tip, txn ledgerdb.Txn) error {
	return a.db.SetTip(tip, dbTxn(txn))
}

func (a Adapter) SumTransactionFeesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.Metadata().
		SumTransactionFeesInSlotRange(startSlot, endSlot, metaTxn(txn))
}

func (a Adapter) TruncateAfterSlot(
	ctx context.Context,
	point common.Point,
	mithrilFloor uint64,
	txn ledgerdb.Txn,
) (chainsync.Tip, []byte, error) {
	return a.db.TruncateAfterSlot(ctx, point, mithrilFloor, dbTxn(txn))
}

func (a Adapter) BeginDestructiveTransitionContext(
	ctx context.Context,
) (func(), error) {
	return a.db.BeginDestructiveTransitionContext(ctx)
}

func (a Adapter) BlockBatchTransactionContext(
	ctx context.Context,
) (ledgerdb.Txn, error) {
	return wrapTxn(a.db.BlockBatchTransactionContext(ctx))
}

func (a Adapter) BlockBatchTxnContext(
	ctx context.Context,
) (ledgerdb.Txn, error) {
	return wrapTxn(a.db.BlockBatchTxnContext(ctx))
}

func (a Adapter) EnableBatchUtxoOverlay(
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
	inputs []models.UtxoId,
	deleteOnSpend []models.UtxoId,
	producers [][]byte,
) (bool, error) {
	return a.db.EnableBatchUtxoOverlay(
		acc, dbTxn(txn), inputs, deleteOnSpend, producers,
	)
}

func (a Adapter) FlushBatch(
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
) error {
	return a.db.FlushBatch(acc, dbTxn(txn))
}

func (a Adapter) FlushBatchStakeDeltas(
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
) error {
	return a.db.FlushBatchStakeDeltas(acc, dbTxn(txn))
}

func (a Adapter) NewBatchAccumulator() database.BatchAccumulator {
	return a.db.NewBatchAccumulator()
}

func (a Adapter) NewValidatedBlockBatchAccumulator() database.BatchAccumulator {
	return a.db.NewValidatedBlockBatchAccumulator()
}

func (a Adapter) PersistTransactionOffsets(
	txn ledgerdb.Txn,
	txs []lcommon.Transaction,
	offsets *database.BlockIngestionResult,
	afterBatch func() error,
) error {
	return a.db.PersistTransactionOffsets(dbTxn(txn), txs, offsets, afterBatch)
}

func (a Adapter) SetTransactionBatchedWithOpts(
	ctx context.Context,
	tx lcommon.Transaction,
	point common.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *database.BlockIngestionResult,
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
	opts database.BatchedTxIngestOpts,
) error {
	return a.db.SetTransactionBatchedWithOpts(
		ctx,
		tx,
		point,
		idx,
		updateEpoch,
		pparamUpdates,
		certDeposits,
		offsets,
		acc,
		dbTxn(txn),
		opts,
	)
}

func (a Adapter) SetTransactionWithOpts(
	ctx context.Context,
	tx lcommon.Transaction,
	point common.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *database.BlockIngestionResult,
	txn ledgerdb.Txn,
	opts database.BatchedTxIngestOpts,
) error {
	return a.db.SetTransactionWithOpts(
		ctx,
		tx,
		point,
		idx,
		updateEpoch,
		pparamUpdates,
		certDeposits,
		offsets,
		dbTxn(txn),
		opts,
	)
}

func (a Adapter) GetTransactionByHash(
	ctx context.Context,
	hash []byte,
	txn ledgerdb.Txn,
) (*models.Transaction, error) {
	return a.db.GetTransactionByHash(ctx, hash, dbTxn(txn))
}

func (a Adapter) GetTransactionsByHashes(
	ctx context.Context,
	hashes [][]byte,
	txn ledgerdb.Txn,
) ([]models.Transaction, error) {
	return a.db.GetTransactionsByHashes(ctx, hashes, dbTxn(txn))
}

func (a Adapter) ResolveUtxoCborWithRecovery(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn ledgerdb.Txn,
) ([]byte, error) {
	return a.db.ResolveUtxoCborWithRecovery(ctx, txId, outputIdx, dbTxn(txn))
}

func (a Adapter) GetLeiosEBManifest(hash []byte, slot uint64) ([]byte, error) {
	return a.db.GetLeiosEBManifest(hash, slot)
}

func (a Adapter) GetLeiosEBTxs(
	hash []byte,
	slot uint64,
) ([]cbor.RawMessage, error) {
	return a.db.GetLeiosEBTxs(hash, slot)
}

func (a Adapter) MaxLeiosEBSlot() (uint64, error) {
	return a.db.MaxLeiosEBSlot()
}

func (a Adapter) MaxLeiosEBSlotContext(ctx context.Context) (uint64, error) {
	return a.db.MaxLeiosEBSlotContext(ctx)
}

func (a Adapter) NewLeiosClosureBatchAccumulator() database.BatchAccumulator {
	return a.db.NewLeiosClosureBatchAccumulator()
}

func (a Adapter) PrepareLeiosClosureBatch(
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
	transactions []lcommon.Transaction,
) (int, error) {
	return a.db.PrepareLeiosClosureBatch(acc, dbTxn(txn), transactions)
}

func (a Adapter) PruneLeiosEBBeforeSlot(
	ctx context.Context,
	beforeSlot uint64,
) (int, error) {
	return a.db.PruneLeiosEBBeforeSlot(ctx, beforeSlot)
}

func (a Adapter) SetLeiosEB(
	slot uint64,
	hash []byte,
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) error {
	return a.db.SetLeiosEB(slot, hash, manifestRaw, txsRaw)
}

func (a Adapter) CountTransactionsByAddress(
	ctx context.Context,
	addr lcommon.Address,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.CountTransactionsByAddress(ctx, addr, from, to, dbTxn(txn))
}

func (a Adapter) CountTransactionsByMetadataLabel(
	ctx context.Context,
	label uint64,
	txn ledgerdb.Txn,
) (int, error) {
	return a.db.CountTransactionsByMetadataLabel(ctx, label, dbTxn(txn))
}

func (a Adapter) GetDatums(
	ctx context.Context,
	hashes [][]byte,
) ([]*models.Datum, error) {
	return a.db.GetDatums(ctx, hashes)
}

func (a Adapter) GetExistingTransactionHashes(
	ctx context.Context,
	hashes [][]byte,
) ([][]byte, error) {
	return a.db.GetExistingTransactionHashes(ctx, hashes)
}

func (a Adapter) GetTransactionsByAddress(
	ctx context.Context,
	addr lcommon.Address,
	limit int,
	offset int,
	txn ledgerdb.Txn,
) ([]models.Transaction, error) {
	return a.db.GetTransactionsByAddress(ctx, addr, limit, offset, dbTxn(txn))
}

func (a Adapter) GetTransactionsByAddressWithOrder(
	ctx context.Context,
	addr lcommon.Address,
	limit int,
	offset int,
	order string,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
	txn ledgerdb.Txn,
) ([]models.Transaction, error) {
	return a.db.GetTransactionsByAddressWithOrder(
		ctx,
		addr,
		limit,
		offset,
		order,
		from,
		to,
		dbTxn(txn),
	)
}

func (a Adapter) GetTransactionsByBlockHash(
	ctx context.Context,
	blockHash []byte,
	txn ledgerdb.Txn,
) ([]models.Transaction, error) {
	return a.db.GetTransactionsByBlockHash(ctx, blockHash, dbTxn(txn))
}

func (a Adapter) ScanUtxosByAddress(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
	visit func([]models.UtxoWithOrdering) error,
) (chainsync.Tip, error) {
	return a.db.ScanUtxosByAddress(ctx, q, maxCandidates, maxBytes, visit)
}

func (a Adapter) UtxoBalanceByAddressAtSlot(
	ctx context.Context,
	address lcommon.Address,
	slot uint64,
	txn ledgerdb.Txn,
) (models.AddressBalance, error) {
	return a.db.UtxoBalanceByAddressAtSlot(ctx, address, slot, dbTxn(txn))
}

func (a Adapter) UtxosByAddressBoundedPage(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	return a.db.UtxosByAddressBoundedPage(ctx, q, maxCandidates, maxBytes)
}

func (a Adapter) GetAccountsActiveAtSlot(
	stakeCredentials []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]struct{}, error) {
	return a.db.Metadata().
		GetAccountsActiveAtSlot(stakeCredentials, slot, metaTxn(txn))
}

func (a Adapter) RefreshRewardLiveStakeRefs(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	return a.db.RefreshRewardLiveStakeRefs(refs, slot, dbTxn(txn))
}

func (a Adapter) MergeImportedPoolBlockCounts(
	epoch uint64,
	epochStartSlot uint64,
	counts map[string]uint64,
	totalBlocks uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	return database.MergeImportedPoolBlockCounts(
		a.db.Metadata(),
		metaTxn(txn),
		epoch,
		epochStartSlot,
		counts,
		totalBlocks,
	)
}

func (a Adapter) GetPParamsRows(
	epoch uint64,
	eraId uint,
	txn ledgerdb.Txn,
) ([]models.PParams, error) {
	return a.db.Metadata().GetPParams(epoch, eraId, metaTxn(txn))
}

func (a Adapter) UtxoRollbackChanges(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.UtxoId, []models.Utxo, error) {
	return a.db.UtxoRollbackChanges(ctx, slot, dbTxn(txn))
}
