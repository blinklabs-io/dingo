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
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

func (a Adapter) GetLiveUtxoLovelace(txn ledgerdb.Txn) (uint64, error) {
	if err := a.requireStore("live UTxO lovelace"); err != nil {
		return 0, err
	}
	return a.db.Metadata().GetLiveUtxoLovelace(metaTxn(txn))
}

func (a Adapter) IterateLiveUtxoRefs(
	ctx context.Context,
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return a.db.IterateLiveUtxoRefs(ctx, dbTxn(txn), fn)
}

func (a Adapter) IterateAccounts(
	txn ledgerdb.Txn,
	fn func(*models.Account) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().IterateAccounts(metaTxn(txn), fn)
}

func (a Adapter) ListPoolKeyHashes(txn ledgerdb.Txn) ([][]byte, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().ListPoolKeyHashes(metaTxn(txn))
}

func (a Adapter) IterateDreps(
	txn ledgerdb.Txn,
	fn func(*models.Drep) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return err
	}
	return a.db.Metadata().IterateDreps(metaTxn(txn), fn)
}

func (a Adapter) IterateLiveDrepDelegators(
	txn ledgerdb.Txn,
	fn func(drep, stake models.StakeCredentialRef) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return err
	}
	return a.db.Metadata().IterateLiveDrepDelegators(metaTxn(txn), fn)
}

func (a Adapter) IterateAccountImportBaselines(
	txn ledgerdb.Txn,
	fn func(models.AccountImportBaselineRow) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().IterateAccountImportBaselines(metaTxn(txn), fn)
}

func (a Adapter) IterateAccountWitnessSlots(
	txn ledgerdb.Txn,
	fn func(ref models.StakeCredentialRef, slot uint64) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().IterateAccountWitnessSlots(metaTxn(txn), fn)
}

func (a Adapter) IterateStakeDelegationEvents(
	txn ledgerdb.Txn,
	fn func(models.StakeDelegationEventRow) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().IterateStakeDelegationEvents(metaTxn(txn), fn)
}

func (a Adapter) IterateAppliedTransactions(
	txn ledgerdb.Txn,
	fn func(models.AppliedTransaction) error,
) error {
	if err := a.requireStore("applied transactions"); err != nil {
		return err
	}
	return a.db.Metadata().IterateAppliedTransactions(metaTxn(txn), fn)
}

func (a Adapter) IterateLeiosClosureUtxoEffects(
	txn ledgerdb.Txn,
	fn func(models.LeiosClosureUtxoEffect) error,
) error {
	if err := a.requireStore("Leios closure UTxO effects"); err != nil {
		return err
	}
	return a.db.Metadata().IterateLeiosClosureUtxoEffects(metaTxn(txn), fn)
}

func (a Adapter) IterateAccountWitnessEffectiveSlots(
	txn ledgerdb.Txn,
	fn func(ref models.StakeCredentialRef, slot, effective uint64) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().IterateAccountWitnessEffectiveSlots(metaTxn(txn), fn)
}

func (a Adapter) IterateAccountWithdrawalEffectiveSlots(
	txn ledgerdb.Txn,
	fn func(ref models.StakeCredentialRef, slot, effective uint64) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().IterateAccountWithdrawalEffectiveSlots(metaTxn(txn), fn)
}

func (a Adapter) IterateLiveUtxos(
	ctx context.Context,
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return a.db.IterateLiveUtxos(ctx, dbTxn(txn), fn)
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

func (a Adapter) UtxosByRefs(
	ctx context.Context,
	refs []models.UtxoId,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByRefs(ctx, refs, dbTxn(txn))
}

func (a Adapter) UtxosByRefsBounded(
	ctx context.Context,
	refs []models.UtxoId,
	maxBytes int64,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return a.db.UtxosByRefsBounded(ctx, refs, maxBytes, dbTxn(txn))
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.AccountLastWitnessSlots(ctx, refs, maxSlot, dbTxn(txn))
}

func (a Adapter) GetAccountByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Account, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.GetAccountsByCredential(ctx, refs, includeInactive, dbTxn(txn))
}

func (a Adapter) GetAccountsByCredentialAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.GetAccountsByCredentialAtSlot(ctx, refs, slot, dbTxn(txn))
}

func (a Adapter) GetActiveAccountCredentials(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetActiveAccountCredentials(metaTxn(txn))
}

func (a Adapter) GetGenesisDelegationForSlot(
	genesisHash []byte,
	blockSlot uint64,
	stabilityWindow uint64,
	txn ledgerdb.Txn,
) (*models.GenesisDelegation, error) {
	if err := a.requireMetadata(nodesettings.FamilyMIRGenesis); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetGenesisDelegationForSlot(genesisHash, blockSlot, stabilityWindow, metaTxn(txn))
}

func (a Adapter) GetGenesisDelegationsInSlotRange(
	fromSlot uint64,
	uptoSlot uint64,
	txn ledgerdb.Txn,
) ([]models.GenesisDelegation, error) {
	if err := a.requireMetadata(nodesettings.FamilyMIRGenesis); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetStakeCredentialsWithRegistrationEvents(fromSlot, toSlot, metaTxn(txn))
}

func (a Adapter) GetStakeRegistrationsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.GetStakeRegistrationsByCredential(
		credentialTag,
		stakingKey,
		dbTxn(txn),
	)
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

func (a Adapter) AddAccountRewardByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.AddAccountRewardsByCredential(ctx, credits, dbTxn(txn))
}

func (a Adapter) AddAppliedRewardCreditRound(
	round models.RewardCreditRound,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
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

func (a Adapter) ClaimPendingRewardCreditsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		ClaimPendingRewardCreditsForCredential(credentialTag, stakingKey, metaTxn(txn))
}

func (a Adapter) ClaimUnfoldedRewardCredits(
	snapshotEpoch uint64,
	limit int,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		ClaimUnfoldedRewardCredits(snapshotEpoch, limit, metaTxn(txn))
}

func (a Adapter) DeleteRewardInputsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
	return a.db.Metadata().DeleteRewardInputsForEpoch(epoch, metaTxn(txn))
}

func (a Adapter) DeleteRewardOutputsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return err
	}
	return a.db.Metadata().DeleteRewardOutputsForEpoch(epoch, metaTxn(txn))
}

func (a Adapter) DeleteRewardStakeInputBeforeEpoch(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
	return a.db.Metadata().
		DeleteRewardStakeInputBeforeEpoch(ctx, epoch, metaTxn(txn))
}

func (a Adapter) DeleteRewardStateBeforeEpoch(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return err
	}
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetPendingRewardAccountOutputsForCredential(credentialTag, stakingKey, metaTxn(txn))
}

func (a Adapter) IterateUnfoldedRewardAccountOutputs(
	txn ledgerdb.Txn,
	fn func(*models.RewardAccountOutput) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return err
	}
	return a.db.Metadata().IterateUnfoldedRewardAccountOutputs(metaTxn(txn), fn)
}

func (a Adapter) GetPendingRewardCreditRounds(
	txn ledgerdb.Txn,
) ([]models.RewardCreditRound, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetPendingRewardCreditRounds(metaTxn(txn))
}

func (a Adapter) GetPostSnapshotRewardCredits(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetPostSnapshotRewardCredits(slot, metaTxn(txn))
}

func (a Adapter) GetRewardAccountOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRewardAccountOutputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardAccountOutputsForEligibility(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetRewardAccountOutputsForEligibility(epochs, credentialTag, stakingKey, metaTxn(txn))
}

func (a Adapter) GetRewardAdaPots(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.RewardAdaPots, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRewardAdaPots(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardPoolInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolInput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRewardPoolInputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardPoolOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolOutput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRewardPoolOutputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (string, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return "", err
	}
	return a.db.Metadata().
		GetRewardSeedFailure(epoch, snapshotType, metaTxn(txn))
}

// GetRewardSeedFailureSlot returns the imported basis failure and the slot it
// was captured at. A metadata store that cannot report the slot is an error:
// the failure would otherwise be kept at a slot it was never captured at.
func (a Adapter) GetRewardSeedFailureSlot(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (string, uint64, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return "", 0, err
	}
	r, ok := a.db.Metadata().(metadata.RewardSeedFailureSlotReader)
	if !ok {
		return "", 0, errors.New(
			"metadata store cannot report a reward seed failure slot",
		)
	}
	return r.GetRewardSeedFailureSlot(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (*models.RewardSnapshot, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRewardSnapshot(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) GetRewardStakeInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRewardStakeInputs(epoch, metaTxn(txn))
}

func (a Adapter) GetRewardStakeInputsInPoolKeyHashRange(
	epoch uint64,
	poolKeyHashLo []byte,
	poolKeyHashHi []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetRewardStakeInputsInPoolKeyHashRange(epoch, poolKeyHashLo, poolKeyHashHi, metaTxn(txn))
}

func (a Adapter) HasAppliedRewardCreditRound(
	epoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return false, err
	}
	return a.db.Metadata().HasAppliedRewardCreditRound(epoch, metaTxn(txn))
}

func (a Adapter) HasPendingRewardCreditRounds(txn ledgerdb.Txn) (bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return false, err
	}
	return a.db.Metadata().HasPendingRewardCreditRounds(metaTxn(txn))
}

func (a Adapter) HasUnfoldedRewardCreditsThroughEpoch(
	snapshotEpoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return false, err
	}
	return a.db.Metadata().
		HasUnfoldedRewardCreditsThroughEpoch(snapshotEpoch, metaTxn(txn))
}

func (a Adapter) SaveRewardAccountOutputs(
	outputs []*models.RewardAccountOutput,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return err
	}
	return a.db.Metadata().SaveRewardAccountOutputs(outputs, metaTxn(txn))
}

func (a Adapter) SaveRewardAdaPots(
	pots *models.RewardAdaPots,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
	return a.db.Metadata().SaveRewardAdaPots(pots, metaTxn(txn))
}

func (a Adapter) SaveRewardPoolInputs(
	inputs []*models.RewardPoolInput,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
	return a.db.Metadata().SaveRewardPoolInputs(inputs, metaTxn(txn))
}

func (a Adapter) SaveRewardPoolOutputs(
	outputs []*models.RewardPoolOutput,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return err
	}
	return a.db.Metadata().SaveRewardPoolOutputs(outputs, metaTxn(txn))
}

func (a Adapter) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
	return a.db.Metadata().SaveRewardSnapshot(snapshot, metaTxn(txn))
}

func (a Adapter) SaveRewardStakeInputs(
	inputs []*models.RewardStakeInput,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return err
	}
	return a.db.Metadata().SaveRewardStakeInputs(inputs, metaTxn(txn))
}

func (a Adapter) TakeRewardEligibilityRecheck(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardOutputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().TakeRewardEligibilityRecheck(metaTxn(txn))
}

func (a Adapter) ClearDelegationsToRetiredPool(
	poolKeyHash []byte,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	return a.db.Metadata().
		ClearDelegationsToRetiredPool(poolKeyHash, boundarySlot, metaTxn(txn))
}

func (a Adapter) CountPoolBlocksInSlotRange(
	poolKeyHashes []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return nil, 0, err
	}
	return a.db.Metadata().
		CountPoolBlocksInSlotRange(poolKeyHashes, startSlot, endSlot, metaTxn(txn))
}

func (a Adapter) DeletePoolStakeSnapshotsBeforeEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return err
	}
	return a.db.Metadata().
		DeletePoolStakeSnapshotsBeforeEpoch(epoch, metaTxn(txn))
}

func (a Adapter) DeletePoolStakeSnapshotsForEpoch(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return err
	}
	return a.db.Metadata().
		DeletePoolStakeSnapshotsForEpoch(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) GetActiveDelegatorCountsByPool(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetActiveDelegatorCountsByPool(poolKeyHashes, metaTxn(txn))
}

func (a Adapter) GetActiveDelegatorCredentialsByPool(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetDelegatedPoolKeyHashes(metaTxn(txn))
}

func (a Adapter) GetEpochBoundaryActivePoolKeyHashes(
	slot uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetEpochBoundaryActivePoolKeyHashes(slot, boundarySlot, metaTxn(txn))
}

func (a Adapter) GetEpochBoundaryDelegatedPoolKeyHashes(
	snapshotSlot uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, nil, err
	}
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

func (a Adapter) GetPoolStatus(
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (models.PoolStatus, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return models.PoolStatus{}, err
	}
	return a.db.Metadata().GetPoolStatus(pkh, metaTxn(txn))
}

func (a Adapter) GetPoolBlockIssuersInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolOpCertSequence, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetPoolDepositsHeld(poolKeyHashes, metaTxn(txn))
}

func (a Adapter) GetPoolDepositsHeldAtSlot(
	poolKeyHashes []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetPoolDepositsHeldAtSlot(poolKeyHashes, slot, metaTxn(txn))
}

func (a Adapter) GetPoolEarliestVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, false, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetPoolRegistrationsEffectiveForEpoch(poolKeyHashes, epochStartSlot, endedEpoch, snapshotSlot, metaTxn(txn))
}

func (a Adapter) GetPoolVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, false, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetRetiringPools(currentEpoch, metaTxn(txn))
}

func (a Adapter) GetRetiringPoolsAtSlot(
	currentEpoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
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

func (a Adapter) LatestPoolOpCertCounters(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]models.PoolOpCertCounter, error) {
	return a.db.LatestPoolOpCertCounters(ctx, dbTxn(txn))
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

func (a Adapter) GetLiveStakeInputsForPools(
	poolKeyHashes [][]byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyRewardInputs); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetPointerStakeInputsForPools(poolKeyHashes, slot, boundarySlot, expiryEpoch, metaTxn(txn))
}

func (a Adapter) GetPoolStakeSnapshot(
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	txn ledgerdb.Txn,
) (*models.PoolStakeSnapshot, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetPoolStakeSnapshotsByEpochBounded(epoch, snapshotType, maxRows, metaTxn(txn))
}

func (a Adapter) GetPoolStakeSnapshotsForPools(
	epoch uint64,
	snapshotType string,
	poolKeyHashes [][]byte,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return 0, err
	}
	return a.db.Metadata().
		GetTotalActiveStake(epoch, snapshotType, metaTxn(txn))
}

func (a Adapter) SavePoolStakeSnapshots(
	snapshots []*models.PoolStakeSnapshot,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return err
	}
	return a.db.Metadata().SavePoolStakeSnapshots(snapshots, metaTxn(txn))
}

func (a Adapter) BumpDormantDRepExpiries(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return 0, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return 0, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return nil, err
	}
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

func (a Adapter) GetAllGovernanceProposals(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return a.db.GetAllGovernanceProposals(ctx, dbTxn(txn))
}

func (a Adapter) GetAllGovernanceVotes(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVoteRef, error) {
	return a.db.GetAllGovernanceVotes(ctx, dbTxn(txn))
}

func (a Adapter) LatestCommitteeQuorum(
	ctx context.Context,
	txn ledgerdb.Txn,
) (*models.CommitteeQuorum, error) {
	return a.db.LatestCommitteeQuorum(ctx, dbTxn(txn))
}

func (a Adapter) LatestCommitteeResignations(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.ResignCommitteeCold, error) {
	return a.db.LatestCommitteeResignations(ctx, dbTxn(txn))
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return 0, err
	}
	return a.db.GetDormantDRepEpochs(ctx, dbTxn(txn))
}

func (a Adapter) GetDrepByCredential(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Drep, error) {
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyNetworkDonations); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return nil, err
	}
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
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetNetworkState(metaTxn(txn))
}

func (a Adapter) GetNetworkStateAsOfSlot(
	slot uint64,
	txn ledgerdb.Txn,
) (*models.NetworkState, error) {
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return nil, err
	}
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
	return a.db.ListPParamsForEra(eraID, dbTxn(txn))
}

func (a Adapter) ListPParamUpdates(
	txn ledgerdb.Txn,
) ([]models.PParamUpdate, error) {
	return a.db.ListPParamUpdates(dbTxn(txn))
}

func (a Adapter) GetPParamUpdates(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]models.PParamUpdate, error) {
	return a.db.GetPParamUpdates(epoch, dbTxn(txn))
}

func (a Adapter) SaveEpochSummary(
	summary *models.EpochSummary,
	txn ledgerdb.Txn,
) error {
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyNetworkDonations); err != nil {
		return 0, err
	}
	return a.db.Metadata().SumNetworkDonationsForEpoch(epoch, metaTxn(txn))
}

func (a Adapter) ListNetworkDonations(
	txn ledgerdb.Txn,
) ([]models.NetworkDonation, error) {
	if err := a.requireMetadata(nodesettings.FamilyNetworkDonations); err != nil {
		return nil, err
	}
	return a.db.Metadata().ListNetworkDonations(metaTxn(txn))
}

func (a Adapter) BlockAtOrAfterIndex(
	blockIndex uint64,
	txn ledgerdb.Txn,
) (models.Block, error) {
	return a.db.BlockAtOrAfterIndex(blockIndex, dbTxn(txn))
}

func (a Adapter) FirstBlockAtOrAfterSlot(
	slot uint64,
	txn ledgerdb.Txn,
) (models.Block, error) {
	t := dbTxn(txn)
	if t == nil {
		t = a.db.BlobTxn(false)
		defer t.Rollback() //nolint:errcheck
	}
	return database.FirstBlockAtOrAfterSlotTxn(t, slot)
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

func (a Adapter) DeferredHeaderFloor() (uint64, bool, error) {
	return a.db.DeferredHeaderFloor()
}

func (a Adapter) SetDeferredHeaderFloor(slot uint64) error {
	return a.db.SetDeferredHeaderFloor(slot)
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
	if err := a.requireRewardSyncKeyMetadata(key); err != nil {
		return "", err
	}
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

func (a Adapter) ImportedLedgerStateBoundaryHash() (string, error) {
	return a.db.ImportedLedgerStateBoundaryHash()
}

func (a Adapter) ImportedLedgerStateBoundarySlotStrict(
	txn ledgerdb.Txn,
) (uint64, error) {
	return a.db.ImportedLedgerStateBoundarySlotStrict(dbTxn(txn))
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
	if err := a.requireRewardSyncKeyMetadata(key); err != nil {
		return err
	}
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
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return 0, err
	}
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

func (a Adapter) PersistTransactionOffsets(
	txn ledgerdb.Txn,
	txs []lcommon.Transaction,
	offsets *database.BlockIngestionResult,
	afterBatch func() error,
) error {
	return a.db.PersistTransactionOffsets(dbTxn(txn), txs, offsets, afterBatch)
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
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetAccountsActiveAtSlot(stakeCredentials, slot, metaTxn(txn))
}

// EachRewardLiveStake lists the metadata store's reward_live_stake rows for
// the drain's comparison.
func (a Adapter) EachRewardLiveStake(
	txn ledgerdb.Txn,
	fn func(*models.RewardLiveStake) error,
) error {
	return a.db.EachRewardLiveStake(dbTxn(txn), fn)
}

func (a Adapter) MergeImportedPoolBlockCounts(
	epoch uint64,
	epochStartSlot uint64,
	counts map[string]uint64,
	totalBlocks uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return nil, 0, false, err
	}
	return database.MergeImportedPoolBlockCounts(
		a.db.Metadata(),
		metaTxn(txn),
		epoch,
		epochStartSlot,
		counts,
		totalBlocks,
	)
}

func (a Adapter) CountEpochPoolBlocks(
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return nil, 0, false, err
	}
	counts, total, err := a.db.Metadata().CountPoolBlocksInSlotRange(
		poolKeys, startSlot, endSlot, metaTxn(txn),
	)
	if err != nil {
		return nil, 0, false, err
	}
	return a.MergeImportedPoolBlockCounts(
		epoch, startSlot, counts, total, txn,
	)
}

func (a Adapter) GetImportedPoolBlockCounts(
	epoch uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return nil, 0, false, err
	}
	return a.db.Metadata().GetImportedPoolBlockCounts(epoch, metaTxn(txn))
}

// ImportedBlockRows lists the imported block counts of every epoch, as the
// metadata store exports them.
func (a Adapter) ImportedBlockRows(
	txn ledgerdb.Txn,
) (map[uint64]ledgerdb.ImportedBlockRows, error) {
	if err := a.requireMetadata(nodesettings.FamilyRewardBlocksFees); err != nil {
		return nil, err
	}
	store, ok := a.db.Metadata().(metadata.FactRowStore)
	if !ok {
		return nil, errors.New("metadata store cannot export fact rows")
	}
	tables := map[string]metadata.FactTable{}
	for _, t := range metadata.FactTables {
		tables[t.Name] = t
	}
	read := func(name string) (metadata.FactRows, error) {
		rows, _, err := store.ReadFactRows(
			context.Background(), tables[name], 0, math.MaxInt64, 0, 0,
			metaTxn(txn),
		)
		return rows, err
	}
	out := map[uint64]ledgerdb.ImportedBlockRows{}
	totals, err := read("imported_epoch_block_total")
	if err != nil {
		return nil, err
	}
	for _, r := range totals.Rows {
		m, err := factRowMap(totals.Columns, r)
		if err != nil {
			return nil, err
		}
		epoch, total, slot := factUint(m["epoch"]), factUint(m["total_blocks"]),
			factUint(m["captured_slot"])
		out[epoch] = ledgerdb.ImportedBlockRows{
			Pools: map[string]uint64{}, Total: total, CapturedSlot: slot,
		}
	}
	pools, err := read("imported_pool_block_count")
	if err != nil {
		return nil, err
	}
	for _, r := range pools.Rows {
		m, err := factRowMap(pools.Columns, r)
		if err != nil {
			return nil, err
		}
		epoch := factUint(m["epoch"])
		row, ok := out[epoch]
		if !ok {
			return nil, fmt.Errorf(
				"imported pool block counts for epoch %d have no total", epoch,
			)
		}
		hash, _ := m["pool_key_hash"].([]byte)
		row.Pools[string(hash)] = factUint(m["blocks_produced"])
	}
	return out, nil
}

// DRepHistoryRows lists the DRep history tables, as the metadata store
// exports them.
func (a Adapter) DRepHistoryRows(
	txn ledgerdb.Txn,
) (ledgerdb.DRepHistoryRows, error) {
	var out ledgerdb.DRepHistoryRows
	if err := a.requireMetadata(nodesettings.FamilyDReps); err != nil {
		return out, err
	}
	store, ok := a.db.Metadata().(metadata.FactRowStore)
	if !ok {
		return out, errors.New("metadata store cannot export fact rows")
	}
	tables := map[string]metadata.FactTable{}
	for _, t := range metadata.FactTables {
		tables[t.Name] = t
	}
	each := func(name string, fn func(map[string]any)) error {
		table := tables[name]
		var cursor int64
		for {
			rows, next, err := store.ReadFactRows(
				context.Background(), table, 0, math.MaxInt64, cursor, 1000,
				metaTxn(txn),
			)
			if err != nil {
				return err
			}
			for _, r := range rows.Rows {
				m, err := factRowMap(rows.Columns, r)
				if err != nil {
					return err
				}
				fn(m)
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	}
	if err := each("drep_expiry_history", func(m map[string]any) {
		cred, _ := m["credential"].([]byte)
		out.Expiry = append(out.Expiry, ledgerdb.DRepExpiryHistoryRow{
			Tag:        uint8(factUint(m["credential_tag"])), //nolint:gosec // a credential tag
			Credential: cred, Slot: factUint(m["added_slot"]),
			Activity: factUint(m["last_activity_epoch"]),
			Expiry:   factUint(m["expiry_epoch"]),
		})
	}); err != nil {
		return ledgerdb.DRepHistoryRows{}, err
	}
	if err := each("drep_dormancy_history", func(m map[string]any) {
		out.Dormancy = append(out.Dormancy, ledgerdb.DRepDormancyHistoryRow{
			Slot:     factUint(m["added_slot"]),
			Previous: factUint(m["previous_dormant_epochs"]),
		})
	}); err != nil {
		return ledgerdb.DRepHistoryRows{}, err
	}
	if err := each("drep_expiry_epoch_event", func(m map[string]any) {
		out.Events = append(out.Events, factUint(m["added_slot"]))
	}); err != nil {
		return ledgerdb.DRepHistoryRows{}, err
	}
	if err := each("account_drep_clear", func(m map[string]any) {
		key, _ := m["staking_key"].([]byte)
		out.Clears = append(out.Clears, ledgerdb.DRepClearRow{
			Tag:        uint8(factUint(m["credential_tag"])), //nolint:gosec // a credential tag
			StakingKey: key, Slot: factUint(m["added_slot"]),
		})
	}); err != nil {
		return ledgerdb.DRepHistoryRows{}, err
	}
	return out, nil
}

func factRowMap(cols []string, row []any) (map[string]any, error) {
	if len(cols) != len(row) {
		return nil, errors.New("fact row does not match its columns")
	}
	m := make(map[string]any, len(cols))
	for i, c := range cols {
		m[c] = row[i]
	}
	return m, nil
}

func factUint(v any) uint64 {
	n, _ := v.(int64)
	return uint64(max(n, 0)) //nolint:gosec // clamped
}

func (a Adapter) GetPParamsRows(
	epoch uint64,
	eraId uint,
	txn ledgerdb.Txn,
) ([]models.PParams, error) {
	return a.db.GetPParamRows(epoch, eraId, dbTxn(txn))
}

func (a Adapter) GetLatestEpochSummary(
	txn ledgerdb.Txn,
) (*models.EpochSummary, error) {
	if err := a.requireMetadata(nodesettings.FamilySummaries); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetLatestEpochSummary(metaTxn(txn))
}

func (a Adapter) GetPoolKeyHashesRetiredByEpoch(
	epoch uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if err := a.requireMetadata(nodesettings.FamilyPoolsHistory); err != nil {
		return nil, err
	}
	return a.db.Metadata().
		GetPoolKeyHashesRetiredByEpoch(epoch, boundarySlot, metaTxn(txn))
}

func (a Adapter) GetGovernanceProposalsRefundedAt(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	if err := a.requireMetadata(nodesettings.FamilyGovernance); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetGovernanceProposalsRefundedAt(epoch, metaTxn(txn))
}

func (a Adapter) GetPostSnapshotRewardCreditEvents(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return nil, err
	}
	return a.db.Metadata().GetPostSnapshotRewardCreditEvents(slot, metaTxn(txn))
}

// requireMetadata refuses a read or write of the SQL rows of f on the
// ledger's database, which keeps no metadata store: an answer would be empty
// rather than true. The ledger's LedgerDB answers these from the set.
func (a Adapter) requireMetadata(f nodesettings.Family) error {
	if a.db.Metadata() == nil {
		return fmt.Errorf("%w: %s", database.ErrNoMetadataStore, f)
	}
	return nil
}

// requireStore is requireMetadata for SQL rows no ledger-state family owns,
// such as the transaction index a v0.94.2 store kept.
func (a Adapter) requireStore(what string) error {
	if a.db.Metadata() == nil {
		return fmt.Errorf("%w: %s", database.ErrNoMetadataStore, what)
	}
	return nil
}

// requireRewardSyncKeyMetadata is requireMetadata for a sync_state key a
// reward family owns: the precompute cursors and the eligibility recheck
// belong to reward outputs, the pending stake-input record to reward inputs.
func (a Adapter) requireRewardSyncKeyMetadata(key string) error {
	if strings.HasPrefix(
		key,
		models.StakeRewardSourcePrefix+"precompute-cursor:",
	) ||
		key == models.RewardEligibilityRecheckKey {
		return a.requireMetadata(nodesettings.FamilyRewardOutputs)
	}
	if key == models.RewardStakeInputsPendingKey {
		return a.requireMetadata(nodesettings.FamilyRewardInputs)
	}
	return nil
}

// EachRewardJournalRow lists the reward journal in the order it was written.
func (a Adapter) EachRewardJournalRow(
	txn ledgerdb.Txn,
	fn func(ledgerdb.RewardJournalRow) error,
) error {
	if err := a.requireMetadata(nodesettings.FamilyAccounts); err != nil {
		return err
	}
	store, ok := a.db.Metadata().(metadata.FactRowStore)
	if !ok {
		return errors.New("metadata store cannot export fact rows")
	}
	table := metadata.AccountRewardCreditTable
	table.FilterColumn, table.FilterValue = "", nil
	var cursor int64
	for {
		rows, next, err := store.ReadFactRows(
			context.Background(), table, 0, math.MaxInt64, cursor, 5000,
			metaTxn(txn),
		)
		if err != nil {
			return err
		}
		for _, r := range rows.Rows {
			m, err := factRowMap(rows.Columns, r)
			if err != nil {
				return err
			}
			key, _ := m["staking_key"].([]byte)
			hash, _ := m["tx_hash"].([]byte)
			amount, err := factDecimal(m["amount"])
			if err != nil {
				return fmt.Errorf("reward journal amount: %w", err)
			}
			if err := fn(ledgerdb.RewardJournalRow{
				StakingKey:    key,
				CredentialTag: uint8(factUint(m["credential_tag"])), //nolint:gosec // a credential tag
				TxHash:        hash,
				Amount:        amount,
				Slot:          factUint(m["added_slot"]),
				Withdrawal:    factFlag(m["withdrawal"]),
				PostSnapshot:  factFlag(m["post_snapshot"]),
			}); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// factDecimal reads an unsigned amount a store holds as an integer or as
// decimal text. An unparsable or negative amount is an error: reading it as
// zero would silently drop a reward movement.
func factDecimal(v any) (uint64, error) {
	switch x := v.(type) {
	case string:
		return strconv.ParseUint(x, 10, 64)
	case []byte:
		return strconv.ParseUint(string(x), 10, 64)
	case int64:
		if x < 0 {
			return 0, fmt.Errorf("negative amount %d", x)
		}
		return uint64(x), nil
	}
	return 0, fmt.Errorf("amount has type %T", v)
}

// factFlag reads a boolean a store holds as a bool or an integer.
func factFlag(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case []byte:
		return len(x) > 0 && x[0] != '0' && x[0] != 0
	}
	return factUint(v) != 0
}
