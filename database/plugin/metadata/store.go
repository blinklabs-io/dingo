// Copyright 2025 Blink Labs Software
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

package metadata

import (
	"log/slog"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"gorm.io/gorm"
)

type MetadataStore interface {
	// Database
	Close() error
	DB() *gorm.DB
	GetCommitTimestamp() (int64, error)
	SetCommitTimestamp(*gorm.DB, int64) error
	Transaction() *gorm.DB

	// Ledger state
	AddUtxos(
		[]types.UtxoSlot,
		*gorm.DB,
	) error
	GetPoolRegistrations(
		lcommon.PoolKeyHash,
		*gorm.DB,
	) ([]lcommon.PoolRegistrationCertificate, error)
	GetStakeRegistrations(
		[]byte, // stakeKey
		*gorm.DB,
	) ([]lcommon.StakeRegistrationCertificate, error)
	GetTip(*gorm.DB) (ochainsync.Tip, error)

	GetAccount(
		[]byte, // stakeKey
		*gorm.DB,
	) (models.Account, error)
	GetDatum(
		lcommon.Blake2b256,
		*gorm.DB,
	) (models.Datum, error)
	GetPParams(
		uint64, // epoch
		*gorm.DB,
	) ([]models.PParams, error)
	GetPParamUpdates(
		uint64, // epoch
		*gorm.DB,
	) ([]models.PParamUpdate, error)
	GetUtxo(
		[]byte, // txId
		uint32, // idx
		*gorm.DB,
	) (models.Utxo, error)

	SetAccount(
		[]byte, // stakeKey
		[]byte, // pkh
		[]byte, // drep
		uint64, // slot
		bool, // active
		*gorm.DB,
	) error
	SetDatum(
		lcommon.Blake2b256,
		[]byte,
		uint64, // slot
		*gorm.DB,
	) error
	SetDeregistration(
		*lcommon.DeregistrationCertificate,
		uint64, // slot
		*gorm.DB,
	) error
	SetDeregistrationDrep(
		*lcommon.DeregistrationDrepCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetEpoch(
		uint64, // slot
		uint64, // epoch
		[]byte, // nonce
		uint, // era
		uint, // slotLength
		uint, // lengthInSlots
		*gorm.DB,
	) error
	SetPoolRegistration(
		*lcommon.PoolRegistrationCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetPoolRetirement(
		*lcommon.PoolRetirementCertificate,
		uint64, // slot
		*gorm.DB,
	) error
	SetPParams(
		[]byte, // pparams
		uint64, // slot
		uint64, // epoch
		uint, // era
		*gorm.DB,
	) error
	SetPParamUpdate(
		[]byte, // genesis
		[]byte, // update
		uint64, // slot
		uint64, // epoch
		*gorm.DB,
	) error
	SetRegistration(
		*lcommon.RegistrationCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetRegistrationDrep(
		*lcommon.RegistrationDrepCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetStakeDelegation(
		*lcommon.StakeDelegationCertificate,
		uint64, // slot
		*gorm.DB,
	) error
	SetStakeDeregistration(
		*lcommon.StakeDeregistrationCertificate,
		uint64, // slot
		*gorm.DB,
	) error
	SetStakeRegistration(
		*lcommon.StakeRegistrationCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetStakeRegistrationDelegation(
		*lcommon.StakeRegistrationDelegationCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetStakeVoteRegistrationDelegation(
		*lcommon.StakeVoteRegistrationDelegationCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetTip(
		ochainsync.Tip,
		*gorm.DB,
	) error
	SetUtxo(
		[]byte, // hash
		uint32, // idx
		uint64, // slot
		[]byte, // payment
		[]byte, // stake
		*gorm.DB,
	) error
	SetVoteRegistrationDelegation(
		*lcommon.VoteRegistrationDelegationCertificate,
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error

	// Helpers
	DeleteUtxo(any, *gorm.DB) error
	DeleteUtxos([]any, *gorm.DB) error
	DeleteUtxosAfterSlot(uint64, *gorm.DB) error
	DeleteUtxosBeforeSlot(uint64, *gorm.DB) error
	GetEpochLatest(*gorm.DB) (models.Epoch, error)
	GetEpochsByEra(uint, *gorm.DB) ([]models.Epoch, error)
	GetEpochs(*gorm.DB) ([]models.Epoch, error)
	GetUtxosAddedAfterSlot(uint64, *gorm.DB) ([]models.Utxo, error)
	GetUtxosByAddress(ledger.Address, *gorm.DB) ([]models.Utxo, error)
	GetUtxosDeletedBeforeSlot(uint64, *gorm.DB) ([]models.Utxo, error)
	SetUtxoDeletedAtSlot(ledger.TransactionInput, uint64, *gorm.DB) error
	SetUtxosNotDeletedAfterSlot(uint64, *gorm.DB) error
}

// For now, this always returns a sqlite plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
) (MetadataStore, error) {
	return sqlite.New(dataDir, logger)
}
