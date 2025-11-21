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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
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
		[]models.UtxoSlot,
		*gorm.DB,
	) error
	GetPoolRegistrations(
		lcommon.PoolKeyHash,
		*gorm.DB,
	) ([]lcommon.PoolRegistrationCertificate, error)
	GetPool(
		lcommon.PoolKeyHash,
		*gorm.DB,
	) (*models.Pool, error)
	GetStakeRegistrations(
		[]byte, // stakeKey
		*gorm.DB,
	) ([]lcommon.StakeRegistrationCertificate, error)
	GetTip(*gorm.DB) (ochainsync.Tip, error)

	GetAccount(
		[]byte, // stakeKey
		bool, // includeInactive
		*gorm.DB,
	) (*models.Account, error)
	GetBlockNonce(
		ocommon.Point,
		*gorm.DB,
	) ([]byte, error)
	GetDatum(
		lcommon.Blake2b256,
		*gorm.DB,
	) (*models.Datum, error)
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
	) (*models.Utxo, error)
	GetTransactionByHash(
		[]byte, // hash
		*gorm.DB,
	) (*models.Transaction, error)

	SetBlockNonce(
		[]byte, // blockHash
		uint64, // slotNumber
		[]byte, // nonce
		bool, // isCheckpoint
		*gorm.DB,
	) error
	SetDatum(
		lcommon.Blake2b256,
		[]byte,
		uint64, // slot
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
	SetTip(
		ochainsync.Tip,
		*gorm.DB,
	) error
	SetTransaction(
		ocommon.Point,
		lcommon.Transaction,
		uint32, // transaction index within block
		map[int]uint64, // deposits: certificate index -> deposit amount in lovelace. Keys must align with transaction certificate slice order. Missing keys default to 0.
		*gorm.DB,
	) error

	// Helpers
	DeleteBlockNoncesBeforeSlot(uint64, *gorm.DB) error
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(uint64, *gorm.DB) error
	DeleteUtxo(any, *gorm.DB) error
	DeleteUtxos([]any, *gorm.DB) error
	DeleteUtxosAfterSlot(uint64, *gorm.DB) error
	GetEpochsByEra(uint, *gorm.DB) ([]models.Epoch, error)
	GetEpochs(*gorm.DB) ([]models.Epoch, error)
	GetUtxosAddedAfterSlot(uint64, *gorm.DB) ([]models.Utxo, error)
	GetUtxosByAddress(lcommon.Address, *gorm.DB) ([]models.Utxo, error)
	GetUtxosDeletedBeforeSlot(uint64, int, *gorm.DB) ([]models.Utxo, error)
	SetUtxoDeletedAtSlot(lcommon.TransactionInput, uint64, *gorm.DB) error
	SetUtxosNotDeletedAfterSlot(uint64, *gorm.DB) error
}

// For now, this always returns a sqlite plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (MetadataStore, error) {
	return sqlite.New(dataDir, logger, promRegistry)
}
