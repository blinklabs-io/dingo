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
	"math/big"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
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
	// GetEpoch(uint64, *gorm.DB)
	// GetEpochs(*gorm.DB) ([]models.Epoch, error)
	GetPoolRegistrations(
		[]byte, // pkh
		*gorm.DB,
	) ([]models.PoolRegistration, error)
	GetPParams(
		uint64, // epoch
		*gorm.DB,
	) ([]models.PParams, error)
	GetPParamUpdates(
		uint64, // epoch
		*gorm.DB,
	) ([]models.PParamUpdate, error)
	GetStakeRegistrations(
		[]byte, // stakeKey
		*gorm.DB,
	) ([]models.StakeRegistration, error)
	GetTip(*gorm.DB) (ochainsync.Tip, error)
	GetUtxo(
		[]byte, // txId
		uint32, // idx
		*gorm.DB,
	) (models.Utxo, error)

	SetEpoch(
		uint64, // epoch
		uint64, // slot
		[]byte, // nonce
		uint, // era
		uint, // slotLength
		uint, // lengthInSlots
		*gorm.DB,
	) error
	SetPoolRegistration(
		[]byte, // pkh
		[]byte, // vrf
		uint64, // pledge
		uint64, // cost
		uint64, // slot
		uint64, // deposit
		*big.Rat, // margin
		[]lcommon.AddrKeyHash, // owners
		[]lcommon.PoolRelay, // relays
		*lcommon.PoolMetadata, // metadata
		*gorm.DB,
	) error
	SetPoolRetirement(
		[]byte, // pkh
		uint64, // slot
		uint64, // epoch
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
	SetStakeDelegation(
		[]byte, // stakeKey
		[]byte, // pkh
		uint64, // slot
		*gorm.DB,
	) error
	SetStakeDeregistration(
		[]byte, // stakeKey
		uint64, // slot
		*gorm.DB,
	) error
	SetStakeRegistration(
		[]byte, // stakeKey
		uint64, // slot
		uint64, // deposit
		*gorm.DB,
	) error
	SetTip(
		ochainsync.Tip,
		*gorm.DB,
	) error

	// Helpers
	GetEpochLatest(*gorm.DB) (models.Epoch, error)
	GetUtxosByAddress(ledger.Address, *gorm.DB) ([]models.Utxo, error)
	DeleteUtxo(any, *gorm.DB) error
	DeleteUtxos([]any, *gorm.DB) error
}

// For now, this always returns a sqlite plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
) (MetadataStore, error) {
	return sqlite.New(dataDir, logger)
}
