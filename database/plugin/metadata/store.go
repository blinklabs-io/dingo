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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

type MetadataStore interface {
	// matches gorm.DB
	AutoMigrate(...interface{}) error
	// Really gorm.DB.DB() but close enough
	Close() error
	Create(interface{}) *gorm.DB
	DB() *gorm.DB
	First(interface{}) *gorm.DB
	Order(interface{}) *gorm.DB
	Where(interface{}, ...interface{}) *gorm.DB

	// Our specific functions
	GetCommitTimestamp() (int64, error)
	SetCommitTimestamp(*gorm.DB, int64) error
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
		[]byte,
		uint64,
		uint64,
		*gorm.DB,
	) error // pkh, slot, epoch
	SetPParams(
		[]byte,
		uint64,
		uint64,
		uint,
		*gorm.DB,
	) error // params, slot, epoch
	SetPParamUpdate(
		[]byte,
		[]byte,
		uint64,
		uint64,
		*gorm.DB,
	) error // genesis, update, slot, epoch
	SetStakeDelegation(
		[]byte,
		[]byte,
		uint64,
		*gorm.DB,
	) error // stakeKey, pkh, slot
	SetStakeDeregistration(
		[]byte,
		uint64,
		*gorm.DB,
	) error // stakeKey, slot
	SetStakeRegistration(
		[]byte,
		uint64,
		uint64,
		*gorm.DB,
	) error // stakeKey, slot, deposit
	// GetTip() models.Tip
	// SetTip(models.Tip) error
	Transaction() *gorm.DB
	// Helpers
	GetEpochLatest(*gorm.DB) (models.Epoch, error)
	SetPoolOwners(
		[]byte,
		[][]byte,
		*gorm.DB,
	) error // pkh, owners
	SetPoolRelays(
		[]byte,
		[]models.PoolRegistrationRelay,
		*gorm.DB,
	) error // pkh, relays
}

// For now, this always returns a sqlite plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
) (MetadataStore, error) {
	return sqlite.New(dataDir, logger)
}
