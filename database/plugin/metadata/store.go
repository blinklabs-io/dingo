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
	GetEpochLatest(*gorm.DB) (models.Epoch, error)
	GetPParams(uint64, *gorm.DB) ([]models.PParams, error)
	GetPParamUpdates(uint64, *gorm.DB) ([]models.PParamUpdate, error)
	GetStakeRegistrations([]byte, *gorm.DB) ([]models.StakeRegistration, error)
	SetEpoch(uint64, uint64, []byte, uint, uint, uint, *gorm.DB) error
	SetPoolRetirement([]byte, uint64, uint64, *gorm.DB) error
	SetPParams([]byte, uint64, uint64, uint, *gorm.DB) error
	SetPParamUpdate([]byte, []byte, uint64, uint64, *gorm.DB) error
	SetStakeDelegation([]byte, []byte, uint64, *gorm.DB) error
	SetStakeDeregistration([]byte, uint64, *gorm.DB) error
	SetStakeRegistration([]byte, uint64, uint64, *gorm.DB) error
	// GetTip() models.Tip
	// SetEpoch(models.Epoch) error
	// SetPoolRegistration(models.PoolRegistration) error
	// SetPoolRetirement(models.PoolRetirement) error
	// SetProtocolParameterUpdate(models.PParamUpdate) error
	// SetTip(models.Tip) error
	Transaction() *gorm.DB
}

// For now, this always returns a sqlite plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
) (MetadataStore, error) {
	return sqlite.New(dataDir, logger)
}
