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
	"database/sql"
	"log/slog"

	"gorm.io/gorm"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
)

type MetadataStore interface {
	// matches gorm.DB
	AutoMigrate(...interface{}) error
	Begin(...*sql.TxOptions) *gorm.DB
	Create(interface{}) *gorm.DB
	DB() *gorm.DB
	First(interface{}) *gorm.DB
	Order(interface{}) *gorm.DB
	Where(interface{}, ...interface{}) *gorm.DB

	// Our specific functions
	Close() error
	GetCommitTimestamp() (int64, error)
	SetCommitTimestamp(*gorm.DB, int64) error
	Transaction() *gorm.DB
}

// For now, this always returns a sqlite plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
) (MetadataStore, error) {
	return sqlite.New(dataDir, logger)
}
