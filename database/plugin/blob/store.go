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

package blob

import (
	"log/slog"

	badgerPlugin "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	badger "github.com/dgraph-io/badger/v4"
)

type BlobStore interface {
	// matches badger.DB
	Close() error
	NewTransaction(bool) *badger.Txn

	// Our specific functions
	GetCommitTimestamp() (int64, error)
	SetCommitTimestamp(*badger.Txn, int64) error
}

// For now, this always returns a badger plugin
func New(
	pluginName, dataDir string,
	logger *slog.Logger,
) (BlobStore, error) {
	return badgerPlugin.New(dataDir, logger)
}
