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
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin"
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

// New returns the started blob plugin selected by name
func New(pluginName string) (BlobStore, error) {
	// Get and start the plugin
	p, err := plugin.StartPlugin(plugin.PluginTypeBlob, pluginName)
	if err != nil {
		return nil, err
	}

	// Type assert to BlobStore interface
	blobStore, ok := p.(BlobStore)
	if !ok {
		return nil, fmt.Errorf(
			"plugin '%s' does not implement BlobStore interface",
			pluginName,
		)
	}

	return blobStore, nil
}
