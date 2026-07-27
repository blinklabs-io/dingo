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

package lifecycle

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
)

// newStorageHost builds a plugin host with only the storage-capability
// providers registered — not internal/plugins.NewHost, which additionally
// wires up API and mempool providers this package has no use for and
// would otherwise pull database/lifecycle (under the database import
// boundary) into depending on the api/mempool package trees.
func newStorageHost() (*plugin.Host, error) {
	host := plugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register badger provider: %w", err)
	}
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register sqlite provider: %w", err)
	}
	if err := registerExtraStorage(host); err != nil {
		return nil, err
	}
	return host, nil
}
