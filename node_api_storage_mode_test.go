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

package dingo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// API storage mode always serves the API from the IndexerDB while the
// consensus store runs in core mode, so API servers gate on the layout.
func TestAPIStorageModeOnSplitLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		mode         StorageMode
		databasePath string
		want         StorageMode
	}{
		{"core on disk", StorageModeCore, "/data", StorageModeCore},
		{"api on disk", StorageModeAPI, "/data", StorageModeAPI},
		{"api in memory", StorageModeAPI, "", StorageModeAPI},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := NewConfig(
				WithDatabasePath(tt.databasePath),
				WithStorageMode(tt.mode),
			)
			n := &Node{config: c}
			require.Equal(t, tt.want, n.apiStorageMode())
			split := tt.mode == StorageModeAPI
			require.Equal(t, split, n.splitIndexer())
			if split {
				require.Equal(t, StorageModeCore, c.storageMode,
					"the ledger's own store holds no API rows")
			}
		})
	}
}
