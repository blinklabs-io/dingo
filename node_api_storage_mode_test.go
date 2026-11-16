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

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// The split layout runs the consensus store in core mode and serves the API
// from the IndexerDB, so the API servers must be gated on the layout, not on
// the consensus store's storage mode.
func TestAPIStorageModeOnSplitLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mode   StorageMode
		layout string
		want   StorageMode
	}{
		{"core shared", StorageModeCore, "", StorageModeCore},
		{
			"core shared explicit",
			StorageModeCore,
			internalconfig.IndexerLayoutShared,
			StorageModeCore,
		},
		{
			"api shared",
			StorageModeAPI,
			internalconfig.IndexerLayoutShared,
			StorageModeAPI,
		},
		{
			"core consensus split",
			StorageModeCore,
			internalconfig.IndexerLayoutSplit,
			StorageModeAPI,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := &Node{config: Config{
				storageMode:   tt.mode,
				indexerLayout: tt.layout,
			}}
			require.Equal(t, tt.want, n.apiStorageMode())
		})
	}
}
