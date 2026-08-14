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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/stretchr/testify/require"
)

func TestMetadataStoreConformance(t *testing.T) {
	storagetest.RunMetadataStoreConformance(
		t,
		func(t *testing.T) metadata.MetadataStore {
			t.Helper()
			store, _, _, err := openSQLStore(
				Config{DataDir: t.TempDir()},
				metadata.ProviderDependencies{},
			)
			require.NoError(t, err)
			require.NoError(t, store.Start(t.Context()))
			t.Cleanup(func() {
				require.NoError(t, store.Close())
			})
			return store
		},
	)
}

func TestMetadataStoreResourceCleanup(t *testing.T) {
	storagetest.AssertNoGoroutineLeak(t, func(t *testing.T) {
		store, _, _, err := openSQLStore(
			Config{DataDir: t.TempDir()},
			metadata.ProviderDependencies{},
		)
		require.NoError(t, err)
		require.NoError(t, store.Start(t.Context()))
		txn := store.Transaction()
		require.NoError(t, store.SetCommitTimestamp(1, txn))
		require.NoError(t, txn.Commit())
		require.NoError(t, store.Close())
	})
}
