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

package badger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/stretchr/testify/require"
)

func TestBlobStoreConformance(t *testing.T) {
	storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
		t.Helper()
		store, err := New(WithDataDir(t.TempDir()))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, store.Stop())
		})
		return store
	})
}

func TestBlobStoreResourceCleanup(t *testing.T) {
	storagetest.AssertNoGoroutineLeak(t, func(t *testing.T) {
		store, err := New(WithDataDir(t.TempDir()))
		require.NoError(t, err)
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, []byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		require.NoError(t, store.Stop())
	})
}
