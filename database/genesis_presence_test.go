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

package database

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type failedGenesisIterator struct {
	types.BlobIterator
	err error
}

func (i failedGenesisIterator) ValidForPrefix([]byte) bool { return false }
func (i failedGenesisIterator) Err() error                 { return i.err }

type failedGenesisScanStore struct {
	blob.BlobStore
	err error
}

func (s failedGenesisScanStore) NewIterator(txn types.Txn, opts types.BlobIteratorOptions) types.BlobIterator {
	return failedGenesisIterator{BlobIterator: s.BlobStore.NewIterator(txn, opts), err: s.err}
}

func TestGenesisPresencePropagatesStorageErrors(t *testing.T) {
	t.Parallel()
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	defer db.SetBlobStore(base)
	hash := make([]byte, 32)
	present, err := db.HasGenesisCbor(0, hash)
	require.NoError(t, err)
	require.False(t, present)
	present, err = db.HasAnyGenesisCbor(0)
	require.NoError(t, err)
	require.False(t, present)
	db.SetBlobStore(poisonBlobStore{BlobStore: base})
	present, err = db.HasGenesisCbor(0, hash)
	require.ErrorIs(t, err, errPinnedStoreBypassed)
	require.False(t, present)
	present, err = db.HasAnyGenesisCbor(0)
	require.ErrorContains(t, err, "iterator unavailable")
	require.False(t, present)
	failure := errors.New("scan failed")
	db.SetBlobStore(failedGenesisScanStore{BlobStore: base, err: failure})
	present, err = db.HasAnyGenesisCbor(0)
	require.ErrorIs(t, err, failure)
	require.False(t, present)
	db.SetBlobStore(base)
	txn := db.BlobTxn(true)
	require.NoError(t, base.SetBlock(txn.Blob(), 0, hash, []byte{1}, 1, 0, 0, nil))
	require.NoError(t, txn.Commit())
	present, err = db.HasAnyGenesisCbor(0)
	require.NoError(t, err)
	require.False(t, present, "a real slot-zero block is not synthetic genesis")
	require.NoError(t, db.SetGenesisCbor(0, hash, []byte{1}, nil))
	present, err = db.HasGenesisCbor(0, hash)
	require.NoError(t, err)
	require.True(t, present)
	present, err = db.HasAnyGenesisCbor(0)
	require.NoError(t, err)
	require.True(t, present)
}
