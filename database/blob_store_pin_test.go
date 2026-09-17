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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// errPinnedStoreBypassed is returned by poisonBlobStore's Get so a call that
// reached it, instead of the transaction's pinned store, is unambiguous.
var errPinnedStoreBypassed = errors.New(
	"poisonBlobStore: pinned store was bypassed",
)

// poisonBlobStore wraps a real store but makes every read observably wrong:
// NewIterator returns nil and Get returns errPinnedStoreBypassed. Installing
// it via Database.SetBlobStore after a transaction has opened simulates a
// concurrent blob-store replacement; a caller that reads db.Blob() again
// instead of using the transaction's pinned Txn.BlobStore() hits this store
// and observably fails.
type poisonBlobStore struct {
	blob.BlobStore
}

func (poisonBlobStore) Get(types.Txn, []byte) ([]byte, error) {
	return nil, errPinnedStoreBypassed
}

func (poisonBlobStore) NewIterator(
	types.Txn,
	types.BlobIteratorOptions,
) types.BlobIterator {
	return nil
}

// TestBlockNumberBoundTxnUsesPinnedStore is the regression test for
// ResolveBlockNumberBoundTxn, blockIndexEntryAtOrAfterTxn and
// blockMetadataByKey re-reading Database.Blob (the currently installed
// store) instead of the transaction's pinned Txn.BlobStore(). A store
// replacement between the transaction's construction and these calls must
// not change which store they read.
func TestBlockNumberBoundTxnUsesPinnedStore(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)
	t.Cleanup(func() { db.SetBlobStore(base) })

	txn := db.Transaction(false)
	t.Cleanup(txn.Release)

	// Replace the installed store after the transaction opened. The
	// transaction's pin must still resolve to base, not this poison store.
	db.SetBlobStore(poisonBlobStore{BlobStore: base})

	_, err = ResolveBlockNumberBoundTxn(txn)
	require.NoError(t, err)

	_, err = blockIndexEntryAtOrAfterTxn(txn, 0)
	require.True(
		t,
		err == nil || errors.Is(err, models.ErrBlockNotFound),
		"blockIndexEntryAtOrAfterTxn used the live store instead of the pin: %v",
		err,
	)

	_, err = blockMetadataByKey(txn, []byte("nonexistent-block-key"))
	require.False(
		t,
		errors.Is(err, errPinnedStoreBypassed),
		"blockMetadataByKey used the live store instead of the pin: %v",
		err,
	)
}
