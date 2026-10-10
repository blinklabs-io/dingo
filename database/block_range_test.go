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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type invalidRangeItem struct{ types.BlobItem }

func (invalidRangeItem) Key() []byte { return nil }

type invalidRangeIterator struct {
	types.BlobIterator
	nilItem bool
}

func (i invalidRangeIterator) Item() types.BlobItem {
	if i.nilItem {
		return nil
	}
	return invalidRangeItem{BlobItem: i.BlobIterator.Item()}
}

type invalidRangeStore struct {
	blob.BlobStore
	nilItem bool
}

func (s invalidRangeStore) NewIterator(txn types.Txn, opts types.BlobIteratorOptions) types.BlobIterator {
	return invalidRangeIterator{BlobIterator: s.BlobStore.NewIterator(txn, opts), nilItem: s.nilItem}
}

func TestBlockRangeRejectsInvalidIteratorItems(t *testing.T) {
	t.Parallel()
	for _, nilItem := range []bool{true, false} {
		t.Run(map[bool]string{true: "nil item", false: "nil key"}[nilItem], func(t *testing.T) {
			db := newTestDB(t)
			insertTestBlock(t, db, 1, randomHash(t), []byte{1})
			base := db.Blob()
			defer db.SetBlobStore(base)
			db.SetBlobStore(invalidRangeStore{BlobStore: base, nilItem: nilItem})
			visited := 0
			visit := func(models.Block) error { visited++; return nil }
			err := ForEachBlockInRangeDB(t.Context(), db, 0, 2, visit)
			require.ErrorContains(t, err, "iterator returned a nil")
			require.Zero(t, visited)
			db.SetBlobStore(base)
			require.NoError(t, ForEachBlockInRangeDB(t.Context(), db, 0, 2, visit))
			require.Equal(t, 1, visited)
		})
	}
}
