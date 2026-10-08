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

package dbtest

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/fakecloud"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// RunCloudBlockNumberBoundWork checks that resolving the highest indexed block
// of a cloud-backed database costs a bounded number of list requests however
// many blocks the archive holds. store must be backed by the empty fc bucket;
// objectName maps a blob key to the object name the store keeps it under.
func RunCloudBlockNumberBoundWork(
	t *testing.T,
	fc *fakecloud.Store,
	bucket string,
	store blob.BlobStore,
	objectName func(key []byte) string,
) {
	t.Helper()
	// alonzopparams:word-not-required: the harness creates the database it
	// opens, so it never meets a legacy Alonzo row.
	db, err := NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	db.SetBlobStore(store)

	seeded := uint64(0)
	seedTo := func(n uint64) {
		for id := seeded + 1; id <= n; id++ {
			hash := make([]byte, 32)
			binary.BigEndian.PutUint64(hash, id)
			blockKey := types.BlockBlobKey(id*20, hash)
			meta, err := cbor.Encode(types.BlockMetadata{ID: id, Height: id})
			require.NoError(t, err)
			fc.Put(bucket, objectName(types.BlockBlobIndexKey(id)), blockKey)
			fc.Put(
				bucket,
				objectName(types.BlockBlobMetadataKey(blockKey)),
				meta,
			)
		}
		seeded = n
	}

	// Budget: one initial seek, 64 bisection probes, and five requests
	// of headroom for bounded changes to the lookup algorithm.
	const maxLists = 70
	for _, n := range []uint64{200, 75_000} {
		seedTo(n)
		lists, listed := fc.Requests("LIST"), fc.Listed()
		bound, err := database.ResolveBlockNumberBound(context.Background(), db)
		require.NoError(t, err)
		lists, listed = fc.Requests("LIST")-lists, fc.Listed()-listed

		t.Logf("%d blocks: %d list requests, %d keys listed", n, lists, listed)
		require.True(t, bound.Resolved)
		require.Equal(t, n, bound.HighestID)
		require.Equal(t, n, bound.HighestNumber)
		require.LessOrEqualf(
			t,
			lists,
			maxLists,
			"%d indexed blocks: bound resolution issued %d list requests",
			n,
			lists,
		)
		require.LessOrEqualf(t, listed, maxLists*1000,
			"%d indexed blocks: bound resolution listed %d keys", n, listed)
	}
}
