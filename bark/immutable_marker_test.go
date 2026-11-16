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

package bark

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// A promoted block's bp value is a marker in the store beneath Bark. Bark must
// hand callers the block body from the chunk files and never reach for the
// archive, which would treat the marker as a locally missing block.
func TestBarkGetBlockResolvesPromotedMarker(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 1000,
		},
	})
	require.NoError(t, err)
	hash := bytes.Repeat([]byte{0x07}, 32)
	body := []byte{0x82, 0x41, 0x07, 0x41, 0x02}
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 1, Slot: 70, Number: 1, Type: 1, Hash: hash, Cbor: body,
	}, nil))
	_, err = db.PromoteThrough(context.Background(), 1)
	require.NoError(t, err)

	baseURL, archiveSvc, httpClient := startFakeArchive(t, nil)
	store := newBarkBlobStoreForTest(t, db, baseURL, httpClient)

	txn := db.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	got, md, err := store.GetBlock(txn.Blob(), 70, hash)
	require.NoError(t, err)
	require.Equal(t, body, got)
	require.False(t, types.IsImmutableMarker(got))
	require.Equal(t, uint64(1), md.ID)

	local, _, err := store.GetBlockLocal(txn.Blob(), 70, hash)
	require.NoError(t, err)
	require.Equal(t, body, local)
	require.Zero(t, archiveSvc.fetchCalls, "a promoted block is local")

	_, ok := any(store).(blob.LocalBlockReader)
	require.True(t, ok)
}

// The database looks through wrappers for the marker store when it classifies
// stored values; Bark has to expose the store it wraps for that to work.
func TestBarkBlobStoreUnwrapsToItsUpstream(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	upstream := db.Blob()
	baseURL, _, httpClient := startFakeArchive(t, nil)
	store := newBarkBlobStoreForTest(t, db, baseURL, httpClient)
	require.Same(t, upstream, store.Unwrap())
}
