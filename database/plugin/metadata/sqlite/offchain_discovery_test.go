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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestOffchainDiscoveryRereadsInPlaceDrepAnchor reproduces the pointer-discovery
// regression introduced by the keyset-cursor change: the drep table stores one
// row per DRep and updates that row in place when the DRep publishes a new
// anchor, so the row id never changes. A keyset cursor that only ever moves
// forward therefore never re-reads the updated anchor.
func TestOffchainDiscoveryRereadsInPlaceDrepAnchor(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	credential := bytes.Repeat([]byte{0x11}, 32)
	hashA := bytes.Repeat([]byte{0x41}, 32)
	hashB := bytes.Repeat([]byte{0x42}, 32)
	urlA := "https://metadata.example.test/drep-a.json"
	urlB := "https://metadata.example.test/drep-b.json"

	// DRep registers with anchor A.
	require.NoError(t, store.SetDrep(0, credential, 10, urlA, hashA, true, nil))
	created, err := store.EnsureOffchainMetadataPointers(t.Context(), now, nil)
	require.NoError(t, err)
	require.Equal(t, 1, created)

	doc, err := store.GetOffchainMetadata(
		models.OffchainMetadataSourceDrep,
		urlA,
		hashA,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, doc)

	// DRep publishes a new anchor: production updates the drep row in place
	// (same row id) through the (credential_tag, credential) upsert.
	require.NoError(t, store.SetDrep(0, credential, 20, urlB, hashB, true, nil))

	created, err = store.EnsureOffchainMetadataPointers(t.Context(), now, nil)
	require.NoError(t, err)
	doc, err = store.GetOffchainMetadata(
		models.OffchainMetadataSourceDrep,
		urlB,
		hashB,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, doc, "updated drep anchor was never discovered")
	require.Equal(t, 1, created)
}

// TestOffchainDiscoveryPageBoundAndCursor pins the per-pass page bound of 64
// rows per source, the persisted keyset cursor, and its wrap at table end.
func TestOffchainDiscoveryPageBoundAndCursor(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	const total = 70
	const cursorKey = "offchain-metadata-discovery:" +
		models.OffchainMetadataSourceDrep
	for i := range total {
		credential := bytes.Repeat([]byte{byte(i + 1)}, 32)
		hash := bytes.Repeat([]byte{byte(i + 1)}, 32)
		url := fmt.Sprintf("https://metadata.example.test/drep-%d.json", i)
		require.NoError(
			t,
			store.SetDrep(0, credential, 10, url, hash, true, nil),
		)
	}

	created, err := store.EnsureOffchainMetadataPointers(t.Context(), now, nil)
	require.NoError(t, err)
	require.Equal(t, 64, created)
	cursor, err := store.GetSyncState(cursorKey, nil)
	require.NoError(t, err)
	require.NotEqual(t, "", cursor)
	require.NotEqual(t, "0", cursor)

	created, err = store.EnsureOffchainMetadataPointers(t.Context(), now, nil)
	require.NoError(t, err)
	require.Equal(t, total-64, created)
	cursor, err = store.GetSyncState(cursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "0", cursor)
}
