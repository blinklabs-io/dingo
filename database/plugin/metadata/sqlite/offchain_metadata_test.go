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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlite

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffchainMetadataPointersRoundTrip(t *testing.T) {
	store := setupTestDBWithMode(t, types.StorageModeAPI)
	url := "https://pool.example.test/metadata.json"
	hash := bytes.Repeat([]byte{0x42}, 32)
	pool := models.Pool{
		PoolKeyHash: bytes.Repeat([]byte{0x11}, 28),
		VrfKeyHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	require.NoError(t, store.DB().Create(&pool).Error)
	reg := models.PoolRegistration{
		PoolID:       pool.ID,
		PoolKeyHash:  pool.PoolKeyHash,
		VrfKeyHash:   pool.VrfKeyHash,
		MetadataUrl:  url,
		MetadataHash: hash,
		AddedSlot:    10,
	}
	require.NoError(t, store.DB().Create(&reg).Error)

	now := time.Unix(100, 0).UTC()
	created, err := store.EnsureOffchainMetadataPointers(
		context.Background(),
		now,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, created)

	created, err = store.EnsureOffchainMetadataPointers(
		context.Background(),
		now,
		nil,
	)
	require.NoError(t, err)
	require.Zero(t, created)

	batch, err := store.GetOffchainMetadataFetchBatch(
		context.Background(),
		10,
		now,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, models.OffchainMetadataSourcePool, batch[0].SourceType)
	require.Equal(t, url, batch[0].URL)
	require.Equal(t, hash, batch[0].Hash)

	fetchedAt := now.Add(time.Second)
	batch[0].Status = models.OffchainMetadataStatusFetched
	batch[0].ContentType = "application/json"
	batch[0].Content = []byte(`{"ticker":"TEST"}`)
	batch[0].BodyHash = bytes.Repeat([]byte{0x24}, 32)
	batch[0].FetchedAt = &fetchedAt
	batch[0].NextFetchAfter = nil
	batch[0].FetchAttempts = 1
	batch[0].LastHTTPStatus = http.StatusOK
	require.NoError(t, store.SetOffchainMetadataFetchResult(
		context.Background(),
		&batch[0],
		nil,
	))

	got, err := store.GetOffchainMetadata(
		models.OffchainMetadataSourcePool,
		url,
		hash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, models.OffchainMetadataStatusFetched, got.Status)
	require.Equal(t, []byte(`{"ticker":"TEST"}`), got.Content)
	require.Equal(t, uint(http.StatusOK), got.LastHTTPStatus)
	require.NotNil(t, got.FetchedAt)
	require.True(t, got.FetchedAt.Equal(fetchedAt))
}

// TestGetOffchainMetadataBatch covers GetOffchainMetadataBatch's batching
// contract for /pools/extended-style callers: many URLs of the same
// source type resolved in one query, matching rows returned even when two
// documents share a URL under different hashes, non-matching/empty URLs
// ignored, and an empty input returning no rows without querying.
func TestGetOffchainMetadataBatch(t *testing.T) {
	store := setupTestDBWithMode(t, types.StorageModeAPI)

	urlA := "https://pool.example.test/a.json"
	urlB := "https://pool.example.test/b.json"
	hashA := bytes.Repeat([]byte{0xa1}, 32)
	hashA2 := bytes.Repeat([]byte{0xa2}, 32)
	hashB := bytes.Repeat([]byte{0xb1}, 32)

	for _, doc := range []models.OffchainMetadata{
		{
			SourceType: models.OffchainMetadataSourcePool,
			URL:        urlA,
			Hash:       hashA,
			Status:     models.OffchainMetadataStatusFetched,
			Content:    []byte(`{"ticker":"AAA"}`),
		},
		{
			// Same URL as above, different on-chain hash: a pool that
			// republished its metadata file's content without changing
			// the URL. Both rows must come back from the batch query.
			SourceType: models.OffchainMetadataSourcePool,
			URL:        urlA,
			Hash:       hashA2,
			Status:     models.OffchainMetadataStatusFetched,
			Content:    []byte(`{"ticker":"AA2"}`),
		},
		{
			SourceType: models.OffchainMetadataSourcePool,
			URL:        urlB,
			Hash:       hashB,
			Status:     models.OffchainMetadataStatusPending,
		},
		{
			// Different source type, same URL: must not leak into a
			// source_type = pool batch query.
			SourceType: models.OffchainMetadataSourceDrep,
			URL:        urlA,
			Hash:       hashA,
			Status:     models.OffchainMetadataStatusFetched,
		},
	} {
		require.NoError(t, store.DB().Create(&doc).Error)
	}

	docs, err := store.GetOffchainMetadataBatch(
		models.OffchainMetadataSourcePool,
		[]string{urlA, urlB, "", urlA}, // empty and duplicate entries
		nil,
	)
	require.NoError(t, err)
	require.Len(t, docs, 3)

	byKey := make(map[string]models.OffchainMetadata, len(docs))
	for _, d := range docs {
		byKey[d.URL+"\x00"+string(d.Hash)] = d
	}
	docA, ok := byKey[urlA+"\x00"+string(hashA)]
	require.True(t, ok)
	require.Equal(t, models.OffchainMetadataStatusFetched, docA.Status)
	docA2, ok := byKey[urlA+"\x00"+string(hashA2)]
	require.True(t, ok)
	require.Equal(t, []byte(`{"ticker":"AA2"}`), docA2.Content)
	docB, ok := byKey[urlB+"\x00"+string(hashB)]
	require.True(t, ok)
	require.Equal(t, models.OffchainMetadataStatusPending, docB.Status)

	empty, err := store.GetOffchainMetadataBatch(
		models.OffchainMetadataSourcePool, nil, nil,
	)
	require.NoError(t, err)
	require.Empty(t, empty)
}

// TestGetOffchainMetadataBatchDeduplicatesAcrossChunks pins the sqlite
// backend's dedup-before-chunk behavior. sqlite splits the url IN (...) list
// at sqliteBindVarLimit, and offchain.GetBatch only removes duplicates within
// whatever slice it is handed, so deduplicating per chunk instead of over the
// whole list would query a repeated URL once per chunk it appears in and
// append its row that many times. Two pools can share a metadata anchor, so a
// repeated URL is expected input rather than a pathological case.
func TestGetOffchainMetadataBatchDeduplicatesAcrossChunks(t *testing.T) {
	store := setupTestDBWithMode(t, types.StorageModeAPI)

	url := "https://pool.example.test/shared.json"
	hash := bytes.Repeat([]byte{0xc1}, 32)
	require.NoError(t, store.DB().Create(&models.OffchainMetadata{
		SourceType: models.OffchainMetadataSourcePool,
		URL:        url,
		Hash:       hash,
		Status:     models.OffchainMetadataStatusFetched,
		Content:    []byte(`{"ticker":"SHR"}`),
	}).Error)

	// Place the same URL at both ends of a list longer than one chunk, so
	// the repeats land in different chunks.
	urls := make([]string, 0, sqliteBindVarLimit+2)
	urls = append(urls, url)
	for i := range sqliteBindVarLimit {
		urls = append(
			urls,
			fmt.Sprintf("https://pool.example.test/filler-%d.json", i),
		)
	}
	urls = append(urls, url)
	require.Greater(t, len(urls), sqliteBindVarLimit)

	docs, err := store.GetOffchainMetadataBatch(
		models.OffchainMetadataSourcePool, urls, nil,
	)
	require.NoError(t, err)
	require.Len(
		t,
		docs,
		1,
		"a URL repeated across chunk boundaries must yield one row, not one per chunk",
	)
	assert.Equal(t, url, docs[0].URL)
	assert.Equal(t, hash, docs[0].Hash)
}
