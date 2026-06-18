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
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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
