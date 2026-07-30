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
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type offchainStore interface {
	SetConstitution(*models.Constitution, types.Txn) error
	EnsureOffchainMetadataPointers(
		context.Context,
		time.Time,
		types.Txn,
	) (int, error)
	GetOffchainMetadataFetchBatch(
		context.Context,
		int,
		time.Time,
		types.Txn,
	) ([]models.OffchainMetadata, error)
	SetOffchainMetadataFetchResult(
		context.Context,
		*models.OffchainMetadata,
		types.Txn,
	) error
	GetOffchainMetadata(
		string,
		string,
		[]byte,
		types.Txn,
	) (*models.OffchainMetadata, error)
}

type offchainState struct {
	created      int
	createdAgain int
	firstBatch   []models.OffchainMetadata
	secondBatch  []models.OffchainMetadata
	fetched      *models.OffchainMetadata
}

func TestSharedSQLStoreOffchainParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseOffchainStore(t, store)
}

func exerciseOffchainStore(t *testing.T, store offchainStore) offchainState {
	t.Helper()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	fetchedAt := now.Add(time.Minute)
	nextFetch := now.Add(time.Hour)
	hash := bytes.Repeat([]byte{0x42}, 32)
	const url = "https://metadata.example.test/constitution.json"
	require.NoError(t, store.SetConstitution(
		&models.Constitution{
			AnchorURL:  "  " + url + " ",
			AnchorHash: hash,
			AddedSlot:  10,
		},
		nil,
	))
	var ret offchainState
	var err error
	ret.created, err = store.EnsureOffchainMetadataPointers(
		t.Context(),
		now,
		nil,
	)
	require.NoError(t, err)
	ret.createdAgain, err = store.EnsureOffchainMetadataPointers(
		t.Context(),
		now,
		nil,
	)
	require.NoError(t, err)
	ret.firstBatch, err = store.GetOffchainMetadataFetchBatch(
		t.Context(),
		10,
		now,
		nil,
	)
	require.NoError(t, err)
	ret.secondBatch, err = store.GetOffchainMetadataFetchBatch(
		t.Context(),
		10,
		now,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, ret.firstBatch, 1)
	doc := ret.firstBatch[0]
	doc.Status = models.OffchainMetadataStatusFetched
	doc.ContentType = "application/json"
	doc.BodyHash = bytes.Repeat([]byte{0x24}, 32)
	doc.Content = []byte(`{"name":"constitution"}`)
	doc.FetchedAt = &fetchedAt
	doc.NextFetchAfter = &nextFetch
	doc.FetchAttempts = 1
	doc.LastHTTPStatus = 200
	require.NoError(t, store.SetOffchainMetadataFetchResult(
		t.Context(),
		&doc,
		nil,
	))
	ret.fetched, err = store.GetOffchainMetadata(
		models.OffchainMetadataSourceConstitution,
		url,
		hash,
		nil,
	)
	require.NoError(t, err)
	normalizeOffchainTimes(ret.firstBatch)
	normalizeOffchainDocument(ret.fetched)
	return ret
}

func normalizeOffchainTimes(docs []models.OffchainMetadata) {
	for i := range docs {
		docs[i].CreatedAt = time.Time{}
		docs[i].UpdatedAt = time.Time{}
	}
}

func normalizeOffchainDocument(doc *models.OffchainMetadata) {
	if doc == nil {
		return
	}
	doc.CreatedAt = time.Time{}
	doc.UpdatedAt = time.Time{}
}
