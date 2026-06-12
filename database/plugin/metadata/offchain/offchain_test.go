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

package offchain

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestFetchBatchClaimsDueRows(t *testing.T) {
	db := newOffchainTestDB(t)
	now := time.Unix(1000, 0).UTC()
	dueAt := now.Add(-time.Minute)
	future := now.Add(fetchClaimLease + time.Minute)

	pending := createOffchainTestDoc(
		t,
		db,
		0x01,
		models.OffchainMetadataStatusPending,
		nil,
	)
	failed := createOffchainTestDoc(
		t,
		db,
		0x02,
		models.OffchainMetadataStatusFailed,
		&dueAt,
	)
	createOffchainTestDoc(
		t,
		db,
		0x03,
		models.OffchainMetadataStatusPending,
		&future,
	)
	createOffchainTestDoc(
		t,
		db,
		0x04,
		models.OffchainMetadataStatusFetched,
		nil,
	)

	firstBatch, err := FetchBatch(context.Background(), db, 10, now)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint{pending.ID, failed.ID}, docIDs(firstBatch))

	secondBatch, err := FetchBatch(context.Background(), db, 10, now)
	require.NoError(t, err)
	require.Empty(t, secondBatch)

	for _, doc := range firstBatch {
		var stored models.OffchainMetadata
		require.NoError(t, db.First(&stored, doc.ID).Error)
		require.NotNil(t, stored.NextFetchAfter)
		require.True(t, stored.NextFetchAfter.After(now))
		require.WithinDuration(
			t,
			now.Add(fetchClaimLease),
			*stored.NextFetchAfter,
			time.Second,
		)
		require.Equal(t, doc.Status, stored.Status)
	}

	afterLease, err := FetchBatch(
		context.Background(),
		db,
		10,
		now.Add(fetchClaimLease+time.Second),
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint{pending.ID, failed.ID}, docIDs(afterLease))
}

func TestFetchBatchConcurrentCallersClaimRowOnce(t *testing.T) {
	db := newOffchainTestDB(t)
	now := time.Unix(2000, 0).UTC()
	doc := createOffchainTestDoc(
		t,
		db,
		0x11,
		models.OffchainMetadataStatusPending,
		nil,
	)

	const callers = 8
	start := make(chan struct{})
	results := make(chan fetchBatchResult, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			batch, err := FetchBatch(context.Background(), db, 1, now)
			results <- fetchBatchResult{
				batch: batch,
				err:   err,
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	claimCounts := map[uint]int{}
	for result := range results {
		require.NoError(t, result.err)
		for _, claimed := range result.batch {
			claimCounts[claimed.ID]++
		}
	}
	require.Equal(t, map[uint]int{doc.ID: 1}, claimCounts)
}

type fetchBatchResult struct {
	batch []models.OffchainMetadata
	err   error
}

func newOffchainTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := gorm.Open(
		sqlite.Open(
			fmt.Sprintf(
				"file:%s?_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
				dbPath,
			),
		),
		&gorm.Config{SkipDefaultTransaction: true},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})
	require.NoError(t, db.AutoMigrate(&models.OffchainMetadata{}))
	return db
}

func createOffchainTestDoc(
	t *testing.T,
	db *gorm.DB,
	seed byte,
	status string,
	nextFetchAfter *time.Time,
) models.OffchainMetadata {
	t.Helper()
	doc := models.OffchainMetadata{
		SourceType:     models.OffchainMetadataSourcePool,
		URL:            fmt.Sprintf("https://metadata.example.test/%02x.json", seed),
		Hash:           bytes.Repeat([]byte{seed}, 32),
		Status:         status,
		NextFetchAfter: nextFetchAfter,
	}
	require.NoError(t, db.Create(&doc).Error)
	return doc
}

func docIDs(docs []models.OffchainMetadata) []uint {
	ids := make([]uint, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids
}
