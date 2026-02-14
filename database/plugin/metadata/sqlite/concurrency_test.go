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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// setupFileBasedStore creates a file-based MetadataStoreSqlite in a
// temp directory. This exercises the separate read/write connection
// pools used in production.
func setupFileBasedStore(
	t *testing.T,
) *MetadataStoreSqlite {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := NewWithOptions(
		WithDataDir(tmpDir),
		WithMaxConnections(DefaultMaxConnections),
	)
	require.NoError(t, err, "failed to create store")
	require.NoError(t, store.Start(), "failed to start store")
	return store
}

// TestConcurrentReadsDuringWrites verifies that concurrent reads
// do not produce SQLITE_BUSY errors while writes are in progress.
// This is the core scenario the read/write pool separation fixes.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	const (
		numWriters   = 3
		numReaders   = 5
		opsPerWorker = 20
	)

	var (
		writeErrors atomic.Int64
		readErrors  atomic.Int64
		wg          sync.WaitGroup
	)

	// Seed some initial data so reads have something to find
	for i := range 10 {
		snapshot := &models.PoolStakeSnapshot{
			Epoch:          1,
			SnapshotType:   "go",
			PoolKeyHash:    []byte(fmt.Sprintf("pool_%028d", i)),
			TotalStake:     1000000,
			DelegatorCount: 100,
			CapturedSlot:   1000,
		}
		require.NoError(
			t,
			store.SavePoolStakeSnapshot(snapshot, nil),
		)
	}

	// Start concurrent writers
	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := range opsPerWorker {
				snapshot := &models.PoolStakeSnapshot{
					Epoch:        uint64(writerID*1000 + i + 2),
					SnapshotType: "go",
					PoolKeyHash: []byte(
						fmt.Sprintf(
							"pool_w%d_%023d",
							writerID,
							i,
						),
					),
					TotalStake:     1000000,
					DelegatorCount: 100,
					CapturedSlot:   uint64(writerID*1000 + i),
				}
				if err := store.SavePoolStakeSnapshot(
					snapshot, nil,
				); err != nil {
					writeErrors.Add(1)
					t.Logf(
						"writer %d op %d error: %v",
						writerID, i, err,
					)
				}
			}
		}(w)
	}

	// Start concurrent readers
	for r := range numReaders {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for range opsPerWorker {
				_, err := store.GetPoolStakeSnapshotsByEpoch(
					1, "go", nil,
				)
				if err != nil {
					readErrors.Add(1)
					t.Logf(
						"reader %d error: %v",
						readerID, err,
					)
				}
			}
		}(r)
	}

	wg.Wait()

	assert.Equal(
		t,
		int64(0),
		writeErrors.Load(),
		"expected zero write errors (SQLITE_BUSY)",
	)
	assert.Equal(
		t,
		int64(0),
		readErrors.Load(),
		"expected zero read errors (SQLITE_BUSY)",
	)
}

// TestConcurrentWriteTransactions verifies that multiple
// goroutines can perform write transactions without SQLITE_BUSY
// errors. With a single-writer connection pool, writes serialize
// naturally at the Go connection pool level.
func TestConcurrentWriteTransactions(t *testing.T) {
	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	const (
		numWriters   = 5
		opsPerWriter = 10
	)

	var (
		errors atomic.Int64
		wg     sync.WaitGroup
	)

	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := range opsPerWriter {
				txn := store.Transaction()

				snapshot := &models.PoolStakeSnapshot{
					Epoch: uint64(
						writerID*1000 + i + 1,
					),
					SnapshotType: "go",
					PoolKeyHash: []byte(
						fmt.Sprintf(
							"pool_tw%d_%022d",
							writerID,
							i,
						),
					),
					TotalStake:     1000000,
					DelegatorCount: 100,
					CapturedSlot: uint64(
						writerID*1000 + i,
					),
				}
				if err := store.SavePoolStakeSnapshot(
					snapshot, txn,
				); err != nil {
					errors.Add(1)
					t.Logf(
						"writer %d op %d save error: %v",
						writerID, i, err,
					)
					_ = txn.Rollback()
					continue
				}
				if err := txn.Commit(); err != nil {
					errors.Add(1)
					t.Logf(
						"writer %d op %d commit error: %v",
						writerID, i, err,
					)
					_ = txn.Rollback()
				}
			}
		}(w)
	}

	wg.Wait()

	assert.Equal(
		t,
		int64(0),
		errors.Load(),
		"expected zero transaction errors",
	)
}

// TestReadWritePoolSeparation verifies that the file-based store
// has separate read and write database handles.
func TestReadWritePoolSeparation(t *testing.T) {
	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	// For file-based stores, ReadDB should be a different handle
	// than DB (write handle)
	assert.NotNil(t, store.DB(), "write DB should not be nil")
	assert.NotNil(t, store.ReadDB(), "read DB should not be nil")

	// The read and write handles should be different objects for
	// file-based stores
	writeDB := store.DB()
	readDB := store.ReadDB()
	assert.NotSame(
		t,
		writeDB,
		readDB,
		"file-based store should have separate read/write pools",
	)
}

// TestInMemorySharedPool verifies that the in-memory store shares
// a single pool for reads and writes.
func TestInMemorySharedPool(t *testing.T) {
	store, err := New("", nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	defer store.Close() //nolint:errcheck

	// For in-memory stores, ReadDB and DB should be the same
	assert.Same(
		t,
		store.DB(),
		store.ReadDB(),
		"in-memory store should share read/write pool",
	)
}

// TestHighContentionReadWrite exercises a high-contention scenario
// with many goroutines performing mixed read/write operations. This
// is a stress test to verify the fix under load.
func TestHighContentionReadWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	const (
		numGoroutines = 20
		opsPerRoutine = 50
	)

	var (
		busyErrors atomic.Int64
		otherErrs  atomic.Int64
		wg         sync.WaitGroup
	)

	// Seed data
	for i := range 5 {
		summary := &models.EpochSummary{
			Epoch:            uint64(i + 1),
			TotalActiveStake: 30000000000000000,
			TotalPoolCount:   3000,
			TotalDelegators:  1200000,
			BoundarySlot:     uint64(i * 43200),
			SnapshotReady:    true,
		}
		require.NoError(t, store.SaveEpochSummary(summary, nil))
	}

	for g := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range opsPerRoutine {
				var err error
				if id%2 == 0 {
					// Writer
					summary := &models.EpochSummary{
						Epoch: uint64(
							id*10000 + i + 100,
						),
						TotalActiveStake: 30000000000000000,
						TotalPoolCount:   3000,
						TotalDelegators:  1200000,
						BoundarySlot: uint64(
							id*10000 + i,
						),
						SnapshotReady: true,
					}
					err = store.SaveEpochSummary(
						summary, nil,
					)
				} else {
					// Reader
					_, err = store.GetEpochSummary(
						uint64(i%5+1), nil,
					)
				}
				if err != nil {
					errMsg := err.Error()
					if strings.Contains(
						errMsg,
						"SQLITE_BUSY",
					) || strings.Contains(
						errMsg,
						"database is locked",
					) {
						busyErrors.Add(1)
					} else {
						otherErrs.Add(1)
					}
					t.Logf(
						"goroutine %d op %d error: %v",
						id, i, err,
					)
				}
			}
		}(g)
	}

	wg.Wait()

	assert.Equal(
		t,
		int64(0),
		busyErrors.Load(),
		"expected zero SQLITE_BUSY errors",
	)
	assert.Equal(
		t,
		int64(0),
		otherErrs.Load(),
		"expected zero database errors",
	)
}

// TestWritePoolSingleConnection verifies that the write pool has
// exactly 1 max open connection for file-based databases.
func TestWritePoolSingleConnection(t *testing.T) {
	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	writeSqlDB, err := store.DB().DB()
	require.NoError(t, err)

	stats := writeSqlDB.Stats()
	assert.Equal(
		t,
		1,
		stats.MaxOpenConnections,
		"write pool should have exactly 1 max open connection",
	)
}

// TestReadPoolMultipleConnections verifies that the read pool
// allows multiple connections for file-based databases.
func TestReadPoolMultipleConnections(t *testing.T) {
	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	readSqlDB, err := store.ReadDB().DB()
	require.NoError(t, err)

	stats := readSqlDB.Stats()
	assert.Equal(
		t,
		DefaultMaxConnections,
		stats.MaxOpenConnections,
		"read pool should have DefaultMaxConnections open conns",
	)
}

// TestConcurrentReadsDontBlock verifies that many concurrent reads
// complete quickly and don't block each other.
func TestConcurrentReadsDontBlock(t *testing.T) {
	store := setupFileBasedStore(t)
	defer store.Close() //nolint:errcheck

	// Seed a small amount of data
	for i := range 10 {
		snapshot := &models.PoolStakeSnapshot{
			Epoch:          1,
			SnapshotType:   "go",
			PoolKeyHash:    []byte(fmt.Sprintf("pool_%028d", i)),
			TotalStake:     1000000,
			DelegatorCount: 100,
			CapturedSlot:   1000,
		}
		require.NoError(
			t,
			store.SavePoolStakeSnapshot(snapshot, nil),
		)
	}

	const numReaders = 10

	var (
		wg     sync.WaitGroup
		errors atomic.Int64
	)

	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_, err := store.GetPoolStakeSnapshotsByEpoch(
					1, "go", nil,
				)
				if err != nil {
					errors.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	// Verify no errors occurred - the key metric is zero
	// errors, not raw speed (race detector adds overhead).
	assert.Equal(
		t,
		int64(0),
		errors.Load(),
		"concurrent reads should not produce errors",
	)
}
