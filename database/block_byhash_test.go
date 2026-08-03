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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetBlockByHashStats zeros the hit/miss counters between tests.
func resetBlockByHashStats() {
	blockByHashIndexHits.Store(0)
	blockByHashIndexMisses.Store(0)
}

// TestBlockByHashTxn_UnknownHashRecordsMissAndNotFound verifies that an
// unknown hash increments the miss counter (so operators can track the
// index miss rate from #2105) and returns ErrBlockNotFound directly on
// the index miss, without any fallback scan.
func TestBlockByHashTxn_UnknownHashRecordsMissAndNotFound(t *testing.T) {
	db := newTestDB(t)
	resetBlockByHashStats()

	const seeded = 16
	for i := range seeded {
		insertTestBlock(t, db, uint64(i+1), randomHash(t), []byte("cbor"))
	}

	unknown := randomHash(t)
	_, err := BlockByHash(db, unknown)
	require.ErrorIs(t, err, models.ErrBlockNotFound,
		"unknown hash must surface as ErrBlockNotFound so fork-resolution can rotate peers")

	hits, misses := BlockByHashStats()
	assert.Equal(t, uint64(0), hits, "no hash-index hit expected for unknown hash")
	assert.Equal(t, uint64(1), misses,
		"miss counter must record the false-fallback so operators can track the back-fill rate (#2105)")
}

// TestBlockByHashTxn_KnownHashStillResolves guards the fast path: every
// block written via BlockCreate gets a hash-index entry (#1915), and a
// lookup must hit it in O(1) and return the block.
func TestBlockByHashTxn_KnownHashStillResolves(t *testing.T) {
	db := newTestDB(t)
	resetBlockByHashStats()

	hash := randomHash(t)
	insertTestBlock(t, db, 42, hash, []byte("payload"))

	got, err := BlockByHash(db, hash)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got.Slot)
	assert.Equal(t, hash, got.Hash)

	hits, misses := BlockByHashStats()
	assert.Equal(t, uint64(1), hits, "indexed lookup must take the fast path")
	assert.Equal(t, uint64(0), misses)
}

// TestBlockByHashTxn_EmptyIndexEntryIsCorruption asserts that a hash-
// index entry whose value is an empty byte slice surfaces a descriptive
// non-ErrBlockNotFound error rather than a soft miss. An empty value
// means the index was written but the pointer is invalid: a local DB
// problem the operator needs to see, not a fork-resolution miss.
func TestBlockByHashTxn_EmptyIndexEntryIsCorruption(t *testing.T) {
	db := newTestDB(t)
	resetBlockByHashStats()

	hash := randomHash(t)
	hashIndexKey := types.BlockHashIndexKey(hash)
	txn := db.BlobTxn(true)
	require.NoError(t, db.Blob().Set(txn.Blob(), hashIndexKey, []byte{}))
	require.NoError(t, txn.Commit())

	_, err := BlockByHash(db, hash)
	require.Error(t, err)
	assert.NotErrorIs(t, err, models.ErrBlockNotFound,
		"empty index entry must not be reported as a soft miss")
	assert.True(t,
		strings.Contains(err.Error(), "empty block hash index entry"),
		"error should identify corruption: got %v", err)

	hits, misses := BlockByHashStats()
	assert.Equal(t, uint64(0), hits)
	assert.Equal(t, uint64(0), misses,
		"corruption must not be folded into the miss counter")
}

// TestRegisterBlockByHashMetrics_PerRegistry verifies that every registry
// passed to RegisterBlockByHashMetrics exposes the hash-index counters, not
// just the first one in the process, and that reusing a registry is a no-op.
func TestRegisterBlockByHashMetrics_PerRegistry(t *testing.T) {
	resetBlockByHashStats()
	blockByHashIndexMisses.Add(3)

	for i := range 2 {
		reg := prometheus.NewRegistry()
		require.NoError(t, RegisterBlockByHashMetrics(reg))
		// reuse must not error or duplicate
		require.NoError(t, RegisterBlockByHashMetrics(reg))

		families, err := reg.Gather()
		require.NoError(t, err)
		found := map[string]float64{}
		for _, mf := range families {
			for _, m := range mf.GetMetric() {
				found[mf.GetName()] = m.GetCounter().GetValue()
			}
		}
		assert.Contains(t, found,
			"dingo_database_block_hash_index_hits_total",
			"registry %d must expose the hit counter", i)
		assert.Equal(t, float64(3),
			found["dingo_database_block_hash_index_misses_total"],
			"registry %d must read the shared miss total", i)
	}
	resetBlockByHashStats()
}

// BenchmarkBlockByHashTxn_UnknownHash measures the cost of the fork-
// resolution miss path on a small DB.
//
// Run with: go test -bench=BenchmarkBlockByHashTxn -benchmem ./database/
func BenchmarkBlockByHashTxn_UnknownHash(b *testing.B) {
	db := newBenchDB(b)
	const seeded = 1024
	for i := range seeded {
		hash := make([]byte, 32)
		hash[0] = byte(i)
		hash[1] = byte(i >> 8)
		block := models.Block{
			Slot: uint64(i + 1),
			Hash: hash,
			Cbor: []byte("cbor"),
			Type: 1,
		}
		require.NoError(b, db.BlockCreate(block, nil))
	}

	unknown := make([]byte, 32)
	for i := range unknown {
		unknown[i] = 0xFF
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = BlockByHash(db, unknown)
	}
}

func newBenchDB(b *testing.B) *Database {
	b.Helper()
	cfg := &Config{DataDir: ""}
	db, err := newTestDatabase(b, cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	return db
}
