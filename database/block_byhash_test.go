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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockByHashTxn_UnknownHashIsHardMiss verifies that looking up an
// unknown hash returns ErrBlockNotFound without exercising the legacy
// iterator-fallback prefix scan. This is the regression guard for
// blinklabs-io/dingo#2105, fork-resolution probes one ancestor per
// peer per depth, and pre-fix every miss ran a full block-blob prefix
// scan, dominating catch-up CPU.
func TestBlockByHashTxn_UnknownHashIsHardMiss(t *testing.T) {
	db := newTestDB(t)
	resetBlockByHashStats()

	// Seed enough blocks to make a prefix scan obviously expensive if
	// it were still in place.
	const seeded = 64
	for i := 0; i < seeded; i++ {
		insertTestBlock(t, db, uint64(i+1), randomHash(t), []byte("cbor"))
	}

	unknown := randomHash(t)
	_, err := BlockByHash(db, unknown)
	require.ErrorIs(t, err, models.ErrBlockNotFound,
		"unknown hash must surface as ErrBlockNotFound so fork-resolution "+
			"can rotate peers without scanning the whole block blob")

	hits, misses := BlockByHashStats()
	assert.Equal(t, uint64(0), hits, "no hash-index hit expected for unknown hash")
	assert.Equal(t, uint64(1), misses,
		"miss counter must record the false-fallback so operators can "+
			"track the back-fill rate (issue #2105 ask)")
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

// BenchmarkBlockByHashTxn_UnknownHash measures the cost of the fork-
// resolution miss path. Pre-fix this benchmark scaled with the number of
// seeded blocks (full prefix iterator); post-fix it is constant-time.
//
// Run with: go test -bench=BenchmarkBlockByHashTxn -benchmem ./database/
func BenchmarkBlockByHashTxn_UnknownHash(b *testing.B) {
	db := newBenchDB(b)
	const seeded = 1024
	for i := 0; i < seeded; i++ {
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
	db, err := New(cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	return db
}
