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

package models

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// legacyBlockNonce mirrors the original BlockNonce schema where the
// hash_slot index was non-unique. The TableName override matches the
// production model so the migration helper sees the same table.
type legacyBlockNonce struct {
	Hash         []byte `gorm:"index:hash_slot;size:32"`
	Nonce        []byte
	ID           uint   `gorm:"primarykey"`
	Slot         uint64 `gorm:"index:hash_slot"`
	IsCheckpoint bool
}

func (legacyBlockNonce) TableName() string {
	return "block_nonce"
}

func openMemoryDB(t *testing.T) *gorm.DB {
	t.Helper()
	// A unique DSN per test prevents state leaking between sibling
	// tests through the SQLite shared-cache in-memory database.
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:mem_%s?mode=memory&cache=shared", name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	return db
}

func TestMigrateBlockNonceUniqueIndex_NoTable(t *testing.T) {
	db := openMemoryDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, MigrateBlockNonceUniqueIndex(db, logger))
}

func TestMigrateBlockNonceUniqueIndex_PromotesLegacyIndex(t *testing.T) {
	db := openMemoryDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	require.NoError(t, db.AutoMigrate(&legacyBlockNonce{}))

	hashA := make([]byte, 32)
	hashA[0] = 0xAA
	hashB := make([]byte, 32)
	hashB[0] = 0xBB
	require.NoError(t, db.Create(&legacyBlockNonce{
		Hash: hashA, Slot: 1, Nonce: []byte{0x01},
	}).Error)
	require.NoError(t, db.Create(&legacyBlockNonce{
		Hash: hashA, Slot: 1, Nonce: []byte{0x02}, IsCheckpoint: true,
	}).Error)
	require.NoError(t, db.Create(&legacyBlockNonce{
		Hash: hashB, Slot: 2, Nonce: []byte{0x03},
	}).Error)

	require.NoError(t, MigrateBlockNonceUniqueIndex(db, logger))

	// Dedup prefers an IsCheckpoint row first, falling back to the
	// highest-id row when no checkpoint flag is set on any duplicate.
	var rows []BlockNonce
	require.NoError(t, db.Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	for _, r := range rows {
		switch r.Slot {
		case 1:
			assert.Equal(t, hashA, r.Hash)
			assert.Equal(t, []byte{0x02}, r.Nonce)
			assert.True(t, r.IsCheckpoint)
		case 2:
			assert.Equal(t, hashB, r.Hash)
		default:
			t.Fatalf("unexpected slot in remaining rows: %d", r.Slot)
		}
	}

	// The migration helper drops the legacy index. AutoMigrate then
	// recreates it as the unique index declared on BlockNonce, which
	// is the contract SetBlockNonce's clause.OnConflict relies on.
	require.NoError(t, db.AutoMigrate(&BlockNonce{}))
	indexes, err := db.Migrator().GetIndexes(&BlockNonce{})
	require.NoError(t, err)
	var found bool
	for _, idx := range indexes {
		if idx.Name() != "hash_slot" {
			continue
		}
		found = true
		unique, ok := idx.Unique()
		require.True(t, ok, "driver did not report uniqueness")
		assert.True(t, unique, "hash_slot must be unique after migration")
	}
	require.True(t, found, "hash_slot index missing after AutoMigrate")
}

func TestMigrateBlockNonceUniqueIndex_AlreadyUnique(t *testing.T) {
	db := openMemoryDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	require.NoError(t, db.AutoMigrate(&BlockNonce{}))
	// Second invocation must be a no-op when the index is already unique.
	require.NoError(t, MigrateBlockNonceUniqueIndex(db, logger))
	indexes, err := db.Migrator().GetIndexes(&BlockNonce{})
	require.NoError(t, err)
	var found bool
	for _, idx := range indexes {
		if idx.Name() != "hash_slot" {
			continue
		}
		found = true
		unique, ok := idx.Unique()
		require.True(t, ok)
		assert.True(t, unique)
	}
	require.True(t, found, "hash_slot index should still be present")
}
