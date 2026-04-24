// Copyright 2025 Blink Labs Software
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
	"log/slog"

	"gorm.io/gorm"
)

type BlockNonce struct {
	Hash         []byte `gorm:"uniqueIndex:hash_slot;size:32"`
	Nonce        []byte
	ID           uint   `gorm:"primarykey"`
	Slot         uint64 `gorm:"uniqueIndex:hash_slot"`
	IsCheckpoint bool
}

// TableName overrides default table name
func (BlockNonce) TableName() string {
	return "block_nonce"
}

// MigrateBlockNonceUniqueIndex prepares the block_nonce table for the
// uniqueIndex:hash_slot tag introduced after the original non-unique
// index. AutoMigrate will not promote an existing non-unique index to
// unique, so an upgrading database keeps the legacy index and the new
// SetBlockNonce upsert (clause.OnConflict on hash+slot) fails because
// no matching unique constraint exists.
//
// This helper deduplicates any (hash, slot) collisions, then drops the
// legacy non-unique index so AutoMigrate can recreate it as unique.
// It is a no-op when the table does not exist or the existing index
// is already unique.
func MigrateBlockNonceUniqueIndex(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&BlockNonce{}) {
		return nil
	}

	indexes, err := db.Migrator().GetIndexes(&BlockNonce{})
	if err != nil {
		return fmt.Errorf("getting block_nonce indexes: %w", err)
	}
	legacyIndex := false
	for _, idx := range indexes {
		if idx.Name() != "hash_slot" {
			continue
		}
		unique, ok := idx.Unique()
		if !ok || !unique {
			legacyIndex = true
		}
		break
	}
	if !legacyIndex {
		return nil
	}

	type dupGroup struct {
		Hash []byte
		Slot uint64
	}
	var dups []dupGroup
	err = db.Raw(`
		SELECT hash, slot
		FROM block_nonce
		GROUP BY hash, slot
		HAVING COUNT(*) > 1
	`).Scan(&dups).Error
	if err != nil {
		return fmt.Errorf(
			"query duplicate block_nonce groups: %w", err,
		)
	}
	if len(dups) > 0 {
		logger.Info(
			"deduplicating block_nonce rows before creating unique index",
			"duplicate_groups", len(dups),
		)
		for _, d := range dups {
			// Prefer a checkpoint row when one exists so the
			// is_checkpoint flag is not silently dropped during
			// dedup; fall back to MAX(id) otherwise.
			var keepID uint
			err := db.Raw(`
				SELECT id FROM block_nonce
				WHERE hash = ? AND slot = ?
				ORDER BY is_checkpoint DESC, id DESC
				LIMIT 1
			`, d.Hash, d.Slot).Scan(&keepID).Error
			if err != nil {
				return fmt.Errorf(
					"select keep id for block_nonce (slot=%d): %w",
					d.Slot, err,
				)
			}
			err = db.Exec(`
				DELETE FROM block_nonce
				WHERE hash = ? AND slot = ? AND id != ?
			`, d.Hash, d.Slot, keepID).Error
			if err != nil {
				return fmt.Errorf(
					"delete duplicate block_nonce (slot=%d): %w",
					d.Slot, err,
				)
			}
		}
		logger.Info("block_nonce deduplication complete")
	}

	if err := db.Migrator().DropIndex(
		&BlockNonce{}, "hash_slot",
	); err != nil {
		return fmt.Errorf(
			"dropping legacy non-unique block_nonce hash_slot index: %w",
			err,
		)
	}
	return nil
}
