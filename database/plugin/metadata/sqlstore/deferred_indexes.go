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

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
)

var _ metadata.DeferredIndexManager = (*Store)(nil)

// DropDeferredIndexes records the durable recovery marker and drops the
// manifest in one SQLite transaction.
func (s *Store) DropDeferredIndexes() error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	return s.withWriteTransaction(
		context.Background(),
		nil,
		func(db queryer) error {
			if _, err := db.ExecContext(
				context.Background(),
				`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
				 ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`,
				deferred.SyncStateKey,
				deferred.SyncStateValue,
			); err != nil {
				return fmt.Errorf("mark deferred indexes pending: %w", err)
			}
			for _, index := range deferred.Manifest {
				if !s.dialect.CanDropIndex(index.Name, index.Table) {
					continue
				}
				exists, err := s.deferredIndexExists(db, index)
				if err != nil {
					return fmt.Errorf(
						"check deferred index %s: %w",
						index.Name,
						err,
					)
				}
				if !exists {
					continue
				}
				statement := s.dialect.DropIndexSQL(index.Name, index.Table)
				if _, err := db.ExecContext(
					context.Background(),
					statement,
				); err != nil {
					// InnoDB requires an index for every foreign-key child
					// column. Those indexes cannot be dropped independently;
					// keep them in place while deferring the rest of the
					// manifest and let BuildDeferredIndexes treat them as
					// already present.
					if s.dialect.Name() == "mysql" &&
						isMySQLForeignKeyIndexError(err) {
						continue
					}
					return fmt.Errorf(
						"drop deferred index %s: %w",
						index.Name,
						err,
					)
				}
			}
			return nil
		},
	)
}

func isMySQLForeignKeyIndexError(err error) bool {
	// Keep this backend-specific fallback narrow to the invariant message
	// emitted for an index required by an InnoDB FK.  Do not import the MySQL
	// driver here: sqlstore is part of the default SQLite build and optional
	// drivers must remain behind dingo_extra_plugins.
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "1553") &&
		strings.Contains(message, "foreign key constraint")
}

// BuildCriticalDeferredIndexes restores the indexes needed before API and
// rollback traffic begins. The recovery marker stays set until the full
// manifest is restored.
func (s *Store) BuildCriticalDeferredIndexes() error {
	return s.buildDeferredIndexes(deferred.CriticalManifest(), false)
}

// BuildDeferredIndexes restores the full manifest and clears the durable
// recovery marker in the same transaction.
func (s *Store) BuildDeferredIndexes() error {
	return s.buildDeferredIndexes(deferred.Manifest, true)
}

func (s *Store) buildDeferredIndexes(
	indexes []deferred.Index,
	clearPending bool,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	return s.withWriteTransaction(
		context.Background(),
		nil,
		func(db queryer) error {
			for _, index := range indexes {
				exists, err := s.deferredIndexExists(db, index)
				if err != nil {
					return fmt.Errorf(
						"check deferred index %s: %w",
						index.Name,
						err,
					)
				}
				if exists {
					continue
				}
				statement := s.dialect.CreateIndexSQL(
					index.Name,
					index.Table,
					index.Columns,
				)
				if _, err := db.ExecContext(
					context.Background(),
					statement,
				); err != nil {
					return fmt.Errorf(
						"build deferred index %s: %w",
						index.Name,
						err,
					)
				}
			}
			if clearPending {
				if _, err := db.ExecContext(
					context.Background(),
					"DELETE FROM sync_state WHERE sync_key = ?",
					deferred.SyncStateKey,
				); err != nil {
					return fmt.Errorf(
						"clear deferred indexes marker: %w",
						err,
					)
				}
			}
			return nil
		},
	)
}

func (s *Store) deferredIndexExists(
	db queryer,
	index deferred.Index,
) (bool, error) {
	var found int
	var query string
	var args []any
	switch s.dialect.Name() {
	case "mysql":
		query = `SELECT 1 FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? LIMIT 1`
		args = []any{index.Table, index.Name}
	case "postgres":
		query = `SELECT 1 FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = ? AND indexname = ? LIMIT 1`
		args = []any{index.Table, index.Name}
	default:
		query = `SELECT 1 FROM sqlite_master
WHERE type = 'index' AND name = ? LIMIT 1`
		args = []any{index.Name}
	}
	err := db.QueryRowContext(context.Background(), query, args...).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return found != 0, err
}

// HasDeferredIndexesPending reports whether a prior drop/rebuild cycle still
// owns the durable recovery marker.
func (s *Store) HasDeferredIndexesPending() (bool, error) {
	if err := s.ensureReady(); err != nil {
		return false, err
	}
	var value string
	err := newDialectQueryer(s.readDB, s.dialect.Name()).QueryRowContext(
		context.Background(),
		"SELECT value FROM sync_state WHERE sync_key = ?",
		deferred.SyncStateKey,
	).Scan(&value)
	if err != nil {
		if errors.Is(normalizeNoRows(err), metadata.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("read deferred indexes marker: %w", err)
	}
	return value != "", nil
}
