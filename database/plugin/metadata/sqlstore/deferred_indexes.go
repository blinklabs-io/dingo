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
				statement := "DROP INDEX IF EXISTS " +
					s.dialect.QuoteIdentifier(index.Name)
				if _, err := db.ExecContext(
					context.Background(),
					statement,
				); err != nil {
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
				columns := make([]string, len(index.Columns))
				for i, column := range index.Columns {
					columns[i] = s.dialect.QuoteIdentifier(column)
				}
				statement := "CREATE INDEX IF NOT EXISTS " +
					s.dialect.QuoteIdentifier(index.Name) +
					" ON " + s.dialect.QuoteIdentifier(index.Table) +
					" (" + strings.Join(columns, ", ") + ")"
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
