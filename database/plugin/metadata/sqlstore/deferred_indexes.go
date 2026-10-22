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
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
)

var (
	_ metadata.DeferredIndexManager                      = (*Store)(nil)
	_ metadata.ContextDeferredIndexDropper               = (*Store)(nil)
	_ metadata.LazyDeferredIndexDropper                  = (*Store)(nil)
	_ metadata.CatchUpIndexManager                       = (*Store)(nil)
	_ metadata.CatchUpPragmaTuner                        = (*Store)(nil)
	_ metadata.MissingCriticalDeferredIndexLister        = (*Store)(nil)
	_ metadata.ContextMissingCriticalDeferredIndexLister = (*Store)(nil)
	_ metadata.MissingDeferredIndexLister                = (*Store)(nil)
	_ metadata.ContextDeferredIndexBuilder               = (*Store)(nil)
	_ metadata.ContextCriticalDeferredIndexBuilder       = (*Store)(nil)
	_ metadata.DeferredIndexProgressBuilder              = (*Store)(nil)
	_ metadata.CriticalDeferredIndexProgressBuilder      = (*Store)(nil)
)

// withDeferredIndexWriteContext runs fn in one write transaction, with every
// deferred.Retained index guaranteed resident before fn sees the database.
//
// Every entry point in this file returns with the store able to serve writes,
// and SetTransaction's per-row idempotency deletes are full scans of a growing
// table without those indexes. An older binary's manifest may have dropped one
// that this manifest keeps out; the versioned migration that created it is
// recorded complete, so restoring it here is its only way back. Enforcing that
// once, on the path every drop and rebuild takes, is what keeps a rebuild path
// from being added without it.
func (s *Store) withDeferredIndexWriteContext(
	ctx context.Context,
	fn func(db queryer, ctx context.Context) error,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	return s.withWriteTransactionContext(
		ctx,
		nil,
		func(db queryer, ctx context.Context) error {
			if err := s.ensureRetainedIndexes(ctx, db); err != nil {
				return err
			}
			return fn(db, ctx)
		},
	)
}

// DropDeferredIndexes records the durable recovery marker and drops the
// manifest in one SQLite transaction.
func (s *Store) DropDeferredIndexes() error {
	return s.DropDeferredIndexesContext(context.Background())
}

// DropDeferredIndexesContext is DropDeferredIndexes bound to ctx.
func (s *Store) DropDeferredIndexesContext(ctx context.Context) error {
	return s.dropDeferredIndexes(ctx, deferred.Manifest)
}

// DropLazyDeferredIndexesContext records the recovery marker and drops only
// the non-critical manifest entries, in one transaction.
func (s *Store) DropLazyDeferredIndexesContext(ctx context.Context) error {
	return s.dropDeferredIndexes(ctx, deferred.LazyManifest())
}

// DropCatchUpIndexesContext records the recovery marker and drops the lazy
// and catch-up droppable manifest entries, in one transaction.
func (s *Store) DropCatchUpIndexesContext(ctx context.Context) error {
	return s.dropDeferredIndexes(
		ctx,
		append(deferred.LazyManifest(), deferred.CatchUpDroppableManifest()...),
	)
}

// BuildCatchUpIndexesContext rebuilds any missing catch-up droppable entry and
// leaves the recovery marker for the full rebuild to clear.
func (s *Store) BuildCatchUpIndexesContext(ctx context.Context) error {
	return s.buildDeferredIndexes(
		ctx, deferred.CatchUpDroppableManifest(), false,
	)
}

func (s *Store) dropDeferredIndexes(
	ctx context.Context,
	indexes []deferred.Index,
) error {
	return s.withDeferredIndexWriteContext(
		ctx,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(
				ctx,
				`-- name: UpsertSyncState :exec
				 INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
				 ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`,
				deferred.SyncStateKey,
				deferred.SyncStateValue,
			); err != nil {
				return fmt.Errorf("mark deferred indexes pending: %w", err)
			}
			for _, index := range indexes {
				if !s.dialect.CanDropIndex(index.Name, index.Table) {
					continue
				}
				exists, err := s.deferredIndexExists(ctx, db, index)
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
					ctx,
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
	return s.BuildCriticalDeferredIndexesContext(context.Background())
}

// BuildCriticalDeferredIndexesContext is BuildCriticalDeferredIndexes bound
// to ctx for cancellable startup repair.
func (s *Store) BuildCriticalDeferredIndexesContext(
	ctx context.Context,
) error {
	return s.buildDeferredIndexes(ctx, deferred.CriticalManifest(), false)
}

// BuildCriticalDeferredIndexesContextWithProgress is
// BuildCriticalDeferredIndexesContext with per-index progress callbacks.
func (s *Store) BuildCriticalDeferredIndexesContextWithProgress(
	ctx context.Context,
	before func(string),
	after func(string, time.Duration),
) error {
	return s.buildDeferredIndexesWithProgress(
		ctx, deferred.CriticalManifest(), false, before, after,
	)
}

// BuildDeferredIndexes restores the full manifest and clears the durable
// recovery marker in the same transaction.
func (s *Store) BuildDeferredIndexes() error {
	return s.buildDeferredIndexes(
		context.Background(),
		deferred.Manifest,
		true,
	)
}

// BuildDeferredIndexesContext is BuildDeferredIndexes bound to ctx. Restore
// runs the full manifest rebuild inside a cancellable operation, and an
// uninterruptible build there would outlive the cancellation by however long
// the largest index takes.
func (s *Store) BuildDeferredIndexesContext(ctx context.Context) error {
	return s.buildDeferredIndexes(ctx, deferred.Manifest, true)
}

// BuildDeferredIndexesContextWithProgress is the cancellable full-manifest
// rebuild used by restore so operators can see which index is taking time.
func (s *Store) BuildDeferredIndexesContextWithProgress(
	ctx context.Context,
	before func(string),
	after func(string, time.Duration),
) error {
	return s.buildDeferredIndexesWithProgress(
		ctx, deferred.Manifest, true, before, after,
	)
}

func (s *Store) buildDeferredIndexes(
	buildCtx context.Context,
	indexes []deferred.Index,
	clearPending bool,
) error {
	return s.buildDeferredIndexesWithProgress(
		buildCtx, indexes, clearPending, nil, nil,
	)
}

func (s *Store) buildDeferredIndexesWithProgress(
	buildCtx context.Context,
	indexes []deferred.Index,
	clearPending bool,
	before func(string),
	after func(string, time.Duration),
) error {
	if s.dialect.Name() == "sqlite" {
		// SQLite runs wal_autocheckpoint at commit, so one transaction around
		// all CREATE INDEX statements would defer it until the rebuild ends.
		var missing []deferred.Index
		err := s.withDeferredIndexWriteContext(
			buildCtx,
			func(db queryer, ctx context.Context) error {
				var err error
				missing, err = s.missingDeferredIndexes(ctx, db, indexes)
				if err != nil {
					return err
				}
				if len(missing) == 0 && clearPending {
					if _, err := db.ExecContext(
						ctx,
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
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return s.buildSQLiteDeferredIndexesWithProgress(
				buildCtx, missing, indexes, clearPending, before, after,
			)
		}
		return nil
	}
	return s.withDeferredIndexWriteContext(
		buildCtx,
		func(db queryer, ctx context.Context) error {
			for _, index := range indexes {
				exists, err := s.deferredIndexExists(ctx, db, index)
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
				if before != nil {
					before(index.Name)
				}
				started := time.Now()
				statement := deferredIndexCreateSQL(s.dialect, index)
				if _, err := db.ExecContext(
					ctx,
					statement,
				); err != nil {
					return fmt.Errorf(
						"build deferred index %s: %w",
						index.Name,
						err,
					)
				}
				if after != nil {
					after(index.Name, time.Since(started))
				}
			}
			if clearPending {
				if _, err := db.ExecContext(
					ctx,
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

func (s *Store) buildSQLiteDeferredIndexesWithProgress(
	ctx context.Context,
	indexesToBuild []deferred.Index,
	requestedIndexes []deferred.Index,
	clearPending bool,
	before func(string),
	after func(string, time.Duration),
) error {
	for {
		for _, index := range indexesToBuild {
			var started time.Time
			built := false
			err := s.withDeferredIndexWriteContext(
				ctx,
				func(db queryer, txCtx context.Context) error {
					exists, err := s.deferredIndexExists(
						txCtx, db, index,
					)
					if err != nil {
						return fmt.Errorf(
							"check deferred index %s: %w",
							index.Name,
							err,
						)
					}
					if exists {
						return nil
					}
					if before != nil {
						before(index.Name)
					}
					statement := deferredIndexCreateSQL(s.dialect, index)
					started = time.Now()
					if _, err := db.ExecContext(
						txCtx,
						statement,
					); err != nil {
						return fmt.Errorf(
							"build deferred index %s: %w",
							index.Name,
							err,
						)
					}
					built = true
					return nil
				},
			)
			if err != nil {
				return err
			}
			if built && after != nil {
				after(index.Name, time.Since(started))
			}
		}

		complete := true
		missing := make([]deferred.Index, 0)
		err := s.withDeferredIndexWriteContext(
			ctx,
			func(db queryer, txCtx context.Context) error {
				var err error
				missing, err = s.missingDeferredIndexes(
					txCtx, db, requestedIndexes,
				)
				if err != nil {
					return err
				}
				complete = len(missing) == 0
				if complete && clearPending {
					if _, err := db.ExecContext(
						txCtx,
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
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		indexesToBuild = missing
	}
}

func (s *Store) missingDeferredIndexes(
	ctx context.Context,
	db queryer,
	indexes []deferred.Index,
) ([]deferred.Index, error) {
	var missing []deferred.Index
	for _, index := range indexes {
		exists, err := s.deferredIndexExists(ctx, db, index)
		if err != nil {
			return nil, fmt.Errorf(
				"check deferred index %s: %w",
				index.Name,
				err,
			)
		}
		if !exists {
			missing = append(missing, index)
		}
	}
	return missing, nil
}

// ensureRetainedIndexes creates any deferred.Retained index missing from the
// schema. Present indexes cost one catalog lookup each and are left alone.
func (s *Store) ensureRetainedIndexes(
	ctx context.Context,
	db queryer,
) error {
	for _, index := range deferred.Retained {
		exists, err := s.deferredIndexExists(ctx, db, index)
		if err != nil {
			return fmt.Errorf(
				"check retained index %s: %w",
				index.Name,
				err,
			)
		}
		if exists {
			continue
		}
		statement := deferredIndexCreateSQL(s.dialect, index)
		if _, err := db.ExecContext(
			ctx,
			statement,
		); err != nil {
			return fmt.Errorf(
				"restore retained index %s: %w",
				index.Name,
				err,
			)
		}
	}
	return nil
}

func (s *Store) deferredIndexExists(
	ctx context.Context,
	db queryer,
	index deferred.Index,
) (bool, error) {
	var found int
	var query string
	var args []any
	switch s.dialect.Name() {
	case "mysql":
		query = `-- name: MySQLIndexExists :one
SELECT 1 FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? LIMIT 1`
		args = []any{index.Table, index.Name}
	case "postgres":
		query = `-- name: PostgresIndexExists :one
SELECT 1 FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = ? AND indexname = ? LIMIT 1`
		args = []any{index.Table, index.Name}
	default:
		query = `-- name: SQLiteIndexExists :one
SELECT 1 FROM sqlite_master
WHERE type = 'index' AND name = ? LIMIT 1`
		args = []any{index.Name}
	}
	err := db.QueryRowContext(ctx, query, args...).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return found != 0, err
}

// MissingCriticalDeferredIndexes reports the Critical=true manifest entries
// absent from the schema, in manifest order, using the read connection and no
// DDL. Callers use it to name the indexes a rebuild is about to build before
// the rebuild starts.
func (s *Store) MissingCriticalDeferredIndexes() ([]string, error) {
	return s.MissingCriticalDeferredIndexesContext(context.Background())
}

// MissingCriticalDeferredIndexesContext reports absent critical entries using
// the caller's cancellation context and no DDL.
func (s *Store) MissingCriticalDeferredIndexesContext(
	ctx context.Context,
) ([]string, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	db := s.instrumentedQueryer(s.readDB)
	var missing []string
	for _, index := range deferred.CriticalManifest() {
		exists, err := s.deferredIndexExists(ctx, db, index)
		if err != nil {
			return nil, fmt.Errorf(
				"check deferred index %s: %w",
				index.Name,
				err,
			)
		}
		if !exists {
			missing = append(missing, index.Name)
		}
	}
	return missing, nil
}

// MissingDeferredIndexes reports every manifest entry absent from the schema,
// in manifest order, using the read connection and no DDL. The full-manifest
// repair paths use it to name what a rebuild is about to build before it
// starts; the rebuild itself is silent while it runs.
func (s *Store) MissingDeferredIndexes() ([]string, error) {
	return s.MissingDeferredIndexesContext(context.Background())
}

// MissingDeferredIndexesContext reports every manifest entry absent from the
// schema using the caller's cancellation context and no DDL.
func (s *Store) MissingDeferredIndexesContext(
	ctx context.Context,
) ([]string, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	db := s.instrumentedQueryer(s.readDB)
	var missing []string
	for _, index := range deferred.Manifest {
		exists, err := s.deferredIndexExists(ctx, db, index)
		if err != nil {
			return nil, fmt.Errorf(
				"check deferred index %s: %w",
				index.Name,
				err,
			)
		}
		if !exists {
			missing = append(missing, index.Name)
		}
	}
	return missing, nil
}

// HasDeferredIndexesPending reports whether a prior drop/rebuild cycle still
// owns the durable recovery marker.
func (s *Store) HasDeferredIndexesPending() (bool, error) {
	if err := s.ensureReady(); err != nil {
		return false, err
	}
	var value string
	err := s.instrumentedQueryer(s.readDB).QueryRowContext(
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

// deferredIndexCreateSQL builds the CREATE INDEX for a manifest or retained
// entry, keeping a partial entry partial on the engines that support it.
func deferredIndexCreateSQL(d Dialect, index deferred.Index) string {
	statement := d.CreateIndexSQL(index.Name, index.Table, index.Columns)
	if index.Where == "" || d.Name() == "mysql" {
		return statement
	}
	return statement + " WHERE " + index.Where
}
