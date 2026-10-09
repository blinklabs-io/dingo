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

package lifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	snapshotCatalogFileName = ".dingo-snapshot-catalog-v1.sqlite"
	// SnapshotCatalogPageLimit bounds index rows and manifests materialized by
	// one page request.
	SnapshotCatalogPageLimit = 100
)

const (
	firstSnapshotPageQuery = `SELECT id, created_sec, created_ns FROM snapshots
ORDER BY created_sec DESC, created_ns DESC, id DESC LIMIT ?`
	nextSnapshotPageQuery = `SELECT id, created_sec, created_ns FROM snapshots
WHERE (created_sec, created_ns, id) < (?, ?, ?)
ORDER BY created_sec DESC, created_ns DESC, id DESC LIMIT ?`
)

var (
	snapshotCatalogMu sync.Mutex
	// ErrSnapshotCatalogChanged means a page token names a prior catalog
	// generation. The caller must restart pagination from the first page.
	ErrSnapshotCatalogChanged = errors.New("snapshot catalog changed")
	// ErrSnapshotCatalogIncomplete means the catalog was rebuilt with every
	// readable snapshot while one or more directory entries were unreadable.
	ErrSnapshotCatalogIncomplete = errors.New("snapshot catalog incomplete")
	// ErrSnapshotCatalogCorrupt means the persistent catalog contains data
	// that cannot represent a snapshot below its configured root.
	ErrSnapshotCatalogCorrupt = errors.New("snapshot catalog corrupt")
)

// SnapshotCatalogCursor identifies the last row in one immutable catalog
// generation. The next page starts after this CreatedAt and ID tuple.
type SnapshotCatalogCursor struct {
	Generation uint64
	CreatedSec int64
	CreatedNS  int
	ID         string
}

func snapshotCatalogPath(baseDir string) string {
	return filepath.Join(baseDir, snapshotCatalogFileName)
}

func validateSnapshotCatalogID(id string) error {
	if id == "" || id == "." || id == ".." || id != filepath.Base(id) {
		return fmt.Errorf("%w: invalid snapshot ID %q", ErrSnapshotCatalogCorrupt, id)
	}
	return nil
}

func openSnapshotCatalog(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func createSnapshotCatalogSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE catalog_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation INTEGER NOT NULL CHECK (generation > 0)
);
CREATE TABLE snapshots (
    id TEXT PRIMARY KEY,
    created_sec INTEGER NOT NULL,
    created_ns INTEGER NOT NULL CHECK (created_ns >= 0 AND created_ns < 1000000000)
);
CREATE INDEX snapshots_created
    ON snapshots (created_sec DESC, created_ns DESC, id DESC);
`)
	return err
}

type catalogQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readCatalogGeneration(query catalogQuerier) (uint64, error) {
	var generation int64
	if err := query.QueryRowContext(
		context.Background(),
		"SELECT generation FROM catalog_meta WHERE singleton = 1",
	).Scan(&generation); err != nil {
		return 0, err
	}
	if generation <= 0 {
		return 0, errors.New("invalid snapshot catalog generation")
	}
	return uint64(generation), nil
}

// EnsureSnapshotCatalog atomically rebuilds the persistent local snapshot
// index. Callers run it during service initialization, outside request
// handling. It imports snapshot directories created while the service was not
// running.
func EnsureSnapshotCatalog(baseDir string, opts ...ManifestOption) error {
	snapshotCatalogMu.Lock()
	defer snapshotCatalogMu.Unlock()
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create snapshot catalog directory: %w", err)
	}
	entries, listErr := ListSnapshots(baseDir, opts...)
	if listErr != nil && entries == nil {
		return listErr
	}
	generation := uint64(1)
	catalogPath := snapshotCatalogPath(baseDir)
	if _, err := os.Stat(catalogPath); err == nil {
		current, err := openSnapshotCatalog(catalogPath)
		if err != nil {
			return fmt.Errorf("open existing snapshot catalog: %w", err)
		}
		oldGeneration, err := readCatalogGeneration(current)
		closeErr := current.Close()
		if err != nil {
			return fmt.Errorf("read existing snapshot catalog: %w", err)
		}
		if closeErr != nil {
			return closeErr
		}
		if oldGeneration == math.MaxInt64 {
			return errors.New("snapshot catalog generation overflow")
		}
		generation = oldGeneration + 1
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(baseDir, snapshotCatalogFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create snapshot catalog: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	defer os.Remove(tmpPath) //nolint:errcheck
	db, err := openSnapshotCatalog(tmpPath)
	if err != nil {
		return err
	}
	if err := createSnapshotCatalogSchema(db); err != nil {
		_ = db.Close()
		return err
	}
	txn, err := db.Begin()
	if err != nil {
		_ = db.Close()
		return err
	}
	defer txn.Rollback() //nolint:errcheck
	if _, err := txn.Exec(
		"INSERT INTO catalog_meta(singleton, generation) VALUES (1, ?)",
		generation,
	); err != nil {
		_ = db.Close()
		return err
	}
	for _, entry := range entries {
		if err := validateSnapshotCatalogID(entry.ID); err != nil {
			_ = db.Close()
			return err
		}
		if _, err := txn.Exec(
			"INSERT INTO snapshots(id, created_sec, created_ns) VALUES (?, ?, ?)",
			entry.ID, entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond(),
		); err != nil {
			_ = db.Close()
			return err
		}
	}
	if err := txn.Commit(); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, catalogPath); err != nil {
		return err
	}
	if err := syncDir(baseDir); err != nil {
		return err
	}
	if listErr != nil {
		return fmt.Errorf("%w: %v", ErrSnapshotCatalogIncomplete, listErr)
	}
	return nil
}

func mutateSnapshotCatalog(
	baseDir string,
	mutate func(*sql.Tx) (bool, error),
) error {
	snapshotCatalogMu.Lock()
	defer snapshotCatalogMu.Unlock()
	catalogPath := snapshotCatalogPath(baseDir)
	if _, err := os.Stat(catalogPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := openSnapshotCatalog(catalogPath)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback() //nolint:errcheck
	changed, err := mutate(txn)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	generation, err := readCatalogGeneration(txn)
	if err != nil {
		return err
	}
	if generation == math.MaxInt64 {
		return errors.New("snapshot catalog generation overflow")
	}
	if _, err := txn.Exec(
		"UPDATE catalog_meta SET generation = ? WHERE singleton = 1",
		generation+1,
	); err != nil {
		return err
	}
	return txn.Commit()
}

func updateSnapshotCatalogIfPresent(baseDir string, entry SnapshotEntry) error {
	if err := validateSnapshotCatalogID(entry.ID); err != nil {
		return err
	}
	return mutateSnapshotCatalog(baseDir, func(txn *sql.Tx) (bool, error) {
		_, err := txn.Exec(`
INSERT INTO snapshots(id, created_sec, created_ns) VALUES (?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns`,
			entry.ID, entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond(),
		)
		return err == nil, err
	})
}

func removeSnapshotCatalogEntryIfPresent(baseDir, id string) error {
	if err := validateSnapshotCatalogID(id); err != nil {
		return err
	}
	return mutateSnapshotCatalog(baseDir, func(txn *sql.Tx) (bool, error) {
		result, err := txn.Exec("DELETE FROM snapshots WHERE id = ?", id)
		if err != nil {
			return false, err
		}
		changed, err := result.RowsAffected()
		return changed > 0, err
	})
}

// RemoveSnapshot removes a local snapshot directory and its persistent
// catalog entry.
func RemoveSnapshot(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return removeSnapshotCatalogEntryIfPresent(
		filepath.Dir(dir), filepath.Base(dir),
	)
}

// ListSnapshotPage reads at most pageSize+1 index rows and pageSize manifests.
// Entries preserve ListSnapshots' newest-first ordering.
func ListSnapshotPage(
	baseDir string,
	pageSize int,
	cursor *SnapshotCatalogCursor,
	opts ...ManifestOption,
) (entries []SnapshotEntry, next *SnapshotCatalogCursor, err error) {
	if pageSize <= 0 || pageSize > SnapshotCatalogPageLimit {
		return nil, nil, fmt.Errorf("invalid snapshot page size %d", pageSize)
	}
	db, err := openSnapshotCatalog(snapshotCatalogPath(baseDir))
	if err != nil {
		return nil, nil, err
	}
	defer db.Close() //nolint:errcheck
	txn, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Rollback() //nolint:errcheck
	generation, err := readCatalogGeneration(txn)
	if err != nil {
		return nil, nil, err
	}
	if cursor != nil && cursor.Generation != generation {
		return nil, nil, ErrSnapshotCatalogChanged
	}
	query := firstSnapshotPageQuery
	args := []any{pageSize + 1}
	if cursor != nil {
		if cursor.ID == "" || cursor.CreatedNS < 0 || cursor.CreatedNS >= int(time.Second) {
			return nil, nil, errors.New("invalid snapshot catalog cursor")
		}
		query = nextSnapshotPageQuery
		args = []any{
			cursor.CreatedSec, cursor.CreatedNS, cursor.ID,
			pageSize + 1,
		}
	}
	rows, err := txn.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close() //nolint:errcheck
	type catalogRow struct {
		id  string
		sec int64
		ns  int
	}
	rowsRead := make([]catalogRow, 0, pageSize+1)
	for rows.Next() {
		var row catalogRow
		if err := rows.Scan(&row.id, &row.sec, &row.ns); err != nil {
			return nil, nil, err
		}
		if err := validateSnapshotCatalogID(row.id); err != nil {
			return nil, nil, err
		}
		rowsRead = append(rowsRead, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, nil, err
	}
	// The index transaction is no longer needed once its bounded row set has
	// been copied. Release it before filesystem manifest reads so catalog
	// mutations do not wait on page processing.
	_ = txn.Rollback()
	if len(rowsRead) > pageSize {
		last := rowsRead[pageSize-1]
		next = &SnapshotCatalogCursor{
			Generation: generation,
			CreatedSec: last.sec,
			CreatedNS:  last.ns,
			ID:         last.id,
		}
		rowsRead = rowsRead[:pageSize]
	}
	entries = make([]SnapshotEntry, 0, len(rowsRead))
	var problems []error
	for _, row := range rowsRead {
		manifest, readErr := ReadManifest(filepath.Join(baseDir, row.id), opts...)
		if readErr != nil {
			problems = append(problems, fmt.Errorf(
				"snapshot %q: %w", row.id, readErr,
			))
			continue
		}
		entries = append(entries, SnapshotEntry{ID: row.id, Manifest: manifest})
	}
	return entries, next, errors.Join(problems...)
}
