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
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	driversqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	snapshotCatalogFileName = ".dingo-snapshot-catalog-v1.sqlite"
	snapshotCatalogBusyMS   = 5000
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
	firstAvailableSnapshotPageQuery = `SELECT id, created_sec, created_ns,
is_local, manifest FROM available_snapshots
ORDER BY created_sec DESC, created_ns DESC, id DESC LIMIT ?`
	nextAvailableSnapshotPageQuery = `SELECT id, created_sec, created_ns,
is_local, manifest FROM available_snapshots
WHERE (created_sec, created_ns, id) < (?, ?, ?)
ORDER BY created_sec DESC, created_ns DESC, id DESC LIMIT ?`
	firstLocalAvailableSnapshotPageQuery = `SELECT id, created_sec, created_ns,
1, NULL FROM snapshots
ORDER BY created_sec DESC, created_ns DESC, id DESC LIMIT ?`
	nextLocalAvailableSnapshotPageQuery = `SELECT id, created_sec, created_ns,
1, NULL FROM snapshots
WHERE (created_sec, created_ns, id) < (?, ?, ?)
ORDER BY created_sec DESC, created_ns DESC, id DESC LIMIT ?`
)

var (
	snapshotCatalogMu = make(chan struct{}, 1)
	// ErrSnapshotCatalogChanged means a page token names a prior catalog
	// generation. The caller must restart pagination from the first page.
	ErrSnapshotCatalogChanged = errors.New("snapshot catalog changed")
	// ErrSnapshotCatalogIncomplete means the catalog was rebuilt with every
	// readable snapshot while one or more directory entries were unreadable.
	ErrSnapshotCatalogIncomplete = errors.New("snapshot catalog incomplete")
	// ErrSnapshotCatalogCorrupt means the persistent catalog contains data
	// that cannot represent a snapshot below its configured root.
	ErrSnapshotCatalogCorrupt = errors.New("snapshot catalog corrupt")
	// ErrSnapshotCatalogUpdate means the durable snapshot was written but its
	// repairable catalog index could not be updated.
	ErrSnapshotCatalogUpdate = errors.New("snapshot catalog update failed")
)

// SnapshotCatalogCursor identifies the last row in one immutable catalog
// generation. The next page starts after this CreatedAt and ID tuple.
type SnapshotCatalogCursor struct {
	Generation uint64
	CreatedSec int64
	CreatedNS  int
	ID         string
}

// AvailableSnapshotCatalogEntry is one local-or-cloud catalog result. Local
// entries carry their filesystem path; cloud-only entries carry a redacted
// display URI that cannot be used as an authenticated provider location.
type AvailableSnapshotCatalogEntry struct {
	Entry    SnapshotEntry
	Location string
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

func lockSnapshotCatalog(ctx context.Context) error {
	select {
	case snapshotCatalogMu <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func unlockSnapshotCatalog() {
	<-snapshotCatalogMu
}

func snapshotCatalogDSN(path string, create bool) string {
	if path == ":memory:" {
		return "file::memory:?cache=private&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	}
	values := url.Values{}
	if create {
		values.Set("mode", "rwc")
	} else {
		values.Set("mode", "rw")
	}
	values.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", snapshotCatalogBusyMS))
	values.Add("_pragma", "journal_mode(WAL)")
	return (&url.URL{
		Scheme:   "file",
		Path:     filepath.ToSlash(path),
		RawQuery: values.Encode(),
	}).String()
}

func openSnapshotCatalog(
	ctx context.Context,
	path string,
	create bool,
) (*sql.DB, error) {
	db, err := sql.Open("sqlite", snapshotCatalogDSN(path, create))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func snapshotCatalogCanRebuild(err error) bool {
	if errors.Is(err, ErrSnapshotCatalogCorrupt) {
		return true
	}
	var sqliteErr *driversqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	baseCode := sqliteErr.Code() & 0xff
	if baseCode == sqlite3.SQLITE_CORRUPT || baseCode == sqlite3.SQLITE_NOTADB {
		return true
	}
	if baseCode != sqlite3.SQLITE_ERROR {
		return false
	}
	// These messages come from the fixed generation query and identify a
	// structurally valid SQLite database with the wrong catalog schema.
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such table") ||
		strings.Contains(message, "no such column") ||
		strings.Contains(message, "has no column named")
}

type snapshotCatalogGenerationSource func() (int64, error)

func randomSnapshotCatalogGeneration() (int64, error) {
	value, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64-1))
	if err != nil {
		return 0, err
	}
	return value.Int64() + 2, nil
}

func createSnapshotCatalogSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE catalog_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation INTEGER NOT NULL CHECK (generation > 0),
    cloud_source TEXT NOT NULL
);
CREATE TABLE snapshots (
    id TEXT PRIMARY KEY,
    created_sec INTEGER NOT NULL,
    created_ns INTEGER NOT NULL CHECK (created_ns >= 0 AND created_ns < 1000000000)
);
CREATE INDEX snapshots_created
    ON snapshots (created_sec DESC, created_ns DESC, id DESC);
CREATE TABLE cloud_snapshots (
    id TEXT PRIMARY KEY,
    created_sec INTEGER NOT NULL,
    created_ns INTEGER NOT NULL CHECK (created_ns >= 0 AND created_ns < 1000000000),
    manifest BLOB NOT NULL
);
CREATE TABLE available_snapshots (
    id TEXT PRIMARY KEY,
    created_sec INTEGER NOT NULL,
    created_ns INTEGER NOT NULL CHECK (created_ns >= 0 AND created_ns < 1000000000),
    is_local INTEGER NOT NULL CHECK (is_local IN (0, 1)),
    manifest BLOB,
    CHECK ((is_local = 1 AND manifest IS NULL) OR
           (is_local = 0 AND manifest IS NOT NULL))
);
CREATE INDEX available_snapshots_created
    ON available_snapshots (created_sec DESC, created_ns DESC, id DESC);
`)
	return err
}

type catalogQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type cloudCatalogRow struct {
	id       string
	sec      int64
	ns       int
	manifest []byte
}

func readCloudCatalogRows(
	ctx context.Context,
	db *sql.DB,
) ([]cloudCatalogRow, error) {
	rows, err := db.QueryContext(ctx, `
SELECT id, created_sec, created_ns, manifest FROM cloud_snapshots`)
	if err != nil {
		if snapshotCatalogCanRebuild(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	ret := make([]cloudCatalogRow, 0)
	var problems []error
	for rows.Next() {
		var row cloudCatalogRow
		if err := rows.Scan(&row.id, &row.sec, &row.ns, &row.manifest); err != nil {
			return nil, err
		}
		if err := validateSnapshotCatalogID(row.id); err != nil {
			problems = append(problems, err)
			continue
		}
		manifest, err := ParseManifest(row.manifest)
		if err != nil || manifest.CreatedAt.Unix() != row.sec ||
			manifest.CreatedAt.Nanosecond() != row.ns {
			problems = append(problems, fmt.Errorf(
				"cloud snapshot %q has inconsistent catalog data", row.id,
			))
			continue
		}
		ret = append(ret, row)
	}
	problems = append(problems, rows.Err())
	return ret, errors.Join(problems...)
}

func readCatalogGeneration(
	ctx context.Context,
	query catalogQuerier,
) (uint64, error) {
	var value any
	if err := query.QueryRowContext(
		ctx,
		"SELECT generation FROM catalog_meta WHERE singleton = 1",
	).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) || snapshotCatalogCanRebuild(err) {
			return 0, fmt.Errorf("%w: invalid catalog metadata: %w", ErrSnapshotCatalogCorrupt, err)
		}
		return 0, err
	}
	generation, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf(
			"%w: generation has type %T", ErrSnapshotCatalogCorrupt, value,
		)
	}
	if generation <= 0 {
		return 0, fmt.Errorf(
			"%w: invalid generation %d", ErrSnapshotCatalogCorrupt, generation,
		)
	}
	return uint64(generation), nil
}

func readCatalogCloudSource(
	ctx context.Context,
	query catalogQuerier,
) (string, error) {
	var source string
	if err := query.QueryRowContext(
		ctx,
		"SELECT cloud_source FROM catalog_meta WHERE singleton = 1",
	).Scan(&source); err != nil {
		if errors.Is(err, sql.ErrNoRows) || snapshotCatalogCanRebuild(err) {
			return "", fmt.Errorf(
				"%w: invalid cloud source metadata: %w",
				ErrSnapshotCatalogCorrupt, err,
			)
		}
		return "", err
	}
	return source, nil
}

// EnsureSnapshotCatalog atomically rebuilds the persistent local snapshot
// index. Callers run it during service initialization, outside request
// handling. It imports snapshot directories created while the service was not
// running.
func EnsureSnapshotCatalog(baseDir string, opts ...ManifestOption) error {
	return EnsureSnapshotCatalogContext(
		context.Background(), baseDir, opts...,
	)
}

// EnsureSnapshotCatalogContext atomically rebuilds the persistent local
// snapshot index using ctx for catalog operations.
func EnsureSnapshotCatalogContext(
	ctx context.Context,
	baseDir string,
	opts ...ManifestOption,
) error {
	return ensureSnapshotCatalogContext(
		ctx, baseDir, randomSnapshotCatalogGeneration, opts...,
	)
}

func ensureSnapshotCatalogContext(
	ctx context.Context,
	baseDir string,
	generationSource snapshotCatalogGenerationSource,
	opts ...ManifestOption,
) error {
	if err := lockSnapshotCatalog(ctx); err != nil {
		return err
	}
	defer unlockSnapshotCatalog()
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create snapshot catalog directory: %w", err)
	}
	entries, listErr := ListSnapshotsContext(ctx, baseDir, opts...)
	if listErr != nil && entries == nil {
		return listErr
	}
	generation := uint64(1)
	needsFreshGeneration := false
	var preservedCloud []cloudCatalogRow
	var preservedCloudSource string
	var catalogProblems []error
	catalogPath := snapshotCatalogPath(baseDir)
	if _, err := os.Stat(catalogPath); err == nil {
		current, err := openSnapshotCatalog(ctx, catalogPath, false)
		if err != nil {
			if !snapshotCatalogCanRebuild(err) {
				return fmt.Errorf("open existing snapshot catalog: %w", err)
			}
			needsFreshGeneration = true
		} else {
			oldGeneration, generationErr := readCatalogGeneration(ctx, current)
			if generationErr == nil {
				preservedCloudSource, err = readCatalogCloudSource(ctx, current)
				if err == nil {
					preservedCloud, err = readCloudCatalogRows(ctx, current)
					if err != nil {
						catalogProblems = append(catalogProblems, err)
					}
				} else if !snapshotCatalogCanRebuild(err) {
					_ = current.Close()
					return fmt.Errorf("read existing cloud source: %w", err)
				} else {
					generationErr = err
				}
			}
			closeErr := current.Close()
			if generationErr == nil {
				if closeErr != nil {
					return closeErr
				}
				if oldGeneration == math.MaxInt64 {
					return errors.New("snapshot catalog generation overflow")
				}
				generation = oldGeneration + 1
			} else {
				if !snapshotCatalogCanRebuild(generationErr) {
					return fmt.Errorf("read existing snapshot catalog: %w", generationErr)
				}
				needsFreshGeneration = true
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if needsFreshGeneration {
		freshGeneration, err := generationSource()
		if err != nil {
			return fmt.Errorf("create snapshot catalog generation: %w", err)
		}
		if freshGeneration <= 1 {
			return errors.New("snapshot catalog recovery generation must be greater than one")
		}
		generation = uint64(freshGeneration)
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
	db, err := openSnapshotCatalog(ctx, tmpPath, true)
	if err != nil {
		return err
	}
	if err := createSnapshotCatalogSchema(db); err != nil {
		_ = db.Close()
		return err
	}
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		_ = db.Close()
		return err
	}
	defer txn.Rollback() //nolint:errcheck
	if _, err := txn.ExecContext(
		ctx,
		"INSERT INTO catalog_meta(singleton, generation, cloud_source) VALUES (1, ?, ?)",
		generation, preservedCloudSource,
	); err != nil {
		_ = db.Close()
		return err
	}
	for _, entry := range entries {
		if err := validateSnapshotCatalogID(entry.ID); err != nil {
			_ = db.Close()
			return err
		}
		if _, err := txn.ExecContext(
			ctx,
			"INSERT INTO snapshots(id, created_sec, created_ns) VALUES (?, ?, ?)",
			entry.ID, entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond(),
		); err != nil {
			_ = db.Close()
			return err
		}
		if _, err := txn.ExecContext(ctx, `
INSERT INTO available_snapshots(
    id, created_sec, created_ns, is_local, manifest
) VALUES (?, ?, ?, 1, NULL)`, entry.ID,
			entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond(),
		); err != nil {
			_ = db.Close()
			return err
		}
	}
	for _, row := range preservedCloud {
		if _, err := txn.ExecContext(ctx, `
INSERT INTO cloud_snapshots(id, created_sec, created_ns, manifest)
VALUES (?, ?, ?, ?)`, row.id, row.sec, row.ns, row.manifest); err != nil {
			_ = db.Close()
			return err
		}
		if _, err := txn.ExecContext(ctx, `
INSERT INTO available_snapshots(
    id, created_sec, created_ns, is_local, manifest
) SELECT ?, ?, ?, 0, ?
WHERE NOT EXISTS (SELECT 1 FROM snapshots WHERE id = ?)`, row.id,
			row.sec, row.ns, row.manifest, row.id); err != nil {
			_ = db.Close()
			return err
		}
	}
	if err := txn.Commit(); err != nil {
		_ = db.Close()
		return err
	}
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(catalogPath + suffix); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale snapshot catalog%s: %w", suffix, err)
		}
	}
	if err := os.Rename(tmpPath, catalogPath); err != nil {
		return err
	}
	if err := syncDir(baseDir); err != nil {
		return err
	}
	if problem := errors.Join(listErr, errors.Join(catalogProblems...)); problem != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotCatalogIncomplete, problem)
	}
	return nil
}

func mutateSnapshotCatalog(
	ctx context.Context,
	baseDir string,
	mutate func(*sql.Tx) (bool, error),
) error {
	if err := lockSnapshotCatalog(ctx); err != nil {
		return err
	}
	defer unlockSnapshotCatalog()
	return mutateSnapshotCatalogLocked(ctx, baseDir, mutate)
}

func mutateSnapshotCatalogLocked(
	ctx context.Context,
	baseDir string,
	mutate func(*sql.Tx) (bool, error),
) error {
	catalogPath := snapshotCatalogPath(baseDir)
	if _, err := os.Stat(catalogPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := openSnapshotCatalog(ctx, catalogPath, false)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck
	txn, err := db.BeginTx(ctx, nil)
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
	generation, err := readCatalogGeneration(ctx, txn)
	if err != nil {
		return err
	}
	if generation == math.MaxInt64 {
		return errors.New("snapshot catalog generation overflow")
	}
	if _, err := txn.ExecContext(
		ctx,
		"UPDATE catalog_meta SET generation = ? WHERE singleton = 1",
		generation+1,
	); err != nil {
		return err
	}
	return txn.Commit()
}

func updateSnapshotCatalogIfPresent(
	ctx context.Context,
	baseDir string,
	entry SnapshotEntry,
) error {
	if err := validateSnapshotCatalogID(entry.ID); err != nil {
		return err
	}
	return mutateSnapshotCatalog(ctx, baseDir, func(txn *sql.Tx) (bool, error) {
		if _, err := txn.ExecContext(ctx, `
INSERT INTO snapshots(id, created_sec, created_ns) VALUES (?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns`,
			entry.ID, entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond(),
		); err != nil {
			return false, err
		}
		_, err := txn.ExecContext(ctx, `
INSERT INTO available_snapshots(
    id, created_sec, created_ns, is_local, manifest
) VALUES (?, ?, ?, 1, NULL)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns,
    is_local = 1,
    manifest = NULL`, entry.ID, entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond())
		return err == nil, err
	})
}

func removeSnapshotCatalogEntryIfPresent(
	ctx context.Context,
	baseDir string,
	id string,
) error {
	if err := validateSnapshotCatalogID(id); err != nil {
		return err
	}
	return mutateSnapshotCatalog(ctx, baseDir, func(txn *sql.Tx) (bool, error) {
		result, err := txn.ExecContext(
			ctx, "DELETE FROM snapshots WHERE id = ?", id,
		)
		if err != nil {
			return false, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed == 0 {
			return false, err
		}
		var sec int64
		var ns int
		var manifest []byte
		err = txn.QueryRowContext(ctx, `
SELECT created_sec, created_ns, manifest
FROM cloud_snapshots WHERE id = ?`, id).Scan(&sec, &ns, &manifest)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = txn.ExecContext(
				ctx, "DELETE FROM available_snapshots WHERE id = ?", id,
			)
			return err == nil, err
		}
		if err != nil {
			return false, err
		}
		_, err = txn.ExecContext(ctx, `
UPDATE available_snapshots SET
    created_sec = ?, created_ns = ?, is_local = 0, manifest = ?
WHERE id = ?`, sec, ns, manifest, id)
		return err == nil, err
	})
}

// RebuildCloudSnapshotCatalogContext replaces the disposable cloud side of
// the snapshot catalog after a provider compatibility scan at service startup.
func RebuildCloudSnapshotCatalogContext(
	ctx context.Context,
	baseDir string,
	cloudDest string,
	entries []SnapshotEntry,
) error {
	if err := lockSnapshotCatalog(ctx); err != nil {
		return err
	}
	defer unlockSnapshotCatalog()
	return rebuildCloudSnapshotCatalogLocked(
		ctx, baseDir, cloudDest, entries,
	)
}

func rebuildCloudSnapshotCatalogLocked(
	ctx context.Context,
	baseDir string,
	cloudDest string,
	entries []SnapshotEntry,
) error {
	cloudSource, err := cloudDestinationIdentity(cloudDest)
	if err != nil {
		return err
	}
	var problems []error
	err = mutateSnapshotCatalogLocked(ctx, baseDir, func(txn *sql.Tx) (bool, error) {
		if _, err := txn.ExecContext(ctx, "DELETE FROM cloud_snapshots"); err != nil {
			return false, err
		}
		if _, err := txn.ExecContext(
			ctx, "DELETE FROM available_snapshots WHERE is_local = 0",
		); err != nil {
			return false, err
		}
		if _, err := txn.ExecContext(
			ctx,
			"UPDATE catalog_meta SET cloud_source = ? WHERE singleton = 1",
			cloudSource,
		); err != nil {
			return false, err
		}
		for _, entry := range entries {
			if err := validateSnapshotCatalogID(entry.ID); err != nil {
				problems = append(problems, err)
				continue
			}
			data, err := json.Marshal(entry.Manifest)
			if err != nil {
				problems = append(problems, fmt.Errorf(
					"marshal cloud snapshot %q manifest: %w", entry.ID, err,
				))
				continue
			}
			if _, err := ParseManifest(data); err != nil {
				problems = append(problems, fmt.Errorf(
					"validate cloud snapshot %q manifest: %w", entry.ID, err,
				))
				continue
			}
			if _, err := txn.ExecContext(ctx, `
INSERT INTO cloud_snapshots(id, created_sec, created_ns, manifest)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns,
    manifest = excluded.manifest`, entry.ID, entry.Manifest.CreatedAt.Unix(),
				entry.Manifest.CreatedAt.Nanosecond(), data); err != nil {
				return false, err
			}
			if _, err := txn.ExecContext(ctx, `
INSERT INTO available_snapshots(
    id, created_sec, created_ns, is_local, manifest
) SELECT ?, ?, ?, 0, ?
WHERE NOT EXISTS (SELECT 1 FROM snapshots WHERE id = ?)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns,
    manifest = excluded.manifest
WHERE available_snapshots.is_local = 0`, entry.ID,
				entry.Manifest.CreatedAt.Unix(),
				entry.Manifest.CreatedAt.Nanosecond(), data, entry.ID); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if problem := errors.Join(problems...); problem != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotCatalogIncomplete, problem)
	}
	return nil
}

func updateCloudSnapshotCatalogIfPresent(
	ctx context.Context,
	baseDir string,
	entry SnapshotEntry,
	cloudDest string,
) error {
	if err := validateSnapshotCatalogID(entry.ID); err != nil {
		return err
	}
	data, err := json.Marshal(entry.Manifest)
	if err != nil {
		return err
	}
	if _, err := ParseManifest(data); err != nil {
		return err
	}
	cloudSource, err := cloudDestinationIdentity(cloudDest)
	if err != nil {
		return err
	}
	return mutateSnapshotCatalog(ctx, baseDir, func(txn *sql.Tx) (bool, error) {
		storedSource, err := readCatalogCloudSource(ctx, txn)
		if err != nil {
			return false, err
		}
		if storedSource != cloudSource {
			if _, err := txn.ExecContext(
				ctx, "DELETE FROM cloud_snapshots",
			); err != nil {
				return false, err
			}
			if _, err := txn.ExecContext(
				ctx, "DELETE FROM available_snapshots WHERE is_local = 0",
			); err != nil {
				return false, err
			}
		}
		if _, err := txn.ExecContext(
			ctx,
			"UPDATE catalog_meta SET cloud_source = ? WHERE singleton = 1",
			cloudSource,
		); err != nil {
			return false, err
		}
		if _, err := txn.ExecContext(ctx, `
INSERT INTO cloud_snapshots(id, created_sec, created_ns, manifest)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns,
    manifest = excluded.manifest`, entry.ID, entry.Manifest.CreatedAt.Unix(),
			entry.Manifest.CreatedAt.Nanosecond(), data); err != nil {
			return false, err
		}
		_, err = txn.ExecContext(ctx, `
INSERT INTO available_snapshots(
    id, created_sec, created_ns, is_local, manifest
) VALUES (?, ?, ?, 0, ?)
ON CONFLICT(id) DO UPDATE SET
    created_sec = excluded.created_sec,
    created_ns = excluded.created_ns,
    manifest = excluded.manifest
WHERE available_snapshots.is_local = 0`, entry.ID,
			entry.Manifest.CreatedAt.Unix(), entry.Manifest.CreatedAt.Nanosecond(),
			data)
		return err == nil, err
	})
}

// RemoveCloudSnapshotCatalogEntryContext removes a deleted cloud copy while
// retaining any local row with the same snapshot ID.
func RemoveCloudSnapshotCatalogEntryContext(
	ctx context.Context,
	baseDir string,
	id string,
	registry *DestinationRegistry,
	cloudDest string,
) error {
	if err := validateSnapshotCatalogID(id); err != nil {
		return err
	}
	err := removeCloudSnapshotCatalogEntryIfPresent(ctx, baseDir, id)
	if err == nil {
		return nil
	}
	if repairErr := RepairSnapshotCatalogContext(
		context.WithoutCancel(ctx), baseDir, registry, cloudDest,
	); repairErr != nil {
		return errors.Join(err, repairErr)
	}
	return nil
}

func removeCloudSnapshotCatalogEntryIfPresent(
	ctx context.Context,
	baseDir string,
	id string,
) error {
	if err := validateSnapshotCatalogID(id); err != nil {
		return err
	}
	return mutateSnapshotCatalog(ctx, baseDir, func(txn *sql.Tx) (bool, error) {
		result, err := txn.ExecContext(
			ctx, "DELETE FROM cloud_snapshots WHERE id = ?", id,
		)
		if err != nil {
			return false, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed == 0 {
			return false, err
		}
		var sec int64
		var ns int
		err = txn.QueryRowContext(ctx, `
SELECT created_sec, created_ns FROM snapshots WHERE id = ?`, id).Scan(&sec, &ns)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = txn.ExecContext(
				ctx, "DELETE FROM available_snapshots WHERE id = ?", id,
			)
			return err == nil, err
		}
		if err != nil {
			return false, err
		}
		_, err = txn.ExecContext(ctx, `
UPDATE available_snapshots SET
    created_sec = ?, created_ns = ?, is_local = 1, manifest = NULL
WHERE id = ?`, sec, ns, id)
		return err == nil, err
	})
}

// RemoveSnapshot removes a local snapshot directory and its persistent
// catalog entry.
func RemoveSnapshot(dir string) error {
	return RemoveSnapshotContext(context.Background(), dir)
}

// RemoveSnapshotContext removes a local snapshot directory and its persistent
// catalog entry, using ctx for catalog operations.
func RemoveSnapshotContext(ctx context.Context, dir string) error {
	return removeSnapshotContext(ctx, dir, os.RemoveAll)
}

func removeSnapshotContext(
	ctx context.Context,
	dir string,
	remove func(string) error,
) error {
	return removeSnapshotContextWithGenerationSource(
		ctx, dir, remove, randomSnapshotCatalogGeneration,
	)
}

func removeSnapshotContextWithGenerationSource(
	ctx context.Context,
	dir string,
	remove func(string) error,
	generationSource snapshotCatalogGenerationSource,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := remove(dir); err != nil {
		return err
	}
	err := removeSnapshotCatalogEntryIfPresent(
		context.WithoutCancel(ctx),
		filepath.Dir(dir), filepath.Base(dir),
	)
	if err == nil {
		return nil
	}
	// The snapshot directory is authoritative. Rebuild the disposable index
	// immediately so a failed delete mutation cannot leave a stale row until
	// the service restarts.
	if repairErr := ensureSnapshotCatalogContext(
		context.WithoutCancel(ctx), filepath.Dir(dir), generationSource,
	); repairErr != nil {
		return errors.Join(err, repairErr)
	}
	return nil
}

// ListSnapshotPage reads at most pageSize+1 index rows and pageSize manifests.
// Entries preserve ListSnapshots' newest-first ordering.
func ListSnapshotPage(
	baseDir string,
	pageSize int,
	cursor *SnapshotCatalogCursor,
	opts ...ManifestOption,
) (entries []SnapshotEntry, next *SnapshotCatalogCursor, err error) {
	return ListSnapshotPageContext(
		context.Background(), baseDir, pageSize, cursor, opts...,
	)
}

// ListSnapshotPageContext reads one bounded snapshot catalog page using ctx.
func ListSnapshotPageContext(
	ctx context.Context,
	baseDir string,
	pageSize int,
	cursor *SnapshotCatalogCursor,
	opts ...ManifestOption,
) (entries []SnapshotEntry, next *SnapshotCatalogCursor, err error) {
	if pageSize <= 0 || pageSize > SnapshotCatalogPageLimit {
		return nil, nil, fmt.Errorf("invalid snapshot page size %d", pageSize)
	}
	if err := lockSnapshotCatalog(ctx); err != nil {
		return nil, nil, err
	}
	locked := true
	defer func() {
		if locked {
			unlockSnapshotCatalog()
		}
	}()
	db, err := openSnapshotCatalog(ctx, snapshotCatalogPath(baseDir), false)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close() //nolint:errcheck
	txn, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Rollback() //nolint:errcheck
	generation, err := readCatalogGeneration(ctx, txn)
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
	rows, err := txn.QueryContext(ctx, query, args...)
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
	_ = db.Close()
	unlockSnapshotCatalog()
	locked = false
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
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
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

// ListAvailableSnapshotPageContext reads a bounded, stable page across local
// and cloud snapshots. A local row wins when both stores contain the same ID.
func ListAvailableSnapshotPageContext(
	ctx context.Context,
	baseDir string,
	cloudDest string,
	pageSize int,
	cursor *SnapshotCatalogCursor,
	opts ...ManifestOption,
) (entries []AvailableSnapshotCatalogEntry, next *SnapshotCatalogCursor, err error) {
	if pageSize <= 0 || pageSize > SnapshotCatalogPageLimit {
		return nil, nil, fmt.Errorf("invalid snapshot page size %d", pageSize)
	}
	if err := lockSnapshotCatalog(ctx); err != nil {
		return nil, nil, err
	}
	locked := true
	defer func() {
		if locked {
			unlockSnapshotCatalog()
		}
	}()
	db, err := openSnapshotCatalog(ctx, snapshotCatalogPath(baseDir), false)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close() //nolint:errcheck
	txn, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer txn.Rollback() //nolint:errcheck
	generation, err := readCatalogGeneration(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	if cursor != nil && cursor.Generation != generation {
		return nil, nil, ErrSnapshotCatalogChanged
	}
	configuredSource, identityErr := cloudDestinationIdentity(cloudDest)
	if identityErr != nil {
		configuredSource = ""
	}
	storedSource, err := readCatalogCloudSource(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	useCloud := configuredSource != "" && configuredSource == storedSource
	query := firstLocalAvailableSnapshotPageQuery
	if useCloud {
		query = firstAvailableSnapshotPageQuery
	}
	args := []any{pageSize + 1}
	if cursor != nil {
		if cursor.ID == "" || cursor.CreatedNS < 0 || cursor.CreatedNS >= int(time.Second) {
			return nil, nil, errors.New("invalid snapshot catalog cursor")
		}
		query = nextLocalAvailableSnapshotPageQuery
		if useCloud {
			query = nextAvailableSnapshotPageQuery
		}
		args = []any{cursor.CreatedSec, cursor.CreatedNS, cursor.ID, pageSize + 1}
	}
	type catalogRow struct {
		id       string
		sec      int64
		ns       int
		local    bool
		manifest []byte
	}
	rows, err := txn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close() //nolint:errcheck
	rowsRead := make([]catalogRow, 0, pageSize+1)
	for rows.Next() {
		var row catalogRow
		if err := rows.Scan(
			&row.id, &row.sec, &row.ns, &row.local, &row.manifest,
		); err != nil {
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
	_ = txn.Rollback()
	_ = db.Close()
	unlockSnapshotCatalog()
	locked = false
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
	entries = make([]AvailableSnapshotCatalogEntry, 0, len(rowsRead))
	var problems []error
	for _, row := range rowsRead {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		var manifest Manifest
		var location string
		if row.local {
			location = filepath.Join(baseDir, row.id)
			manifest, err = ReadManifest(location, opts...)
		} else {
			location = JoinCloudURI(CloudDestinationDisplay(cloudDest), row.id)
			manifest, err = ParseManifest(row.manifest, opts...)
		}
		if err != nil {
			problems = append(problems, fmt.Errorf("snapshot %q: %w", row.id, err))
			continue
		}
		entries = append(entries, AvailableSnapshotCatalogEntry{
			Entry:    SnapshotEntry{ID: row.id, Manifest: manifest},
			Location: location,
		})
	}
	return entries, next, errors.Join(problems...)
}
