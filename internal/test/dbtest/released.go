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

package dbtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
)

// ReleasedSchemaVersion is the metadata schema version of the v0.94.2
// release, the last one without the storage-role layout.
const ReleasedSchemaVersion = 68

// openReleasedMetadata starts the SQLite metadata store at path with a
// migration registry that ends at ReleasedSchemaVersion, as the v0.94.2
// binary does: it creates that schema in an empty file and refuses a file
// that holds a newer one.
func openReleasedMetadata(path, options string) (*sqlstore.Store, error) {
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path+options)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store, err := sqlstore.New(sqlstore.Config{
		WriteDB:         db,
		Dialect:         sqlstore.SQLiteDialect(),
		StorageMode:     "api",
		Migrations:      registry[:ReleasedSchemaVersion],
		MigrationLocker: migrations.NewProcessLocker(),
		Logger:          slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := store.Start(context.Background()); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

// releasedMetadataTemplate is an API-mode SQLite metadata store at
// ReleasedSchemaVersion, migrated once per test binary.
var releasedMetadataTemplate = sync.OnceValues(func() ([]byte, error) {
	dir, err := os.MkdirTemp("", "dingo-released-metadata-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, metadataTemplateFile)
	store, err := openReleasedMetadata(
		path, "?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
	)
	if err != nil {
		return nil, err
	}
	err = store.SetNodeSettingsGates(
		context.Background(),
		nodesettings.Values{"storage_mode": "api", "network": "preview"},
		0, 0,
	)
	if err := errors.Join(err, store.Close()); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
})

// SeedReleasedMetadata writes into dir a SQLite metadata store as the v0.94.2
// release leaves an API-mode one: at ReleasedSchemaVersion, with sync_status
// set to syncStatus unless that is empty.
func SeedReleasedMetadata(dir, syncStatus string) error {
	if err := seedMetadataFile(dir, releasedMetadataTemplate); err != nil {
		return err
	}
	if syncStatus == "" {
		return nil
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, metadataTemplateFile))
	if err != nil {
		return err
	}
	_, err = db.Exec(
		`INSERT INTO sync_state(sync_key, value) VALUES('sync_status', ?)`,
		syncStatus,
	)
	return errors.Join(err, db.Close())
}

// ReleasedMetadataSyncStatus opens the SQLite metadata store in dir as the
// v0.94.2 binary does and returns its sync_status. It fails for a store whose
// schema is past ReleasedSchemaVersion.
func ReleasedMetadataSyncStatus(dir string) (string, error) {
	store, err := openReleasedMetadata(
		filepath.Join(dir, metadataTemplateFile), "",
	)
	if err != nil {
		return "", err
	}
	status, err := store.GetSyncState("sync_status", nil)
	return status, errors.Join(err, store.Close())
}

// MetadataFingerprint returns the schema objects, applied migrations, sync
// state and node settings of the SQLite metadata store in dir, read without
// opening it through a provider.
func MetadataFingerprint(dir string) (string, error) {
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(dir, metadataTemplateFile)+"?mode=ro",
	)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var out strings.Builder
	for _, query := range []string{
		`SELECT type || ' ' || name || ' ' || COALESCE(sql, '') FROM sqlite_master ORDER BY type, name`,
		`SELECT version || ' ' || name || ' ' || phase FROM schema_migrations ORDER BY version`,
		`SELECT sync_key || '=' || value FROM sync_state ORDER BY sync_key`,
		`SELECT name || '=' || value FROM node_settings_gate ORDER BY name`,
	} {
		if err := appendRows(db, query, &out); err != nil {
			return "", fmt.Errorf("%s: %w", query, err)
		}
	}
	return out.String(), nil
}

func appendRows(db *sql.DB, query string, out *strings.Builder) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return err
		}
		out.WriteString(line + "\n")
	}
	return rows.Err()
}
