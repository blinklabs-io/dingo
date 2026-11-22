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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"modernc.org/sqlite"
)

// Running every schema migration dominates the cost of an in-memory test
// store, especially under the race detector. The schema is migrated once per
// storage mode per test binary and each in-memory store starts from a restore
// of it. Tests that exercise the migrations themselves open file-backed
// stores or their own databases, which this hook does not touch.
func init() {
	BeforeOpenMemoryDB = seedMigratedMemory
}

type migratedMemoryTemplate struct {
	once sync.Once
	// db is deliberately never closed: closing the last connection to a
	// shared-cache in-memory database discards it.
	db  *sql.DB
	uri string
	err error
}

var migratedMemoryTemplates sync.Map // storage mode -> *migratedMemoryTemplate

func buildMigratedMemoryTemplate(
	tpl *migratedMemoryTemplate,
	storageMode string,
) error {
	dir, err := os.MkdirTemp("", "dingo-sqlite-memory-template-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	store, _, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{
			DataDir:     dir,
			StorageMode: storageMode,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	)
	if err != nil {
		return err
	}
	if err := store.Start(context.Background()); err != nil {
		_ = store.Close()
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}
	tpl.uri = fmt.Sprintf(
		"file:sqlite_pkg_memory_template_%s?mode=memory&cache=shared",
		storageMode,
	)
	db, err := sql.Open("sqlite", tpl.uri)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if err := restoreInto(db, filepath.Join(dir, "metadata.sqlite")); err != nil {
		_ = db.Close()
		return err
	}
	tpl.db = db
	return nil
}

// restoreInto copies the database named src into db. A backup restore goes
// through SQLite's page cache, so every connection of a shared-cache db sees
// the copy. It reports whether db's driver connection supports restoring.
func restoreInto(db *sql.DB, src string) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	rawErr := conn.Raw(func(driverConn any) error {
		r, ok := driverConn.(interface {
			NewRestore(string) (*sqlite.Backup, error)
		})
		if !ok {
			return errNoRestore
		}
		backup, err := r.NewRestore(src)
		if err != nil {
			return err
		}
		_, stepErr := backup.Step(-1)
		return errors.Join(stepErr, backup.Finish())
	})
	return errors.Join(rawErr, conn.Close())
}

var errNoRestore = errors.New("sqlite driver connection cannot restore")

func seedMigratedMemory(db *sql.DB, storageMode string) error {
	value, _ := migratedMemoryTemplates.LoadOrStore(
		storageMode, &migratedMemoryTemplate{},
	)
	tpl := value.(*migratedMemoryTemplate)
	tpl.once.Do(func() { tpl.err = buildMigratedMemoryTemplate(tpl, storageMode) })
	if tpl.err != nil {
		return tpl.err
	}
	err := restoreInto(db, tpl.uri)
	if errors.Is(err, errNoRestore) {
		// A wrapping driver (statement guard, tracing) hides the restore
		// interface; that store simply runs its own migrations.
		return nil
	}
	return err
}
