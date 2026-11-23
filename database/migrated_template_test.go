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
	"context"
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
	modsqlite "modernc.org/sqlite"
)

// Running every schema migration dominates the cost of a test database,
// especially under the race detector, and each test only needs the migrated
// schema as a starting point. The schema is migrated once per storage mode
// per test binary and each new test directory starts from a copy of it. The
// copy holds only what the migrations write, so state that Open writes on a
// first start is still written by the test's own open.
var migratedMetadataTemplates sync.Map // storage mode -> *migratedMetadataTemplate

type migratedMetadataTemplate struct {
	once  sync.Once
	bytes []byte
	err   error
}

func init() {
	sqlite.BeforeOpenMemoryDB = seedMigratedMemory
}

// memoryTemplate is a migrated schema held in a shared-cache in-memory
// database, so each in-memory test store restores from it without a file.
type memoryTemplate struct {
	once sync.Once
	// db is deliberately never closed: closing the last connection to a
	// shared-cache in-memory database discards it.
	db  *sql.DB
	uri string
	err error
}

var memoryTemplates sync.Map // storage mode -> *memoryTemplate

var errNoRestore = errors.New("sqlite driver connection cannot restore")

func restoreSQLiteInto(db *sql.DB, src string) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	rawErr := conn.Raw(func(driverConn any) error {
		r, ok := driverConn.(interface {
			NewRestore(string) (*modsqlite.Backup, error)
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

func buildMemoryTemplate(tpl *memoryTemplate, storageMode string) error {
	raw, err := migratedMetadataBytes(storageMode)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "dingo-memory-template-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	file := filepath.Join(dir, "metadata.sqlite")
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		return err
	}
	tpl.uri = "file:database_pkg_memory_template_" + storageMode +
		"?mode=memory&cache=shared"
	db, err := sql.Open("sqlite", tpl.uri)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if err := restoreSQLiteInto(db, file); err != nil {
		_ = db.Close()
		return err
	}
	tpl.db = db
	return nil
}

// seedMigratedMemory restores the migrated schema into an in-memory metadata
// store before it runs its migrations.
func seedMigratedMemory(db *sql.DB, storageMode string) error {
	value, _ := memoryTemplates.LoadOrStore(storageMode, &memoryTemplate{})
	tpl := value.(*memoryTemplate)
	tpl.once.Do(func() { tpl.err = buildMemoryTemplate(tpl, storageMode) })
	if tpl.err != nil {
		return tpl.err
	}
	err := restoreSQLiteInto(db, tpl.uri)
	if errors.Is(err, errNoRestore) {
		// A wrapping driver (statement guard, tracing) hides the restore
		// interface; that store simply runs its own migrations.
		return nil
	}
	return err
}

// seedMigratedMetadata places a migrated metadata database in dataDir unless
// one is already there, so a test that reopens an existing directory keeps
// its own data. An empty dataDir means in-memory and is left alone.
func seedMigratedMetadata(tb testing.TB, dataDir, storageMode string) {
	tb.Helper()
	if dataDir == "" {
		return
	}
	dst := filepath.Join(dataDir, "metadata.sqlite")
	if _, err := os.Stat(dst); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return
	}
	bytes, err := migratedMetadataBytes(storageMode)
	require.NoError(tb, err)
	require.NoError(tb, os.MkdirAll(dataDir, 0o750))
	require.NoError(tb, os.WriteFile(dst, bytes, 0o600))
}

func migratedMetadataBytes(storageMode string) ([]byte, error) {
	value, _ := migratedMetadataTemplates.LoadOrStore(
		storageMode, &migratedMetadataTemplate{},
	)
	tpl := value.(*migratedMetadataTemplate)
	tpl.once.Do(func() { tpl.bytes, tpl.err = buildMigratedMetadata(storageMode) })
	return tpl.bytes, tpl.err
}

func buildMigratedMetadata(storageMode string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "dingo-migrated-template-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	host := plugin.NewHost()
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, err
	}
	if _, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir:     dir,
			StorageMode: storageMode,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	); err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	if err := host.Stop(context.Background()); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(dir, "metadata.sqlite"))
}
