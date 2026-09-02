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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
)

// metadataFilesUnderTempRoot lists every metadata file below the root that
// testing.TB.TempDir allocates for a single test. NewDatabaseWithOptions calls
// tb.TempDir() with the test's own tb when it points the provider at a
// directory of its own, so anything it writes lands under that same root and
// its presence or absence distinguishes a file-backed metadata store from an
// in-memory one.
func metadataFilesUnderTempRoot(tb testing.TB, root string) []string {
	tb.Helper()
	var found []string
	err := filepath.WalkDir(
		root,
		func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && entry.Name() == metadataTemplateFile {
				found = append(found, path)
			}
			return nil
		},
	)
	if err != nil {
		tb.Fatalf("walk %s: %v", root, err)
	}
	return found
}

// TestNewDatabaseWithOptionsSeedsCallerDataDir pins the routing that keeps
// RawSQLiteMetadata working: a caller that supplies a DataDir gets the
// template seeded into that directory, with the provider left to resolve its
// own path, so the metadata file is the one at db.DataDir().
func TestNewDatabaseWithOptionsSeedsCallerDataDir(t *testing.T) {
	dataDir := t.TempDir()
	db, err := NewDatabaseWithOptions(t, Options{
		Config: &database.Config{DataDir: dataDir},
	})
	if err != nil {
		t.Fatalf("NewDatabaseWithOptions: %v", err)
	}
	if db.DataDir() != dataDir {
		t.Fatalf("DataDir() = %q, want %q", db.DataDir(), dataDir)
	}
	if _, err := os.Stat(
		filepath.Join(dataDir, metadataTemplateFile),
	); err != nil {
		t.Fatalf("metadata file in caller data dir: %v", err)
	}

	// The raw fixture opens db.DataDir()/metadata.sqlite unconditionally and
	// SQLite creates that file empty rather than failing, so a provider
	// pointed somewhere else surfaces here as a missing table rather than an
	// open error.
	raw, err := RawSQLiteMetadata(t, db)
	if err != nil {
		t.Fatalf("RawSQLiteMetadata: %v", err)
	}
	var completed int
	if err := raw.QueryRow(
		`SELECT count(*) FROM schema_migrations
		 WHERE completed_at IS NOT NULL`,
	).Scan(&completed); err != nil {
		t.Fatalf("read schema_migrations through raw fixture: %v", err)
	}
	if completed == 0 {
		t.Error("raw fixture sees no completed migration")
	}
}

// TestNewDatabaseWithOptionsWithoutDataDir covers the other branch: with no
// DataDir the provider is pointed at a directory of the fixture's own, which
// is still a file-backed database rather than the in-memory store the
// provider would build from an empty data directory.
func TestNewDatabaseWithOptionsWithoutDataDir(t *testing.T) {
	root := filepath.Dir(t.TempDir())
	db, err := NewDatabaseWithOptions(t, Options{
		Config: &database.Config{DataDir: ""},
	})
	if err != nil {
		t.Fatalf("NewDatabaseWithOptions: %v", err)
	}
	if db.DataDir() != "" {
		t.Errorf("DataDir() = %q, want empty", db.DataDir())
	}
	files := metadataFilesUnderTempRoot(t, root)
	if len(files) != 1 {
		t.Fatalf(
			"metadata files under the test temp root = %v, want exactly one",
			files,
		)
	}
	assertMigratedSchema(t, files[0])
}

// TestNewDatabaseWithOptionsInMemoryMetadata pins the opt-out: the template is
// not written anywhere and the provider falls back to the in-memory
// shared-cache store it builds from an empty data directory.
func TestNewDatabaseWithOptionsInMemoryMetadata(t *testing.T) {
	root := filepath.Dir(t.TempDir())
	db, err := NewDatabaseWithOptions(t, Options{
		Config:           &database.Config{DataDir: ""},
		InMemoryMetadata: true,
	})
	if err != nil {
		t.Fatalf("NewDatabaseWithOptions: %v", err)
	}
	if db.Metadata() == nil {
		t.Fatal("no metadata store")
	}
	if files := metadataFilesUnderTempRoot(t, root); len(files) != 0 {
		t.Errorf("in-memory metadata store wrote %v", files)
	}
	// The in-memory store is migrated in place, so it answers a settings
	// query that an unmigrated database could not.
	if _, err := db.Metadata().GetCommitTimestamp(); err != nil {
		t.Errorf("GetCommitTimestamp on in-memory store: %v", err)
	}
}
