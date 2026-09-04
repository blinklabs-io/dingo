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
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestTemplateCleanupError pins the rule that keeps a failed removal of the
// scratch template directory from poisoning the process. buildMetadataTemplate
// runs once behind a sync.Once and its error is cached, so a removal failure
// folded into a successful build would fail every later test in the binary.
func TestTemplateCleanupError(t *testing.T) {
	t.Parallel()
	buildErr := errors.New("build failed")
	removeErr := errors.New("remove failed")
	for _, test := range []struct {
		name      string
		err       error
		removeErr error
		wantBuild bool
		wantRemov bool
	}{
		{
			name: "success with clean removal returns nil",
		},
		{
			// The regression: a cleanup failure must not turn a usable
			// template into a cached, permanent failure.
			name:      "success with failed removal stays successful",
			removeErr: removeErr,
		},
		{
			name:      "failure with clean removal keeps build error",
			err:       buildErr,
			wantBuild: true,
		},
		{
			name:      "failure with failed removal reports both",
			err:       buildErr,
			removeErr: removeErr,
			wantBuild: true,
			wantRemov: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := templateCleanupError(
				test.err,
				"/tmp/example",
				test.removeErr,
			)
			if !test.wantBuild && !test.wantRemov && got != nil {
				t.Fatalf("expected nil error, got %v", got)
			}
			if test.wantBuild && !errors.Is(got, buildErr) {
				t.Errorf("expected build error in %v", got)
			}
			if test.wantRemov != errors.Is(got, removeErr) {
				t.Errorf(
					"removal error present = %t, want %t (%v)",
					errors.Is(got, removeErr),
					test.wantRemov,
					got,
				)
			}
		})
	}
}

// minTemplateTables is a floor on the tables a fully migrated metadata
// database carries. The schema had 84 when the template was introduced; the
// floor exists to separate a complete copy from a truncated or empty one, not
// to pin the schema's size, so it sits well below the real count.
const minTemplateTables = 50

// openMetadataFile opens a database/sql handle directly on a materialized
// template file. The sqlite driver is registered by this package's own blank
// import in database.go.
func openMetadataFile(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	raw, err := sql.Open(
		"sqlite",
		"file:"+path+"?_pragma=busy_timeout(30000)",
	)
	if err != nil {
		tb.Fatalf("open %s: %v", path, err)
	}
	tb.Cleanup(func() {
		if err := raw.Close(); err != nil {
			tb.Errorf("close %s: %v", path, err)
		}
	})
	if err := raw.Ping(); err != nil {
		tb.Fatalf("ping %s: %v", path, err)
	}
	return raw
}

// assertMigratedSchema fails unless path holds a complete migrated schema.
//
// A copy taken from a still-open database would carry the header and whatever
// pages had already been written back, with the rest of the schema stranded in
// a companion -wal file the template does not include, so an opened copy is
// not evidence on its own: only reading the tables out of it is.
func assertMigratedSchema(tb testing.TB, path string) {
	tb.Helper()
	raw := openMetadataFile(tb, path)
	var tables int
	if err := raw.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table'`,
	).Scan(&tables); err != nil {
		tb.Fatalf("count tables in %s: %v", path, err)
	}
	if tables < minTemplateTables {
		tb.Errorf(
			"%s has %d tables, want at least %d",
			path,
			tables,
			minTemplateTables,
		)
	}
	var completed int
	if err := raw.QueryRow(
		`SELECT count(*) FROM schema_migrations
		 WHERE completed_at IS NOT NULL`,
	).Scan(&completed); err != nil {
		tb.Fatalf("read schema_migrations in %s: %v", path, err)
	}
	if completed == 0 {
		tb.Errorf("%s records no completed migration", path)
	}
}

// materializeTemplate writes raw into a fresh directory and returns the path
// it wrote, so the bytes can be inspected as a database.
func materializeTemplate(tb testing.TB, raw []byte) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), metadataTemplateFile)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		tb.Fatalf("write template copy: %v", err)
	}
	return path
}

// scratchTemplateDirs lists the scratch directories buildMetadataTemplate
// creates, by the prefix it passes to os.MkdirTemp.
//
// That prefix carries the process ID, which is what keeps this bounded to the
// build under test. The directories live in the shared temp directory, so an
// unscoped pattern would also match every other test binary that imports
// dbtest -- under `go test ./...` a sibling's build in flight would read as a
// directory this build leaked.
func scratchTemplateDirs(tb testing.TB) []string {
	tb.Helper()
	dirs, err := filepath.Glob(
		filepath.Join(os.TempDir(), metadataTemplateDirPrefix()+"*"),
	)
	if err != nil {
		tb.Fatalf("glob scratch template dirs: %v", err)
	}
	return dirs
}

// TestBuildMetadataTemplate covers the build directly rather than through the
// consuming packages: it must hand back the bytes of a fully migrated
// database and keep nothing on disk afterwards.
func TestBuildMetadataTemplate(t *testing.T) {
	// Drive the process-wide template to completion first. sync.Once returns
	// only after its function has, so once this call returns no build started
	// by another test can still be holding a scratch directory while this one
	// samples the temp directory.
	if _, err := migratedMetadataTemplate(); err != nil {
		t.Fatalf("prime metadata template: %v", err)
	}
	before := make(map[string]struct{})
	for _, dir := range scratchTemplateDirs(t) {
		before[dir] = struct{}{}
	}

	raw, err := buildMetadataTemplate()
	if err != nil {
		t.Fatalf("buildMetadataTemplate: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("buildMetadataTemplate returned no bytes")
	}

	t.Run("removes its scratch directory", func(t *testing.T) {
		var leaked []string
		for _, dir := range scratchTemplateDirs(t) {
			if _, ok := before[dir]; !ok {
				leaked = append(leaked, dir)
			}
		}
		if len(leaked) > 0 {
			t.Errorf("build left scratch directories behind: %v", leaked)
		}
	})

	t.Run("bytes carry the migrated schema", func(t *testing.T) {
		assertMigratedSchema(t, materializeTemplate(t, raw))
	})
}

// TestMigratedMetadataTemplateMemoizes pins the memoization that the whole
// fixture exists for: the migration runs once per process. Identical contents
// would not show that -- two separate builds produce identical bytes -- so
// this compares the backing arrays, which only match when the second call
// returned the cached slice instead of building again.
func TestMigratedMetadataTemplateMemoizes(t *testing.T) {
	first, err := migratedMetadataTemplate()
	if err != nil {
		t.Fatalf("first migratedMetadataTemplate: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("migratedMetadataTemplate returned no bytes")
	}
	second, err := migratedMetadataTemplate()
	if err != nil {
		t.Fatalf("second migratedMetadataTemplate: %v", err)
	}
	if len(second) != len(first) {
		t.Fatalf(
			"template length changed between calls: %d then %d",
			len(first),
			len(second),
		)
	}
	if &second[0] != &first[0] {
		t.Error("second call rebuilt the template instead of caching it")
	}
}

// TestSeedMetadataTemplateCreatesDir covers seeding into a directory that does
// not exist yet, which is what a caller-supplied DataDir usually is.
func TestSeedMetadataTemplateCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing", "data")
	if err := seedMetadataTemplate(dir); err != nil {
		t.Fatalf("seedMetadataTemplate: %v", err)
	}
	assertMigratedSchema(t, filepath.Join(dir, metadataTemplateFile))
}

// writeSentinel records a row in the seeded database that a rewrite of the
// file would destroy, and closes the handle so SQLite checkpoints the row into
// the database file itself.
//
// The close is what makes the sentinel evidence. SQLite writes to a companion
// -wal file first, and that log is valid against the template bytes it was
// written over, so a reseed that overwrote the database file would leave the
// log in place and a reader would replay the sentinel back over the fresh
// copy -- the test would pass whether or not the no-op branch exists.
func writeSentinel(tb testing.TB, path string) {
	tb.Helper()
	raw, err := sql.Open(
		"sqlite",
		"file:"+path+"?_pragma=busy_timeout(30000)",
	)
	if err != nil {
		tb.Fatalf("open %s: %v", path, err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			tb.Errorf("close %s: %v", path, err)
		}
		// A surviving log would put the sentinel back after an overwrite.
		// A present log is a nil stat error, so the file's presence is
		// what gets reported; only an unexpected stat failure carries one.
		wal := path + "-wal"
		switch _, err := os.Stat(wal); {
		case err == nil:
			tb.Errorf("write-ahead log %s still present after close", wal)
		case !errors.Is(err, os.ErrNotExist):
			tb.Errorf("stat write-ahead log %s: %v", wal, err)
		}
	}()
	if _, err := raw.Exec(
		`CREATE TABLE dbtest_sentinel (id INTEGER PRIMARY KEY)`,
	); err != nil {
		tb.Fatalf("create sentinel table: %v", err)
	}
	if _, err := raw.Exec(
		`INSERT INTO dbtest_sentinel (id) VALUES (1)`,
	); err != nil {
		tb.Fatalf("insert sentinel row: %v", err)
	}
}

// TestSeedMetadataTemplateKeepsExistingData pins the no-op branch with data a
// rewrite would destroy. Comparing bytes would not do it: the template is
// byte-identical to itself, so an unconditional rewrite would still compare
// equal. A row written into the seeded file after the first call survives only
// if the second call left the file alone.
func TestSeedMetadataTemplateKeepsExistingData(t *testing.T) {
	dir := t.TempDir()
	if err := seedMetadataTemplate(dir); err != nil {
		t.Fatalf("first seedMetadataTemplate: %v", err)
	}
	path := filepath.Join(dir, metadataTemplateFile)
	writeSentinel(t, path)

	if err := seedMetadataTemplate(dir); err != nil {
		t.Fatalf("second seedMetadataTemplate: %v", err)
	}

	raw := openMetadataFile(t, path)
	var sentinel int
	if err := raw.QueryRow(
		`SELECT count(*) FROM dbtest_sentinel`,
	).Scan(&sentinel); err != nil {
		t.Fatalf("read sentinel table after reseed: %v", err)
	}
	if sentinel != 1 {
		t.Errorf("sentinel rows after reseed = %d, want 1", sentinel)
	}
	// The file must still be the migrated template as well, so a second call
	// that wiped it back to an empty database cannot pass by leaving nothing
	// for the sentinel query to contradict.
	assertMigratedSchema(t, path)
}
