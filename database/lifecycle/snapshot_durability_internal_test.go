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
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestSnapshotDurabilityBeforeManifest(t *testing.T) {
	for _, failAt := range []string{"", BlobBackupFileName, MetadataBackupFileName, "directory", "parent"} {
		t.Run("failure_"+failAt, func(t *testing.T) {
			db := newRestoreInternalTestDB(t)
			require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))
			base := t.TempDir()
			dir := filepath.Join(base, "snapshot")
			originalFileSync, originalDirSync := syncSnapshotFile, syncDir
			t.Cleanup(func() { syncSnapshotFile, syncDir = originalFileSync, originalDirSync })
			injected := errors.New("snapshot durability failure")
			var calls []string
			check := func(stage string) error {
				calls = append(calls, stage)
				_, err := ReadManifest(dir)
				require.ErrorIs(t, err, os.ErrNotExist, "manifest published before backup durability")
				// Persistence no longer needs the snapshot's commit pause.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				resume, err := db.PauseCommitsContext(ctx)
				require.NoError(t, err, "durability work retained the commit barrier")
				resume()
				if stage == failAt {
					return injected
				}
				return nil
			}
			syncSnapshotFile = func(file *os.File) error {
				info, err := file.Stat()
				require.NoError(t, err)
				require.Positive(t, info.Size(), "sync must follow completed backup")
				if err := check(filepath.Base(file.Name())); err != nil {
					return err
				}
				return originalFileSync(file)
			}
			syncDir = func(path string) error {
				stage := "directory"
				if path == base {
					stage = "parent"
				}
				if err := check(stage); err != nil {
					return err
				}
				return originalDirSync(path)
			}
			_, err := Snapshot(context.Background(), db, dir, TriggerManual, "test", "badger", "sqlite")
			if failAt == "" {
				require.NoError(t, err)
				require.Equal(t, []string{BlobBackupFileName, MetadataBackupFileName, "directory", "parent"}, calls)
				entries, err := ListSnapshots(base)
				require.NoError(t, err)
				require.Len(t, entries, 1)
				syncSnapshotFile, syncDir = originalFileSync, originalDirSync
				_, err = Restore(context.Background(), newRestoreInternalTestHost(t), nil, dir, filepath.Join(t.TempDir(), "restored"), RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()})
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, injected, "snapshot accepted an unsynchronized backup")
			require.NoDirExists(t, dir)
			entries, err := ListSnapshots(base)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

// metadataTemplateSentinelTable is a marker table
// writeMetadataTemplateWithSentinel adds to a copy of this process's
// migrated metadata template, and requireMetadataTemplateSentinel checks
// for afterward. Its presence in a database is what lets
// TestSnapshotInterruptedBeforeManifest's forked child prove it actually
// reused the parent-built template instead of silently rebuilding an
// equivalent (but sentinel-free) one of its own -- a byte-for-byte
// comparison of schemas would not otherwise distinguish the two, since a
// fresh migration produces the same tables.
const metadataTemplateSentinelTable = "dingo_snapshot_interrupt_template_sentinel"

// writeMetadataTemplateWithSentinel materializes this process's migrated
// metadata template at path, with metadataTemplateSentinelTable added and
// populated.
func writeMetadataTemplateWithSentinel(t *testing.T, path string) {
	t.Helper()
	raw, err := dbtest.MetadataTemplateBytes()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	conn, err := sql.Open(
		"sqlite", "file:"+path+"?_pragma=busy_timeout(30000)",
	)
	require.NoError(t, err)
	_, err = conn.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER PRIMARY KEY)`,
		metadataTemplateSentinelTable,
	))
	require.NoError(t, err)
	_, err = conn.Exec(fmt.Sprintf(
		`INSERT INTO %s (id) VALUES (1)`, metadataTemplateSentinelTable,
	))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// requireMetadataTemplateSentinel fails unless db's metadata store carries
// metadataTemplateSentinelTable -- see writeMetadataTemplateWithSentinel.
func requireMetadataTemplateSentinel(t *testing.T, db *database.Database) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var count int
	require.NoError(t, raw.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM %s`, metadataTemplateSentinelTable,
	)).Scan(&count))
	require.Equal(
		t, 1, count,
		"child rebuilt its own metadata template instead of reusing "+
			"the one the parent process built and provided",
	)
}

// newRestoreInternalTestDBWithTemplate is newRestoreInternalTestDB, but
// seeds the database's data directory from templatePath (see
// writeMetadataTemplateWithSentinel) before construction when templatePath
// is non-empty, instead of letting dbtest.NewDatabase build and cache its
// own migrated template in this process.
func newRestoreInternalTestDBWithTemplate(
	t *testing.T, templatePath string,
) *database.Database {
	t.Helper()
	dir := t.TempDir()
	if templatePath != "" {
		raw, err := os.ReadFile(templatePath)
		require.NoError(t, err)
		require.NoError(t, dbtest.SeedMetadataTemplateBytes(dir, raw))
	}
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	return db
}

// A process exit bypasses Snapshot's cleanup, leaving an interrupted directory
// on disk. A fresh reader must neither catalog nor restore that incomplete copy.
func TestSnapshotInterruptedBeforeManifest(t *testing.T) {
	if stage := os.Getenv("DINGO_SNAPSHOT_INTERRUPT_STAGE"); stage != "" {
		templatePath := os.Getenv("DINGO_SNAPSHOT_INTERRUPT_METADATA_TEMPLATE")
		db := newRestoreInternalTestDBWithTemplate(t, templatePath)
		if templatePath != "" {
			requireMetadataTemplateSentinel(t, db)
			require.False(
				t, dbtest.MetadataTemplateBuilt(),
				"child ran its own metadata migration despite the "+
					"template the parent process provided",
			)
		}
		require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))
		syncSnapshotFile = func(file *os.File) error {
			if filepath.Base(file.Name()) == stage {
				os.Exit(23)
			}
			return file.Sync()
		}
		originalDirSync := syncDir
		syncDir = func(path string) error {
			if (stage == "directory" && path == os.Getenv("DINGO_SNAPSHOT_INTERRUPT_DIR")) ||
				(stage == "parent" && path == filepath.Dir(os.Getenv("DINGO_SNAPSHOT_INTERRUPT_DIR"))) {
				os.Exit(23)
			}
			return originalDirSync(path)
		}
		_, err := Snapshot(context.Background(), db, os.Getenv("DINGO_SNAPSHOT_INTERRUPT_DIR"), TriggerManual, "test", "badger", "sqlite")
		require.NoError(t, err)
		t.Fatal("snapshot did not reach interruption boundary")
	}

	// Build the migrated metadata template once, in this process, and hand
	// it to every forked child below instead of letting each of the four
	// repeat the same one-per-process migration cost in its own fresh
	// process -- see dbtest.MetadataTemplateBytes' doc comment. The
	// sentinel table lets the child assertion above tell whether it
	// actually reused these bytes.
	templatePath := filepath.Join(t.TempDir(), "metadata-template.sqlite")
	writeMetadataTemplateWithSentinel(t, templatePath)

	for _, stage := range []string{BlobBackupFileName, MetadataBackupFileName, "directory", "parent"} {
		t.Run(stage, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "interrupted")
			// The child seeds its test database from the template built
			// above instead of running its own migration, but still needs
			// a bounded, generous deadline for the remaining setup and
			// process-start overhead, which can be slow on Windows.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSnapshotInterruptedBeforeManifest$")
			cmd.Env = append(
				os.Environ(),
				"TMPDIR="+base, "TMP="+base, "TEMP="+base,
				"DINGO_SNAPSHOT_INTERRUPT_STAGE="+stage,
				"DINGO_SNAPSHOT_INTERRUPT_DIR="+dir,
				"DINGO_SNAPSHOT_INTERRUPT_METADATA_TEMPLATE="+templatePath,
			)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "%s", output)
			require.Equal(t, 23, exitErr.ExitCode(), "%s", output)
			require.DirExists(t, dir)
			entries, err := ListSnapshots(base)
			require.NoError(t, err)
			require.Empty(t, entries, "interrupted snapshot appeared in catalog")
			target := filepath.Join(t.TempDir(), "restore")
			_, err = Restore(context.Background(), newRestoreInternalTestHost(t), nil, dir, target, RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()})
			require.ErrorIs(t, err, os.ErrNotExist)
			require.NoDirExists(t, target)
		})
	}
}

func TestSnapshotDurableAncestors(t *testing.T) {
	for _, failAt := range []string{"", "outer", "base"} {
		t.Run("failure_"+failAt, func(t *testing.T) {
			db := newRestoreInternalTestDB(t)
			base := t.TempDir()
			outer := filepath.Join(base, "outer")
			parent := filepath.Join(outer, "inner")
			dir := filepath.Join(parent, "snapshot")
			original := syncDir
			t.Cleanup(func() { syncDir = original })
			injected := errors.New("ancestor sync failed")
			var calls []string
			syncDir = func(path string) error {
				calls = append(calls, path)
				_, err := ReadManifest(dir)
				require.ErrorIs(
					t,
					err,
					os.ErrNotExist,
					"manifest precedes ancestor durability",
				)
				if (failAt == "outer" && path == outer) ||
					(failAt == "base" && path == base) {
					return injected
				}
				return original(path)
			}
			_, err := Snapshot(
				context.Background(),
				db,
				dir,
				TriggerManual,
				"test",
				"badger",
				"sqlite",
			)
			if failAt != "" {
				require.ErrorIs(
					t,
					err,
					injected,
					"snapshot succeeded without durable ancestors",
				)
				require.NoDirExists(t, dir)
				return
			}
			require.NoError(t, err)
			require.Equal(
				t,
				[]string{dir, parent, outer, base},
				calls,
				"new ancestor entries were not all synchronized",
			)
			_, err = ReadManifest(dir)
			require.NoError(t, err)
		})
	}
}
