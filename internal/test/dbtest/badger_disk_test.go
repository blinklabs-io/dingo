package dbtest_test

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// maxTestBlobFileBytes bounds any single file a test database is allowed to
// reserve. Badger truncates its memtable and value log to their configured
// sizes up front; on NTFS that truncation actually allocates, so production
// defaults (1GiB value log, 128MiB memtable) exhaust the Windows runner's
// disk once a package builds several stores. See issue #3980.
const maxTestBlobFileBytes = 32 << 20

func TestNewDatabaseBoundsBadgerFileReservation(t *testing.T) {
	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	require.NotNil(t, db)

	var largest int64
	var largestPath string
	err = filepath.WalkDir(
		dataDir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if info.Size() > largest {
				largest, largestPath = info.Size(), path
			}
			return nil
		},
	)
	require.NoError(t, err)

	t.Logf("largest reserved file: %s (%d bytes)", largestPath, largest)
	require.LessOrEqualf(
		t, largest, int64(maxTestBlobFileBytes),
		"a test database reserved %d bytes for %s; badger sizes must be bounded for tests",
		largest, largestPath,
	)
}

// A test that supplies its own Blob.Config for unrelated knobs must still get
// the bounded sizes; otherwise the production defaults silently come back.
func TestBoundedBadgerSizesSurviveAPartialCallerConfig(t *testing.T) {
	dataDir := t.TempDir()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: dataDir},
		Blob: dbtest.StorageProvider{
			// Sets an unrelated knob and says nothing about sizes.
			Config: map[string]any{"gc": false},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, db)

	var largest int64
	var largestPath string
	require.NoError(t, filepath.WalkDir(dataDir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if info.Size() > largest {
				largest, largestPath = info.Size(), path
			}
			return nil
		}))

	t.Logf("largest reserved file: %s (%d bytes)", largestPath, largest)
	require.LessOrEqualf(
		t, largest, int64(maxTestBlobFileBytes),
		"a partial caller config reserved %d bytes for %s; bounded sizes must still apply",
		largest, largestPath,
	)
}
