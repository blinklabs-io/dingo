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
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// maxTestBlobFileBytes bounds any single file a test database is allowed to
// reserve. badger truncates its memtable WAL and value log to their
// configured sizes when it opens a store; on NTFS that truncation really
// allocates, so the production defaults (1 GiB value log, 128 MiB memtable)
// exhaust the Windows runner's disk once a package builds several stores.
//
// The value log is the larger of the two and badger opens it at twice
// ValueLogFileSize so the last entry always fits (badger/v4 value.go:536).
// The bound is derived from testutil's sizing rather than restated, so the
// two cannot disagree about what the reservation rule is.
const maxTestBlobFileBytes = 2 * testutil.TestBadgerValueLogFileSize

// largestFile reports the biggest file under dir and its path. badger
// truncates its value log and memtable WAL to their configured sizes on open,
// and os.Stat reports that truncated length even where the filesystem stores
// the file sparsely, so this measures the reservation on every platform
// rather than only where it costs real blocks.
func largestFile(tb testing.TB, dir string) (int64, string) {
	tb.Helper()
	var size int64
	var path string
	err := filepath.WalkDir(
		dir,
		func(p string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Size() > size {
				size, path = info.Size(), p
			}
			return nil
		},
	)
	if err != nil {
		tb.Fatalf("walk %s: %v", dir, err)
	}
	return size, path
}

// requireBoundedBadgerReservation asserts that the store under dataDir opened
// on disk and that no file it reserved exceeds maxTestBlobFileBytes.
//
// The walk alone would pass vacuously if the store never wrote to disk, so
// the value log badger creates on open is stat'ed first. Walking the whole
// directory rather than stat'ing only that file also bounds the memtable WAL,
// the file that actually failed to open on the Windows runner.
func requireBoundedBadgerReservation(tb testing.TB, dataDir string) {
	tb.Helper()
	vlog := filepath.Join(dataDir, "blob", "000001.vlog")
	if _, err := os.Stat(vlog); err != nil {
		tb.Fatalf("stat value log: %v", err)
	}
	largest, path := largestFile(tb, dataDir)
	tb.Logf("largest reserved file: %s (%d bytes)", path, largest)
	if largest > int64(maxTestBlobFileBytes) {
		tb.Fatalf(
			"a test database reserved %d bytes for %s, want at most %d: "+
				"the bounded badger sizes are not reaching the store",
			largest,
			path,
			maxTestBlobFileBytes,
		)
	}
}

// TestNewDatabaseBoundsBadgerFileReservation pins what the default fixture
// reserves.
func TestNewDatabaseBoundsBadgerFileReservation(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	if _, err := NewDatabase(t, &database.Config{DataDir: dataDir}); err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}

	requireBoundedBadgerReservation(t, dataDir)
}

// TestBoundedBadgerSizesSurviveAPartialCallerConfig pins the per-key merge in
// NewDatabaseWithOptions: a caller config that sets an unrelated knob and says
// nothing about sizes must still get the bounded ones. The badger provider
// decodes its config from the production defaults, so applying testutil's
// sizes only when the caller supplied no config at all lets the 2 GiB
// reservation back in.
func TestBoundedBadgerSizesSurviveAPartialCallerConfig(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	if _, err := NewDatabaseWithOptions(t, Options{
		Config: &database.Config{DataDir: dataDir},
		Blob: StorageProvider{
			// An unrelated knob, saying nothing about sizes.
			Config: map[string]any{"gc": false},
		},
	}); err != nil {
		t.Fatalf("NewDatabaseWithOptions: %v", err)
	}

	requireBoundedBadgerReservation(t, dataDir)
}
