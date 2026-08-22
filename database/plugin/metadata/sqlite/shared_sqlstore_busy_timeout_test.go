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
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// The driver applies _pragma directives verbatim, in the order they appear in
// the DSN -- github.com/glebarez/go-sqlite does not carry modernc's fix that
// hoists busy_timeout ahead of the rest. So any pragma listed before
// busy_timeout runs with no busy handler installed and fails immediately on
// contention rather than waiting.
//
// Every pragma after it that touches the database file -- cache_size and
// mmap_size both do -- is then one that gives up instantly instead of
// waiting out a concurrent writer.
//
// Pin the ordering directly. A concurrency test alone would only fail when
// the race is actually lost, which makes an ordering regression look flaky
// instead of broken.
func TestCommonPragmasSetBusyTimeoutFirst(t *testing.T) {
	pragmas := parsePragmas(t, sqliteCommonPragmas)
	require.NotEmpty(t, pragmas, "no pragmas parsed out of the DSN fragment")
	require.NotContains(t, sqliteCommonPragmas, "journal_mode",
		"journal_mode must not be a per-connection pragma; "+
			"ensureWALJournalMode owns the conversion")
	require.Truef(
		t,
		strings.HasPrefix(pragmas[0], "busy_timeout("),
		"busy_timeout must be the first pragma in the DSN so it is in "+
			"effect before any pragma that touches the database file; got %v",
		pragmas,
	)
}

// The backup connection builds its own DSN rather than reusing the shared
// fragment, and it opens against a database a running node already holds
// open -- so it contends by construction, not just at first open.
func TestBackupDSNSetsBusyTimeoutFirst(t *testing.T) {
	dsn := backupSourceDSN("/tmp/example/metadata.sqlite")
	pragmas := parsePragmas(t, dsn)
	require.NotEmpty(t, pragmas, "no pragmas parsed out of the backup DSN")
	require.Truef(
		t,
		strings.HasPrefix(pragmas[0], "busy_timeout("),
		"busy_timeout must be the first pragma in the backup DSN; got %v",
		pragmas,
	)
}

// parsePragmas pulls the _pragma values out of a DSN (or DSN fragment) in the
// order the driver will execute them.
func parsePragmas(t *testing.T, dsn string) []string {
	t.Helper()
	var out []string
	for part := range strings.SplitSeq(dsn, "&") {
		if v, ok := strings.CutPrefix(part, "_pragma="); ok {
			out = append(out, v)
		}
	}
	return out
}

// The ordering test above pins the cause; this pins the symptom it produced.
// Two openers racing to create the same metadata database must both get
// through, because the loser waits out the winner's journal_mode conversion
// instead of failing on it. Before the fix this surfaced as
// "ping write database: database is locked (5) (SQLITE_BUSY)" from whichever
// opener lost, which is what made TestPhase1ConcurrentFirstOpenOneWinnerOne-
// Mismatch flaky in CI: the loser died before it could reach the node
// settings comparison the test was actually asserting on.
func TestConcurrentFirstOpenDoesNotFailOnLockedDatabase(t *testing.T) {
	const openers = 8
	dataDir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, openers)
	wg.Add(openers)
	for i := range openers {
		go func() {
			defer wg.Done()
			store, err := NewSQLStore(
				Config{DataDir: dataDir},
				metadata.ProviderDependencies{},
			)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() {
				_ = store.Close()
			}()
			// Start, not construction: constructing only builds the pools,
			// and the WAL conversion is deliberately deferred to Start so
			// that constructing a store does not materialise the database
			// file. Start is also where the original failure surfaced, as
			// "ping write database: database is locked".
			errs[i] = store.Start(t.Context())
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "opener %d failed to start", i)
	}
}
