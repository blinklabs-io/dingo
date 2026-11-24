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
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	sqlite "modernc.org/sqlite"
)

// ledgerDirGuards maps a cleaned data directory to its metadata.StatementGuard.
var ledgerDirGuards sync.Map

// The driver's hook list is read by every connection open and unsynchronised
// against a write, so the hook is registered before any test runs rather than
// when the first guard is armed.
func init() {
	sqlite.RegisterConnectionHook(checkLedgerDirConnection)
}

// guardLedgerDir puts every SQLite connection opened on a file under dir
// before guard. The ledger's database has no metadata store for a statement
// guard to sit on, so SQL run on its behalf can only come from a connection
// someone opened in its data directory: the guard sees the open, and an armed
// guard refuses it, so no statement can follow.
func guardLedgerDir(
	tb testing.TB,
	dir string,
	guard metadata.StatementGuard,
) error {
	if dir == "" {
		return errors.New(
			"a statement guard on the ledger's database needs a data directory",
		)
	}
	dir = filepath.Clean(dir)
	// A reopen of the same directory replaces the guard it was opened with.
	ledgerDirGuards.Store(dir, guard)
	tb.Cleanup(func() { ledgerDirGuards.Delete(dir) })
	return nil
}

func checkLedgerDirConnection(_ sqlite.ExecQuerierContext, dsn string) error {
	path := strings.TrimPrefix(dsn, "file:")
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	if path == "" || path == ":memory:" {
		return nil
	}
	path = filepath.Clean(path)
	var err error
	ledgerDirGuards.Range(func(key, value any) bool {
		dir := key.(string)
		if path != dir &&
			!strings.HasPrefix(path, dir+string(filepath.Separator)) {
			return true
		}
		statement := "open " + dsn
		// A connection opened before the test arms its guard would stay in
		// its pool and run statements unseen, so an SQLRefusal records
		// every open whatever its state.
		if refusal, ok := value.(*SQLRefusal); ok {
			err = refusal.observe(statement)
		} else {
			err = value.(metadata.StatementGuard).CheckStatement(statement)
		}
		return false
	})
	return err
}
