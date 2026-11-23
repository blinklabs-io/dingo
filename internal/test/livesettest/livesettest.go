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

// Package livesettest opens the LedgerDB live UTxO set that every
// ledger.LedgerState requires, for tests that build a ledger.
package livesettest

import (
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// Open opens an empty live UTxO set in a temporary directory and closes it
// when tb ends, after anything registered later, such as the ledger reading
// it. It holds nothing until an import loads it; a ledger is built on
// OpenLedgerDB's set.
func Open(tb testing.TB) *utxoset.Set {
	tb.Helper()
	set, err := utxoset.Open(utxoset.Config{
		Dir:          tb.TempDir(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MemTableSize: 8 << 20,
	})
	require.NoError(tb, err)
	closing := set
	tb.Cleanup(func() {
		target := closing
		closing = nil
		require.NoError(tb, target.Close())
	})
	return set
}

// ledgerDBs holds the open set of each database, so every ledger a test
// builds over one database shares its set, as ledgers of one node do.
var ledgerDBs sync.Map

type ledgerDB struct {
	set *utxoset.Set
	// dir is the data directory the set was opened under, or empty for a set
	// the caller opened.
	dir string
}

// OpenLedgerDB opens the LedgerDB set of db the way the node does at startup:
// whatever ledger state db holds in SQL moves into the set and is deleted
// from SQL, so the set is the only copy before a ledger is built on it. A
// second call for the same database returns the same set. The set closes
// when tb ends.
func OpenLedgerDB(tb testing.TB, db *database.Database) *utxoset.Set {
	tb.Helper()
	if open, ok := ledgerDBs.Load(db); ok {
		return open.(ledgerDB).set
	}
	return OpenLedgerDBAt(tb, db, tb.TempDir())
}

// OpenLedgerDBAt is OpenLedgerDB for a set kept under dataDir, where a
// fixture restarted over that directory finds it again. A set already open
// for db must be the one under dataDir: a fixture that seeded db before
// naming the directory would otherwise build its ledger, and restart, on a
// set that holds none of what it seeded.
func OpenLedgerDBAt(
	tb testing.TB,
	db *database.Database,
	dataDir string,
) *utxoset.Set {
	tb.Helper()
	if open, ok := ledgerDBs.Load(db); ok {
		require.Equal(
			tb, dataDir, open.(ledgerDB).dir,
			"the database's LedgerDB set is open under another directory",
		)
		return open.(ledgerDB).set
	}
	// A set already under dataDir is a restarted node's, which took the
	// database's SQL ledger state over when it first opened.
	_, statErr := os.Stat(utxoset.DirFor(dataDir))
	reopened := statErr == nil
	set, err := utxoset.Open(utxoset.Config{
		Dir:           utxoset.DirFor(dataDir),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Only:          true,
		Authoritative: true,
		MemTableSize:  8 << 20,
	})
	require.NoError(tb, err)
	adopt(tb, db, set, dataDir, dataDir, !reopened)
	return set
}

// AdoptLedgerDB makes set, which the caller opened and closes, the LedgerDB
// set of db: it takes over db's ledger state as OpenLedgerDB's own does, and
// later OpenLedgerDB calls for db return it. db must have no set open yet,
// or what was seeded into that one would not reach set.
func AdoptLedgerDB(tb testing.TB, db *database.Database, set *utxoset.Set) {
	tb.Helper()
	adopt(tb, db, set, tb.TempDir(), "", true)
}

func adopt(
	tb testing.TB,
	db *database.Database,
	set *utxoset.Set,
	dataDir string,
	dir string,
	takeOver bool,
) {
	tb.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	entry := ledgerDB{set: set, dir: dir}
	if _, loaded := ledgerDBs.LoadOrStore(db, entry); loaded {
		require.FailNow(tb, "the database already has a LedgerDB set open")
	}
	// testing keeps a finished test's cleanups reachable until the run ends;
	// dropping the references lets the closed set and database be collected.
	closingDB, closing := db, entry
	tb.Cleanup(func() {
		key, open := closingDB, closing
		closingDB, closing = nil, ledgerDB{}
		ledgerDBs.CompareAndDelete(key, open)
		_ = open.set.Close()
	})
	// A database without a metadata store has no SQL ledger state to hand
	// over.
	if takeOver && db.Metadata() != nil {
		require.NoError(tb, nodestorage.MigrateUtxoRows(
			tb.Context(), db, set, dataDir, logger,
		))
		require.NoError(
			tb, set.ReconcileReleased(tb.Context(), dbadapter.New(db)),
		)
	}
	require.NoError(tb, nodestorage.Reconcile{
		DB:     db,
		Set:    set,
		Logger: logger,
	}.Run(tb.Context()))
}

// LedgerDBOf returns the set OpenLedgerDB or AdoptLedgerDB gave db, if one is
// open.
func LedgerDBOf(db *database.Database) (*utxoset.Set, bool) {
	open, ok := ledgerDBs.Load(db)
	if !ok {
		return nil, false
	}
	return open.(ledgerDB).set, true
}

// CloseLedgerDB closes the set OpenLedgerDB opened for db, so a fixture can
// reopen it over the same directory as a restarted node does.
func CloseLedgerDB(db *database.Database) error {
	open, ok := ledgerDBs.LoadAndDelete(db)
	if !ok {
		return nil
	}
	return open.(ledgerDB).set.Close()
}
