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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/statedrain"
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
	tb.Cleanup(func() { require.NoError(tb, set.Close()) })
	return set
}

// ledgerDBs holds the open set of each database, so every ledger a test
// builds over one database shares its set, as ledgers of one node do.
var ledgerDBs sync.Map

// OpenLedgerDB opens the LedgerDB set of db the way the node does at startup:
// whatever ledger state db holds in SQL moves into the set and is deleted
// from SQL, so the set is the only copy before a ledger is built on it. A second call for the same database returns the
// same set. The set closes when tb ends.
func OpenLedgerDB(tb testing.TB, db *database.Database) *utxoset.Set {
	tb.Helper()
	return OpenLedgerDBAt(tb, db, tb.TempDir())
}

// OpenLedgerDBAt is OpenLedgerDB for a set kept under dataDir, where a
// fixture restarted over that directory finds it again.
func OpenLedgerDBAt(
	tb testing.TB,
	db *database.Database,
	dataDir string,
) *utxoset.Set {
	tb.Helper()
	if set, ok := ledgerDBs.Load(db); ok {
		return set.(*utxoset.Set)
	}
	set, err := utxoset.Open(utxoset.Config{
		Dir:           utxoset.DirFor(dataDir),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Only:          true,
		Authoritative: true,
		MemTableSize:  8 << 20,
	})
	require.NoError(tb, err)
	adopt(tb, db, set, dataDir)
	return set
}

// AdoptLedgerDB makes set, which the caller opened and closes, the LedgerDB
// set of db: it takes over db's ledger state as OpenLedgerDB's own does, and
// later OpenLedgerDB calls for db return it.
func AdoptLedgerDB(tb testing.TB, db *database.Database, set *utxoset.Set) {
	tb.Helper()
	adopt(tb, db, set, tb.TempDir())
}

func adopt(
	tb testing.TB,
	db *database.Database,
	set *utxoset.Set,
	dataDir string,
) {
	tb.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ledgerDBs.Store(db, set)
	tb.Cleanup(func() {
		ledgerDBs.Delete(db)
		_ = set.Close()
	})
	var moved nodesettings.FamilyMask
	require.NoError(tb, nodestorage.Reconcile{
		DB:      db,
		Set:     set,
		DataDir: dataDir,
		Logger:  logger,
		Publish: func(m nodesettings.FamilyMask) {
			moved |= m
			set.SetDrainedFamilies(m)
		},
	}.Run(tb.Context()))
	for _, e := range statedrain.Graduated(nodestorage.DrainSource{Set: set}) {
		require.True(
			tb, moved.Has(e.Family),
			"family %s still has its SQL rows", e.Family,
		)
	}
}

// LedgerDBOf returns the set OpenLedgerDB or AdoptLedgerDB gave db, if one is
// open.
func LedgerDBOf(db *database.Database) (*utxoset.Set, bool) {
	set, ok := ledgerDBs.Load(db)
	if !ok {
		return nil, false
	}
	return set.(*utxoset.Set), true
}

// CloseLedgerDB closes the set OpenLedgerDB opened for db, so a fixture can
// reopen it over the same directory as a restarted node does.
func CloseLedgerDB(db *database.Database) error {
	set, ok := ledgerDBs.LoadAndDelete(db)
	if !ok {
		return nil
	}
	return set.(*utxoset.Set).Close()
}
