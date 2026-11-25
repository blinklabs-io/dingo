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

package utxoset_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A set the ledger starts at origin holds exactly what loading the empty
// ledger state of a v0.94.2 database at origin gives it: every family loaded
// with no rows, and the same tip and marks.
func TestReconcileAtOriginStartsTheEmptyLedgerState(t *testing.T) {
	t.Parallel()
	released, err := dbtest.NewReleasedDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	ledgerDB, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)

	fromReleased := t.TempDir()
	set, err := utxoset.Open(utxoset.Config{Dir: fromReleased, Only: true})
	require.NoError(t, err)
	require.NoError(t, set.ReconcileReleased(
		t.Context(), dbadapter.New(released),
	))
	require.NoError(t, set.Close())

	started := t.TempDir()
	set, err = utxoset.Open(utxoset.Config{Dir: started, Only: true})
	require.NoError(t, err)
	require.NoError(t, set.Reconcile(
		t.Context(), dbadapter.New(ledgerDB),
		func(context.Context, ocommon.Point) (bool, error) {
			return false, nil
		},
	))
	require.NoError(t, set.Close())

	want := dumpBadger(t, fromReleased)
	require.NotEmpty(t, want)
	require.Equal(t, want, dumpBadger(t, started))
}

// The node serves every ledger-state family from the set before it
// reconciles, and a brand-new set at origin still starts empty then.
func TestReconcileAtOriginStartsEmptyWithEveryFamilyServedByTheSet(
	t *testing.T,
) {
	t.Parallel()
	ledgerDB, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.ServeLedgerOnly()

	require.NoError(t, set.Reconcile(
		t.Context(), dbadapter.New(ledgerDB),
		func(context.Context, ocommon.Point) (bool, error) {
			return false, nil
		},
	))
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, tip.Slot)
}

func dumpBadger(t *testing.T, dir string) []string {
	t.Helper()
	db, err := badger.Open(
		badger.DefaultOptions(dir).WithLogger(nil).WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var out []string
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			out = append(out, fmt.Sprintf(
				"%x=%x", bytes.Clone(it.Item().Key()), v,
			))
		}
		return nil
	}))
	return out
}

// Past origin nothing outside the set holds the ledger state, so a set that
// lacks a family there stops the node instead of loading it.
func TestReconcileRefusesASetPastOriginThatLacksAFamily(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{Only: true})
	applySlots(t, set, 3)
	db := ledgerAt(t, slotPoint(3))
	err := set.Reconcile(t.Context(), dbadapter.New(db), neverResume)
	require.ErrorContains(t, err, "does not hold the")
	require.ErrorContains(t, err, "ledger state")
}
