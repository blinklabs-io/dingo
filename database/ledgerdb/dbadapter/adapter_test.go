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

package dbadapter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func newAdapter(t *testing.T) dbadapter.Adapter {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return dbadapter.New(db)
}

func TestRunCommitsWhenFnSucceeds(t *testing.T) {
	t.Parallel()
	a := newAdapter(t)
	err := a.Run(
		a.Begin(context.Background(), true),
		func(txn ledgerdb.Txn) error {
			return a.SetSyncState("seam-key", "committed", txn)
		},
	)
	require.NoError(t, err)
	got, err := a.GetSyncState("seam-key", nil)
	require.NoError(t, err)
	require.Equal(t, "committed", got)
}

func TestRunRollsBackWhenFnFails(t *testing.T) {
	t.Parallel()
	a := newAdapter(t)
	errBoom := errors.New("boom")
	err := a.Run(
		a.Begin(context.Background(), true),
		func(txn ledgerdb.Txn) error {
			if err := a.SetSyncState("seam-key", "discarded", txn); err != nil {
				return err
			}
			return errBoom
		},
	)
	require.ErrorIs(t, err, errBoom)
	got, err := a.GetSyncState("seam-key", nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestRunRollsBackWhenFnPanics(t *testing.T) {
	t.Parallel()
	a := newAdapter(t)
	err := a.Run(
		a.Begin(context.Background(), true),
		func(txn ledgerdb.Txn) error {
			if err := a.SetSyncState("seam-key", "discarded", txn); err != nil {
				return err
			}
			panic("boom")
		},
	)
	require.ErrorIs(t, err, database.ErrTxnPanic)
	got, err := a.GetSyncState("seam-key", nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestNilTxnRunsInItsOwnTransaction(t *testing.T) {
	t.Parallel()
	a := newAdapter(t)
	require.NoError(t, a.SetSyncState("seam-key", "nil-interface", nil))
	got, err := a.GetSyncState("seam-key", nil)
	require.NoError(t, err)
	require.Equal(t, "nil-interface", got)

	// A nil *database.Txn stored in the interface is still nil to the
	// database, not a transaction to dereference.
	var typedNil *database.Txn
	require.NoError(t, a.SetSyncState("seam-key", "typed-nil", typedNil))
	got, err = a.GetSyncState("seam-key", typedNil)
	require.NoError(t, err)
	require.Equal(t, "typed-nil", got)
}

func TestUnwrapReturnsTheDatabaseTxn(t *testing.T) {
	t.Parallel()
	a := newAdapter(t)
	txn := a.Begin(context.Background(), false)
	defer txn.Release()
	dbTxn, ok := txn.(*database.Txn)
	require.True(t, ok)
	require.Same(t, dbTxn, dbadapter.Unwrap(txn))
	require.Nil(t, dbadapter.Unwrap(nil))
}

type foreignTxn struct{ ledgerdb.Txn }

func TestForeignTxnPanics(t *testing.T) {
	t.Parallel()
	a := newAdapter(t)
	require.PanicsWithValue(
		t,
		"dbadapter: transaction dbadapter_test.foreignTxn is not a *database.Txn",
		func() { _, _ = a.GetSyncState("seam-key", foreignTxn{}) },
	)
}

func TestAdapterInInterfaceDoesNotAllocate(t *testing.T) {
	// Not t.Parallel: testing.AllocsPerRun is a process-wide measurement.
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	var sink ledgerdb.LedgerDB
	allocs := testing.AllocsPerRun(100, func() {
		sink = dbadapter.New(db)
	})
	require.NotNil(t, sink)
	require.Zero(t, allocs)
}

func TestTxnKeepsNilDatabaseTxnNil(t *testing.T) {
	t.Parallel()
	var typedNil *database.Txn
	var direct ledgerdb.Txn = typedNil
	// require.NotNil looks through the interface, so compare directly.
	require.True(
		t,
		direct != nil,
		"a nil pointer converts to a non-nil interface",
	)
	require.Nil(t, dbadapter.Txn(typedNil))

	a := newAdapter(t)
	txn := a.Begin(context.Background(), false)
	defer txn.Release()
	dbTxn, ok := txn.(*database.Txn)
	require.True(t, ok)
	require.Same(t, dbTxn, dbadapter.Txn(dbTxn))
}
