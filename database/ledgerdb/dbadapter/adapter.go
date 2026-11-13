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

// Package dbadapter implements ledgerdb.LedgerDB and indexerdb.Reader over a
// *database.Database.
package dbadapter

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/types"
)

var (
	_ ledgerdb.LedgerDB = Adapter{}
	_ indexerdb.Reader  = Adapter{}
	_ ledgerdb.Txn      = (*database.Txn)(nil)
)

// Adapter serves the LedgerDB and IndexerDB roles from one *database.Database.
// It is a single pointer, so wrapping a database is free: an Adapter value
// stored in an interface does not allocate.
// Its transactions are *database.Txn values. Their savepoints cover the
// metadata store only: RollbackTo does not undo blob-store writes.
type Adapter struct {
	db *database.Database
}

// New returns an Adapter over db.
func New(db *database.Database) Adapter {
	return Adapter{db: db}
}

// Begin opens a *database.Txn spanning the metadata and blob stores.
func (a Adapter) Begin(ctx context.Context, readWrite bool) ledgerdb.Txn {
	return a.db.Transaction(ctx, readWrite)
}

// BeginLedgerOnly opens a metadata-only *database.Txn.
func (a Adapter) BeginLedgerOnly(
	ctx context.Context,
	readWrite bool,
) ledgerdb.Txn {
	return a.db.MetadataTxn(ctx, readWrite)
}

// Run runs fn in txn through (*database.Txn).Do, or in a new read-write
// transaction when txn is nil.
func (a Adapter) Run(txn ledgerdb.Txn, fn func(ledgerdb.Txn) error) error {
	t := dbTxn(txn)
	if t == nil {
		t = a.db.Transaction(context.Background(), true)
	}
	return t.Do(func(t *database.Txn) error { return fn(t) })
}

// StorageMode reports the database's configured storage mode.
func (a Adapter) StorageMode() string {
	return a.db.StorageMode()
}

// Logger returns the database's logger.
func (a Adapter) Logger() *slog.Logger {
	return a.db.Logger()
}

// ResolveTxCbor resolves a stored transaction body through the database's
// CBOR cache.
func (a Adapter) ResolveTxCbor(
	txn ledgerdb.Txn,
	txHash []byte,
) ([]byte, error) {
	return a.db.CborCache().ResolveTxCbor(dbTxn(txn), txHash)
}

// Txn returns t as a ledgerdb.Txn, keeping a nil t nil. Converting a nil
// *database.Txn directly yields a non-nil interface, which defeats callees
// that treat a nil Txn as "run in your own transaction".
func Txn(t *database.Txn) ledgerdb.Txn {
	if t == nil {
		return nil
	}
	return t
}

// Unwrap returns the *database.Txn behind txn, or nil for a nil txn. It is
// for callers that still need the database directly while they move onto the
// LedgerDB.
func Unwrap(txn ledgerdb.Txn) *database.Txn {
	return dbTxn(txn)
}

// dbTxn returns the *database.Txn behind txn. A transaction from another
// LedgerDB implementation is a programming error.
func dbTxn(txn ledgerdb.Txn) *database.Txn {
	if txn == nil {
		return nil
	}
	t, ok := txn.(*database.Txn)
	if !ok {
		panic(
			fmt.Sprintf(
				"dbadapter: transaction %T is not a *database.Txn",
				txn,
			),
		)
	}
	return t
}

// metaTxn returns the metadata-store handle of txn, or nil for a nil txn.
func metaTxn(txn ledgerdb.Txn) types.Txn {
	t := dbTxn(txn)
	if t == nil {
		return nil
	}
	return t.Metadata()
}

// wrapTxn converts a *database.Txn result without turning a nil pointer into
// a non-nil interface.
func wrapTxn(t *database.Txn, err error) (ledgerdb.Txn, error) {
	if t == nil {
		return nil, err
	}
	return t, err
}
