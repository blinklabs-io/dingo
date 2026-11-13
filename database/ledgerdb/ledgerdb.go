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

package ledgerdb

import (
	"context"
	"log/slog"
)

// Txn is a LedgerDB transaction.
//
// Every method that takes a Txn accepts nil, which runs that call in its own
// transaction. A Txn is used by one goroutine at a time.
type Txn interface {
	// Commit makes the transaction's writes durable and finishes it.
	Commit() error
	// Rollback discards the transaction's writes and finishes it.
	Rollback() error
	// Release finishes the transaction, rolling back anything not
	// committed. It is safe after Commit or Rollback.
	Release()
	// AfterCommit runs fn once the transaction commits. It never runs if the
	// transaction rolls back.
	AfterCommit(fn func())
	// OnFinish runs fn when the transaction finishes, whether it commits or
	// rolls back.
	OnFinish(fn func())
	// SavePoint marks a point that RollbackTo can return to.
	SavePoint(name string) error
	// RollbackTo discards the writes made since the savepoint name, keeping
	// the transaction open. An implementation that cannot undo some kinds of
	// write documents which; callers must clean those up themselves.
	RollbackTo(name string) error
	// IsReadWrite reports whether the transaction may write.
	IsReadWrite() bool
	// IsCommitted reports whether Commit has succeeded.
	IsCommitted() bool
}

// LedgerDB is the ledger state consensus reads and writes.
type LedgerDB interface {
	// Begin opens a transaction. The caller must Commit, Rollback or
	// Release it.
	Begin(ctx context.Context, readWrite bool) Txn
	// BeginLedgerOnly opens a transaction over ledger state only. Reads that
	// need block bytes, such as an output's CBOR, must use Begin instead.
	// A read-only one does not contend with writers.
	BeginLedgerOnly(ctx context.Context, readWrite bool) Txn
	// Run calls fn with txn, then commits if fn returns nil and rolls back
	// otherwise. A panic in fn rolls back and is returned as an error. txn is
	// finished either way.
	Run(txn Txn, fn func(Txn) error) error
	// StorageMode reports the node's storage mode, "core" or "api".
	StorageMode() string
	// Logger returns the logger the store reports through. It may be nil.
	Logger() *slog.Logger

	UtxoStore
	AccountStore
	RewardStore
	PoolStore
	SnapshotStore
	GovernanceStore
	EpochStore
	ChainStore
	Applier
	TxHistory
	LeiosStore
}
