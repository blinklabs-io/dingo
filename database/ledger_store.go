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

package database

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
)

// LedgerMigratedKey is the block store's chain-state key recording that the
// ledger state of this data directory lives in the LedgerDB alone. A block
// store gets it when it is first opened, or as the last write of the move out
// of a v0.94.2 metadata store.
const LedgerMigratedKey = "ledger_migrated"

// LedgerStoreState says where a block store's ledger state lives.
type LedgerStoreState int

const (
	// LedgerStoreCurrent is a block store whose ledger state is in the
	// LedgerDB.
	LedgerStoreCurrent LedgerStoreState = iota
	// LedgerStoreFresh is a block store no release has opened.
	LedgerStoreFresh
	// LedgerStoreReleased is a block store v0.94.2 opened: its ledger state
	// is in the metadata store until the migration moves it.
	LedgerStoreReleased
)

// ErrLedgerNotMigrated is returned when the ledger's database is opened over
// a block store whose ledger state is still in a v0.94.2 metadata store.
var ErrLedgerNotMigrated = errors.New(
	"the ledger state of this database is still in its v0.94.2 metadata " +
		"store; the migration must run before the ledger opens it",
)

// ReadLedgerStoreState reports where the ledger state of the data directory
// that store belongs to lives. v0.94.2 mints the block store's identity the
// first time it opens a database, before anything else is written, so an
// identity without the marker is a store that release opened. This build
// writes the marker before it mints the identity.
func ReadLedgerStoreState(store blob.BlobStore) (LedgerStoreState, error) {
	if store == nil || isNilStore(store) {
		return 0, types.ErrBlobStoreUnavailable
	}
	txn := store.NewTransaction(false)
	defer func() { _ = txn.Rollback() }()
	if _, err := store.Get(
		txn, chainStateBlobKey(LedgerMigratedKey),
	); err == nil {
		return LedgerStoreCurrent, nil
	} else if !errors.Is(err, types.ErrBlobKeyNotFound) {
		return 0, fmt.Errorf("read the ledger migration marker: %w", err)
	}
	if _, err := store.Get(txn, blobStoreIDKey); err == nil {
		return LedgerStoreReleased, nil
	} else if !errors.Is(err, types.ErrBlobKeyNotFound) {
		return 0, fmt.Errorf("read blob store id: %w", err)
	}
	return LedgerStoreFresh, nil
}

// MarkLedgerMigrated records in store that its ledger state lives in the
// LedgerDB, and syncs it: a marker lost to an unclean host shutdown would send
// the next start back to a metadata store the ledger no longer writes.
func MarkLedgerMigrated(store blob.BlobStore) error {
	if store == nil || isNilStore(store) {
		return types.ErrBlobStoreUnavailable
	}
	txn := store.NewTransaction(true)
	if err := store.Set(
		txn, chainStateBlobKey(LedgerMigratedKey), []byte("1"),
	); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("write the ledger migration marker: %w", err)
	}
	if err := txn.Commit(); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("commit the ledger migration marker: %w", err)
	}
	if err := store.Sync(); err != nil {
		return fmt.Errorf("sync the ledger migration marker: %w", err)
	}
	return nil
}

// settleLedgerStore marks a fresh block store and refuses one whose ledger
// state is still in a v0.94.2 metadata store. It runs before anything else
// the ledger's database writes, in particular before the block store's
// identity is minted.
func (d *Database) settleLedgerStore() error {
	if d.config.Role != "" {
		return nil
	}
	store, release := d.PinBlob()
	defer release()
	state, err := ReadLedgerStoreState(store)
	if err != nil {
		return err
	}
	switch state {
	case LedgerStoreFresh:
		return MarkLedgerMigrated(store)
	case LedgerStoreReleased:
		return ErrLedgerNotMigrated
	case LedgerStoreCurrent:
	}
	return nil
}
