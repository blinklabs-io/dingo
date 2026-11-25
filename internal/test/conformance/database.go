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

package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	dledger "github.com/blinklabs-io/dingo/ledger"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// ledgerBackend is the storage a conformance state manager replays a vector
// against: the ledger's database, which keeps no metadata store, its LedgerDB
// set, and the LedgerDB a ledger-state import writes the set through.
type ledgerBackend struct {
	db   *database.Database
	host *hostplugin.Host
	set  *utxoset.Set
	ldb  *dledger.ImportLedgerDB
}

// openLedgerBackend opens the ledger's database and its LedgerDB set under
// dataDir the way the node does before a Mithril import: the block store
// through plugin resolution, then the set, which is reconciled with the
// database before anything is written to it.
func openLedgerBackend(dataDir string) (*ledgerBackend, error) {
	if dataDir == "" {
		return nil, errors.New(
			"conformance: ledger backend requires a data directory",
		)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	host := hostplugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register badger blob provider: %w", err)
	}
	blobStore, err := hostplugin.Resolve[blob.BlobStore](
		ctx, host,
		hostplugin.CapabilityStorageBlob, "badger",
		testutil.BadgerBlobConfig(),
		blob.ProviderDependencies{DataDir: dataDir, Logger: logger},
	)
	if err != nil {
		_ = host.Stop(ctx)
		return nil, fmt.Errorf("resolve badger blob store: %w", err)
	}
	db, err := database.New(
		ctx,
		// alonzopparams:word-not-required -- this harness creates the
		// database it opens, so it never holds a protocol-parameter row
		// written before gouroboros v0.205.7.
		&database.Config{DataDir: dataDir, Logger: logger},
		database.Stores{Blob: blobStore},
	)
	if err != nil {
		_ = host.Stop(ctx)
		return nil, fmt.Errorf("construct database: %w", err)
	}
	set, err := utxoset.Open(utxoset.Config{
		Dir:           utxoset.DirFor(dataDir),
		Logger:        logger,
		Only:          true,
		Authoritative: true,
		MemTableSize:  8 << 20,
	})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("open LedgerDB set: %w", err),
			closeLedgerStores(db, host),
		)
	}
	b := &ledgerBackend{
		db:   db,
		host: host,
		set:  set,
		ldb:  dledger.NewImportLedgerDB(db, set, logger),
	}
	if err := (nodestorage.Reconcile{
		DB:     db,
		Set:    set,
		Logger: logger,
	}).Run(ctx); err != nil {
		return nil, errors.Join(
			fmt.Errorf("reconcile LedgerDB set: %w", err),
			b.close(),
		)
	}
	return b, nil
}

// close closes the set, then the database and its provider host, joining
// their errors. A nil backend closes nothing.
func (b *ledgerBackend) close() error {
	if b == nil {
		return nil
	}
	var err error
	if b.set != nil {
		err = b.set.Close()
	}
	return errors.Join(err, closeLedgerStores(b.db, b.host))
}

func closeLedgerStores(db *database.Database, host *hostplugin.Host) error {
	var err error
	if db != nil {
		err = db.Close()
	}
	if host != nil {
		err = errors.Join(err, host.Stop(context.Background()))
	}
	return err
}
