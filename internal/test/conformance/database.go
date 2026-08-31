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
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// realBackendOptions configures the real, production metadata plugin that
// backs a conformance DingoStateManager's *database.Database. The blob
// store is always the local Badger provider -- UTxO/tx CBOR bytes have no
// reason to leave the machine running the suite even when the metadata
// store is a remote Postgres/MySQL server, matching how a real Dingo node
// is normally deployed (local blob store, possibly-remote metadata store).
type realBackendOptions struct {
	// dataDir is where the local Badger blob store (and, for the sqlite
	// metadata provider, the metadata file too) keeps its files. Required.
	dataDir string

	// metadataName is the registered plugin name ("sqlite", "postgres", or
	// "mysql").
	metadataName string

	// metadataConfig is the provider-specific config map, decoded the same
	// way plugin.Resolve decodes any other provider's YAML/env config (e.g.
	// {"dsn": "..."} for postgres/mysql).
	metadataConfig map[string]any

	// registerMetadata installs the metadata provider on a fresh host.
	registerMetadata func(*hostplugin.Host) error
}

// openRealDatabase composes a real blob+metadata backed *database.Database
// for the conformance harness. It mirrors internal/test/dbtest's
// NewDatabaseWithOptions composition (fresh plugin.Host -> RegisterProvider
// -> plugin.Resolve -> database.New) -- the same plugin-resolution path the
// production node uses at startup -- but without a testing.TB dependency,
// so it can be called from non-test constructors (NewDingoStateManager,
// NewDingoPostgresStateManager, NewDingoMysqlStateManager) and propagate a
// construction error to the caller instead of failing the test process
// outright. That propagation is what the "invalid DSN must fail
// construction" acceptance tests assert on.
func openRealDatabase(
	opts realBackendOptions,
) (*database.Database, *hostplugin.Host, error) {
	if opts.dataDir == "" {
		return nil, nil, errors.New(
			"conformance: real backend requires a data directory",
		)
	}
	if opts.registerMetadata == nil {
		return nil, nil, errors.New(
			"conformance: real backend requires a metadata provider registrar",
		)
	}

	host := hostplugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, nil, fmt.Errorf(
			"register badger blob provider: %w",
			err,
		)
	}
	if err := opts.registerMetadata(host); err != nil {
		return nil, nil, fmt.Errorf(
			"register %s metadata provider: %w",
			opts.metadataName,
			err,
		)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	blobStore, err := hostplugin.Resolve[blob.BlobStore](
		ctx, host,
		hostplugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{DataDir: opts.dataDir, Logger: logger},
	)
	if err != nil {
		_ = host.Stop(ctx)
		return nil, nil, fmt.Errorf("resolve badger blob store: %w", err)
	}

	metadataStore, err := hostplugin.Resolve[metadata.MetadataStore](
		ctx, host,
		hostplugin.CapabilityStorageMetadata, opts.metadataName,
		opts.metadataConfig,
		metadata.ProviderDependencies{
			DataDir: opts.dataDir,
			Logger:  logger,
		},
	)
	if err != nil {
		_ = host.Stop(ctx)
		return nil, nil, fmt.Errorf(
			"resolve %s metadata store: %w",
			opts.metadataName,
			err,
		)
	}

	db, err := database.New(
		&database.Config{DataDir: opts.dataDir, Logger: logger},
		database.Stores{Blob: blobStore, Metadata: metadataStore},
	)
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		_ = host.Stop(ctx)
		return nil, nil, fmt.Errorf("construct database: %w", err)
	}
	return db, host, nil
}

// closeRealDatabase closes db and stops host, in that dependency order,
// joining any errors from both. Either argument may be nil.
func closeRealDatabase(db *database.Database, host *hostplugin.Host) error {
	var err error
	if db != nil {
		err = db.Close()
	}
	if host != nil {
		if stopErr := host.Stop(context.Background()); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
	}
	return err
}
