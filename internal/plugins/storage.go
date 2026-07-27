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

package plugins

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
)

// StorageSelections identifies the providers and provider-owned
// configuration for the two storage capabilities.
type StorageSelections struct {
	Blob     plugin.Selection
	Metadata plugin.Selection
}

// StorageDependencies contains shared application settings injected into
// storage providers. These settings are deliberately not provider config.
type StorageDependencies struct {
	DataDir        string
	RunMode        string
	StorageMode    string
	MaxConnections int
	Logger         *slog.Logger
	PromRegistry   prometheus.Registerer
}

// ResolveStorage starts the selected storage providers in dependency order.
// The supplied host owns their lifecycle; callers must stop it if resolution
// fails after one provider has started.
func ResolveStorage(
	ctx context.Context,
	host *plugin.Host,
	selections StorageSelections,
	deps StorageDependencies,
) (database.Stores, error) {
	blobStore, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		selections.Blob.Provider,
		selections.Blob.Config,
		blob.ProviderDependencies{
			DataDir:      deps.DataDir,
			RunMode:      deps.RunMode,
			StorageMode:  deps.StorageMode,
			Logger:       deps.Logger,
			PromRegistry: deps.PromRegistry,
		},
	)
	if err != nil {
		return database.Stores{}, fmt.Errorf("resolve blob storage: %w", err)
	}
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		selections.Metadata.Provider,
		selections.Metadata.Config,
		metadata.ProviderDependencies{
			DataDir:        deps.DataDir,
			StorageMode:    deps.StorageMode,
			MaxConnections: deps.MaxConnections,
			Logger:         deps.Logger,
			PromRegistry:   deps.PromRegistry,
		},
	)
	if err != nil {
		return database.Stores{}, fmt.Errorf(
			"resolve metadata storage: %w",
			err,
		)
	}
	return database.Stores{Blob: blobStore, Metadata: metadataStore}, nil
}

// DatabaseRuntime is a composed database plus the plugin host that owns its
// injected stores. Close preserves database-before-store shutdown ordering.
type DatabaseRuntime struct {
	Database      *database.Database
	Host          *plugin.Host
	once          sync.Once
	err           error
	recoveryError error
}

// RecoveryError reports a recoverable error encountered while opening the
// database. OpenDatabase returns these errors on the owned runtime instead of
// as its error result, so a non-nil returned error always means there is no
// live runtime for the caller to close.
func (r *DatabaseRuntime) RecoveryError() error {
	if r == nil {
		return nil
	}
	return r.recoveryError
}

// Close stops database-owned resources before provider-owned stores.
func (r *DatabaseRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		if r.Database != nil {
			r.err = errors.Join(r.err, r.Database.Close()) //nolint:contextcheck
		}
		if r.Host != nil {
			r.err = errors.Join(r.err, r.Host.Stop(ctx))
		}
	})
	return r.err
}

// OpenDatabase explicitly composes a fresh host, storage providers, and a
// database. A non-nil error is returned only after all started resources have
// been stopped. Recoverable database initialization errors are exposed through
// DatabaseRuntime.RecoveryError on the successfully owned runtime.
func OpenDatabase(
	ctx context.Context,
	dbConfig *database.Config,
	selections StorageSelections,
	deps StorageDependencies,
) (*DatabaseRuntime, error) {
	host, err := NewHost()
	if err != nil {
		return nil, err
	}
	stores, err := ResolveStorage(ctx, host, selections, deps)
	if err != nil {
		_ = host.Stop(context.WithoutCancel(ctx))
		return nil, err
	}
	db, err := database.New(dbConfig, stores) //nolint:contextcheck
	runtime := &DatabaseRuntime{Database: db, Host: host}
	if db == nil {
		closeErr := runtime.Close(context.WithoutCancel(ctx))
		if err != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, errors.Join(
			errors.New("database construction returned no database"),
			closeErr,
		)
	}
	if err != nil {
		if _, ok := errors.AsType[database.CommitTimestampError](err); ok {
			runtime.recoveryError = err
			return runtime, nil
		}
		return nil, errors.Join(
			err,
			runtime.Close(context.WithoutCancel(ctx)),
		)
	}
	return runtime, nil
}
