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

package dbtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
)

// Every test that builds a database used to run the full metadata schema
// migration -- 84 tables -- from scratch. Measured under -race on a fresh
// in-memory store that cost ~1.75s per test, of which Badger's blob store was
// ~24ms and the migration was the rest. It is the dominant cost of the whole
// ledger package: a database-using ledger test took 1.94-3.00s while a test
// that builds no database took 0.00-0.01s, so the tests themselves are
// milliseconds and the fixture is everything.
//
// Roughly three quarters of that migration time is the migration runner's own
// per-version machinery rather than executing DDL, and -race inflates Go-side
// work far more than SQLite's C-side execution, which is why the release
// gate's race run felt it most. `ledger` reached 1036s there, 58% of the 30m
// per-package budget, after an earlier release attempt failed outright at the
// previous 20m budget.
//
// So the migration runs once per test process, against a throwaway directory,
// and the resulting file is kept in memory as a template. Each test writes
// that template into its own directory and opens it, which the runner sees as
// already migrated:
//
//	fresh directory, runs migrations   1.923s
//	already-migrated directory           296ms
//	template write + open              ~5ms + ~250ms
//
// The template directory itself is removed as soon as its bytes are read, so
// nothing is leaked for the life of the process.
var (
	metadataTemplateOnce sync.Once
	metadataTemplate     []byte
	metadataTemplateErr  error
)

// metadataTemplateFile is the SQLite database file the metadata provider owns
// inside its data directory.
const metadataTemplateFile = "metadata.sqlite"

// migratedMetadataTemplate returns the bytes of a fully migrated SQLite
// metadata database, running the migration exactly once per process.
func migratedMetadataTemplate() ([]byte, error) {
	metadataTemplateOnce.Do(func() {
		metadataTemplate, metadataTemplateErr = buildMetadataTemplate()
	})
	return metadataTemplate, metadataTemplateErr
}

// buildMetadataTemplate migrates a scratch database and returns its bytes.
//
// The provider is stopped before the file is read so SQLite checkpoints and
// closes cleanly; reading a live database would capture a torn page or leave
// the content in a companion -wal file this template does not carry.
func buildMetadataTemplate() (_ []byte, err error) {
	dir, err := os.MkdirTemp("", "dingo-dbtest-metadata-template-")
	if err != nil {
		return nil, fmt.Errorf("create metadata template dir: %w", err)
	}
	defer func() {
		// The bytes are what callers need, so the directory is disposable
		// the moment it has been read. Removing it here rather than at
		// process exit keeps this from leaking a directory per test binary,
		// which matters because package dbtest has no TestMain to hook.
		err = errors.Join(err, os.RemoveAll(dir))
	}()

	host := plugin.NewHost()
	if regErr := sqlite.RegisterProvider(host); regErr != nil {
		return nil, fmt.Errorf("register sqlite provider: %w", regErr)
	}
	stopped := false
	defer func() {
		// Only stops on an error path; the success path stops explicitly
		// below, before the file is read, and a second Stop would report a
		// spurious failure.
		if !stopped {
			err = errors.Join(err, host.Stop(context.Background()))
		}
	}()

	config := database.DefaultConfig
	if _, resolveErr := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite",
		map[string]any{"dataDir": dir},
		metadata.ProviderDependencies{
			DataDir:      dir,
			StorageMode:  config.StorageMode,
			Logger:       config.Logger,
			PromRegistry: config.PromRegistry,
		},
	); resolveErr != nil {
		return nil, fmt.Errorf("migrate metadata template: %w", resolveErr)
	}

	// Stop before reading, so SQLite has checkpointed and closed.
	stopped = true
	if stopErr := host.Stop(context.Background()); stopErr != nil {
		return nil, fmt.Errorf("stop metadata template host: %w", stopErr)
	}

	raw, readErr := os.ReadFile(filepath.Join(dir, metadataTemplateFile))
	if readErr != nil {
		return nil, fmt.Errorf("read metadata template: %w", readErr)
	}
	if len(raw) == 0 {
		return nil, errors.New("metadata template is empty")
	}
	return raw, nil
}

// writeMetadataTemplate materializes the migrated template into dir and
// returns the provider config that points SQLite at it.
func writeMetadataTemplate(dir string) (map[string]any, error) {
	raw, err := migratedMetadataTemplate()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, metadataTemplateFile)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, fmt.Errorf("write metadata template: %w", err)
	}
	return map[string]any{"dataDir": dir}, nil
}

// seedMetadataTemplate materializes the migrated template inside an existing
// data directory the caller owns, leaving the provider to resolve its own
// path. It is a no-op when a metadata file is already present, so a caller
// reusing a directory across two constructions keeps the first one's data.
func seedMetadataTemplate(dir string) error {
	path := filepath.Join(dir, metadataTemplateFile)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat metadata file: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create metadata data dir: %w", err)
	}
	raw, err := migratedMetadataTemplate()
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write metadata template: %w", err)
	}
	return nil
}
