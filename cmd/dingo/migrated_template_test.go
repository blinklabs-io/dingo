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

package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// Running every schema migration dominates the cost of opening a fresh test
// database, especially under the race detector. The schema is migrated once
// per storage mode per test binary and each fresh test directory starts from
// a copy of it. The copy holds only what the migrations write, so state that
// Open writes on a first start is still written by the test's own open.
var migratedMetadataTemplates sync.Map // storage mode -> *migratedMetadataTemplate

type migratedMetadataTemplate struct {
	once  sync.Once
	bytes []byte
	err   error
}

// seedMigratedMetadata places a migrated metadata database in the
// configured data directory unless one is already there, so a test that
// reopens an existing directory keeps its own data.
func seedMigratedMetadata(tb testing.TB, cfg *config.Config) {
	tb.Helper()
	dataDir := cfg.DatabasePath
	if dataDir == "" {
		return
	}
	dst := filepath.Join(dataDir, "metadata.sqlite")
	if _, err := os.Stat(dst); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return
	}
	mode := string(effectiveStorageMode(cfg))
	value, _ := migratedMetadataTemplates.LoadOrStore(
		mode, &migratedMetadataTemplate{},
	)
	tpl := value.(*migratedMetadataTemplate)
	tpl.once.Do(func() { tpl.bytes, tpl.err = buildMigratedMetadata(mode) })
	require.NoError(tb, tpl.err)
	require.NoError(tb, os.MkdirAll(dataDir, 0o750))
	require.NoError(tb, os.WriteFile(dst, tpl.bytes, 0o600))
}

func buildMigratedMetadata(storageMode string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "dingo-migrated-template-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	host := plugin.NewHost()
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, err
	}
	if _, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir:     dir,
			StorageMode: storageMode,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	); err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	if err := host.Stop(context.Background()); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(dir, "metadata.sqlite"))
}
