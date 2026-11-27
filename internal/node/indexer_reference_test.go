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

package node

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if storeTemplateRoot != "" {
		_ = os.RemoveAll(storeTemplateRoot)
	}
	os.Exit(code)
}

// The reference tables are what an IndexerDB indexed from genesis holds over
// the fixture chains, and a loaded store is what LoadWithDB leaves of them.
// Both depend only on the immutable test data, so each is computed once per
// test binary and every test works on its own copy.

// storeTemplate is a ledger database loaded with the first chunks of the
// fixture chain and closed. Each test copies the data directory, so loading
// the chain is paid once per binary and no test sees another's writes.
type storeTemplate struct {
	chunks int
	// inactivity, when non-zero, loads the chain with CIP-0163 delegator
	// inactivity on and this window in epochs.
	inactivity uint64
	once       sync.Once
	builds     atomic.Int32
	dir        string
}

var (
	chainStoreTemplate = &storeTemplate{chunks: equivalenceChunks}
	// inactivityWindow is the CIP-0163 window of inactivityStoreTemplate.
	inactivityWindow        uint64 = 100
	inactivityStoreTemplate        = &storeTemplate{
		chunks: equivalenceChunks, inactivity: inactivityWindow,
	}

	storeTemplateRootOnce sync.Once
	storeTemplateRoot     string
	storeTemplateRootErr  error
)

// storeConfig is how a node's ledger database over dataDir is opened.
func storeConfig(dataDir string) (
	*database.Config,
	internalplugins.StorageSelections,
	internalplugins.StorageDependencies,
) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &database.Config{
			DataDir: dataDir, Logger: logger, StorageMode: "core",
			IndexerLayout: "split", Network: "preview",
		},
		internalplugins.StorageSelections{
			Blob: plugin.Selection{
				Provider: "badger", Config: testutil.BadgerBlobConfig(),
			},
			Metadata: plugin.Selection{
				Provider: "sqlite", Config: map[string]any{},
			},
		},
		internalplugins.StorageDependencies{
			DataDir: dataDir, StorageMode: "core", Logger: logger,
		}
}

func (s *storeTemplate) build(t *testing.T) string {
	t.Helper()
	s.builds.Add(1)
	storeTemplateRootOnce.Do(func() {
		storeTemplateRoot, storeTemplateRootErr = os.MkdirTemp(
			"", "dingo-node-template-",
		)
	})
	require.NoError(t, storeTemplateRootErr)
	dir, err := os.MkdirTemp(storeTemplateRoot, "store-")
	require.NoError(t, err)
	ctx := context.Background()
	dbConfig, selections, deps := storeConfig(dir)
	runtime, err := internalplugins.OpenDatabase(ctx, dbConfig, selections, deps)
	require.NoError(t, err)
	closed := false
	closeRuntime := func() error {
		if closed {
			return nil
		}
		closed = true
		return runtime.Close(context.WithoutCancel(ctx))
	}
	t.Cleanup(func() { _ = closeRuntime() })
	loadCfg := equivalenceLoadConfig()
	if s.inactivity > 0 {
		loadCfg.DelegatorInactivityEnabled = true
		loadCfg.DelegatorInactivity = s.inactivity
	}
	require.NoError(t, LoadWithDB(
		ctx, loadCfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		copyImmutableChunkPrefix(t, s.chunks), runtime.Database,
	))
	require.NoError(t, closeRuntime())
	return dir
}

// copyDir returns a new test directory holding a copy of the loaded store.
func (s *storeTemplate) copyDir(t *testing.T) string {
	t.Helper()
	s.once.Do(func() { s.dir = s.build(t) })
	require.NotEmpty(
		t, s.dir, "the store template build failed in the test that ran it",
	)
	dst := t.TempDir()
	require.NoError(t, filepath.WalkDir(
		s.dir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(s.dir, path)
			if err != nil {
				return err
			}
			target := filepath.Join(dst, rel)
			if d.IsDir() {
				return os.MkdirAll(target, 0o700)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, 0o600)
		},
	))
	return dst
}

// openEquivalence is a consensus database holding a copy of the loaded store,
// as newEquivalenceDB followed by LoadWithDB would leave it.
func (s *storeTemplate) openEquivalence(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		StorageMode:   types.StorageModeCore,
		IndexerLayout: config.IndexerLayoutIndexerDB,
		DataDir:       s.copyDir(t),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Network:       "preview",
	})
	require.NoError(t, err)
	return db
}

// Each loaded store is loaded once per test binary however many tests copy
// it, and a write to one copy does not reach another.
func TestLoadedStoreTemplatesAreLoadedOncePerBinary(t *testing.T) {
	t.Parallel()
	for name, tmpl := range map[string]*storeTemplate{
		"chain": chainStoreTemplate,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			first := tmpl.copyDir(t)
			second := tmpl.copyDir(t)
			require.NotEqual(t, first, second)
			require.NoError(t, os.WriteFile(
				filepath.Join(first, "scratch"), []byte("edit"), 0o600,
			))
			require.NoFileExists(t, filepath.Join(second, "scratch"))
			require.NoFileExists(t, filepath.Join(tmpl.dir, "scratch"))
			require.Equal(
				t, int32(1), tmpl.builds.Load(), "the store was loaded again",
			)
		})
	}
}
