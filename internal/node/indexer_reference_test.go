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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
	once   sync.Once
	builds atomic.Int32
	dir    string
}

var (
	anchorStoreTemplate = &storeTemplate{chunks: chainChunks}
	chainStoreTemplate  = &storeTemplate{chunks: equivalenceChunks}

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
	require.NoError(t, LoadWithDB(
		ctx, equivalenceLoadConfig(),
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
		IndexerLayout: config.IndexerLayoutSplit,
		DataDir:       s.copyDir(t),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Network:       "preview",
	})
	require.NoError(t, err)
	return db
}

// anchorReferenceTables is the reference of the anchor fixture.
type anchorReferenceTables struct {
	atAnchor  map[string][]string
	atTip     map[string][]string
	anchorTip ochainsync.Tip
}

var (
	anchorReferenceOnce   sync.Once
	anchorReferenceBuilds atomic.Int32
	anchorReferenceResult *anchorReferenceTables

	chainReferenceOnce   sync.Once
	chainReferenceBuilds atomic.Int32
	chainReferenceResult map[string][]string
)

// cloneTables copies the tables so a test may edit its view of the reference
// without another test observing the edit.
func cloneTables(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for name, rows := range in {
		out[name] = slices.Clone(rows)
	}
	return out
}

// anchorReference returns the tables of an IndexerDB indexed from genesis at
// the anchor and at the end of the fixture chain, and the anchor's tip.
func anchorReference(t *testing.T) *anchorReferenceTables {
	t.Helper()
	anchorReferenceOnce.Do(func() {
		anchorReferenceResult = buildAnchorReference(t)
	})
	require.NotNil(
		t,
		anchorReferenceResult,
		"the reference build failed in the test that ran it",
	)
	return &anchorReferenceTables{
		atAnchor:  cloneTables(anchorReferenceResult.atAnchor),
		atTip:     cloneTables(anchorReferenceResult.atTip),
		anchorTip: anchorReferenceResult.anchorTip,
	}
}

func buildAnchorReference(t *testing.T) *anchorReferenceTables {
	t.Helper()
	anchorReferenceBuilds.Add(1)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	out := &anchorReferenceTables{}

	// A ledger database loaded to the anchor and indexed from genesis, read
	// there and again at the end of the chain.
	ref := newEquivalenceDB(t)
	require.NoError(t, LoadWithDB(
		ctx, equivalenceLoadConfig(), logger,
		copyImmutableChunkPrefix(t, anchorChunks), ref,
	))
	var err error
	out.anchorTip, err = ref.GetTip(nil)
	require.NoError(t, err)
	refLedger, closeRefLedger := openFactLedger(t, ref)
	_, refRaw := indexFromGenesis(t, ref, refLedger, nil)
	out.atAnchor = dumpTables(t, refRaw)
	closeRefLedger()
	require.NoError(t, LoadWithDB(
		ctx, equivalenceLoadConfig(), logger,
		copyImmutableChunkPrefix(t, chainChunks), ref,
	))
	// A second index from genesis, not the first one followed to the tip:
	// the rows of an epoch whose reward round completed past the anchor are
	// then the final ones.
	refLedger, _ = openFactLedger(t, ref)
	_, refRaw = indexFromGenesis(t, ref, refLedger, nil)
	out.atTip = dumpTables(t, refRaw)
	for _, name := range anchorSkipTables {
		delete(out.atAnchor, name)
		delete(out.atTip, name)
	}
	return out
}

// chainReference returns the tables of an IndexerDB indexed from genesis over
// the equivalenceChunks chain.
func chainReference(t *testing.T) map[string][]string {
	t.Helper()
	chainReferenceOnce.Do(func() {
		chainReferenceResult = buildChainReference(t)
	})
	require.NotNil(
		t,
		chainReferenceResult,
		"the reference build failed in the test that ran it",
	)
	return cloneTables(chainReferenceResult)
}

func buildChainReference(t *testing.T) map[string][]string {
	t.Helper()
	chainReferenceBuilds.Add(1)
	consensus := chainStoreTemplate.openEquivalence(t)
	ls, _ := openFactLedger(t, consensus)
	_, raw := indexFromGenesis(t, consensus, ls, nil)
	return dumpTables(t, raw)
}

// anchorPoint is the point the anchor fixture's Mithril anchor records: the
// tip of the first anchorChunks chunks.
func anchorPoint(t *testing.T) ocommon.Point {
	t.Helper()
	imm, err := immutable.New(copyImmutableChunkPrefix(t, anchorChunks))
	require.NoError(t, err)
	tip, err := imm.GetTip()
	require.NoError(t, err)
	require.NotNil(t, tip)
	return *tip
}

// Each reference is built once per test binary however many tests ask for it,
// and a caller's edits to its copy do not reach the next caller.
func TestIndexerReferencesAreBuiltOncePerBinary(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		get    func(*testing.T) map[string][]string
		builds *atomic.Int32
	}{
		"anchor": {
			get: func(t *testing.T) map[string][]string {
				return anchorReference(t).atTip
			},
			builds: &anchorReferenceBuilds,
		},
		"chain": {get: chainReference, builds: &chainReferenceBuilds},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			first := tc.get(t)
			require.NotEmpty(t, first)
			// A table with rows, so the row-level edit is exercised too.
			var table string
			for _, name := range slices.Sorted(maps.Keys(first)) {
				if len(first[name]) > 0 {
					table = name
					break
				}
			}
			require.NotEmpty(t, table, "the reference holds no rows")
			want := first[table][0]
			first[table][0] += " edited"
			other := slices.Sorted(maps.Keys(first))[0]
			delete(first, other)

			second := tc.get(t)
			require.Contains(
				t, second, other, "an edit reached the next caller",
			)
			require.Equal(
				t, want, second[table][0], "a row edit reached the next caller",
			)
			require.Equal(
				t,
				int32(1),
				tc.builds.Load(),
				"the reference was rebuilt",
			)
		})
	}
}

// Each loaded store is loaded once per test binary however many tests copy
// it, and a write to one copy does not reach another.
func TestLoadedStoreTemplatesAreLoadedOncePerBinary(t *testing.T) {
	t.Parallel()
	for name, tmpl := range map[string]*storeTemplate{
		"anchor": anchorStoreTemplate,
		"chain":  chainStoreTemplate,
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

// The anchor the fixture reads from the immutable files is the tip the
// reference's ledger database had at the anchor.
func TestAnchorPointIsTheReferenceAnchorTip(t *testing.T) {
	t.Parallel()
	require.Equal(
		t,
		anchorReference(t).anchorTip.Point,
		anchorPoint(t),
	)
}
