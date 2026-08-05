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

package mithril

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireChunkTrio copies one immutable file's chunk/primary/secondary trio out
// of the shared testdata into dir, producing a real single-chunk ImmutableDB.
//
// Real files rather than fabricated ones, because the assertions below are
// about which tree a tip was read from, and a tip can only be read from an
// ImmutableDB the reader can actually parse.
func requireChunkTrio(t *testing.T, name, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o750))
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		data, err := os.ReadFile(
			filepath.Join(immutableTestdataDir, name+ext),
		)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, name+ext), data, 0o640,
		))
	}
}

// requireTip reads the tip of an ImmutableDB, which identifies which tree was
// read: two trees built from different immutable files have different tips.
func requireTip(t *testing.T, imm *immutable.ImmutableDb) ocommon.Point {
	t.Helper()
	tip, err := imm.GetTip()
	require.NoError(t, err)
	require.NotNil(t, tip)
	return *tip
}

// TestBootstrapImmutableSurvivesHandoffSwap covers the window between a
// cache-reuse lookup accepting a tree and the load actually reading it.
//
// The lookups vet a directory through a handle, but the ImmutableDB is opened
// downstream, after Bootstrap has returned. Handing back only a pathname would
// end the guarantee at that boundary: a writer with access to the download
// directory can repoint the name in between, and the load would read their tree
// having vetted ours. The handle refers to the directory rather than to a name
// for it, so it is the handle that is carried across and opened through.
//
// The swap is staged rather than raced, because it has to land in a window that
// is not observable from outside the process; the steps below are the ones the
// bootstrap performs, in order, with the substitution placed exactly where a
// concurrent writer would land it.
func TestBootstrapImmutableSurvivesHandoffSwap(t *testing.T) {
	// The two lookups that produce a BootstrapResult's ImmutableDir: v1 walks
	// the extracted layouts, v2 knows the archives land in `immutable`.
	lookups := map[string]func(extractDir string) *vettedDir{
		"v1 findImmutableDir": findImmutableDir,
		"v2 chunkDirUnder": func(extractDir string) *vettedDir {
			return chunkDirUnder(extractDir, "immutable")
		},
	}

	for name, lookup := range lookups {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			extractDir := filepath.Join(parent, "immutable-abc123")
			requireChunkTrio(
				t, "00000", filepath.Join(extractDir, "immutable"),
			)

			found := lookup(extractDir)
			require.NotNil(t, found)
			t.Cleanup(found.Close)
			result := &BootstrapResult{
				ImmutableDir:  found.Path(),
				ImmutableRoot: found.Root(),
			}

			ours := requireTip(t, mustOpenBootstrapped(t, result))

			// A writer takes the name for a tree of their own, after the
			// lookup returned and before the load opens anything. A different
			// immutable file, so the tree that was read is identifiable.
			theirs := filepath.Join(parent, "theirs")
			requireChunkTrio(
				t, "00001", filepath.Join(theirs, "immutable"),
			)
			requireDirectorySwap(
				t, extractDir, filepath.Join(parent, "moved-aside"),
			)
			requireDirectorySwap(t, theirs, extractDir)

			// The premise: the name now denotes the writer's tree, so a
			// pathname handoff would have loaded it.
			byName, err := immutable.New(result.ImmutableDir)
			require.NoError(t, err)
			assert.NotEqual(t, ours, requireTip(t, byName),
				"the substitution must be observable through the name, "+
					"or this test proves nothing")

			// The load reads the tree the bootstrap vetted regardless.
			assert.Equal(t, ours,
				requireTip(t, mustOpenBootstrapped(t, result)),
				"the load must read the inspected tree, not the one that "+
					"took its name",
			)
		})
	}
}

func mustOpenBootstrapped(
	t *testing.T,
	result *BootstrapResult,
) *immutable.ImmutableDb {
	t.Helper()
	imm, err := openBootstrappedImmutable(result)
	require.NoError(t, err)
	return imm
}

// TestOpenBootstrappedImmutableRefusesUnvettedResult pins the fail-closed side:
// a result carrying no handle is refused rather than opened by name.
//
// A fallback would be invisible — the load would succeed, reading a directory
// nothing vetted — so the absence of a handle has to be an error, not a
// slower path.
func TestOpenBootstrappedImmutableRefusesUnvettedResult(t *testing.T) {
	dir := t.TempDir()
	requireChunkTrio(t, "00000", dir)

	_, err := openBootstrappedImmutable(&BootstrapResult{ImmutableDir: dir})
	require.Error(t, err)
	assert.ErrorContains(t, err, "no verified directory handle")
}

// TestBootstrapResultCloseImmutableRootIsIdempotent pins that the descriptor is
// released once and that a second release — Cleanup after an explicit close, or
// the deferred close after Cleanup — is not an error.
func TestBootstrapResultCloseImmutableRootIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	result := &BootstrapResult{ImmutableDir: dir, ImmutableRoot: root}

	result.CloseImmutableRoot()
	assert.Nil(t, result.ImmutableRoot)
	result.CloseImmutableRoot()

	// A result that never had a handle is a no-op, which is the shape callers
	// that fall back to the pathname produce.
	(&BootstrapResult{}).CloseImmutableRoot()
}
