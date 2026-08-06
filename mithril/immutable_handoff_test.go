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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/ledgerstate"
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

// TestBootstrapResultCloseHandlesIsIdempotent pins that every descriptor is
// released once and that a second release — Cleanup after an explicit close, or
// the deferred close after Cleanup — is not an error.
func TestBootstrapResultCloseHandlesIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	openRoot := func() *os.Root {
		root, err := os.OpenRoot(dir)
		require.NoError(t, err)
		return root
	}
	result := &BootstrapResult{
		ImmutableDir:  dir,
		ImmutableRoot: openRoot(),
		AncillaryRoot: openRoot(),
		ExtractRoot:   openRoot(),
	}

	result.CloseHandles()
	assert.Nil(t, result.ImmutableRoot)
	assert.Nil(t, result.AncillaryRoot)
	assert.Nil(t, result.ExtractRoot)
	result.CloseHandles()

	// A result that never had handles is a no-op, which is the shape a
	// hand-built result produces.
	(&BootstrapResult{}).CloseHandles()
}

// TestImportLedgerStateRefusesUnvettedResult pins the ancillary side's
// fail-closed rule, matching the immutable one.
//
// The ledger-state search reads only through handles the bootstrap vetted. A
// result carrying none is refused rather than searched by pathname, because a
// pathname search would succeed while describing a tree nothing checked — and
// for the ancillary tree that check is a signature.
func TestImportLedgerStateRefusesUnvettedResult(t *testing.T) {
	_, _, err := importLedgerState(
		t.Context(),
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		&BootstrapResult{
			AncillaryDir: t.TempDir(),
			ExtractDir:   t.TempDir(),
		},
		false,
		^uint64(0),
		nil,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "no verified directory handle")
}

// TestImportLedgerStateRefusesSymlinkedState covers the ancillary tree's file
// level from the consumer end.
//
// The directory handle stops the tree being swapped, but says nothing about the
// entries inside it, and `os.Root` follows a symlink whose target stays within
// the root. Extraction never writes one, so a symlink at the state file is
// planted — following it would import a ledger state somebody else chose, from
// inside a directory whose manifest signature checked out.
//
// The state file is a one-element CBOR array, which parses far enough to fail
// distinctively. That is what makes the control case below prove its point: the
// tree is reachable and would have been read, were the entry not a symlink.
func TestImportLedgerStateRefusesSymlinkedState(t *testing.T) {
	build := func(t *testing.T, symlink bool) *BootstrapResult {
		t.Helper()
		dir := t.TempDir()
		slotDir := filepath.Join(dir, "ledger", "100")
		require.NoError(t, os.MkdirAll(slotDir, 0o750))
		name := "state"
		if symlink {
			name = "real"
		}
		require.NoError(t, os.WriteFile(
			filepath.Join(slotDir, name), []byte{0x81, 0x00}, 0o640,
		))
		if symlink {
			requireSymlinkSupport(
				t, "real", filepath.Join(slotDir, "state"),
			)
		}
		root, err := openVerifiedDir(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close() })
		return &BootstrapResult{AncillaryDir: dir, AncillaryRoot: root}
	}

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Control: an ordinary state file is discovered and read, reaching the
	// parser. Without this the refusal below could be any other failure.
	_, _, err := importLedgerState(
		t.Context(), nil, discard, nil, build(t, false),
		false, ^uint64(0), nil,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "parsing ledger state",
		"the control tree must be read, or the refusal proves nothing")

	_, _, err = importLedgerState(
		t.Context(), nil, discard, nil, build(t, true),
		false, ^uint64(0), nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ledgerstate.ErrUnsafeSnapshotPath,
		"a symlinked state file must be refused, not followed")
	// Refused as unsafe rather than reported as absent: the difference
	// matters, because "absent" is what lets the search move on to another
	// tree and import from there instead.
	assert.NotErrorIs(t, err, ledgerstate.ErrNoUsableLedgerState)
}

// TestImportLedgerStateRefusesUnsafeAncillaryTree covers tree selection with
// both trees usable.
//
// The ancillary tree is preferred over the extraction directory, and it is the
// one the signed manifest covers. Moving on from an ancillary tree that exists
// but holds something unusable would let a planted entry pick the source: an
// attacker who cannot forge a signed ancillary state can make the real one
// unopenable and have the unsigned extraction directory imported instead.
//
// A tree with genuinely no ledger state still falls through — that is the
// v1 layout, where the state lives in the main archive's db/ledger.
func TestImportLedgerStateRefusesUnsafeAncillaryTree(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	// The extraction directory holds a readable state throughout, so a
	// fall-through would visibly succeed rather than merely fail differently.
	newExtract := func(t *testing.T) (string, *os.Root) {
		t.Helper()
		dir := t.TempDir()
		slotDir := filepath.Join(dir, "db", "ledger", "100")
		require.NoError(t, os.MkdirAll(slotDir, 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(slotDir, "state"), []byte{0x81, 0x00}, 0o640,
		))
		root, err := openVerifiedDir(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close() })
		return dir, root
	}

	t.Run("planted ancillary tree is refused", func(t *testing.T) {
		anc := t.TempDir()
		slotDir := filepath.Join(anc, "ledger", "100")
		require.NoError(t, os.MkdirAll(slotDir, 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(slotDir, "real"), []byte{0x81, 0x00}, 0o640,
		))
		requireSymlinkSupport(t, "real", filepath.Join(slotDir, "state"))
		ancRoot, err := openVerifiedDir(anc)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ancRoot.Close() })

		extractDir, extractRoot := newExtract(t)
		_, _, err = importLedgerState(
			t.Context(), nil, discard, nil,
			&BootstrapResult{
				AncillaryDir:  anc,
				AncillaryRoot: ancRoot,
				ExtractDir:    extractDir,
				ExtractRoot:   extractRoot,
			},
			false, ^uint64(0), nil,
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, ledgerstate.ErrUnsafeSnapshotPath,
			"an unusable ancillary tree must fail the import, not hand "+
				"source selection to whoever planted it")
	})

	t.Run("stateless ancillary tree falls through", func(t *testing.T) {
		anc := t.TempDir()
		extractDir, extractRoot := newExtract(t)
		ancRoot, err := openVerifiedDir(anc)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ancRoot.Close() })

		_, _, err = importLedgerState(
			t.Context(), nil, discard, nil,
			&BootstrapResult{
				AncillaryDir:  anc,
				AncillaryRoot: ancRoot,
				ExtractDir:    extractDir,
				ExtractRoot:   extractRoot,
			},
			false, ^uint64(0), nil,
		)
		// Reaches the parser on the extraction directory's state, which is
		// what makes the refusal above about planted content rather than
		// about the fallback being unreachable.
		require.Error(t, err)
		assert.ErrorContains(t, err, "parsing ledger state")
	})
}

// TestImportLedgerStateWillNotLookPastAVerifiedTree covers the downgrade an
// emptied ancillary tree could otherwise force.
//
// A verified ancillary tree's contents are covered by the ancillary key's
// signature; the extraction directory's are not. If emptying the first made the
// import read the second, whoever emptied it would have chosen the source. So
// nothing is looked at after a verified tree, even when it yields no state.
//
// Unverified is the opposite case and must still fall through: that is how the
// v1 layout works, its ledger state living in the main archive, and it also
// covers an ancillary tree holding only states newer than the certified tip.
func TestImportLedgerStateWillNotLookPastAVerifiedTree(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	run := func(t *testing.T, verified bool) error {
		t.Helper()
		// An ancillary tree with a ledger directory but nothing in it: what an
		// emptied tree, or one holding only volatile states, looks like.
		anc := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(anc, "ledger"), 0o750))
		ancRoot, err := openVerifiedDir(anc)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ancRoot.Close() })

		extractDir := t.TempDir()
		slotDir := filepath.Join(extractDir, "db", "ledger", "100")
		require.NoError(t, os.MkdirAll(slotDir, 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(slotDir, "state"), []byte{0x81, 0x00}, 0o640,
		))
		extractRoot, err := openVerifiedDir(extractDir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = extractRoot.Close() })

		_, _, err = importLedgerState(
			t.Context(), nil, discard, nil,
			&BootstrapResult{
				AncillaryDir:      anc,
				AncillaryRoot:     ancRoot,
				AncillaryVerified: verified,
				ExtractDir:        extractDir,
				ExtractRoot:       extractRoot,
			},
			false, ^uint64(0), nil,
		)
		require.Error(t, err)
		return err
	}

	t.Run("verified tree ends the search", func(t *testing.T) {
		assert.ErrorContains(t, run(t, true),
			"refusing to import one from elsewhere",
			"an emptied verified tree must not send the import to an "+
				"unsigned one")
	})

	t.Run("unverified tree falls through", func(t *testing.T) {
		// Reaches the parser on the extraction directory's state, so the
		// refusal above is about the signature and not about the fallback
		// being unreachable.
		assert.ErrorContains(t, run(t, false), "parsing ledger state")
	})
}
