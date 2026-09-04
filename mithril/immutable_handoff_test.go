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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
		// emptied tree, or one holding only volatile states, looks like. A
		// verified one still carries the manifest that covered it — a
		// signature over no files at all is refused long before this point —
		// so the search reaches the decision below rather than the
		// consistency check on the two.
		anc := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(anc, "ledger"), 0o750))
		ancRoot, err := openVerifiedDir(anc)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ancRoot.Close() })
		var ancDigests map[string]string
		if verified {
			ancDigests = map[string]string{"ledger/100/state": "00"}
		}

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
				AncillaryDigests:  ancDigests,
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

// TestDownloadAncillaryReportsArchiveWhenTreeUnusable pins that a downloaded
// ancillary archive is still reported for cleanup when the tree it extracted to
// turns out to hold no ledger state.
//
// The tree and the archive fail together but are cleaned up separately: the
// caller records the archive path from the same return that carries the error,
// and losing it would leave the download behind in an operator-supplied
// directory, where no temp-dir removal sweeps it up.
func TestDownloadAncillaryReportsArchiveWhenTreeUnusable(t *testing.T) {
	// An ancillary archive whose payload has no ledger state at all.
	archive := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "not ledger state",
	})
	downloadDir := t.TempDir()
	served := filepath.Join(downloadDir, "served.tar.zst")
	data, err := os.ReadFile(archive)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(served, data, 0o640))

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, served)
		},
	))
	defer srv.Close()

	tree, archPath, err := downloadAncillary(
		t.Context(),
		BootstrapConfig{
			AllowInsecureHTTP: true,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		&SnapshotListItem{
			SnapshotBase: SnapshotBase{
				Digest:             "abc123",
				Network:            "preprod",
				AncillaryLocations: []string{srv.URL},
			},
		},
		downloadDir,
	)
	require.Error(t, err)
	assert.Nil(t, tree, "an unusable tree must not be returned")
	assert.NotEmpty(t, archPath,
		"the downloaded archive must still be reported so it gets cleaned up")
}

// TestDownloadAncillaryReportsArchiveOnFailure pins that every error after the
// download has begun still names the archive, so Cleanup can remove it.
//
// DownloadSnapshot resumes, which means a failed attempt deliberately leaves a
// partial file at the destination. Combined with an error that clears the path,
// that file is unreachable to cleanup — invisible when the download directory
// is a temp dir that gets removed wholesale, and a leak when the operator
// supplied one.
func TestDownloadAncillaryReportsArchiveOnFailure(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	snapshot := func(locations ...string) *SnapshotListItem {
		return &SnapshotListItem{
			SnapshotBase: SnapshotBase{
				Digest:             "abc123",
				Network:            "preprod",
				AncillaryLocations: locations,
			},
		}
	}

	t.Run("every download location fails", func(t *testing.T) {
		// Truncated mid-body rather than refused outright: a refusal writes
		// nothing, and the point of this case is the partial file a resumable
		// download leaves behind.
		srv := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "4096")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(make([]byte, 512))
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				panic(http.ErrAbortHandler)
			},
		))
		defer srv.Close()

		downloadDir := t.TempDir()
		tree, archPath, err := downloadAncillary(
			t.Context(),
			BootstrapConfig{
				AllowInsecureHTTP:           true,
				Logger:                      discard,
				DownloadMaxTransientRetries: -1,
				DownloadMaxIdleRetries:      1,
			},
			snapshot(srv.URL),
			downloadDir,
		)
		require.Error(t, err)
		assert.Nil(t, tree)
		require.NotEmpty(t, archPath)
		// The assertion that matters: the reported path is the partial file
		// that is actually on disk, not merely some non-empty string.
		fi, statErr := os.Stat(archPath)
		require.NoError(t, statErr,
			"a partial download is left on disk and must stay reachable "+
				"to cleanup")
		assert.NotZero(t, fi.Size(), "the partial file should hold bytes")
	})

	t.Run("extraction fails after a complete download", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				// Downloads fine, is not a valid archive.
				_, _ = w.Write([]byte("not an archive"))
			},
		))
		defer srv.Close()

		downloadDir := t.TempDir()
		tree, archPath, err := downloadAncillary(
			t.Context(),
			BootstrapConfig{
				Logger:            discard,
				AllowInsecureHTTP: true,
			},
			snapshot(srv.URL),
			downloadDir,
		)
		require.Error(t, err)
		assert.Nil(t, tree)
		require.NotEmpty(t, archPath,
			"the downloaded archive must stay reachable to cleanup")
		_, statErr := os.Stat(archPath)
		assert.NoError(t, statErr,
			"the reported path must be the archive that is actually there")
	})
}

// TestDownloadAncillaryKeepsTheReportedArchiveInsideDownloadDir covers the
// archive path reported on failure being reduced the same way the downloader
// reduces it.
//
// The filename carries the network name, which the aggregator supplies, and
// DownloadSnapshot writes to its last element. A path assembled from the raw
// name would name a different file for a network like "../../etc" — one outside
// the download directory, and Cleanup calls os.RemoveAll on whatever it is
// given. So the reported path has to stay inside, whatever the aggregator says.
func TestDownloadAncillaryKeepsTheReportedArchiveInsideDownloadDir(
	t *testing.T,
) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", http.StatusInternalServerError)
		},
	))
	defer srv.Close()

	downloadDir := t.TempDir()
	_, archPath, err := downloadAncillary(
		t.Context(),
		BootstrapConfig{
			AllowInsecureHTTP: true,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			DownloadMaxTransientRetries: -1,
		},
		&SnapshotListItem{
			SnapshotBase: SnapshotBase{
				Digest:             "abc123",
				Network:            filepath.Join("..", "..", "etc"),
				AncillaryLocations: []string{srv.URL},
			},
		},
		downloadDir,
	)
	require.Error(t, err)
	require.NotEmpty(t, archPath)

	rel, relErr := filepath.Rel(downloadDir, archPath)
	require.NoError(t, relErr)
	assert.False(t, strings.HasPrefix(rel, ".."),
		"a network name with a separator must not move the reported "+
			"archive out of the download directory: got %s", archPath)
}

// TestDownloadAncillaryV2ReportsArchiveWhenManifestUnverified pins the v2
// manifest-verification failure onto the same contract as every other failure
// after a download has begun: the archive is still named, so Cleanup can remove
// it.
//
// This path is the one that had to be special-cased into the rule rather than
// falling out of it. Verification failing means the extracted tree is
// destroyed, and destroying the tree reads as having cleaned up — but the
// archive it came from is a separate file, removed only because bootstrapV2
// recorded the path this returns. Clearing the path here leaves a complete
// ancillary download in an operator-supplied directory, which nothing
// afterwards sweeps.
func TestDownloadAncillaryV2ReportsArchiveWhenManifestUnverified(t *testing.T) {
	// Ledger state present, so the tree gets past the usability check and the
	// missing manifest is what fails — the case this test is about.
	archive := writeTestArchive(t, map[string]string{
		"ledger/100/state": "ledger state data",
	})
	data, err := os.ReadFile(archive)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(data)
		},
	))
	defer srv.Close()

	downloadDir := t.TempDir()
	tree, _, archPath, err := downloadAncillaryV2(
		t.Context(),
		BootstrapConfig{
			AllowInsecureHTTP: true,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			// Verification on is what reaches the manifest check at all; with
			// it off the archive would simply be accepted.
			VerifyCertificateChain:   true,
			AncillaryVerificationKey: "unused, the manifest is missing",
		},
		&CardanoDatabaseSnapshot{
			Hash:    "abc123",
			Network: "preprod",
			Ancillary: CardanoDatabaseAncillary{
				Locations: []CardanoDatabaseLocation{{URI: srv.URL}},
			},
		},
		downloadDir,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "ancillary manifest")
	assert.Nil(t, tree, "an unverified tree must not be returned")
	require.NotEmpty(t, archPath,
		"the downloaded archive must still be reported so it gets cleaned up")
	_, statErr := os.Stat(archPath)
	assert.NoError(t, statErr,
		"the reported path must be the archive that is actually there")
}

// TestDownloadAncillaryRemovesAnUnusableExtraction pins that an ancillary
// archive extracting to a tree with no ledger state leaves no tree behind.
//
// This is the residue the archive-path contract does not cover. Cleanup takes
// AncillaryDir from the returned handle, and there is no handle on this path —
// so unless the extraction is removed where it is found unusable, nothing ever
// removes it. Same shape as the archive leak, one directory over: invisible
// when DownloadDir was left unset and the auto-created temp directory goes
// wholesale, residue in the operator's directory when they supplied one.
//
// Both downloaders, because the two reached this branch by different routes and
// only one of them removed.
func TestDownloadAncillaryRemovesAnUnusableExtraction(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	// No ledger state anywhere in it, which is what makes the tree unusable.
	archive := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "not ledger state",
	})
	data, err := os.ReadFile(archive)
	require.NoError(t, err)

	serve := func(t *testing.T) string {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(data)
			},
		))
		t.Cleanup(srv.Close)
		return srv.URL
	}

	const digest = "abc123"
	for name, download := range map[string]func(
		t *testing.T, downloadDir string,
	) (*vettedDir, string, error){
		"v1": func(t *testing.T, downloadDir string) (
			*vettedDir, string, error,
		) {
			return downloadAncillary(
				t.Context(),
				BootstrapConfig{
					Logger:            discard,
					AllowInsecureHTTP: true,
				},
				&SnapshotListItem{
					SnapshotBase: SnapshotBase{
						Digest:             digest,
						Network:            "preprod",
						AncillaryLocations: []string{serve(t)},
					},
				},
				downloadDir,
			)
		},
		"v2": func(t *testing.T, downloadDir string) (
			*vettedDir, string, error,
		) {
			tree, _, archPath, err := downloadAncillaryV2(
				t.Context(),
				BootstrapConfig{
					Logger:            discard,
					AllowInsecureHTTP: true,
				},
				&CardanoDatabaseSnapshot{
					Hash:    digest,
					Network: "preprod",
					Ancillary: CardanoDatabaseAncillary{
						Locations: []CardanoDatabaseLocation{
							{URI: serve(t)},
						},
					},
				},
				downloadDir,
			)
			return tree, archPath, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			downloadDir := t.TempDir()
			tree, archPath, err := download(t, downloadDir)
			require.Error(t, err)
			require.Nil(t, tree, "an unusable tree must not be returned")
			// The archive is the other half of the contract and still has to
			// come back, since removing the extraction is not removing it.
			require.NotEmpty(t, archPath)
			_, statErr := os.Stat(archPath)
			assert.NoError(t, statErr,
				"the archive stays, and stays reported")

			ancillaryDir := filepath.Join(downloadDir, "ancillary-"+digest)
			_, statErr = os.Stat(ancillaryDir)
			assert.True(t, os.IsNotExist(statErr),
				"the unusable extraction must be removed where it is found "+
					"unusable; nothing downstream has a path to it: %s",
				ancillaryDir)
		})
	}
}

// requireTrioDigests is the digest map a v2 bootstrap holds once it has checked
// every archive it downloaded: the certified SHA-256 of each file, keyed by the
// name the reader asks for.
func requireTrioDigests(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	digests := make(map[string]string, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		digests[entry.Name()] = hex.EncodeToString(sum[:])
	}
	return digests
}

// TestBootstrappedImmutableRefusesAFileSubstitutedAfterVerification covers the
// other half of the handoff above.
//
// TestBootstrapImmutableSurvivesHandoffSwap pins that the load reads the
// directory the bootstrap vetted. That is a claim about which directory, and
// it leaves the files inside it unaccounted for: the download pool hashes each
// trio when its archive lands and closes it, and the tip read, the catch-up
// check and the blob copy open it again by name afterwards. A writer who shares
// the download directory never has to leave it — renaming a file of their own
// over `00000.chunk` puts uncertified bytes into the blob store under a
// verified directory handle.
//
// So the digests travel with the handle and each file is verified from the
// descriptor the read then goes through. Staged rather than raced: the window
// is not observable from outside the process, so the substitution is placed
// where a concurrent writer would land it.
func TestBootstrappedImmutableRefusesAFileSubstitutedAfterVerification(
	t *testing.T,
) {
	// Which file is taken decides which read notices, so both the tip read and
	// the block copy are covered.
	for _, tc := range []struct {
		name string
		file string
		read func(*immutable.ImmutableDb) error
	}{
		{
			name: "secondary index, read for the tip",
			file: "00000.secondary",
			read: func(imm *immutable.ImmutableDb) error {
				_, err := imm.GetTip()
				return err
			},
		},
		{
			name: "chunk, read for the blob copy",
			file: "00000.chunk",
			read: func(imm *immutable.ImmutableDb) error {
				iter, err := imm.BlocksFromPoint(ocommon.Point{})
				if err != nil {
					return err
				}
				defer func() { _ = iter.Close() }()
				_, err = iter.Next()
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			extractDir := filepath.Join(parent, "immutable-abc123")
			immutableDir := filepath.Join(extractDir, "immutable")
			requireChunkTrio(t, "00000", immutableDir)

			found := chunkDirUnder(extractDir, "immutable")
			require.NotNil(t, found)
			t.Cleanup(found.Close)
			result := &BootstrapResult{
				ImmutableDir:     found.Path(),
				ImmutableRoot:    found.Root(),
				ImmutableDigests: requireTrioDigests(t, immutableDir),
			}

			// Control: the tree as downloaded reads, so the refusal below is
			// about the substitution and not about verified reads never
			// working.
			require.NoError(t, tc.read(mustOpenBootstrapped(t, result)),
				"the certified tree must read, or the refusal proves nothing")

			// A writer takes one file, after the pool verified it and before
			// the load opens it. Renamed over rather than written through: the
			// extracted file belongs to this process, so replacing the name is
			// what a writer holding the directory actually does.
			theirs := filepath.Join(parent, "theirs")
			data, err := os.ReadFile(
				filepath.Join(
					immutableTestdataDir,
					"00001"+filepath.Ext(tc.file),
				),
			)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(theirs, data, 0o640))
			require.NoError(t, os.Rename(
				theirs, filepath.Join(immutableDir, tc.file),
			))

			err = tc.read(mustOpenBootstrapped(t, result))
			require.Error(t, err)
			assert.ErrorIs(t, err, immutable.ErrDigestMismatch,
				"a file substituted after its digest was checked must not "+
					"be read")
		})
	}
}

// TestImportLedgerStateRefusesStateSubstitutedAfterTheManifest is the ancillary
// counterpart.
//
// verifyAncillaryManifest hashes every file the signed manifest covers and
// closes each one; discovery opens the selected state and table again when the
// import runs. The tree between them is the same tree — the handle guarantees
// that — but a file inside it need not be the same file, and the bytes that get
// parsed are then not the bytes the ancillary key signed. So the manifest
// travels with the handle and the selected files are re-checked from the
// descriptors the import reads through.
func TestImportLedgerStateRefusesStateSubstitutedAfterTheManifest(
	t *testing.T,
) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	// A one-element CBOR array: parses far enough to fail distinctively, so a
	// tree that is read reports "parsing ledger state" rather than anything
	// that could be confused with a refusal.
	stateBytes := []byte{0x81, 0x00}

	// The state only. The table is not hashed here at all any more — its
	// digest travels to the import and is checked against the mapping the
	// decoder walks. That half is covered by
	// TestSignedTableDigestRefusesAnUncoveredTable, further down this file,
	// and by ledgerstate.TestParseUTxOsFromOpenFileChecksTheMappedBytes.
	for _, name := range []string{"state"} {
		t.Run(name, func(t *testing.T) {
			build := func(t *testing.T, substitute bool) *BootstrapResult {
				t.Helper()
				dir := t.TempDir()
				slotDir := filepath.Join(dir, "ledger", "100")
				require.NoError(t, os.MkdirAll(slotDir, 0o750))
				files := map[string][]byte{
					"state":  stateBytes,
					"tables": []byte("utxo table"),
				}
				digests := map[string]string{}
				for entry, data := range files {
					require.NoError(t, os.WriteFile(
						filepath.Join(slotDir, entry), data, 0o640,
					))
					sum := sha256.Sum256(data)
					digests["ledger/100/"+entry] = hex.EncodeToString(sum[:])
				}
				root, err := openVerifiedDir(dir)
				require.NoError(t, err)
				t.Cleanup(func() { _ = root.Close() })
				if substitute {
					// After the manifest passed and before the import opens
					// anything.
					theirs := filepath.Join(t.TempDir(), "theirs")
					require.NoError(t, os.WriteFile(
						theirs, []byte("somebody else's bytes"), 0o640,
					))
					require.NoError(t, os.Rename(
						theirs, filepath.Join(slotDir, name),
					))
				}
				return &BootstrapResult{
					AncillaryDir:      dir,
					AncillaryRoot:     root,
					AncillaryVerified: true,
					AncillaryDigests:  digests,
				}
			}

			// Control: the tree the manifest covered is read.
			_, _, err := importLedgerState(
				t.Context(), nil, discard, nil, build(t, false),
				false, ^uint64(0), nil,
			)
			require.Error(t, err)
			require.ErrorContains(t, err, "parsing ledger state",
				"the signed tree must be read, or the refusal proves nothing")

			_, _, err = importLedgerState(
				t.Context(), nil, discard, nil, build(t, true),
				false, ^uint64(0), nil,
			)
			require.Error(t, err)
			assert.ErrorIs(t, err, errAncillaryDigestMismatch,
				"a signed file substituted after the manifest check must "+
					"not be imported")
			assert.NotContains(t, err.Error(), "parsing ledger state",
				"the substituted bytes must be refused before they are "+
					"parsed")
		})
	}
}

// TestImportLedgerStateRefusesAStateTheManifestDoesNotCover keeps a selected
// file with no manifest entry an error rather than an import.
//
// The completeness walk in verifyAncillaryManifest rejects an uncovered file at
// verification time, but that is a check on the tree as it was then. Selecting
// a file nothing signed has to fail at the point of use too, or an entry
// planted afterwards is refused only by a check that already ran.
func TestImportLedgerStateRefusesAStateTheManifestDoesNotCover(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "200")
	require.NoError(t, os.MkdirAll(slotDir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(slotDir, "state"), []byte{0x81, 0x00}, 0o640,
	))
	root, err := openVerifiedDir(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	_, _, err = importLedgerState(
		t.Context(), nil, discard, nil,
		&BootstrapResult{
			AncillaryDir:      dir,
			AncillaryRoot:     root,
			AncillaryVerified: true,
			// Covers a different slot: the selected state is not in it.
			AncillaryDigests: map[string]string{"ledger/100/state": "00"},
		},
		false, ^uint64(0), nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errAncillaryDigestMismatch)
}

// TestCheckImmutableTrioHashesThroughTheHandle pins the download pool's half.
//
// The pool verifies each trio as its archive lands, while the extraction
// directory is still being filled and its name is still resolvable by anybody
// who shares the download directory. Joining that name to hash a file would
// produce the digest of whatever tree holds it at that instant — and the
// mismatch a repointed name causes would be reported as a corrupt download,
// sending the pool round the locations again and deleting a trio it wrote.
func TestCheckImmutableTrioHashesThroughTheHandle(t *testing.T) {
	parent := t.TempDir()
	ours := filepath.Join(parent, "immutable")
	requireChunkTrio(t, "00000", ours)
	digests := requireTrioDigests(t, ours)

	root, err := os.OpenRoot(ours)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	// A writer takes the name for a tree of their own, while the pool is still
	// working. Same file names, different bytes — a tree with different names
	// would fail on the open rather than on the digest, which is not the case
	// under test.
	theirs := filepath.Join(parent, "theirs")
	require.NoError(t, os.MkdirAll(theirs, 0o750))
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		data, err := os.ReadFile(
			filepath.Join(immutableTestdataDir, "00001"+ext),
		)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(theirs, "00000"+ext), data, 0o640,
		))
	}
	requireDirectorySwap(t, ours, filepath.Join(parent, "moved-aside"))
	requireDirectorySwap(t, theirs, ours)

	// The premise: the name denotes the writer's tree now, and their files
	// carry different bytes, so a digest taken by name would not match.
	planted, err := os.ReadFile(filepath.Join(ours, "00000.chunk"))
	require.NoError(t, err)
	byName := sha256.Sum256(planted)
	require.NotEqual(t, digests["00000.chunk"], hex.EncodeToString(byName[:]),
		"the substitution must be observable through the name, or this "+
			"test proves nothing")

	bytes, err := checkImmutableTrio(root, 0, digests)
	require.NoError(t, err,
		"the trio this process extracted must be what gets hashed")
	assert.Positive(t, bytes)
}

// TestVerifySignedStateChecksTheBytesItIsGiven pins that the check is about a
// buffer rather than about a file.
//
// The caller reads the state once and hands the same slice here and to the
// parser. A check that took the descriptor instead would look identical and
// leave the parser re-reading a file that can change under it, so the shape of
// this signature is the guarantee.
func TestVerifySignedStateChecksTheBytesItIsGiven(t *testing.T) {
	signed := []byte("the signed ledger state")
	sum := sha256.Sum256(signed)
	digests := map[string]string{
		"ledger/100/state": hex.EncodeToString(sum[:]),
	}

	require.NoError(t, verifySignedState("ledger/100/state", signed, digests))

	err := verifySignedState(
		"ledger/100/state", []byte("somebody else's bytes"), digests,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errAncillaryDigestMismatch)

	// A path the manifest does not cover is refused rather than accepted for
	// want of anything to compare against.
	err = verifySignedState("ledger/200/state", signed, digests)
	require.Error(t, err)
	assert.ErrorIs(t, err, errAncillaryDigestMismatch)
}

// TestSignedTableDigestRefusesAnUncoveredTable covers the table's half of the
// handoff.
//
// The table cannot be read into a buffer — it is gigabytes — so its digest
// travels down to the import, which checks it against the mapping the decoder
// walks. What has to happen here is the selection: a table the manifest does
// not cover must fail rather than travel down with no digest, because an empty
// digest is how an unsigned tree is decoded unchecked.
func TestSignedTableDigestRefusesAnUncoveredTable(t *testing.T) {
	digests := map[string]string{"ledger/100/tables": "abc123"}
	snapshot := &ledgerstate.SnapshotFiles{
		StatePath: "ledger/100/state",
		TablePath: "ledger/100/tables",
		Table:     &os.File{},
	}

	// The digest has to arrive on the state the import hands down, not merely
	// be looked up: an absent one is how an unsigned table is decoded, so one
	// that was found and dropped would be decoded unchecked.
	state := &ledgerstate.RawLedgerState{}
	require.NoError(
		t, attachSignedTable(state, snapshot, "/anc", digests),
	)
	assert.Equal(t, "abc123", state.UTxOTableDigest)
	assert.Same(t, snapshot.Table, state.UTxOTableFile)

	snapshot.TablePath = "ledger/100/tables/tvar"
	err := attachSignedTable(
		&ledgerstate.RawLedgerState{}, snapshot, "/anc", digests,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errAncillaryDigestMismatch)

	// An unsigned tree carries no map and decodes unchecked, as before.
	snapshot.TablePath = "ledger/100/tables"
	unsigned := &ledgerstate.RawLedgerState{}
	require.NoError(t, attachSignedTable(unsigned, snapshot, "/anc", nil))
	assert.Empty(t, unsigned.UTxOTableDigest)
}

// TestOpenBootstrappedImmutableRefusesAnEmptyDigestMap keeps the choice
// between a verified and an unverified open a statement about the backend
// rather than about how many entries a map happens to hold.
//
// v1 carries no digests at all, and that is the only case that may read
// unverified. A map that is present but empty is not that case — it is a v2
// result that lost its digests — and selecting on emptiness would read it
// unverified, which is the one outcome nothing should be able to reach by
// removing something.
func TestOpenBootstrappedImmutableRefusesAnEmptyDigestMap(t *testing.T) {
	dir := t.TempDir()
	requireChunkTrio(t, "00000", dir)
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	// Control: no map at all is v1, which reads.
	_, err = openBootstrappedImmutable(&BootstrapResult{
		ImmutableDir:  dir,
		ImmutableRoot: root,
	})
	require.NoError(t, err, "v1 carries no digests and must still open")

	_, err = openBootstrappedImmutable(&BootstrapResult{
		ImmutableDir:     dir,
		ImmutableRoot:    root,
		ImmutableDigests: map[string]string{},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "digest map")
}
