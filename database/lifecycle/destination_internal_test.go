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

package lifecycle

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOrderEntriesManifestLastSortsManifestToEnd guards against a real
// invariant: a snapshot's manifest.json must never upload before every
// other backup payload has succeeded, since a concurrent lister/fetcher
// treats a cloud-visible manifest as "this snapshot is fully there" (see
// FetchCloudManifest/ListCloudSnapshots) and could otherwise download or
// restore an incomplete snapshot. os.ReadDir's alphabetical order would
// place "manifest.json" before "metadata.sqlite", so relying on
// directory order alone is not enough.
func TestOrderEntriesManifestLastSortsManifestToEnd(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{ManifestFileName, BlobBackupFileName, MetadataBackupFileName} {
		require.NoError(
			t,
			os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644),
		)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	ordered := orderEntriesManifestLast(entries)
	require.Len(t, ordered, 3)
	require.Equal(
		t, ManifestFileName, ordered[len(ordered)-1].Name(),
		"manifest.json must sort last regardless of directory order",
	)
	var nonManifest []string
	for _, e := range ordered[:len(ordered)-1] {
		nonManifest = append(nonManifest, e.Name())
	}
	require.ElementsMatch(
		t, []string{BlobBackupFileName, MetadataBackupFileName}, nonManifest,
	)
}

// TestOrderEntriesManifestLastNoManifestPresent verifies the function is
// a safe no-op reordering when no manifest.json entry exists at all.
func TestOrderEntriesManifestLastNoManifestPresent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, BlobBackupFileName), []byte("x"), 0o644,
	))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	ordered := orderEntriesManifestLast(entries)
	require.Len(t, ordered, 1)
	require.Equal(t, BlobBackupFileName, ordered[0].Name())
}

// TestJoinCloudURIPreservesQueryAndFragment guards against a real bug: a
// naive strings.TrimRight(base, "/") + "/" + sub concatenation lands sub
// after base's query string/fragment instead of before it, so every
// snapshot built from the same base (with a query string attached) would
// resolve to the exact same URI regardless of sub — silently discarding
// the snapshot ID from the path Bark then uploads to, lists, fetches, or
// restores from. JoinCloudURI must append sub to the parsed URL's Path
// specifically and re-serialize, keeping Query/Fragment ordered after the
// full path.
func TestJoinCloudURIPreservesQueryAndFragment(t *testing.T) {
	got := JoinCloudURI("s3://bucket/prefix?region=us-east-1", "abc123")
	require.Equal(t, "s3://bucket/prefix/abc123?region=us-east-1", got)

	got = JoinCloudURI("gcs://bucket/prefix#frag", "abc123")
	require.Equal(t, "gcs://bucket/prefix/abc123#frag", got)

	// No query/fragment: behaves exactly like the old plain concatenation.
	got = JoinCloudURI("s3://bucket/prefix", "abc123")
	require.Equal(t, "s3://bucket/prefix/abc123", got)

	// Trailing slash on base is trimmed before joining, same as before.
	got = JoinCloudURI("s3://bucket/prefix/", "abc123")
	require.Equal(t, "s3://bucket/prefix/abc123", got)
}

// TestParseCloudDestinationCleansNoncanonicalPath guards against a real
// bug: destination_s3.go/destination_gcs.go derive their upload prefix
// from the parsed URI's Path via path.Join (which cleans it), but their
// list/download/delete prefix matching compares against that same Path
// left uncleaned — so a URI with repeated slashes or "."/".." segments
// would make UploadDir write under one (cleaned) key while
// ListSnapshots/DownloadDir/Delete search under a different, uncleaned
// prefix, even though both derive from the exact same configured
// destination string. ParseCloudDestination must canonicalize u.Path
// before ever handing it to a registered factory.
func TestParseCloudDestinationCleansNoncanonicalPath(t *testing.T) {
	var gotPath string
	registry := NewDestinationRegistry()
	registry.Register(
		"cleantest",
		func(uri *url.URL) (CloudDestination, error) {
			gotPath = uri.Path
			return &fakeInternalCloudDestination{}, nil
		},
	)

	_, err := ParseCloudDestination(
		registry,
		"cleantest://bucket/prefix//sub/../other",
	)
	require.NoError(t, err)
	require.Equal(
		t,
		"/prefix/other",
		gotPath,
		"factory must see an already-cleaned path, not the raw noncanonical one",
	)
}

// fakeInternalCloudDestination is a minimal CloudDestination used only to
// satisfy ParseCloudDestination's factory signature in this package's
// internal tests, which need to inspect the *url.URL a factory is called
// with directly rather than round-tripping through an actual upload/
// download (destination_test.go's fakeCloudDestination, in the external
// _test package, already covers that).
type fakeInternalCloudDestination struct{}

func (*fakeInternalCloudDestination) UploadDir(
	context.Context,
	string,
) error {
	return nil
}

func (*fakeInternalCloudDestination) DownloadDir(
	context.Context,
	string,
) error {
	return nil
}

// TestDestinationRegistryRegisterNilReceiverIsNoOp guards against a real
// panic: DestinationRegistry's doc comment promises "every method here is
// nil-safe" for a nil *DestinationRegistry, and recognizedCloudScheme/
// ParseCloudDestination both already special-case r == nil, but Register
// used to dereference r.mu directly with no such guard — so registering a
// builtin scheme (RegisterS3/RegisterGCS/RegisterBuiltinDestinations) onto
// a nil registry (a valid, documented configuration for "no cloud
// destinations wanted") would panic instead of silently doing nothing.
func TestDestinationRegistryRegisterNilReceiverIsNoOp(t *testing.T) {
	var r *DestinationRegistry
	require.NotPanics(t, func() {
		r.Register("s3", func(*url.URL) (CloudDestination, error) {
			return nil, nil
		})
	})
}
