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
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type credentialEchoDestination struct{ message string }

type credentialBearingProviderError struct {
	message string
	cause   error
}

func (e *credentialBearingProviderError) Error() string { return e.message }
func (e *credentialBearingProviderError) Unwrap() error { return e.cause }

func (d credentialEchoDestination) UploadDir(context.Context, string) error {
	return errors.New(d.message)
}
func (d credentialEchoDestination) DownloadFiles(context.Context, string, []DownloadFile) error {
	return errors.New(d.message)
}
func (d credentialEchoDestination) ListSnapshots(context.Context, ...ManifestOption) ([]SnapshotEntry, error) {
	return nil, errors.New(d.message)
}
func (d credentialEchoDestination) FetchManifest(context.Context) (Manifest, error) {
	return Manifest{}, errors.New(d.message)
}
func (d credentialEchoDestination) Delete(context.Context) error {
	return errors.New(d.message)
}

func TestCloudDestinationDisplayAndErrorsHideCredentials(t *testing.T) {
	t.Parallel()
	const raw = "leaky://user:secret@bucket/prefix?token=private#fragment"
	require.Equal(t, "leaky://bucket/prefix", CloudDestinationDisplay(raw))
	assertSafe := func(err error) {
		t.Helper()
		require.Error(t, err)
		for _, secret := range []string{"user", "secret", "private", "fragment"} {
			require.NotContains(t, err.Error(), secret)
		}
	}

	factoryRegistry := NewDestinationRegistry()
	factoryRegistry.Register("leaky", func(uri *url.URL) (CloudDestination, error) {
		return nil, errors.New(uri.String())
	})
	_, err := ParseCloudDestination(factoryRegistry, raw)
	assertSafe(err)
	encodedFragmentRegistry := NewDestinationRegistry()
	encodedFragmentRegistry.Register("encoded", func(uri *url.URL) (CloudDestination, error) {
		return nil, errors.New(uri.EscapedFragment())
	})
	_, err = ParseCloudDestination(
		encodedFragmentRegistry,
		"encoded://bucket/prefix#encoded%2Fsecret",
	)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "encoded%2Fsecret")
	require.NotContains(t, err.Error(), "encoded/secret")
	require.NotContains(t, err.Error(), "secret")

	registry := NewDestinationRegistry()
	registry.Register("leaky", func(uri *url.URL) (CloudDestination, error) {
		return credentialEchoDestination{message: uri.String()}, nil
	})
	_, _, err = ListCloudSnapshots(t.Context(), registry, raw)
	assertSafe(err)
	_, _, err = FetchCloudManifest(t.Context(), registry, raw)
	assertSafe(err)
	_, err = DeleteCloudSnapshot(t.Context(), registry, raw)
	assertSafe(err)
	_, cleanup, err := downloadCloudFiles(
		t.Context(), registry, raw, "",
		[]DownloadFile{{Name: ManifestFileName, MaxBytes: MaxManifestBytes}},
	)
	if cleanup != nil {
		cleanup()
	}
	assertSafe(err)
	_, err = parseCloudDestinationURL("%zz-secret")
	assertSafe(err)
}

func TestSanitizedCloudErrorDoesNotExposeProviderError(t *testing.T) {
	t.Parallel()
	const raw = "leaky://user:secret@bucket/prefix?token=private#fragment"
	providerSentinel := errors.New("provider sentinel")
	providerErr := &credentialBearingProviderError{
		message: raw,
		cause:   errors.Join(providerSentinel, context.Canceled),
	}
	err := sanitizeCloudError(raw, providerErr)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "user")
	require.NotContains(t, err.Error(), "secret")
	require.NotContains(t, err.Error(), "private")
	require.NotContains(t, err.Error(), "fragment")
	require.Nil(t, errors.Unwrap(err))
	var recovered *credentialBearingProviderError
	require.False(t, errors.As(err, &recovered))
	require.Nil(t, recovered)
	require.ErrorIs(t, err, providerSentinel)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCloudDestinationIdentityDistinguishesProviderVisibleComponents(t *testing.T) {
	t.Parallel()
	base := "s3://user:secret@bucket/prefix?region=us-east-1#one"
	got, err := cloudDestinationIdentity(base)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "v1:sha256:"))
	require.NotContains(t, got, "user")
	require.NotContains(t, got, "secret")
	for _, distinct := range []string{
		"s3://other:secret@bucket/prefix?region=us-east-1#one",
		"s3://user:secret@bucket/prefix?region=us-west-2#one",
		"s3://user:secret@bucket/prefix?region=us-east-1#two",
		"s3://user:secret@bucket/other?region=us-east-1#one",
	} {
		identity, err := cloudDestinationIdentity(distinct)
		require.NoError(t, err)
		require.NotEqual(t, got, identity, distinct)
	}
}

func writeTestUploadSnapshot(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, BlobBackupFileName), []byte("blob"), 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, MetadataBackupFileName), []byte("metadata"), 0o600,
	))
	require.NoError(t, WriteManifest(dir, Manifest{
		BlobBytes:     4,
		MetadataBytes: 8,
		BlobSHA256: "fa2c8cc4f28176bbeed4b736df569a34" +
			"c79cd3723e9ec42f9674b4d46ac6b8b8",
		MetadataSHA256: "45447b7afbd5e544f7d0f1df0fccd260" +
			"14d9850130abd3f020b89ff96b82079f",
	}))
}

func TestOpenSnapshotUploadFilesUsesDeclaredFilesInPublicationOrder(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestUploadSnapshot(t, dir)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "unrelated"), []byte("secret"), 0o600,
	))

	files, _, root, err := openSnapshotUploadFiles(
		t.Context(), dir, MaxManifestBytes,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, errors.Join(
			closeSnapshotUploadFiles(files), root.Close(),
		))
	})

	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, file.name)
	}
	require.Equal(
		t,
		[]string{BlobBackupFileName, MetadataBackupFileName, ManifestFileName},
		names,
	)
}

func TestOpenSnapshotUploadFilesRejectsSymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestUploadSnapshot(t, dir)
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))
	require.NoError(t, os.Remove(filepath.Join(dir, BlobBackupFileName)))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, BlobBackupFileName)))

	files, _, _, err := openSnapshotUploadFiles(
		t.Context(), dir, MaxManifestBytes,
	)
	require.ErrorContains(t, err, "open snapshot upload file")
	require.Empty(t, files)
}

func TestOpenSnapshotUploadFilesKeepsOpenedFileAfterPathSwap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestUploadSnapshot(t, dir)
	files, _, root, err := openSnapshotUploadFiles(
		t.Context(), dir, MaxManifestBytes,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, errors.Join(
			closeSnapshotUploadFiles(files), root.Close(),
		))
	})

	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))
	require.NoError(t, os.Remove(filepath.Join(dir, BlobBackupFileName)))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, BlobBackupFileName)))

	data, err := io.ReadAll(files[0].file)
	require.NoError(t, err)
	require.Equal(t, "blob", string(data))
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
	t.Parallel()

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

func TestFetchCloudSnapshotEntryRejectsUnsafeIDBeforeFetch(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"", ".", "..", "../escape", `..\escape`, "a/b"} {
		fetches := 0
		_, err := fetchCloudSnapshotEntry(
			t.Context(), id,
			func(context.Context, string) (Manifest, error) {
				fetches++
				return Manifest{}, nil
			},
		)
		require.Error(t, err, id)
		require.Zero(t, fetches, "unsafe ID %q reached manifest fetch", id)
	}

	fetches := 0
	entry, err := fetchCloudSnapshotEntry(
		t.Context(), "safe-id",
		func(_ context.Context, id string) (Manifest, error) {
			fetches++
			require.Equal(t, "safe-id", id)
			return Manifest{CreatedAt: time.Unix(1, 0)}, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, fetches)
	require.Equal(t, "safe-id", entry.ID)
}

// TestParseCloudDestinationCleansNoncanonicalPath guards against a real
// bug: destination_s3.go/destination_gcs.go derive their upload prefix
// from the parsed URI's Path via path.Join (which cleans it), but their
// list/download/delete prefix matching compares against that same Path
// left uncleaned — so a URI with repeated slashes or "."/".." segments
// would make UploadDir write under one (cleaned) key while
// ListSnapshots/DownloadFiles/Delete search under a different, uncleaned
// prefix, even though both derive from the exact same configured
// destination string. ParseCloudDestination must canonicalize u.Path
// before ever handing it to a registered factory.
func TestParseCloudDestinationCleansNoncanonicalPath(t *testing.T) {
	t.Parallel()

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

func (*fakeInternalCloudDestination) DownloadFiles(
	context.Context,
	string,
	[]DownloadFile,
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
	t.Parallel()

	var r *DestinationRegistry
	require.NotPanics(t, func() {
		r.Register("s3", func(*url.URL) (CloudDestination, error) {
			return nil, nil
		})
	})
}

type boundaryFailureReader struct {
	data    *bytes.Reader
	failure error
}

func (r boundaryFailureReader) Read(p []byte) (int, error) {
	if r.data.Len() == 0 {
		return 0, r.failure
	}
	return r.data.Read(p)
}

func TestCopyBoundedPropagatesBoundaryReadFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("transfer failed")
	err := copyBounded(io.Discard, boundaryFailureReader{bytes.NewReader([]byte("abc")), failure}, 3, "payload")
	require.ErrorIs(t, err, failure)
}

func TestWriteBoundedFilePreservesExistingOutput(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(path, []byte("original"), 0o600))
	require.Error(t, writeBoundedFile(path, bytes.NewReader([]byte("oversized")), DownloadFile{Name: "payload", MaxBytes: 1}))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "original", string(got))
}

func TestFetchLocalPayloadsUsesIndependentBoundedCopies(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{BlobBackupFileName, MetadataBackupFileName} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("original"), 0o600))
	}
	copied, cleanup, err := fetchPayloads(context.Background(), nil, dir, t.TempDir(), Manifest{BlobBytes: 8, MetadataBytes: 8})
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NotEqual(t, dir, copied)
	require.NoError(t, os.WriteFile(filepath.Join(dir, BlobBackupFileName), []byte("replaced"), 0o600))
	got, err := os.ReadFile(filepath.Join(copied, BlobBackupFileName))
	require.NoError(t, err)
	require.Equal(t, "original", string(got))
}

func TestHashFileContextRejectsCancellation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0o600))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := hashFileContext(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
}
