//go:build dingo_extra_plugins

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
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestIsS3NotFoundErrorMatchesNoSuchKey verifies the case AWS's own S3
// actually returns from GetObject on a missing key.
func TestIsS3NotFoundErrorMatchesNoSuchKey(t *testing.T) {
	t.Parallel()

	require.True(t, isS3NotFoundError(&types.NoSuchKey{}))
}

// TestIsS3NotFoundErrorMatchesTypedNotFound guards the defensive
// s3types.NotFound check, kept even though AWS's own GetObject never
// actually constructs one.
func TestIsS3NotFoundErrorMatchesTypedNotFound(t *testing.T) {
	t.Parallel()

	require.True(t, isS3NotFoundError(&types.NotFound{}))
}

// TestIsS3NotFoundErrorMatchesGenericNotFoundCode guards against
// a real gap: an S3-compatible (non-AWS) endpoint can
// report a missing key as a generic, untyped API error whose ErrorCode()
// is "NotFound" without the SDK ever deserializing it into the strongly-
// typed s3types.NotFound struct -- errors.As for that concrete type alone
// would miss this, silently treating a confirmed-absent snapshot as a
// real communication failure instead.
func TestIsS3NotFoundErrorMatchesGenericNotFoundCode(t *testing.T) {
	t.Parallel()

	err := &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
	require.True(t, isS3NotFoundError(err))
}

// TestIsS3NotFoundErrorRejectsOtherErrors verifies real failures (auth,
// throttling, a generic error code that isn't "NotFound") are not
// misclassified as a confirmed-absent object.
func TestIsS3NotFoundErrorRejectsOtherErrors(t *testing.T) {
	t.Parallel()

	require.False(t, isS3NotFoundError(errors.New("connection reset")))
	require.False(t, isS3NotFoundError(
		&smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"},
	))
}

var _ smithy.APIError = (*smithy.GenericAPIError)(nil)

type manifestRoundTripper func(*http.Request) (*http.Response, error)

func (f manifestRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestS3ManifestByteLimits(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, WriteManifest(dir, Manifest{Network: "preview"}))
	data, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	require.NoError(t, err)
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprintf("oversized=%t", oversized), func(t *testing.T) {
			body := bytes.NewReader(data)
			client := s3.NewFromConfig(aws.Config{
				Region: "test", Credentials: aws.AnonymousCredentials{},
				HTTPClient: &http.Client{Transport: manifestRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Body: io.NopCloser(body), ContentLength: -1, Header: make(http.Header)}, nil
				})},
			})
			d := s3Destination{client: client, bucket: "test", prefix: "snapshot"}
			limit := int64(len(data))
			if oversized {
				limit = 3
			}
			_, err := d.FetchManifestWithOptions(context.Background(), WithManifestMaxBytes(limit))
			if oversized {
				require.ErrorContains(t, err, "size exceeds maximum")
				require.ErrorIs(t, err, ErrManifestTooLarge)
				require.False(t, errors.Is(err, ErrCloudSnapshotNotFound))
				require.Equal(t, 4, len(data)-body.Len(), "cloud read must stop at limit plus probe")
			} else {
				require.NoError(t, err)
				require.Zero(t, body.Len())
			}
		})
	}
}

func TestS3ListSnapshotsRejectsUnsafePrefixBeforeManifestFetch(t *testing.T) {
	t.Parallel()
	var manifestFetches atomic.Int32
	client := s3.NewFromConfig(aws.Config{
		Region: "test", Credentials: aws.AnonymousCredentials{},
		BaseEndpoint: aws.String("https://example.test"),
		HTTPClient: &http.Client{Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			body := ""
			if req.URL.Query().Get("list-type") == "2" {
				body = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>prefix/../</Prefix></CommonPrefixes></ListBucketResult>`
			} else {
				manifestFetches.Add(1)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})},
	}, func(options *s3.Options) { options.UsePathStyle = true })
	d := s3Destination{client: client, bucket: "test", prefix: "prefix"}

	entries, err := d.ListSnapshots(t.Context())
	require.Error(t, err)
	require.Empty(t, entries)
	require.Zero(t, manifestFetches.Load())
}

func TestS3SnapshotCatalogStopsAtPrefixBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, WriteManifest(dir, Manifest{Network: "preview"}))
	manifest, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	require.NoError(t, err)
	var manifestFetches atomic.Int32
	client := s3.NewFromConfig(aws.Config{
		Region: "test", Credentials: aws.AnonymousCredentials{},
		BaseEndpoint: aws.String("https://example.test"),
		HTTPClient: &http.Client{Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			body := manifest
			if req.URL.Query().Get("list-type") == "2" {
				body = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>prefix/one/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>prefix/two/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>prefix/three/</Prefix></CommonPrefixes></ListBucketResult>`)
			} else {
				manifestFetches.Add(1)
			}
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)), Header: make(http.Header), Request: req,
			}, nil
		})},
	}, func(options *s3.Options) { options.UsePathStyle = true })
	d := s3Destination{client: client, bucket: "test", prefix: "prefix"}

	entries, err := d.ListSnapshotCatalog(t.Context(), SnapshotCatalogScanBudget{
		MaxPrefixes: 2, MaxManifests: 2, MaxEntries: 2, MaxProblems: 2,
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogScanLimit)
	require.Len(t, entries, 2)
	require.Equal(t, int32(2), manifestFetches.Load())
}

func TestS3SnapshotCatalogCountsNonPrefixListResults(t *testing.T) {
	t.Parallel()
	client := s3.NewFromConfig(aws.Config{
		Region: "test", Credentials: aws.AnonymousCredentials{},
		BaseEndpoint: aws.String("https://example.test"),
		HTTPClient: &http.Client{Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			body := []byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prefix/a</Key></Contents><Contents><Key>prefix/b</Key></Contents><Contents><Key>prefix/c</Key></Contents></ListBucketResult>`)
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)), Header: make(http.Header), Request: req,
			}, nil
		})},
	}, func(options *s3.Options) { options.UsePathStyle = true })
	d := s3Destination{client: client, bucket: "test", prefix: "prefix"}

	entries, err := d.ListSnapshotCatalog(t.Context(), SnapshotCatalogScanBudget{
		MaxPrefixes: 2, MaxManifests: 2, MaxEntries: 2, MaxProblems: 2,
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogScanLimit)
	require.Empty(t, entries)
}

func TestS3UploadExcludesAndRemovesCloudMirrorMarker(t *testing.T) {
	t.Parallel()
	var methods []string
	client := s3.NewFromConfig(aws.Config{
		Region: "test", Credentials: aws.AnonymousCredentials{},
		BaseEndpoint: aws.String("https://example.test"),
		HTTPClient: &http.Client{Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method+" "+req.URL.EscapedPath())
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header), Request: req,
			}, nil
		})},
	})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, cloudMirrorMarkerName), []byte("legacy secret"), 0o600,
	))
	d := s3Destination{client: client, bucket: "bucket", prefix: "prefix/snapshot"}
	require.NoError(t, d.UploadDir(t.Context(), dir))
	require.Equal(t, []string{
		"DELETE /prefix/snapshot/.cloud-mirrored",
	}, methods)
	require.NoError(t, os.Remove(filepath.Join(dir, cloudMirrorMarkerName)))
	methods = nil
	require.NoError(t, d.UploadDir(t.Context(), dir))
	require.Equal(t, []string{
		"DELETE /prefix/snapshot/.cloud-mirrored",
	}, methods, "every successful upload removes a remote legacy marker")
}

func TestS3DownloadSkipsCloudMirrorMarker(t *testing.T) {
	t.Parallel()
	var objectFetches atomic.Int32
	client := s3.NewFromConfig(aws.Config{
		Region: "test", Credentials: aws.AnonymousCredentials{},
		BaseEndpoint: aws.String("https://example.test"),
		HTTPClient: &http.Client{Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			body := `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prefix/snapshot/.cloud-mirrored</Key></Contents></ListBucketResult>`
			if req.URL.Query().Get("list-type") != "2" {
				objectFetches.Add(1)
				body = "legacy secret"
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header), Request: req,
			}, nil
		})},
	}, func(options *s3.Options) { options.UsePathStyle = true })
	d := s3Destination{client: client, bucket: "bucket", prefix: "prefix/snapshot"}
	dir := t.TempDir()
	require.NoError(t, d.DownloadDir(t.Context(), dir))
	require.Zero(t, objectFetches.Load())
	require.NoFileExists(t, filepath.Join(dir, cloudMirrorMarkerName))
}
