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
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	gcsapi "google.golang.org/api/storage/v1"
)

func newTestGCSCatalogPageLister(
	t *testing.T,
	client *http.Client,
) gcsCatalogPageLister {
	t.Helper()
	service, err := gcsapi.NewService(
		t.Context(),
		option.WithHTTPClient(client),
	)
	require.NoError(t, err)
	return &gcsJSONCatalogPageLister{service: service}
}

// Exercise the destination through a real GCS SDK reader with an in-memory
// HTTP transport. Production uses the SDK's gRPC transport; this does not
// claim live GCS or gRPC framing coverage.
func TestGCSManifestByteLimits(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, WriteManifest(dir, Manifest{Network: "preview"}))
	data, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	require.NoError(t, err)
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprintf("oversized=%t", oversized), func(t *testing.T) {
			body := bytes.NewReader(data)
			client, err := storage.NewClient(context.Background(), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{
				Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(body), ContentLength: int64(len(data)), Header: make(http.Header), Request: req}, nil
				}),
			}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			d := gcsDestination{client: client, bucket: client.Bucket("test"), prefix: "snapshot"}
			limit := int64(len(data))
			if oversized {
				limit = 3
			}
			manifest, err := d.FetchManifestWithOptions(context.Background(), WithManifestMaxBytes(limit))
			if oversized {
				require.ErrorIs(t, err, ErrManifestTooLarge)
				require.NotErrorIs(t, err, ErrCloudSnapshotNotFound)
				require.Equal(t, 4, len(data)-body.Len(), "GCS reader must stop at limit plus probe")
			} else {
				require.NoError(t, err)
				require.Equal(t, "preview", manifest.Network)
				require.Zero(t, body.Len())
			}
		})
	}
}

func TestGCSListSnapshotsRejectsUnsafePrefixBeforeManifestFetch(t *testing.T) {
	t.Parallel()
	var manifestFetches atomic.Int32
	httpClient := &http.Client{
		Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			if req.URL.Query().Get("delimiter") != "/" {
				manifestFetches.Add(1)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"prefixes":["prefix/../"]}`,
				)),
				Header:  make(http.Header),
				Request: req,
			}, nil
		}),
	}
	client, err := storage.NewClient(
		t.Context(),
		option.WithoutAuthentication(),
		option.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	d := gcsDestination{
		client: client, bucket: client.Bucket("test"), bucketName: "test",
		catalogPages: newTestGCSCatalogPageLister(t, httpClient), prefix: "prefix",
	}

	entries, err := d.ListSnapshots(t.Context())
	require.Error(t, err)
	require.Empty(t, entries)
	require.Zero(t, manifestFetches.Load())
}

func TestGCSSnapshotCatalogStopsAtPrefixBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, WriteManifest(dir, Manifest{Network: "preview"}))
	manifest, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	require.NoError(t, err)
	var manifestFetches atomic.Int32
	httpClient := &http.Client{
		Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			body := manifest
			if req.URL.Query().Get("delimiter") == "/" {
				body = []byte(`{"prefixes":["prefix/one/","prefix/two/","prefix/three/"]}`)
			} else {
				manifestFetches.Add(1)
			}
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)), Header: make(http.Header), Request: req,
			}, nil
		}),
	}
	client, err := storage.NewClient(
		t.Context(), option.WithoutAuthentication(),
		option.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	d := gcsDestination{
		client: client, bucket: client.Bucket("test"), bucketName: "test",
		catalogPages: newTestGCSCatalogPageLister(t, httpClient), prefix: "prefix",
	}

	entries, err := d.ListSnapshotCatalog(t.Context(), SnapshotCatalogScanBudget{
		MaxPages: 2, MaxPrefixes: 2, MaxManifests: 2, MaxEntries: 2, MaxProblems: 2,
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogScanLimit)
	require.Len(t, entries, 2)
	require.Equal(t, int32(2), manifestFetches.Load())
}

func TestGCSSnapshotCatalogCountsNonPrefixListResults(t *testing.T) {
	t.Parallel()
	httpClient := &http.Client{
		Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			body := []byte(`{"items":[{"name":"prefix/a"},{"name":"prefix/b"},{"name":"prefix/c"}]}`)
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)), Header: make(http.Header), Request: req,
			}, nil
		}),
	}
	client, err := storage.NewClient(
		t.Context(), option.WithoutAuthentication(),
		option.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	d := gcsDestination{
		client: client, bucket: client.Bucket("test"), bucketName: "test",
		catalogPages: newTestGCSCatalogPageLister(t, httpClient), prefix: "prefix",
	}

	entries, err := d.ListSnapshotCatalog(t.Context(), SnapshotCatalogScanBudget{
		MaxPages: 2, MaxPrefixes: 2, MaxManifests: 2, MaxEntries: 2, MaxProblems: 2,
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogScanLimit)
	require.Empty(t, entries)
}

func TestGCSSnapshotCatalogStopsBeforePageRequestLimit(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	httpClient := &http.Client{
		Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
			request := requests.Add(1)
			var body []byte
			switch request {
			case 1:
				body = []byte(`{"nextPageToken":"page-1"}`)
			case 2:
				body = []byte(`{"items":[{"name":"prefix/noise"}],"nextPageToken":"page-2"}`)
			default:
				return nil, fmt.Errorf(
					"unexpected provider page request %d", request,
				)
			}
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)), Header: make(http.Header), Request: req,
			}, nil
		}),
	}
	client, err := storage.NewClient(
		t.Context(), option.WithoutAuthentication(),
		option.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	d := gcsDestination{
		client: client, bucket: client.Bucket("test"), bucketName: "test",
		catalogPages: newTestGCSCatalogPageLister(t, httpClient), prefix: "prefix",
	}

	entries, err := d.ListSnapshotCatalog(t.Context(), SnapshotCatalogScanBudget{
		MaxPages: 2, MaxPrefixes: 1000, MaxManifests: 1000,
		MaxEntries: 1000, MaxProblems: 2,
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogScanLimit)
	require.Empty(t, entries)
	require.Equal(t, int32(2), requests.Load())
}

func TestGCSUploadExcludesAndRemovesCloudMirrorMarker(t *testing.T) {
	t.Parallel()
	var methods []string
	client, err := storage.NewClient(
		t.Context(), option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{
			Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method+" "+req.URL.EscapedPath())
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       io.NopCloser(strings.NewReader("")),
					Header:     make(http.Header), Request: req,
				}, nil
			}),
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, cloudMirrorMarkerName), []byte("legacy secret"), 0o600,
	))
	d := gcsDestination{
		client: client, bucket: client.Bucket("bucket"), prefix: "prefix/snapshot",
	}
	require.NoError(t, d.UploadDir(t.Context(), dir))
	require.Len(t, methods, 1)
	require.Contains(t, methods[0], "DELETE ")
	require.Contains(t, methods[0], ".cloud-mirrored")
	require.NoError(t, os.Remove(filepath.Join(dir, cloudMirrorMarkerName)))
	methods = nil
	require.NoError(t, d.UploadDir(t.Context(), dir))
	require.Len(t, methods, 1)
	require.Contains(t, methods[0], "DELETE ")
	require.Contains(t, methods[0], ".cloud-mirrored")
}

func TestGCSDownloadSkipsCloudMirrorMarker(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	client, err := storage.NewClient(
		t.Context(), option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{
			Transport: manifestRoundTripper(func(req *http.Request) (*http.Response, error) {
				requests.Add(1)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(strings.NewReader(
						`{"items":[{"name":"prefix/snapshot/.cloud-mirrored"}]}`,
					)),
					Header: make(http.Header), Request: req,
				}, nil
			}),
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	d := gcsDestination{
		client: client, bucket: client.Bucket("bucket"), prefix: "prefix/snapshot",
	}
	dir := t.TempDir()
	require.NoError(t, d.DownloadDir(t.Context(), dir))
	require.Equal(t, int32(1), requests.Load())
	require.NoFileExists(t, filepath.Join(dir, cloudMirrorMarkerName))
}
