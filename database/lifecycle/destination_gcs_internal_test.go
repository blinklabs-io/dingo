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

//go:build dingo_extra_plugins

package lifecycle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

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
