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

package gcs

import (
	"context"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/fakecloud"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const fakeBucket = "test-bucket"

// storeOnFakeCloud returns a store whose client talks to a fresh in-memory
// bucket over the SDK's HTTP transport. Production uses the gRPC transport; this
// does not claim gRPC framing coverage.
func storeOnFakeCloud(t *testing.T) (*BlobStoreGCS, *fakecloud.Store) {
	t.Helper()
	fc := fakecloud.New()
	store, err := NewWithOptions(WithBucket(fakeBucket))
	require.NoError(t, err)
	client, err := storage.NewClient(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Transport: fc}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store.client = client
	store.bucket = client.Bucket(fakeBucket)
	return store, fc
}

func TestGCSCommitUncertainOutcome(t *testing.T) {
	t.Parallel()
	store, fc := storeOnFakeCloud(t)
	storagetest.RunCloudCommitUncertainOutcome(t, fc, store)
}

func TestGCSPruneCommitVisibility(t *testing.T) {
	t.Parallel()
	store, fc := storeOnFakeCloud(t)
	storagetest.RunCloudPruneCommitVisibility(t, fc, store)
}

func TestGCSBlockNumberBoundWorkIsIndependentOfArchiveSize(t *testing.T) {
	t.Parallel()
	store, fc := storeOnFakeCloud(t)
	dbtest.RunCloudBlockNumberBoundWork(
		t, fc, fakeBucket, store,
		func(key []byte) string { return store.fullKey(string(key)) },
	)
}
