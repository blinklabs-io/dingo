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

package mithril

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// The tests below run the store contract and a produce, prune and sync cycle
// against real S3-compatible and GCS-compatible services (MinIO and
// fake-gcs-server). They skip unless the service is configured:
//
//	DINGO_TEST_S3_ENDPOINT          http://host:port of an S3-compatible service
//	AWS_ACCESS_KEY_ID/_SECRET_ACCESS_KEY  its credentials
//	DINGO_TEST_GCS_EMULATOR_HOST    host:port of a GCS emulator (plain HTTP)

// requireStoreCycle produces two snapshots into store, prunes to the newest and
// syncs from a handler over it.
func requireStoreCycle(t *testing.T, store ArtifactStore) {
	t.Helper()
	hashes := createSnapshots(t, store, 1, 3)
	removed, err := PruneSnapshots(context.Background(), store, 1)
	require.NoError(t, err)
	require.Equal(t, hashes[:1], removed)
	require.Equal(t, hashes[1:], snapshotHashes(t, store))

	srv := newMithrilTestServer(t, ServerConfig{Store: store})
	result, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:           "preprod",
		Backend:           BackendV2,
		AggregatorURL:     srv.URL,
		AllowInsecureHTTP: true,
		DownloadDir:       t.TempDir(),
		Logger:            slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	assert.Equal(t, hashes[1], result.Snapshot.Digest)
}

// Not t.Parallel: the S3 SDK reads AWS_ENDPOINT from the environment.
func TestLiveS3ArtifactStore(t *testing.T) {
	endpoint := os.Getenv("DINGO_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("DINGO_TEST_S3_ENDPOINT not set")
	}
	t.Setenv("AWS_ENDPOINT", endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	bucket := fmt.Sprintf("dingo-mithril-%d", time.Now().UnixNano())
	prefix := "snapshots"

	store, err := OpenArtifactStore(
		context.Background(), "s3://"+bucket+"/"+prefix,
	)
	require.NoError(t, err)
	s3Store, ok := store.(*s3ArtifactStore)
	require.True(t, ok)
	_, err = s3Store.client.CreateBucket(
		context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)},
	)
	require.NoError(t, err)
	t.Cleanup(func() { removeS3Bucket(t, s3Store.client, bucket) })

	requireArtifactStoreContract(t, store)
	requireStoreCycle(t, store)
}

// Not t.Parallel: the GCS SDK reads STORAGE_EMULATOR_HOST from the
// environment.
func TestLiveGCSArtifactStore(t *testing.T) {
	host := os.Getenv("DINGO_TEST_GCS_EMULATOR_HOST")
	if host == "" {
		t.Skip("DINGO_TEST_GCS_EMULATOR_HOST not set")
	}
	t.Setenv("STORAGE_EMULATOR_HOST", host)
	bucket := fmt.Sprintf("dingo-mithril-%d", time.Now().UnixNano())

	client, err := storage.NewClient(
		context.Background(), option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Bucket(bucket).Create(
		context.Background(), "dingo-test", nil,
	))
	t.Cleanup(func() { removeGCSBucket(t, client.Bucket(bucket)) })

	store, err := OpenArtifactStore(
		context.Background(), "gcs://"+bucket+"/snapshots",
	)
	require.NoError(t, err)
	require.IsType(t, &gcsArtifactStore{}, store)

	requireArtifactStoreContract(t, store)
	requireStoreCycle(t, store)
}

// removeS3Bucket deletes every object in bucket and then the bucket, so runs
// do not accumulate resources in the service.
func removeS3Bucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(
		client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)},
	)
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		require.NoError(t, err)
		for _, obj := range page.Contents {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: obj.Key,
			})
			require.NoError(t, err)
		}
	}
	_, err := client.DeleteBucket(
		ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)},
	)
	require.NoError(t, err)
}

// removeGCSBucket deletes every object in bucket and then the bucket.
func removeGCSBucket(t *testing.T, bucket *storage.BucketHandle) {
	t.Helper()
	ctx := context.Background()
	objects := bucket.Objects(ctx, nil)
	for {
		attrs, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, bucket.Object(attrs.Name).Delete(ctx))
	}
	require.NoError(t, bucket.Delete(ctx))
}
