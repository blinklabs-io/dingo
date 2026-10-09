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
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/internal/test/fakecloud"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const downloadTestBucket = "test-bucket"

// downloadDestinations returns the S3 and GCS destinations at prefix "snap" of
// a fresh in-memory bucket, over the SDKs' HTTP transports. Production GCS
// uses gRPC; this does not claim gRPC framing coverage.
func downloadDestinations(
	t *testing.T,
) (*fakecloud.Store, map[string]CloudDestination) {
	t.Helper()
	fc := fakecloud.New()
	s3Client := s3.New(s3.Options{
		BaseEndpoint:               aws.String("https://s3.fake.test"),
		Region:                     "us-east-1",
		UsePathStyle:               true,
		RetryMaxAttempts:           1,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		HTTPClient:                 &http.Client{Transport: fc},
		Credentials: credentials.NewStaticCredentialsProvider(
			"test",
			"test",
			"",
		),
	})
	gcsClient, err := storage.NewClient(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Transport: fc}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, gcsClient.Close()) })
	return fc, map[string]CloudDestination{
		"s3": &s3Destination{
			bucket: downloadTestBucket, prefix: "snap", client: s3Client,
		},
		"gcs": &gcsDestination{
			client: gcsClient, bucket: gcsClient.Bucket(downloadTestBucket), prefix: "snap",
		},
	}
}

func TestDownloadFilesFetchesOnlyTheNamedObjects(t *testing.T) {
	t.Parallel()
	fc, dests := downloadDestinations(t)
	fc.Put(downloadTestBucket, "snap/blob.bak", []byte("blob"))
	fc.Put(downloadTestBucket, "snap/metadata.sqlite", []byte("metadata"))
	// Objects nobody declared: a writer to the prefix can add as many as it likes.
	fc.Put(downloadTestBucket, "snap/extra.bin", bytes.Repeat([]byte{1}, 1<<16))
	fc.Put(downloadTestBucket, "snap/more.bin", bytes.Repeat([]byte{2}, 1<<16))

	for name, dest := range dests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			gets := fc.Requests("GET object")
			require.NoError(
				t,
				dest.DownloadFiles(context.Background(), dir, []DownloadFile{
					{Name: "blob.bak", MaxBytes: 4},
					{Name: "metadata.sqlite", MaxBytes: 8},
				}),
			)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			require.Equal(t, 2, fc.Requests("GET object")-gets)
			require.Zero(
				t,
				fc.Requests("LIST"),
				"the prefix must not be listed",
			)
			got, err := os.ReadFile(filepath.Join(dir, "blob.bak"))
			require.NoError(t, err)
			require.Equal(t, []byte("blob"), got)
		})
	}
}

func TestDownloadFilesRejectsObjectLargerThanDeclared(t *testing.T) {
	t.Parallel()
	fc, dests := downloadDestinations(t)
	fc.Put(downloadTestBucket, "snap/blob.bak", bytes.Repeat([]byte{1}, 1025))

	for name, dest := range dests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			err := dest.DownloadFiles(context.Background(), dir, []DownloadFile{
				{Name: "blob.bak", MaxBytes: 1024},
			})
			require.ErrorIs(t, err, ErrDownloadTooLarge)
			require.NoFileExists(t, filepath.Join(dir, "blob.bak"))
		})
	}
}

func TestDownloadFilesReportsMissingObjectAsSnapshotNotFound(t *testing.T) {
	t.Parallel()
	_, dests := downloadDestinations(t)
	for name, dest := range dests {
		t.Run(name, func(t *testing.T) {
			err := dest.DownloadFiles(
				context.Background(),
				t.TempDir(),
				[]DownloadFile{
					{Name: "blob.bak", MaxBytes: 1},
				},
			)
			require.ErrorIs(t, err, ErrCloudSnapshotNotFound)
		})
	}
}

func TestDownloadFilesRejectsUnsafeNames(t *testing.T) {
	t.Parallel()
	_, dests := downloadDestinations(t)
	for name, dest := range dests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			err := dest.DownloadFiles(context.Background(), dir, []DownloadFile{
				{Name: "../escape", MaxBytes: 1},
			})
			require.ErrorContains(t, err, "unsafe snapshot file name")
		})
	}
}
