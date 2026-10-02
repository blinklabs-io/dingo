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

package aws

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/internal/test/fakecloud"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/stretchr/testify/require"
)

const fakeBucket = "test-bucket"

// storeOnFakeCloud returns a store whose client talks to a fresh in-memory
// bucket. The SDK does not retry, so an injected failure reaches Commit once.
func storeOnFakeCloud(t *testing.T) (*BlobStoreS3, *fakecloud.Store) {
	t.Helper()
	fc := fakecloud.New()
	store, err := NewWithOptions(WithBucket(fakeBucket), WithRegion("us-east-1"))
	require.NoError(t, err)
	store.client = s3.New(s3.Options{
		BaseEndpoint:               aws.String("https://s3.fake.test"),
		Region:                     "us-east-1",
		UsePathStyle:               true,
		RetryMaxAttempts:           1,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		HTTPClient:                 &http.Client{Transport: fc},
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	return store, fc
}

func TestS3CommitUncertainOutcome(t *testing.T) {
	t.Parallel()
	store, fc := storeOnFakeCloud(t)
	storagetest.RunCloudCommitUncertainOutcome(t, fc, store)
}

func TestS3PruneCommitVisibility(t *testing.T) {
	t.Parallel()
	store, fc := storeOnFakeCloud(t)
	storagetest.RunCloudPruneCommitVisibility(t, fc, store)
}
