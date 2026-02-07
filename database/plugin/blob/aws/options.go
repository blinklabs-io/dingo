// Copyright 2025 Blink Labs Software
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
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type BlobStoreS3OptionFunc func(*BlobStoreS3)

// WithLogger specifies the logger object to use for logging messages
func WithLogger(logger *slog.Logger) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.logger = NewS3Logger(logger)
	}
}

// WithPromRegistry specifies the prometheus registry to use for metrics
func WithPromRegistry(
	registry prometheus.Registerer,
) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.promRegistry = registry
	}
}

// WithBucket specifies the S3 bucket name
func WithBucket(bucket string) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.bucket = bucket
	}
}

// WithRegion specifies the AWS region
func WithRegion(region string) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.region = region
	}
}

// WithPrefix specifies the S3 object prefix
func WithPrefix(prefix string) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.prefix = prefix
	}
}

// WithTimeout specifies the timeout for AWS config loading
func WithTimeout(timeout time.Duration) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.timeout = timeout
	}
}

// WithEndpoint specifies a custom endpoint for aws s3.  This is generally used for testing
// against a fake s3 implementation such as minio (https://github.com/minio/minio)
func WithEndpoint(endpoint string) BlobStoreS3OptionFunc {
	return func(b *BlobStoreS3) {
		b.endpoint = endpoint
	}
}
