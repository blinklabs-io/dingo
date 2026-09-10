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

package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blobbackup"
)

// Backup streams every key/value currently in the store to w. S3 has no
// native point-in-time snapshot primitive, so this is a full key-iteration
// dump instead -- see blobbackup.Backup's doc comment for the shared
// framing/consistency rationale common to every cloud blob plugin.
func (d *BlobStoreS3) Backup(ctx context.Context, w io.Writer) error {
	return blobbackup.Backup(ctx, d, w, maxBlobReadBytes, "s3 backup")
}

// Restore replaces the store's contents by loading a backup stream produced
// by Backup. See blobbackup.Restore's doc comment for the shared
// partial-batch-commit durability and no-retry-against-the-same-store
// contract common to every cloud blob plugin.
func (d *BlobStoreS3) Restore(ctx context.Context, r io.Reader) error {
	return blobbackup.Restore(ctx, d, r, maxBlobReadBytes, "s3 restore")
}

// ValidateBackup verifies the complete shared cloud backup framing without
// touching the configured bucket.
func (d *BlobStoreS3) ValidateBackup(ctx context.Context, r io.Reader) error {
	return blobbackup.Validate(
		ctx, r, maxBlobReadBytes, "s3 backup validation",
	)
}

// Reset removes the configured prefix after lifecycle restore has retained a
// rollback backup of it.
func (d *BlobStoreS3) Reset(ctx context.Context) error {
	return blobbackup.Reset(ctx, d, d.resetBatch, "s3 reset")
}

// resetBatch uses S3's native multi-object delete instead of the ordinary
// transaction path. Restore has already retained a complete rollback backup,
// so downloading every prior value into a second per-transaction compensation
// log would double the full-prefix transfer without improving recoverability.
func (d *BlobStoreS3) resetBatch(ctx context.Context, keys [][]byte) error {
	if len(keys) > blobbackup.DefaultRestoreBatchRecords {
		return fmt.Errorf(
			"S3 reset batch has %d keys; maximum is %d",
			len(keys), blobbackup.DefaultRestoreBatchRecords,
		)
	}
	objects := make([]s3types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, s3types.ObjectIdentifier{
			Key: aws.String(d.fullKey(string(key))),
		})
	}

	timeout := d.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	deleteCtx, cancel := context.WithTimeout(ctx, timeout)
	output, err := d.client.DeleteObjects(deleteCtx, &s3.DeleteObjectsInput{
		Bucket: aws.String(d.bucket),
		Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	cancel()
	if err != nil {
		return err
	}
	deleteErrs := make([]error, 0, len(output.Errors))
	for _, deleteErr := range output.Errors {
		deleteErrs = append(deleteErrs, fmt.Errorf(
			"delete %q: %s: %s",
			aws.ToString(deleteErr.Key),
			aws.ToString(deleteErr.Code),
			aws.ToString(deleteErr.Message),
		))
	}
	return errors.Join(deleteErrs...)
}
