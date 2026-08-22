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

package gcs

import (
	"context"
	"io"

	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blobbackup"
)

// Backup streams every key/value currently in the store to w. GCS has no
// native point-in-time snapshot primitive, so this is a full key-iteration
// dump instead -- see blobbackup.Backup's doc comment for the shared
// framing/consistency rationale common to every cloud blob plugin.
func (d *BlobStoreGCS) Backup(ctx context.Context, w io.Writer) error {
	return blobbackup.Backup(ctx, d, w, maxBlobReadBytes, "gcs backup")
}

// Restore replaces the store's contents by loading a backup stream produced
// by Backup. See blobbackup.Restore's doc comment for the shared
// partial-batch-commit durability and no-retry-against-the-same-store
// contract common to every cloud blob plugin.
func (d *BlobStoreGCS) Restore(ctx context.Context, r io.Reader) error {
	return blobbackup.Restore(ctx, d, r, maxBlobReadBytes, "gcs restore")
}

// ValidateBackup verifies the complete shared cloud backup framing without
// touching the configured bucket.
func (d *BlobStoreGCS) ValidateBackup(ctx context.Context, r io.Reader) error {
	return blobbackup.Validate(
		ctx, r, maxBlobReadBytes, "gcs backup validation",
	)
}

// Reset removes the configured prefix after lifecycle restore has retained a
// rollback backup of it.
func (d *BlobStoreGCS) Reset(ctx context.Context) error {
	return blobbackup.Reset(ctx, d, "gcs reset")
}
