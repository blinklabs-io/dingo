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
	"io"

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
