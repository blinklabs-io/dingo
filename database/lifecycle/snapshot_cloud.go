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
	"context"
	"fmt"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database"
)

// SnapshotToCloud calls Snapshot to produce the local copy at dir exactly
// as before, then — if cloudDest is non-empty — additionally uploads
// dir's contents to that destination (a base URI like "s3://bucket/prefix"
// or "gcs://bucket/prefix"; see RegisterCloudDestinationScheme), nested
// one level under this snapshot's own ID (dir's base name), mirroring the
// local SnapshotDir/<snapshotID> layout: the actual upload target is
// cloudDest + "/" + filepath.Base(dir), not cloudDest itself. This is what
// makes ListCloudSnapshots able to enumerate multiple snapshots stored at
// the same configured cloudDest — a flat, unnested upload would silently
// overwrite every previous snapshot's files with the newest one's. The
// local copy is always kept; cloudDest is a mirror, not a replacement.
//
// cloudDest == "" skips the upload — existing local-only callers are
// unaffected.
//
// If the upload fails, the local snapshot is still valid and left in
// place, but this still returns an error: the operator asked for both
// copies, so a cloud-only failure is a real (partial) failure, not a
// silent degrade to local-only.
func SnapshotToCloud(
	ctx context.Context,
	db *database.Database,
	dir string,
	trigger string,
	dingoVersion string,
	cloudDest string,
) (Manifest, error) {
	manifest, err := Snapshot(ctx, db, dir, trigger, dingoVersion)
	if err != nil {
		return Manifest{}, err
	}
	if cloudDest == "" {
		return manifest, nil
	}
	snapshotCloudURI := JoinCloudURI(cloudDest, filepath.Base(dir))
	dest, err := ParseCloudDestination(snapshotCloudURI)
	if err != nil {
		return manifest, fmt.Errorf(
			"snapshot written locally to %q, but cloud destination is invalid: %w",
			dir, err,
		)
	}
	if err := dest.UploadDir(ctx, dir); err != nil {
		return manifest, fmt.Errorf(
			"snapshot written locally to %q, but upload to %q failed: %w",
			dir, snapshotCloudURI, err,
		)
	}
	return manifest, nil
}
