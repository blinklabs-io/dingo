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
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database"
)

// cloudMirrorMarkerName is a small marker file written inside a local
// snapshot directory the moment its cloud upload actually succeeds — not
// before. Its presence is what distinguishes "this snapshot is fully
// mirrored to cloud" from "the local copy exists but the cloud upload
// never completed (or failed)", which the snapshot directory's own
// existence alone cannot tell apart. See MirrorToCloud's doc comment for
// why this matters: a bare directory-existence check treats both cases
// identically, permanently skipping (and never retrying) a cloud mirror
// that failed on an earlier attempt.
const cloudMirrorMarkerName = ".cloud-mirrored"

// CloudMirrorMarkerPath returns the marker file path for the local
// snapshot directory dir, per cloudMirrorMarkerName's doc comment.
func CloudMirrorMarkerPath(dir string) string {
	return filepath.Join(dir, cloudMirrorMarkerName)
}

// IsCloudMirrored reports whether dir's cloud mirror marker is present —
// i.e. whether a previous MirrorToCloud call for this exact directory
// actually completed successfully, as opposed to dir merely existing
// locally.
func IsCloudMirrored(dir string) bool {
	_, err := os.Stat(CloudMirrorMarkerPath(dir))
	return err == nil
}

// MirrorToCloud uploads dir's contents to cloudDest (a base URI like
// "s3://bucket/prefix" or "gcs://bucket/prefix"; see
// RegisterCloudDestinationScheme), nested one level under this snapshot's
// own ID (dir's base name), mirroring the local SnapshotDir/<snapshotID>
// layout — see SnapshotToCloud's doc comment for why. Writes
// CloudMirrorMarkerPath(dir) the moment the upload actually succeeds, so
// a caller can later tell a fully-mirrored snapshot apart from one whose
// local copy exists but whose cloud upload never completed, and retry
// only the upload in that case rather than mistaking the local-only
// partial success for "already done".
//
// cloudDest == "" is a no-op (success, no marker written): nothing to
// mirror.
func MirrorToCloud(ctx context.Context, dir string, cloudDest string) error {
	if cloudDest == "" {
		return nil
	}
	snapshotCloudURI := JoinCloudURI(cloudDest, filepath.Base(dir))
	dest, err := ParseCloudDestination(snapshotCloudURI)
	if err != nil {
		return fmt.Errorf(
			"cloud destination %q is invalid: %w", snapshotCloudURI, err,
		)
	}
	defer closeCloudDestination(dest)
	if err := dest.UploadDir(ctx, dir); err != nil {
		return fmt.Errorf(
			"upload to %q failed: %w", snapshotCloudURI, err,
		)
	}
	if err := os.WriteFile(
		CloudMirrorMarkerPath(dir), []byte(snapshotCloudURI+"\n"), 0o600,
	); err != nil {
		return fmt.Errorf(
			"upload to %q succeeded, but recording the cloud-mirrored marker failed: %w",
			snapshotCloudURI, err,
		)
	}
	return nil
}

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
	blobPluginName string,
	metadataPluginName string,
	cloudDest string,
) (Manifest, error) {
	manifest, err := Snapshot(
		ctx, db, dir, trigger, dingoVersion, blobPluginName, metadataPluginName,
	)
	if err != nil {
		return Manifest{}, err
	}
	if err := MirrorToCloud(ctx, dir, cloudDest); err != nil {
		return manifest, fmt.Errorf(
			"snapshot written locally to %q, but %w", dir, err,
		)
	}
	return manifest, nil
}
