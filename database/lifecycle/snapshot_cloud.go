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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/keystore"
)

// cloudMirrorMarkerName is a small marker file written inside a local
// snapshot directory after its cloud upload and catalog update or repair
// succeed. It contains only the destination identity digest. Its presence is
// what distinguishes a complete mirror from a local copy that still needs an
// upload or catalog repair.
const (
	cloudMirrorMarkerName = ".cloud-mirrored"
	// MirrorToCloud adds one newline to the fixed-size destination identity.
	maxCloudMirrorMarkerBytes = cloudDestinationIdentityBytes + 1
)

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
	marker, err := openCloudMirrorMarker(dir)
	if err != nil {
		return false
	}
	return marker.Close() == nil
}

// IsCloudMirroredTo reports whether dir's cloud mirror marker records the
// destination cloudDest resolves to right now, not merely that some marker
// is present. This matters because cloudDest is operator-configured and can
// change (e.g. pointed at a new bucket) between when a snapshot was
// mirrored and now: a marker left over from a since-abandoned destination
// must not be mistaken for "already mirrored to the currently configured
// destination" -- that destination has never actually received this
// snapshot's data, and treating it as done would silently skip mirroring
// it there.
func IsCloudMirroredTo(dir string, cloudDest string) bool {
	if cloudDest == "" {
		return false
	}
	recorded, err := readCloudMirrorMarker(dir)
	if err != nil {
		return false
	}
	want, err := cloudDestinationIdentity(
		JoinCloudURI(cloudDest, filepath.Base(dir)),
	)
	return err == nil && strings.TrimSpace(string(recorded)) == want
}

func openCloudMirrorMarker(dir string) (*os.File, error) {
	return keystore.OpenRegularFileNoFollow(CloudMirrorMarkerPath(dir))
}

func readCloudMirrorMarker(dir string) ([]byte, error) {
	marker, err := openCloudMirrorMarker(dir)
	if err != nil {
		return nil, err
	}
	recorded, readErr := io.ReadAll(io.LimitReader(
		marker,
		int64(maxCloudMirrorMarkerBytes)+1,
	))
	if err := errors.Join(readErr, marker.Close()); err != nil {
		return nil, err
	}
	if len(recorded) > maxCloudMirrorMarkerBytes {
		return nil, fmt.Errorf(
			"cloud mirror marker exceeds maximum size of %d bytes",
			maxCloudMirrorMarkerBytes,
		)
	}
	return recorded, nil
}

func writeCloudMirrorMarker(
	root *os.Root,
	dir string,
	markerValue string,
) (retErr error) {
	data := []byte(markerValue + "\n")
	if len(data) != maxCloudMirrorMarkerBytes {
		return fmt.Errorf(
			"cloud mirror marker has invalid size %d, want %d bytes",
			len(data), maxCloudMirrorMarkerBytes,
		)
	}

	if root == nil {
		return fmt.Errorf("%w: %q", ErrSnapshotDirectoryReplaced, dir)
	}
	if err := snapshotRootMatchesPath(root, dir); err != nil {
		return err
	}

	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("choose temp cloud mirror marker name: %w", err)
	}
	tmpName := cloudMirrorMarkerName + ".tmp-" +
		hex.EncodeToString(random[:])
	tmp, err := root.OpenFile(
		tmpName,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return fmt.Errorf("create temp cloud mirror marker: %w", err)
	}
	renamed := false
	defer func() {
		if renamed {
			return
		}
		if err := root.Remove(tmpName); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(
				retErr,
				fmt.Errorf("remove temp cloud mirror marker: %w", err),
			)
		}
	}()
	if err := writeAndSyncCloudMirrorMarker(tmp, data, tmpName); err != nil {
		return err
	}
	if err := root.Rename(tmpName, cloudMirrorMarkerName); err != nil {
		return fmt.Errorf("publish cloud mirror marker: %w", err)
	}
	renamed = true

	rootDir, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open snapshot directory for sync: %w", err)
	}
	syncErr := rootDir.Sync()
	closeErr := rootDir.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return snapshotRootMatchesPath(root, dir)
}

func writeAndSyncCloudMirrorMarker(
	file *os.File,
	data []byte,
	name string,
) (retErr error) {
	defer func() {
		if err := file.Close(); err != nil {
			retErr = errors.Join(
				retErr,
				fmt.Errorf("close temp cloud mirror marker %q: %w", name, err),
			)
		}
	}()
	written, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("write temp cloud mirror marker %q: %w", name, err)
	}
	if written != len(data) {
		return fmt.Errorf(
			"write temp cloud mirror marker %q: %w",
			name, io.ErrShortWrite,
		)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temp cloud mirror marker %q: %w", name, err)
	}
	return nil
}

// MirrorToCloud uploads dir's contents to cloudDest (a base URI like
// "s3://bucket/prefix" or "gcs://bucket/prefix"; see DestinationRegistry),
// nested one level under this snapshot's own ID (dir's base name),
// mirroring the local SnapshotDir/<snapshotID> layout — see
// SnapshotToCloud's doc comment for why. Writes CloudMirrorMarkerPath(dir)
// after the upload and catalog update or repair succeed, so a caller retries
// both an incomplete upload and a completed upload whose catalog could not be
// reconciled.
//
// cloudDest == "" is a no-op (success, no marker written): nothing to
// mirror.
func MirrorToCloud(
	ctx context.Context,
	registry *DestinationRegistry,
	dir string,
	cloudDest string,
	opts ...ManifestOption,
) error {
	return mirrorToCloud(
		ctx, registry, dir, cloudDest,
		withCloudSnapshotCatalogReconcile(registry, cloudDest, opts),
	)
}

func withCloudSnapshotCatalogReconcile(
	registry *DestinationRegistry,
	cloudDest string,
	opts []ManifestOption,
) []ManifestOption {
	if cloudDest == "" {
		return opts
	}
	configured := append([]ManifestOption(nil), opts...)
	configured = append(configured, withSnapshotCatalogReconcile(
		func(ctx context.Context, baseDir string) error {
			_, err := ReconcileCloudSnapshotCatalogContext(
				ctx, baseDir, registry, cloudDest, configured...,
			)
			return err
		},
	))
	return configured
}

func mirrorToCloud(
	ctx context.Context,
	registry *DestinationRegistry,
	dir string,
	cloudDest string,
	opts []ManifestOption,
) (retErr error) {
	if cloudDest == "" {
		return nil
	}
	if err := requireManifestKey(opts); err != nil {
		return err
	}
	snapshotCloudURI := JoinCloudURI(cloudDest, filepath.Base(dir))
	markerValue, err := cloudDestinationIdentity(snapshotCloudURI)
	if err != nil {
		return err
	}
	dest, err := ParseCloudDestination(registry, snapshotCloudURI)
	if err != nil {
		return fmt.Errorf(
			"cloud destination %q is invalid: %w",
			CloudDestinationDisplay(snapshotCloudURI), err,
		)
	}
	defer closeCloudDestination(dest)
	var upload snapshotUploadResult
	if uploader, ok := dest.(interface {
		uploadAuthenticatedSnapshot(
			context.Context,
			string,
			...ManifestOption,
		) (snapshotUploadResult, error)
	}); ok {
		upload, err = uploader.uploadAuthenticatedSnapshot(ctx, dir, opts...)
	} else {
		upload, err = uploadStagedSnapshot(ctx, dest, dir, opts...)
	}
	if err != nil {
		return fmt.Errorf(
			"upload to %q failed: %w",
			CloudDestinationDisplay(snapshotCloudURI),
			sanitizeCloudError(snapshotCloudURI, err),
		)
	}
	defer func() {
		if err := upload.close(); err != nil {
			retErr = errors.Join(
				retErr,
				fmt.Errorf("close snapshot upload source: %w", err),
			)
		}
	}()
	if err := updateCloudSnapshotCatalogIfPresent(
		context.WithoutCancel(ctx),
		filepath.Dir(dir),
		SnapshotEntry{ID: filepath.Base(dir), Manifest: upload.manifest},
		cloudDest,
		opts...,
	); err != nil {
		if repairErr := RepairSnapshotCatalogContext(
			context.WithoutCancel(ctx), filepath.Dir(dir), registry, cloudDest,
			opts...,
		); repairErr != nil {
			return fmt.Errorf(
				"%w: %w", ErrSnapshotCatalogUpdate,
				errors.Join(err, repairErr),
			)
		}
	}
	if err := writeCloudMirrorMarker(
		upload.root, dir, markerValue,
	); err != nil {
		return fmt.Errorf(
			"upload to %q succeeded, but recording the cloud-mirrored marker failed: %w",
			CloudDestinationDisplay(snapshotCloudURI), err,
		)
	}
	return nil
}

// SnapshotToCloud calls Snapshot to produce the local copy at dir exactly
// as before, then — if name or description is non-empty — labels it (see
// LabelSnapshot), then — if cloudDest is non-empty — additionally uploads
// dir's contents to that destination (a base URI like "s3://bucket/prefix"
// or "gcs://bucket/prefix"; see DestinationRegistry), nested one level
// under this snapshot's own ID (dir's base name), mirroring the local
// SnapshotDir/<snapshotID> layout: the actual upload target is cloudDest +
// "/" + filepath.Base(dir), not cloudDest itself. This is what makes
// ListCloudSnapshots able to enumerate multiple snapshots stored at the
// same configured cloudDest — a flat, unnested upload would silently
// overwrite every previous snapshot's files with the newest one's. The
// local copy is always kept; cloudDest is a mirror, not a replacement.
//
// Labeling happens before mirroring, not after: MirrorToCloud uploads
// whatever is on disk at dir the moment it runs and then writes the
// cloud-mirrored marker recording that destination as fully done. A caller
// that labeled the local manifest only after this returned would leave the
// already-uploaded remote copy permanently without the name/description —
// and since the marker already says this destination is mirrored, nothing
// would ever retry the upload to pick up the label. Labeling first means
// the directory MirrorToCloud uploads already carries it.
//
// cloudDest == "" skips the upload — existing local-only callers are
// unaffected, and registry may be nil in that case.
// A recognized cloud destination requires WithManifestKey; the request is
// rejected before the local snapshot is written when the key is absent.
//
// If the upload fails, the local snapshot is still valid and left in
// place, but this still returns an error: the operator asked for both
// copies, so a cloud-only failure is a real (partial) failure, not a
// silent degrade to local-only.
func SnapshotToCloud(
	ctx context.Context,
	registry *DestinationRegistry,
	db *database.Database,
	dir string,
	trigger string,
	dingoVersion string,
	blobPluginName string,
	metadataPluginName string,
	cloudDest string,
	name string,
	description string,
	opts ...ManifestOption,
) (Manifest, error) {
	if cloudDest != "" {
		opts = withCloudSnapshotCatalogReconcile(registry, cloudDest, opts)
		if err := requireManifestKey(opts); err != nil {
			return Manifest{}, err
		}
	}
	manifest, err := Snapshot(
		ctx, db, dir, trigger, dingoVersion, blobPluginName, metadataPluginName,
		opts...,
	)
	if err != nil {
		return Manifest{}, err
	}
	if name != "" || description != "" {
		if err := labelSnapshot(ctx, dir, name, description, opts...); err != nil {
			return manifest, fmt.Errorf(
				"snapshot written locally to %q, but labeling it failed: %w",
				dir, err,
			)
		}
		manifest, err = ReadManifest(dir, opts...)
		if err != nil {
			return manifest, fmt.Errorf(
				"snapshot written locally to %q and labeled, but re-reading "+
					"the labeled manifest failed: %w",
				dir, err,
			)
		}
	}
	if err := mirrorToCloud(ctx, registry, dir, cloudDest, opts); err != nil {
		return manifest, fmt.Errorf(
			"snapshot written locally to %q, but %w", dir, err,
		)
	}
	return manifest, nil
}
