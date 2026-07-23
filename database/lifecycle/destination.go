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
	"net/url"
	"os"
	"strings"
	"sync"
)

// CloudDestination mirrors a snapshot directory to/from object storage, in
// addition to (not instead of) the local copy Snapshot/Restore already
// produce/consume — see SnapshotToCloud and Restore's cloud-source
// handling. Implementations live in build-tag-gated files (destination_s3.go,
// destination_gcs.go) so this package doesn't itself pull in cloud SDKs;
// they register themselves via RegisterCloudDestinationScheme from an
// init().
type CloudDestination interface {
	// UploadDir uploads every regular file directly inside localDir
	// (Snapshot's manifest.json/blob.bak/metadata.sqlite — it is not
	// recursive) to the destination.
	UploadDir(ctx context.Context, localDir string) error
	// DownloadDir downloads the destination's contents into localDir,
	// which must already exist and be empty.
	DownloadDir(ctx context.Context, localDir string) error
}

// CloudDestinationFactory constructs a CloudDestination from a parsed URI
// (e.g. "s3://bucket/prefix"). Registered per-scheme via
// RegisterCloudDestinationScheme.
type CloudDestinationFactory func(uri *url.URL) (CloudDestination, error)

// SnapshotLister is optionally implemented by a CloudDestination to
// enumerate snapshots already stored under it — used by
// ListCloudSnapshots (in turn used by bark's ListAvailableSnapshots RPC).
// Only meaningful when the CloudDestination was parsed from the base
// destination URI operators configure (databaseLifecycle.
// snapshotCloudDestination), not a specific snapshot's per-ID sub-path:
// each snapshot lives one level under that base, mirroring the local
// SnapshotDir/<snapshotID> layout (see SnapshotToCloud).
type SnapshotLister interface {
	// ListSnapshots returns one entry per snapshot found under this
	// destination, each with its manifest already fetched and validated.
	ListSnapshots(ctx context.Context) ([]SnapshotEntry, error)
}

// CloudManifestFetcher is optionally implemented by a CloudDestination to
// fetch just its own manifest.json, without downloading the rest of a
// (possibly very large) snapshot — used to cheaply check whether a
// specific snapshot exists at a destination (DeleteSnapshot's and
// Restore's cloud-fallback path, used when no local copy exists). Unlike
// SnapshotLister, this is meaningful on a CloudDestination parsed from a
// specific snapshot's own URI (base destination + snapshot ID), the same
// one UploadDir/DownloadDir operate on.
type CloudManifestFetcher interface {
	FetchManifest(ctx context.Context) (Manifest, error)
}

// CloudDeleter is optionally implemented by a CloudDestination to delete
// everything at its own configured location — used by DeleteSnapshot to
// remove a snapshot's cloud copy. Like CloudManifestFetcher, meaningful on
// a CloudDestination parsed from a specific snapshot's own URI, not a base
// destination (which would have no single well-defined "everything" to
// delete).
type CloudDeleter interface {
	Delete(ctx context.Context) error
}

var (
	cloudDestinationMu    sync.RWMutex
	cloudDestinationTypes = map[string]CloudDestinationFactory{}
)

// RegisterCloudDestinationScheme registers factory as the constructor for
// CloudDestination URIs with the given scheme (e.g. "s3", "gs"). Intended
// to be called from a build-tag-gated file's init(), not directly by
// callers. Panics on a duplicate scheme registration, matching the
// fail-fast-at-init-time convention already used by database/plugin's
// registry.
func RegisterCloudDestinationScheme(scheme string, factory CloudDestinationFactory) {
	cloudDestinationMu.Lock()
	defer cloudDestinationMu.Unlock()
	if _, exists := cloudDestinationTypes[scheme]; exists {
		panic(fmt.Sprintf("lifecycle: cloud destination scheme %q already registered", scheme))
	}
	cloudDestinationTypes[scheme] = factory
}

// recognizedCloudScheme reports whether uri parses as a URI whose scheme
// has a registered CloudDestination factory. A plain local filesystem
// path (no scheme, or an unrecognized one) returns false so callers can
// fall back to treating uri as a local path unchanged — this is what
// lets Restore accept either a local directory or a cloud URI in the
// same string parameter without breaking existing local-path callers.
func recognizedCloudScheme(uri string) (scheme string, ok bool) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	cloudDestinationMu.RLock()
	_, ok = cloudDestinationTypes[u.Scheme]
	cloudDestinationMu.RUnlock()
	return u.Scheme, ok
}

// ParseCloudDestination resolves uri to a CloudDestination. uri's scheme
// must have a registered factory (built with the corresponding build tag,
// e.g. dingo_extra_plugins for s3/gs) or this returns an error.
func ParseCloudDestination(uri string) (CloudDestination, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("parse cloud destination %q: %w", uri, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf(
			"cloud destination %q must be a URI like s3://bucket/prefix or gs://bucket/prefix",
			uri,
		)
	}
	cloudDestinationMu.RLock()
	factory, ok := cloudDestinationTypes[u.Scheme]
	cloudDestinationMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf(
			"unsupported cloud destination scheme %q (was dingo built with -tags dingo_extra_plugins?)",
			u.Scheme,
		)
	}
	return factory(u)
}

// downloadCloudSnapshot downloads the snapshot at the given cloud URI into
// a fresh local temp directory and returns its path plus a cleanup func
// that removes it. The caller must call cleanup once done (Restore defers
// it immediately after a successful call).
func downloadCloudSnapshot(
	ctx context.Context,
	uri string,
) (localDir string, cleanup func(), err error) {
	dest, err := ParseCloudDestination(uri)
	if err != nil {
		return "", nil, err
	}
	tempDir, err := os.MkdirTemp("", "dingo-cloud-snapshot-*")
	if err != nil {
		return "", nil, fmt.Errorf(
			"create temp directory for cloud snapshot download: %w", err,
		)
	}
	cleanup = func() { _ = os.RemoveAll(tempDir) }
	if err := dest.DownloadDir(ctx, tempDir); err != nil {
		cleanup()
		return "", nil, fmt.Errorf(
			"download snapshot from %q: %w", uri, err,
		)
	}
	return tempDir, cleanup, nil
}

// JoinCloudURI appends sub as an additional path segment to base (e.g.
// "s3://bucket/prefix" + "abc123" -> "s3://bucket/prefix/abc123"). Plain
// string concatenation rather than filepath.Join: this operates on a URI,
// which always uses "/" regardless of the host OS. Exported so callers
// building a per-snapshot cloud location for display (e.g. bark's
// ListAvailableSnapshots) use the exact same join logic SnapshotToCloud
// uses for the actual upload.
func JoinCloudURI(base string, sub string) string {
	return strings.TrimRight(base, "/") + "/" + sub
}

// ListCloudSnapshots lists the snapshots already stored at the base cloud
// destination URI cloudDest, if its scheme's implementation supports
// listing (SnapshotLister). ok reports whether listing was actually
// attempted: false (with a nil error) means cloudDest is empty or its
// destination type doesn't implement SnapshotLister, which callers like
// ListAvailableSnapshots should treat as "nothing to add," not a failure
// — cloud listing is an optional capability, not every CloudDestination
// implementation provides it.
func ListCloudSnapshots(
	ctx context.Context,
	cloudDest string,
) (entries []SnapshotEntry, ok bool, err error) {
	if cloudDest == "" {
		return nil, false, nil
	}
	dest, err := ParseCloudDestination(cloudDest)
	if err != nil {
		return nil, false, err
	}
	lister, ok := dest.(SnapshotLister)
	if !ok {
		return nil, false, nil
	}
	entries, err = lister.ListSnapshots(ctx)
	if err != nil {
		return nil, true, fmt.Errorf(
			"list snapshots at %q: %w", cloudDest, err,
		)
	}
	return entries, true, nil
}

// FetchCloudManifest resolves the CloudDestination at the given exact
// snapshot URI (a specific snapshot's own location — see JoinCloudURI,
// not a base destination) and fetches its manifest, if that destination
// type implements CloudManifestFetcher. ok=false (nil error) means the
// destination type doesn't support this — distinct from the manifest
// simply not existing there, which is a non-nil err.
func FetchCloudManifest(
	ctx context.Context,
	snapshotURI string,
) (m Manifest, ok bool, err error) {
	dest, err := ParseCloudDestination(snapshotURI)
	if err != nil {
		return Manifest{}, false, err
	}
	fetcher, ok := dest.(CloudManifestFetcher)
	if !ok {
		return Manifest{}, false, nil
	}
	m, err = fetcher.FetchManifest(ctx)
	return m, true, err
}

// DeleteCloudSnapshot resolves the CloudDestination at the given exact
// snapshot URI and deletes it, if that destination type implements
// CloudDeleter. ok=false (nil error) means the destination type doesn't
// support deletion — distinct from a real deletion failure, which is a
// non-nil err.
func DeleteCloudSnapshot(ctx context.Context, snapshotURI string) (ok bool, err error) {
	dest, err := ParseCloudDestination(snapshotURI)
	if err != nil {
		return false, err
	}
	deleter, ok := dest.(CloudDeleter)
	if !ok {
		return false, nil
	}
	return true, deleter.Delete(ctx)
}
