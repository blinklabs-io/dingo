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
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
)

// ErrCloudSnapshotNotFound is what a CloudManifestFetcher implementation
// wraps around its underlying "object doesn't exist" error (e.g. S3's
// NoSuchKey/NotFound, GCS's storage.ErrObjectNotExist) — the one case
// where FetchCloudManifest's non-nil err genuinely means "confirmed
// absent," as opposed to a real communication failure (auth, network,
// timeout, throttling) that happens to occur while checking. Callers
// distinguishing "no such snapshot" from "couldn't check right now" (see
// bark's cloudSnapshotExists) should check errors.Is(err,
// ErrCloudSnapshotNotFound) rather than treating every non-nil err the
// same way.
var ErrCloudSnapshotNotFound = errors.New("cloud snapshot not found")

// orderEntriesManifestLast returns entries reordered so that any entry
// named ManifestFileName sorts last, with every other entry keeping its
// original relative order. Both destination_s3.go's and
// destination_gcs.go's UploadDir use this: a concurrent lister/fetcher
// treats a cloud-visible manifest.json as "this snapshot is fully there"
// (see FetchCloudManifest/ListCloudSnapshots), so uploading it before
// blob.bak/metadata.sqlite finish would let that caller download or
// restore an incomplete snapshot. Uploading the manifest last, after
// every other file has actually succeeded, makes it a true completion
// marker instead of just another file in directory order.
func orderEntriesManifestLast(entries []os.DirEntry) []os.DirEntry {
	ordered := make([]os.DirEntry, 0, len(entries))
	var manifest os.DirEntry
	for _, e := range entries {
		if e.Name() == ManifestFileName {
			manifest = e
			continue
		}
		ordered = append(ordered, e)
	}
	if manifest != nil {
		ordered = append(ordered, manifest)
	}
	return ordered
}

// CloudDestination mirrors a snapshot directory to/from object storage, in
// addition to (not instead of) the local copy Snapshot/Restore already
// produce/consume — see SnapshotToCloud and Restore's cloud-source
// handling. Implementations live in build-tag-gated files (destination_s3.go,
// destination_gcs.go); composition code registers the ones it wants
// available on a *DestinationRegistry via RegisterS3/RegisterGCS (or
// RegisterBuiltinDestinations for all schemes compiled into this build).
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

// CloudDestinationCloser is optionally implemented by a CloudDestination
// that holds a resource needing explicit cleanup once a caller is done with
// it — e.g. GCS's implementation owns a persistent gRPC connection
// (storage.NewGRPCClient) that client.Bucket's returned handle doesn't
// itself expose a way to close. S3's implementation has no such resource
// and doesn't implement this. Every ParseCloudDestination call site in this
// file (and SnapshotToCloud) closes the destination via
// closeCloudDestination once it's done using it.
type CloudDestinationCloser interface {
	Close() error
}

// closeCloudDestination closes dest if it implements CloudDestinationCloser.
// The close error is deliberately dropped: this runs as cleanup after the
// destination's actual operation has already succeeded or failed on its own
// terms, and a cleanup failure at that point shouldn't mask or replace that
// result.
func closeCloudDestination(dest CloudDestination) {
	if closer, ok := dest.(CloudDestinationCloser); ok {
		_ = closer.Close()
	}
}

// DestinationRegistry holds the set of cloud destination schemes (e.g.
// "s3", "gcs") a caller has explicitly chosen to make available, resolved
// at construction time rather than through a process-global registry.
// Composition code (the node or CLI's startup path) owns creating one via
// NewDestinationRegistry and registering whichever schemes this build and
// configuration should support (RegisterS3, RegisterGCS, or
// RegisterBuiltinDestinations for everything compiled in), then threads it
// explicitly into every lifecycle call that needs cloud destination
// support. A nil *DestinationRegistry is valid and behaves as if it had no
// schemes registered — every method here is nil-safe — so callers with no
// use for cloud destinations at all (e.g. a purely local restore) are not
// forced to construct an empty one.
type DestinationRegistry struct {
	mu    sync.RWMutex
	types map[string]CloudDestinationFactory
}

// NewDestinationRegistry returns an empty registry with no cloud
// destination schemes registered.
func NewDestinationRegistry() *DestinationRegistry {
	return &DestinationRegistry{types: make(map[string]CloudDestinationFactory)}
}

// Register adds factory as the constructor for CloudDestination URIs with
// the given scheme (e.g. "s3", "gcs"). Panics on a duplicate scheme
// registration within this registry, matching the fail-fast-at-
// composition-time convention already used by plugin.Host.Register. r may
// be nil (see DestinationRegistry's doc comment on nil-safety), in which
// case this is a no-op: a nil registry has no map to register into and
// behaves as if it will always have no schemes registered, the same as an
// empty one.
func (r *DestinationRegistry) Register(scheme string, factory CloudDestinationFactory) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.types[scheme]; exists {
		panic(fmt.Sprintf("lifecycle: cloud destination scheme %q already registered", scheme))
	}
	r.types[scheme] = factory
}

// recognizedCloudScheme reports whether uri parses as a URI whose scheme
// has a factory registered on r. A plain local filesystem path (no scheme,
// or an unrecognized one) returns false so callers can fall back to
// treating uri as a local path unchanged — this is what lets Restore
// accept either a local directory or a cloud URI in the same string
// parameter without breaking existing local-path callers.
func recognizedCloudScheme(r *DestinationRegistry, uri string) (scheme string, ok bool) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	if r == nil {
		return u.Scheme, false
	}
	r.mu.RLock()
	_, ok = r.types[u.Scheme]
	r.mu.RUnlock()
	return u.Scheme, ok
}

// ParseCloudDestination resolves uri to a CloudDestination using r's
// registered schemes. uri's scheme must have a factory registered on r
// (see RegisterS3/RegisterGCS/RegisterBuiltinDestinations) or this returns
// an error. r may be nil, which behaves as an empty registry.
func ParseCloudDestination(r *DestinationRegistry, uri string) (CloudDestination, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("parse cloud destination %q: %w", uri, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf(
			"cloud destination %q must be a URI like s3://bucket/prefix or gcs://bucket/prefix",
			uri,
		)
	}
	var factory CloudDestinationFactory
	var ok bool
	if r != nil {
		r.mu.RLock()
		factory, ok = r.types[u.Scheme]
		r.mu.RUnlock()
	}
	if !ok {
		return nil, fmt.Errorf(
			"unsupported cloud destination scheme %q (was dingo built with -tags dingo_extra_plugins, and was it registered with this component?)",
			u.Scheme,
		)
	}
	if u.Path != "" {
		// Canonicalize the path before any factory (destination_s3.go's,
		// destination_gcs.go's) ever sees it. Both derive their upload
		// prefix from this same field, but upload keys go through
		// path.Join (which itself calls path.Clean) while list/download
		// prefix-matching compares against the raw prefix string
		// unmodified — so a noncanonical path (repeated slashes, "."/".."
		// segments) would make UploadDir write under one (cleaned) key
		// while ListSnapshots/DownloadDir/Delete search under a different,
		// uncleaned one, silently splitting "upload" and "read" onto two
		// different prefixes even though both came from the same
		// configured URI. u.Path is always rooted ("/...") here since
		// u.Host is non-empty (checked above), so path.Clean can never
		// produce a leading ".." that escapes above the bucket root.
		u.Path = path.Clean(u.Path)
		u.RawPath = ""
	}
	return factory(u)
}

// downloadCloudSnapshot downloads the snapshot at the given cloud URI into
// a fresh local temp directory and returns its path plus a cleanup func
// that removes it. The caller must call cleanup once done (Restore defers
// it immediately after a successful call).
func downloadCloudSnapshot(
	ctx context.Context,
	registry *DestinationRegistry,
	uri string,
) (localDir string, cleanup func(), err error) {
	dest, err := ParseCloudDestination(registry, uri)
	if err != nil {
		return "", nil, err
	}
	defer closeCloudDestination(dest)
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

// IsSafeCloudObjectFileName reports whether fileName is safe to join onto
// a local restore directory (via filepath.Join/os.Create). A cloud object
// key is attacker- or corruption-controlled input, not a trusted local
// path component, so both destination_s3.go's and destination_gcs.go's
// DownloadDir use this rather than only checking for "/": a bare ".."
// resolves outside the target directory via filepath.Join's own cleaning
// even with no separator present, and a literal "\" is a path separator
// on Windows (but not Unix, where a "/"-only check would otherwise miss
// it) regardless of which OS actually wrote the object.
func IsSafeCloudObjectFileName(fileName string) bool {
	if fileName == "" || fileName == "." || fileName == ".." {
		return false
	}
	return !strings.ContainsAny(fileName, `/\`)
}

// JoinCloudURI appends sub as an additional path segment to base (e.g.
// "s3://bucket/prefix" + "abc123" -> "s3://bucket/prefix/abc123"). base is
// parsed as a URI and sub is appended to its Path specifically (not
// filepath.Join, which would use the host OS's separator, and not plain
// string concatenation onto the whole URI, which would land sub after any
// query string or fragment base carries instead of before it — turning
// "s3://bucket/prefix?region=us-east-1" + "abc123" into
// ".../prefix?region=us-east-1/abc123" rather than
// ".../prefix/abc123?region=us-east-1", silently sending every snapshot to
// the same base prefix regardless of sub). Exported so callers building a
// per-snapshot cloud location for display (e.g. bark's
// ListAvailableSnapshots) use the exact same join logic SnapshotToCloud
// uses for the actual upload.
func JoinCloudURI(base string, sub string) string {
	u, err := url.Parse(base)
	if err != nil {
		// base isn't a parseable URI at all (shouldn't happen for any real
		// caller, which always passes a URI already validated by
		// ParseCloudDestination) — fall back to the old plain
		// concatenation rather than silently dropping sub.
		return strings.TrimRight(base, "/") + "/" + sub
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + sub
	// RawPath (the pre-escaped form url.Parse cached from the original
	// string) no longer corresponds to the now-modified Path; clearing it
	// makes u.String() re-escape from Path directly instead of reusing a
	// stale RawPath or a mismatched escaping of it.
	u.RawPath = ""
	return u.String()
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
	registry *DestinationRegistry,
	cloudDest string,
) (entries []SnapshotEntry, ok bool, err error) {
	if cloudDest == "" {
		return nil, false, nil
	}
	dest, err := ParseCloudDestination(registry, cloudDest)
	if err != nil {
		return nil, false, err
	}
	defer closeCloudDestination(dest)
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
// destination type doesn't support this. ok=true with a non-nil err
// means an actual fetch was attempted and failed — check errors.Is(err,
// ErrCloudSnapshotNotFound) to tell "confirmed absent" apart from a real
// communication failure (auth, network, timeout); only the former should
// ever be treated as equivalent to "doesn't exist" by a caller.
func FetchCloudManifest(
	ctx context.Context,
	registry *DestinationRegistry,
	snapshotURI string,
) (m Manifest, ok bool, err error) {
	dest, err := ParseCloudDestination(registry, snapshotURI)
	if err != nil {
		return Manifest{}, false, err
	}
	defer closeCloudDestination(dest)
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
func DeleteCloudSnapshot(
	ctx context.Context,
	registry *DestinationRegistry,
	snapshotURI string,
) (ok bool, err error) {
	dest, err := ParseCloudDestination(registry, snapshotURI)
	if err != nil {
		return false, err
	}
	defer closeCloudDestination(dest)
	deleter, ok := dest.(CloudDeleter)
	if !ok {
		return false, nil
	}
	return true, deleter.Delete(ctx)
}
