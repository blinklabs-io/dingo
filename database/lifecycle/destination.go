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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
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

// ErrSnapshotCatalogScanLimit marks an incomplete provider scan stopped at a
// configured resource bound. Callers must not publish entries returned with
// this error as a complete replacement catalog.
var ErrSnapshotCatalogScanLimit = errors.New(
	"cloud snapshot catalog scan limit exceeded",
)

// SnapshotCatalogScanBudget bounds provider work performed while rebuilding
// the persistent snapshot catalog.
type SnapshotCatalogScanBudget struct {
	// MaxPages bounds provider list requests.
	MaxPages int
	// MaxPrefixes bounds provider list results inspected, including objects
	// that are not snapshot prefixes.
	MaxPrefixes  int
	MaxManifests int
	MaxEntries   int
	MaxProblems  int
}

func defaultSnapshotCatalogScanBudget() SnapshotCatalogScanBudget {
	return SnapshotCatalogScanBudget{
		MaxPages:     10,
		MaxPrefixes:  1000,
		MaxManifests: 1000,
		MaxEntries:   1000,
		MaxProblems:  64,
	}
}

func (b SnapshotCatalogScanBudget) validate() error {
	if b.MaxPages <= 0 || b.MaxPrefixes <= 0 || b.MaxManifests <= 0 ||
		b.MaxEntries <= 0 || b.MaxProblems <= 0 {
		return errors.New("snapshot catalog scan limits must be positive")
	}
	return nil
}

type snapshotCatalogScan struct {
	ctx       context.Context
	budget    *SnapshotCatalogScanBudget
	pages     int
	prefixes  int
	manifests int
	entries   []SnapshotEntry
	problems  []error
}

func newSnapshotCatalogScan(
	ctx context.Context,
	budget *SnapshotCatalogScanBudget,
) (*snapshotCatalogScan, error) {
	if budget != nil {
		if err := budget.validate(); err != nil {
			return nil, err
		}
		budgetCopy := *budget
		budget = &budgetCopy
	}
	return &snapshotCatalogScan{ctx: ctx, budget: budget}, nil
}

func (s *snapshotCatalogScan) consume(kind string, count *int, limit int) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.budget != nil && *count >= limit {
		return fmt.Errorf("%w: maximum %s is %d", ErrSnapshotCatalogScanLimit, kind, limit)
	}
	*count++
	return nil
}

func (s *snapshotCatalogScan) consumePrefix() error {
	limit := 0
	if s.budget != nil {
		limit = s.budget.MaxPrefixes
	}
	return s.consume("prefixes", &s.prefixes, limit)
}

func (s *snapshotCatalogScan) consumePage() error {
	limit := 0
	if s.budget != nil {
		limit = s.budget.MaxPages
	}
	return s.consume("provider page requests", &s.pages, limit)
}

func (s *snapshotCatalogScan) consumeManifest() error {
	limit := 0
	if s.budget != nil {
		limit = s.budget.MaxManifests
	}
	return s.consume("manifest fetches", &s.manifests, limit)
}

func (s *snapshotCatalogScan) addEntry(entry SnapshotEntry) error {
	if s.budget != nil && len(s.entries) >= s.budget.MaxEntries {
		return fmt.Errorf(
			"%w: maximum catalog entries is %d",
			ErrSnapshotCatalogScanLimit, s.budget.MaxEntries,
		)
	}
	s.entries = append(s.entries, entry)
	return nil
}

func (s *snapshotCatalogScan) addProblem(err error) error {
	if s.budget != nil && len(s.problems) >= s.budget.MaxProblems {
		return fmt.Errorf(
			"%w: maximum reported problems is %d",
			ErrSnapshotCatalogScanLimit, s.budget.MaxProblems,
		)
	}
	s.problems = append(s.problems, err)
	return nil
}

func (s *snapshotCatalogScan) result(terminal error) ([]SnapshotEntry, error) {
	return s.entries, errors.Join(errors.Join(s.problems...), terminal)
}

func cloudDestinationIdentity(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	uri, err := parseCloudDestinationURL(raw)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(uri.String()))
	return fmt.Sprintf("v1:sha256:%x", digest), nil
}

// CloudDestinationDisplay returns a credential-free form of a cloud URI for
// logs, errors, and API responses. Provider calls and source identity checks
// continue to use the original URI.
func CloudDestinationDisplay(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<invalid cloud destination>"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

type sanitizedCloudError struct {
	message string
	err     error
}

func (e sanitizedCloudError) Error() string { return e.message }

func (e sanitizedCloudError) Is(target error) bool {
	return errors.Is(e.err, target)
}

func sanitizeCloudError(raw string, err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	u, parseErr := url.Parse(raw)
	if parseErr != nil {
		return sanitizedCloudError{
			message: "cloud destination operation failed",
			err:     err,
		}
	}
	message = strings.ReplaceAll(message, raw, CloudDestinationDisplay(raw))
	secrets := []string{
		u.RawQuery, u.Fragment, u.RawFragment, u.EscapedFragment(),
	}
	if u.User != nil {
		secrets = append(secrets, u.User.String(), u.User.Username())
		if password, ok := u.User.Password(); ok {
			secrets = append(secrets, password)
		}
	}
	for _, values := range u.Query() {
		secrets = append(secrets, values...)
	}
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	return sanitizedCloudError{message: message, err: err}
}

// EffectiveCloudDestination appends the configured per-node prefix before
// any snapshot ID. Manual snapshots, automatic snapshots, and Bark catalog
// discovery must all use this same destination.
func EffectiveCloudDestination(base string, prefix string) string {
	if base == "" || prefix == "" {
		return base
	}
	return JoinCloudURI(base, prefix)
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
	// recursive) to the destination. MirrorToCloud passes a private copy
	// of the snapshot, already authenticated and named for the snapshot ID,
	// not the snapshot directory itself, so the bytes read here are the
	// ones that were verified.
	UploadDir(ctx context.Context, localDir string) error
	// DownloadFiles downloads exactly the named objects into localDir, which
	// must already exist. A snapshot's prefix is writable by anyone with
	// access to the bucket, so objects that were not asked for are never
	// listed or read, and a named object that is larger than its MaxBytes is
	// rejected during transfer rather than written out in full. A missing
	// object wraps ErrCloudSnapshotNotFound.
	DownloadFiles(
		ctx context.Context,
		localDir string,
		files []DownloadFile,
	) error
}

// DownloadFile names one object of a snapshot and the most bytes it may carry.
type DownloadFile struct {
	Name     string
	MaxBytes int64
}

// ErrDownloadTooLarge reports an object that carried more bytes than the
// snapshot's manifest declared for it.
var ErrDownloadTooLarge = errors.New("cloud object exceeds its declared size")

// copyBounded copies src to dst and fails once src yields more than maxBytes,
// having written at most maxBytes. name only labels the error.
func copyBounded(
	dst io.Writer,
	src io.Reader,
	maxBytes int64,
	name string,
) error {
	n, err := io.Copy(dst, io.LimitReader(src, maxBytes))
	if err != nil {
		return err
	}
	if n < maxBytes {
		return nil
	}
	var probe [1]byte
	m, probeErr := io.ReadFull(src, probe[:])
	if m > 0 {
		return fmt.Errorf(
			"%q exceeds %d bytes: %w",
			name,
			maxBytes,
			ErrDownloadTooLarge,
		)
	}
	if probeErr != nil && !errors.Is(probeErr, io.EOF) {
		return probeErr
	}
	return nil
}

// writeBoundedFile creates path and fills it from src, removing it again when
// the copy fails so a rejected object leaves nothing behind.
func writeBoundedFile(path string, src io.Reader, file DownloadFile) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create %q for download: %w", path, err)
	}
	err = copyBounded(f, src, file.MaxBytes, file.Name)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
	}
	return err
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
	// ListSnapshots returns parsed manifests for snapshots found under this
	// destination. ListCloudSnapshots applies the configured trust key before
	// exposing any entries to its caller.
	ListSnapshots(
		ctx context.Context,
		opts ...ManifestOption,
	) ([]SnapshotEntry, error)
}

// SnapshotCatalogLister performs the same provider discovery as
// SnapshotLister while enforcing the supplied reconciliation budget. Returning
// entries with an error means the scan is incomplete; callers must not publish
// those entries as a replacement catalog.
type SnapshotCatalogLister interface {
	ListSnapshotCatalog(
		context.Context,
		SnapshotCatalogScanBudget,
	) ([]SnapshotEntry, error)
}

// CloudManifestFetcher is optionally implemented by a CloudDestination to
// fetch just its own manifest.json, without downloading the rest of a
// (possibly very large) snapshot — used to cheaply check whether a
// specific snapshot exists at a destination (DeleteSnapshot's and
// Restore's cloud-fallback path, used when no local copy exists). Unlike
// SnapshotLister, this is meaningful on a CloudDestination parsed from a
// specific snapshot's own URI (base destination + snapshot ID), the same
// one UploadDir/DownloadFiles operate on.
type CloudManifestFetcher interface {
	FetchManifest(ctx context.Context) (Manifest, error)
}

// ConfigurableCloudManifestFetcher supports a caller's manifest limit in
// place of the destination's configured default. Implementations must bound
// the encoded input before reading or decoding it.
type ConfigurableCloudManifestFetcher interface {
	FetchManifestWithOptions(context.Context, ...ManifestOption) (Manifest, error)
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
func (r *DestinationRegistry) Register(
	scheme string,
	factory CloudDestinationFactory,
) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.types[scheme]; exists {
		panic(
			fmt.Sprintf(
				"lifecycle: cloud destination scheme %q already registered",
				scheme,
			),
		)
	}
	r.types[scheme] = factory
}

// recognizedCloudScheme reports whether uri parses as a URI whose scheme
// has a factory registered on r. A plain local filesystem path (no scheme,
// or an unrecognized one) returns false so callers can fall back to
// treating uri as a local path unchanged — this is what lets Restore
// accept either a local directory or a cloud URI in the same string
// parameter without breaking existing local-path callers.
func recognizedCloudScheme(r *DestinationRegistry, uri string) bool {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" || u.Host == "" || r == nil {
		return false
	}
	r.mu.RLock()
	_, ok := r.types[u.Scheme]
	r.mu.RUnlock()
	return ok
}

// ParseCloudDestination resolves uri to a CloudDestination using r's
// registered schemes. uri's scheme must have a factory registered on r
// (see RegisterS3/RegisterGCS/RegisterBuiltinDestinations) or this returns
// an error. r may be nil, which behaves as an empty registry.
func ParseCloudDestination(
	r *DestinationRegistry,
	uri string,
) (CloudDestination, error) {
	u, err := parseCloudDestinationURL(uri)
	if err != nil {
		return nil, err
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
	dest, err := factory(u)
	if err != nil {
		return nil, sanitizeCloudError(uri, err)
	}
	return dest, nil
}

func parseCloudDestinationURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("parse invalid cloud destination")
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New(
			"cloud destination must be a URI like s3://bucket/prefix or gcs://bucket/prefix",
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
		// while ListSnapshots/DownloadFiles/Delete search under a different,
		// uncleaned one, silently splitting "upload" and "read" onto two
		// different prefixes even though both came from the same
		// configured URI. u.Path is always rooted ("/...") here since
		// u.Host is non-empty (checked above), so path.Clean can never
		// produce a leading ".." that escapes above the bucket root.
		u.Path = path.Clean(u.Path)
		u.RawPath = ""
	}
	return u, nil
}

// downloadCloudFiles downloads the named objects of the snapshot at the given
// cloud URI into a fresh temporary directory under parentDir (the system temp
// directory when empty) and returns its path plus a cleanup func that removes
// it. The caller must call cleanup once done.
func downloadCloudFiles(
	ctx context.Context,
	registry *DestinationRegistry,
	uri string,
	parentDir string,
	files []DownloadFile,
) (localDir string, cleanup func(), err error) {
	dest, err := ParseCloudDestination(registry, uri)
	if err != nil {
		return "", nil, err
	}
	defer closeCloudDestination(dest)
	owned, err := createOwnedRestoreDir(parentDir, ".dingo-cloud-snapshot-")
	if err != nil {
		return "", nil, fmt.Errorf(
			"create temp directory for cloud snapshot download: %w", err,
		)
	}
	tempDir := owned.dir.path
	cleanup = func() { _ = owned.cleanup() }
	if err := dest.DownloadFiles(ctx, tempDir, files); err != nil {
		cleanup()
		return "", nil, fmt.Errorf(
			"download snapshot from %q: %w", CloudDestinationDisplay(uri),
			sanitizeCloudError(uri, err),
		)
	}
	return tempDir, cleanup, nil
}

// IsSafeCloudObjectFileName reports whether fileName is safe to join onto
// a local restore directory (via filepath.Join/os.Create). A cloud object
// key is attacker- or corruption-controlled input, not a trusted local
// path component, so both destination_s3.go's and destination_gcs.go's
// DownloadFiles use this rather than only checking for "/": a bare ".."
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

func fetchCloudSnapshotEntry(
	ctx context.Context,
	snapshotID string,
	fetch func(context.Context, string) (Manifest, error),
) (SnapshotEntry, error) {
	if !IsSafeCloudObjectFileName(snapshotID) {
		return SnapshotEntry{}, fmt.Errorf(
			"invalid cloud snapshot ID %q", snapshotID,
		)
	}
	manifest, err := fetch(ctx, snapshotID)
	if err != nil {
		return SnapshotEntry{}, err
	}
	return SnapshotEntry{ID: snapshotID, Manifest: manifest}, nil
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
// implementation provides it. Invalid or unauthenticated entries are omitted;
// valid entries may be returned alongside an error describing omitted entries
// or an incomplete listing.
func ListCloudSnapshots(
	ctx context.Context,
	registry *DestinationRegistry,
	cloudDest string,
	opts ...ManifestOption,
) (entries []SnapshotEntry, ok bool, err error) {
	if cloudDest == "" {
		return nil, false, nil
	}
	if err := requireManifestKey(opts); err != nil {
		return nil, false, err
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
	entries, listErr := lister.ListSnapshots(ctx, opts...)
	var problems []error
	if listErr != nil {
		problems = append(problems, fmt.Errorf(
			"list snapshots at %q: %w", CloudDestinationDisplay(cloudDest),
			sanitizeCloudError(cloudDest, listErr),
		))
	}
	validated := make([]SnapshotEntry, 0, len(entries))
	for _, entry := range entries {
		if err := authenticateEntry(entry, opts); err != nil {
			problems = append(problems, fmt.Errorf(
				"authenticate cloud snapshot %q: %w", entry.ID, err,
			))
			continue
		}
		validated = append(validated, entry)
	}
	return validated, true, errors.Join(problems...)
}

// RepairSnapshotCatalogContext rebuilds the disposable local and cloud
// snapshot indexes from their authoritative sources.
func RepairSnapshotCatalogContext(
	ctx context.Context,
	baseDir string,
	registry *DestinationRegistry,
	cloudDest string,
	opts ...ManifestOption,
) error {
	localErr := EnsureSnapshotCatalogContext(ctx, baseDir, opts...)
	if localErr != nil && !errors.Is(localErr, ErrSnapshotCatalogIncomplete) {
		return localErr
	}
	if cloudDest == "" {
		return localErr
	}
	_, cloudErr := ReconcileCloudSnapshotCatalogContext(
		ctx, baseDir, registry, cloudDest, opts...,
	)
	return errors.Join(localErr, cloudErr)
}

// ReconcileCloudSnapshotCatalogContext scans the provider outside the catalog
// gate, then publishes only if no catalog mutation occurred during the scan.
// current is true only when the catalog was completely reconciled with
// cloudDest. An incomplete scan leaves the previous cloud set unchanged.
func ReconcileCloudSnapshotCatalogContext(
	ctx context.Context,
	baseDir string,
	registry *DestinationRegistry,
	cloudDest string,
	opts ...ManifestOption,
) (current bool, err error) {
	if cloudDest == "" {
		if err := lockSnapshotCatalog(ctx); err != nil {
			return false, err
		}
		defer unlockSnapshotCatalog()
		err := rebuildCloudSnapshotCatalogLocked(ctx, baseDir, "", nil)
		return err == nil, err
	}
	if err := requireManifestKey(opts); err != nil {
		return false, err
	}
	dest, constructErr := ParseCloudDestination(registry, cloudDest)
	if constructErr != nil {
		return false, constructErr
	}
	defer closeCloudDestination(dest)
	lister, ok := dest.(SnapshotCatalogLister)
	if !ok {
		return false, fmt.Errorf(
			"%w: cloud destination does not support snapshot listing",
			ErrSnapshotCatalogIncomplete,
		)
	}
	if err := lockSnapshotCatalog(ctx); err != nil {
		return false, err
	}
	startGeneration, generationErr := readSnapshotCatalogGenerationLocked(
		ctx, baseDir,
	)
	unlockSnapshotCatalog()
	if errors.Is(generationErr, ErrSnapshotCatalogCorrupt) {
		repairErr := EnsureSnapshotCatalogContext(ctx, baseDir, opts...)
		if repairErr != nil && !errors.Is(repairErr, ErrSnapshotCatalogIncomplete) {
			return false, repairErr
		}
		if err := lockSnapshotCatalog(ctx); err != nil {
			return false, err
		}
		startGeneration, generationErr = readSnapshotCatalogGenerationLocked(
			ctx, baseDir,
		)
		unlockSnapshotCatalog()
	}
	if generationErr != nil {
		return false, generationErr
	}
	entries, listErr := lister.ListSnapshotCatalog(
		ctx, defaultSnapshotCatalogScanBudget(),
	)
	listErr = sanitizeCloudError(cloudDest, listErr)
	if listErr != nil {
		return false, listErr
	}
	for _, entry := range entries {
		if err := authenticateEntry(entry, opts); err != nil {
			return false, fmt.Errorf(
				"%w: authenticate cloud snapshot %q: %w",
				ErrSnapshotCatalogIncomplete, entry.ID, err,
			)
		}
	}
	if err := lockSnapshotCatalog(ctx); err != nil {
		return false, err
	}
	defer unlockSnapshotCatalog()
	currentGeneration, err := readSnapshotCatalogGenerationLocked(ctx, baseDir)
	if err != nil {
		return false, err
	}
	if currentGeneration != startGeneration {
		return false, fmt.Errorf(
			"%w: catalog generation advanced from %d to %d during cloud scan",
			ErrSnapshotCatalogChanged, startGeneration, currentGeneration,
		)
	}
	rebuildErr := rebuildCloudSnapshotCatalogLocked(
		ctx, baseDir, cloudDest, entries,
	)
	current = rebuildErr == nil
	return current, errors.Join(listErr, rebuildErr)
}

// FetchCloudManifest resolves the CloudDestination at the given exact
// snapshot URI (a specific snapshot's own location — see JoinCloudURI,
// not a base destination) and fetches its manifest, if that destination
// type implements ConfigurableCloudManifestFetcher. A manifest trust key is
// required. ok=false (nil error) means the destination type doesn't support
// authenticated fetches. ok=true with a non-nil err
// means an actual fetch was attempted and failed — check errors.Is(err,
// ErrCloudSnapshotNotFound) to tell "confirmed absent" apart from a real
// communication failure (auth, network, timeout); only the former should
// ever be treated as equivalent to "doesn't exist" by a caller.
func FetchCloudManifest(
	ctx context.Context,
	registry *DestinationRegistry,
	snapshotURI string,
	opts ...ManifestOption,
) (m Manifest, ok bool, err error) {
	if _, err := manifestByteLimit(opts); err != nil {
		return Manifest{}, false, err
	}
	if err := requireManifestKey(opts); err != nil {
		return Manifest{}, false, err
	}
	dest, err := ParseCloudDestination(registry, snapshotURI)
	if err != nil {
		return Manifest{}, false, err
	}
	defer closeCloudDestination(dest)
	fetcher, ok := dest.(ConfigurableCloudManifestFetcher)
	if !ok {
		return Manifest{}, false, nil
	}
	opts, err = withCloudManifestID(snapshotURI, opts)
	if err != nil {
		return Manifest{}, true, err
	}
	m, err = fetcher.FetchManifestWithOptions(ctx, opts...)
	if err == nil {
		err = m.Authenticate(opts...)
	}
	return m, true, sanitizeCloudError(snapshotURI, err)
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
	return true, sanitizeCloudError(snapshotURI, deleter.Delete(ctx))
}
