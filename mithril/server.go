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

package mithril

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// hashPattern matches the 64-hex-digit identifiers used for artifact and
// certificate hashes. Every path segment reaching the store is checked
// against it or against downloadNamePattern first.
var (
	hashPattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
	downloadNamePattern = regexp.MustCompile(
		`^(digests|ancillary|[0-9]{5,})\.tar\.zst$`,
	)
)

// publicRequestLimit bounds concurrent artifact reads. One v2 bootstrap holds
// immutableDownloadWorkers archive downloads and the ancillary download at
// once, so the bound admits two of them without shedding a request.
const publicRequestLimit = 2 * (immutableDownloadWorkers + 1)

// signatureRequestLimit bounds signature verification independently from
// artifact reads so downloads cannot starve certificate production.
const signatureRequestLimit = 4

// Signature body reads have their own wider bound. A client may occupy at
// most half of it, leaving capacity for another client while incomplete
// bodies wait for the read deadline.
const (
	signatureRequestReadLimit      = 2 * signatureRequestLimit
	signatureRequestReadsPerClient = signatureRequestLimit
)

const (
	certificatesPrefix            = "certificates"
	publicRequestReadTimeout      = 15 * time.Second
	artifactResponseWriteTimeout  = 15 * time.Second
	snapshotCatalogTTL            = 30 * time.Second
	artifactDownloadsPerClient    = immutableDownloadWorkers + 1
	artifactMinimumBytesPerSecond = 64 << 10
	artifactTransferGrace         = 30 * time.Second
)

type snapshotCatalog struct {
	store ArtifactStore
	ttl   time.Duration
	now   func() time.Time

	mu         sync.Mutex
	snapshots  []CardanoDatabaseSnapshot
	expires    time.Time
	valid      bool
	loading    bool
	ready      chan struct{}
	generation uint64
}

func newSnapshotCatalog(store ArtifactStore) *snapshotCatalog {
	return &snapshotCatalog{
		store: store,
		ttl:   snapshotCatalogTTL,
		now:   time.Now,
	}
}

func (c *snapshotCatalog) list(
	ctx context.Context,
) ([]CardanoDatabaseSnapshot, error) {
	snapshots, _, err := c.listVersioned(ctx)
	return snapshots, err
}

func (c *snapshotCatalog) listVersioned(
	ctx context.Context,
) ([]CardanoDatabaseSnapshot, uint64, error) {
	for {
		c.mu.Lock()
		if c.valid && c.now().Before(c.expires) {
			ret := slices.Clone(c.snapshots)
			generation := c.generation
			c.mu.Unlock()
			return ret, generation, nil
		}
		if c.loading {
			ready := c.ready
			c.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}
		c.loading = true
		c.ready = make(chan struct{})
		ready := c.ready
		generation := c.generation
		c.mu.Unlock()

		snapshots, err := ListSnapshots(ctx, c.store)

		c.mu.Lock()
		if err == nil && generation == c.generation {
			c.snapshots = slices.Clone(snapshots)
			c.expires = c.now().Add(c.ttl)
			c.valid = true
		}
		c.loading = false
		close(ready)
		c.mu.Unlock()
		return snapshots, generation, err
	}
}

func (c *snapshotCatalog) isCurrent(generation uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation == generation
}

func (c *snapshotCatalog) invalidate() {
	c.mu.Lock()
	c.generation++
	c.expires = time.Time{}
	c.valid = false
	c.mu.Unlock()
}

type clientAdmission struct {
	mu        sync.Mutex
	active    map[string]int
	perClient int
}

func newClientAdmission(perClient int) *clientAdmission {
	return &clientAdmission{
		active:    make(map[string]int),
		perClient: perClient,
	}
}

// requestClientKey identifies the client a request counts against. IPv6
// clients are keyed by their /64, since a single host usually controls one
// and can rotate addresses within it; an IPv4-mapped address counts as its
// IPv4 address.
func requestClientKey(remoteAddr string) string {
	host := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = h
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	addr = addr.Unmap().WithZone("")
	if addr.Is4() {
		return addr.String()
	}
	prefix, err := addr.Prefix(64)
	if err != nil {
		return addr.String()
	}
	return prefix.String()
}

func (a *clientAdmission) acquire(remoteAddr string) (func(), bool) {
	client := requestClientKey(remoteAddr)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[client] >= a.perClient {
		return nil, false
	}
	a.active[client]++
	return sync.OnceFunc(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.active[client]--; a.active[client] == 0 {
			delete(a.active, client)
		}
	}), true
}

// ServerConfig configures the snapshot artifact HTTP handler.
type ServerConfig struct {
	// Store holds the produced artifacts.
	Store ArtifactStore
	// PublicBaseURL is the configured public HTTP(S) origin used in artifact
	// download locations. It is required for detail responses and must use
	// HTTPS except for loopback origins used by local deployments and tests.
	PublicBaseURL string
	// RedirectBaseURL, when set, answers artifact file requests with a
	// redirect to RedirectBaseURL + "/" + key instead of streaming the
	// object from Store. It is meant for remote stores whose objects are
	// publicly readable under that base URL.
	RedirectBaseURL string
	// Aggregator, when set, mounts its signer registration, signature and
	// stake distribution endpoints beside the artifact API.
	Aggregator *Aggregator
	// Logger receives request failures; nil discards them.
	Logger *slog.Logger
}

func parsePublicBaseURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("invalid public base URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "https" && scheme != "http") || u.Host == "" ||
		u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery ||
		strings.Contains(value, "#") {
		return nil, errors.New(
			"public base URL must be an HTTP(S) origin without credentials, path, query, or fragment",
		)
	}
	if scheme == "http" {
		host := strings.ToLower(u.Hostname())
		addr, parseErr := netip.ParseAddr(host)
		if host != "localhost" &&
			(parseErr != nil || !addr.Unmap().IsLoopback()) {
			return nil, errors.New(
				"plain HTTP public base URL is allowed only on loopback",
			)
		}
	}
	return &url.URL{Scheme: scheme, Host: u.Host}, nil
}

// ListSnapshots returns the complete snapshots in store, newest first. A
// snapshot is complete once its metadata object exists.
func ListSnapshots(
	ctx context.Context,
	store ArtifactStore,
) ([]CardanoDatabaseSnapshot, error) {
	dirs, err := store.Subdirs(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("listing artifact store: %w", err)
	}
	dirs = slices.DeleteFunc(dirs, func(dir string) bool {
		return !hashPattern.MatchString(dir)
	})
	read := make([]*CardanoDatabaseSnapshot, len(dirs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(storeProbeWorkers)
	for i, dir := range dirs {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			snapshot, err := readSnapshot(gctx, store, dir)
			if errors.Is(err, ErrArtifactNotFound) {
				return nil
			}
			read[i] = snapshot
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	var snapshots []CardanoDatabaseSnapshot
	for _, snapshot := range read {
		if snapshot != nil {
			snapshots = append(snapshots, *snapshot)
		}
	}
	created := func(s CardanoDatabaseSnapshot) time.Time {
		t, _ := time.Parse(time.RFC3339Nano, s.CreatedAt)
		return t
	}
	slices.SortFunc(snapshots, func(a, b CardanoDatabaseSnapshot) int {
		if c := created(b).Compare(created(a)); c != 0 {
			return c
		}
		return strings.Compare(a.Hash, b.Hash)
	})
	return snapshots, nil
}

func readSnapshot(
	ctx context.Context,
	store ArtifactStore,
	hash string,
) (*CardanoDatabaseSnapshot, error) {
	r, err := store.Open(ctx, path.Join(hash, artifactMetadataName))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var snapshot CardanoDatabaseSnapshot
	if err := json.NewDecoder(io.LimitReader(r, 1<<20)).Decode(
		&snapshot,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding metadata of snapshot %s: %w",
			hash,
			err,
		)
	}
	if snapshot.Hash != hash {
		return nil, fmt.Errorf(
			"metadata of snapshot %s names hash %s", hash, snapshot.Hash,
		)
	}
	return &snapshot, nil
}

type snapshotServer struct {
	cfg                  ServerConfig
	catalog              *snapshotCatalog
	publicBaseURL        *url.URL
	publicBaseURLErr     error
	publicRequests       chan struct{}
	signatureReads       chan struct{}
	signatureReadClients *clientAdmission
	signatureRequests    chan struct{}
	artifactDownloads    *clientAdmission
	requestReadTimeout   time.Duration
	responseWriteTimeout time.Duration
}

// NewServerHandler returns the handler serving the Mithril aggregator
// Cardano database artifact API over cfg.Store, plus the archive downloads
// the artifact locations point at.
func NewServerHandler(cfg ServerConfig) http.Handler {
	return newServerHandler(
		cfg,
		publicRequestLimit,
		artifactResponseWriteTimeout,
	)
}

func newServerHandler(
	cfg ServerConfig,
	requestLimit int,
	responseWriteTimeout time.Duration,
) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	publicBaseURL, publicBaseURLErr := parsePublicBaseURL(cfg.PublicBaseURL)
	catalog := newSnapshotCatalog(cfg.Store)
	if cfg.Aggregator != nil {
		cfg.Aggregator.watchCatalog(catalog)
	}
	s := &snapshotServer{
		cfg:                  cfg,
		catalog:              catalog,
		publicBaseURL:        publicBaseURL,
		publicBaseURLErr:     publicBaseURLErr,
		publicRequests:       make(chan struct{}, requestLimit),
		signatureReads:       make(chan struct{}, signatureRequestReadLimit),
		signatureReadClients: newClientAdmission(signatureRequestReadsPerClient),
		signatureRequests:    make(chan struct{}, signatureRequestLimit),
		artifactDownloads:    newClientAdmission(artifactDownloadsPerClient),
		requestReadTimeout:   publicRequestReadTimeout,
		responseWriteTimeout: responseWriteTimeout,
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /artifact/cardano-database",
		s.withPublicRequest(s.handleList),
	)
	mux.HandleFunc(
		"GET /artifact/cardano-database/{hash}",
		s.withPublicRequest(s.handleDetail),
	)
	mux.HandleFunc(
		"GET /download/{hash}/{name}",
		s.withPublicRequest(s.withArtifactDownload(s.handleDownload)),
	)
	mux.HandleFunc(
		"GET /certificate/{hash}",
		s.withPublicRequest(s.handleCertificate),
	)
	if cfg.Aggregator != nil {
		cfg.Aggregator.registerRoutes(
			mux,
			s.withPublicRequest,
			s.withSignatureRequest,
		)
	}
	return mux
}

func (s *snapshotServer) withArtifactDownload(
	next http.HandlerFunc,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		release, ok := s.artifactDownloads.acquire(r.RemoteAddr)
		if !ok {
			w.Header().Set("Retry-After", "1")
			http.Error(
				w,
				"client download limit reached",
				http.StatusTooManyRequests,
			)
			return
		}
		defer release()
		if bounded, ok := w.(*progressResponseWriter); ok {
			bounded.minimumBytesPerSecond = artifactMinimumBytesPerSecond
			bounded.transferStarted = time.Now()
			bounded.transferGrace = artifactTransferGrace
		}
		next(w, r)
	}
}

func (s *snapshotServer) withPublicRequest(
	next http.HandlerFunc,
) http.HandlerFunc {
	return s.withRequestAdmission(s.publicRequests, next)
}

type signatureRequestHandler func(
	http.ResponseWriter,
	*http.Request,
	registerSignatureRequest,
)

func (s *snapshotServer) withSignatureRequest(
	next signatureRequestHandler,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		controller := s.armRequestReadDeadline(w, r)
		bounded := s.boundedResponseWriter(w, controller)
		clientRelease, ok := s.signatureReadClients.acquire(r.RemoteAddr)
		if !ok {
			s.rejectUnreadRequest(
				bounded, controller,
				"client signature read limit reached",
				http.StatusTooManyRequests,
			)
			return
		}
		readRelease, ok := tryAdmitRequest(s.signatureReads)
		if !ok {
			clientRelease()
			s.rejectUnreadRequest(
				bounded, controller,
				"server busy",
				http.StatusServiceUnavailable,
			)
			return
		}
		var req registerSignatureRequest
		decodeErr := func() error {
			defer readRelease()
			defer clientRelease()
			err := decodeJSONBody(bounded, r, &req)
			if err != nil {
				s.disposeUnreadRequest(bounded, controller)
			}
			return err
		}()
		if decodeErr != nil {
			writeAPIError(bounded, decodeErr)
			return
		}

		release, ok := admitRequest(bounded, s.signatureRequests)
		if !ok {
			return
		}
		defer release()
		next(bounded, r, req)
	}
}

func (s *snapshotServer) withRequestAdmission(
	slots chan struct{},
	next http.HandlerFunc,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		controller := s.armRequestReadDeadline(w, r)
		release, ok := admitRequest(w, slots)
		if !ok {
			return
		}
		defer release()
		bounded := s.boundedResponseWriter(w, controller)
		next(bounded, r)
	}
}

func (s *snapshotServer) armRequestReadDeadline(
	w http.ResponseWriter,
	r *http.Request,
) *http.ResponseController {
	controller := http.NewResponseController(w)
	if r.Method == http.MethodPost {
		if err := controller.SetReadDeadline(
			time.Now().Add(s.requestReadTimeout),
		); err != nil && !errors.Is(err, http.ErrNotSupported) {
			s.cfg.Logger.Debug(
				"could not bound public request body read",
				"error", err,
			)
		}
	}
	return controller
}

func (s *snapshotServer) boundedResponseWriter(
	w http.ResponseWriter,
	controller *http.ResponseController,
) *progressResponseWriter {
	return &progressResponseWriter{
		ResponseWriter: w,
		controller:     controller,
		timeout:        s.responseWriteTimeout,
		logger:         s.cfg.Logger,
	}
}

func (s *snapshotServer) rejectUnreadRequest(
	w http.ResponseWriter,
	controller *http.ResponseController,
	message string,
	status int,
) {
	s.disposeUnreadRequest(w, controller)
	w.Header().Set("Retry-After", "1")
	http.Error(w, message, status)
}

func (s *snapshotServer) disposeUnreadRequest(
	w http.ResponseWriter,
	controller *http.ResponseController,
) {
	w.Header().Set("Connection", "close")
	if err := controller.SetReadDeadline(time.Now()); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		s.cfg.Logger.Debug(
			"could not stop rejected request body read",
			"error", err,
		)
	}
}

func admitRequest(
	w http.ResponseWriter,
	slots chan struct{},
) (func(), bool) {
	release, ok := tryAdmitRequest(slots)
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "server busy", http.StatusServiceUnavailable)
	}
	return release, ok
}

func tryAdmitRequest(slots chan struct{}) (func(), bool) {
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		return nil, false
	}
}

// progressResponseWriter refreshes the connection write deadline before each
// body write. It deliberately does not expose io.ReaderFrom: ServeContent must
// pass each copied chunk through Write so steady large transfers keep making
// progress without receiving an absolute response deadline. The last deadline
// remains armed while net/http flushes its response buffer after the handler
// returns; net/http clears it before reusing the connection.
type progressResponseWriter struct {
	http.ResponseWriter
	controller            *http.ResponseController
	timeout               time.Duration
	logger                *slog.Logger
	minimumBytesPerSecond int64
	transferStarted       time.Time
	transferGrace         time.Duration
	written               int64
}

func (w *progressResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *progressResponseWriter) Write(p []byte) (int, error) {
	deadline := time.Now().Add(w.timeout)
	if w.minimumBytesPerSecond > 0 {
		total := w.written + int64(len(p))
		allowed := time.Duration(total/w.minimumBytesPerSecond)*time.Second +
			time.Duration(total%w.minimumBytesPerSecond)*time.Second/
				time.Duration(w.minimumBytesPerSecond)
		transferDeadline := w.transferStarted.Add(w.transferGrace + allowed)
		if transferDeadline.Before(deadline) {
			deadline = transferDeadline
		}
	}
	w.setDeadline(deadline)
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *progressResponseWriter) setDeadline(deadline time.Time) {
	if err := w.controller.SetWriteDeadline(deadline); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		w.logger.Debug(
			"could not bound snapshot response write",
			"error", err,
		)
	}
}

func (s *snapshotServer) fail(
	w http.ResponseWriter,
	err error,
) {
	if errors.Is(err, ErrArtifactNotFound) {
		http.NotFound(w, nil)
		return
	}
	s.cfg.Logger.Error(
		"snapshot server request failed",
		"component", "mithril",
		"error", err,
	)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func (s *snapshotServer) handleList(
	w http.ResponseWriter,
	r *http.Request,
) {
	snapshots, err := s.catalog.list(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	items := make([]CardanoDatabaseSnapshotListItem, 0, len(snapshots))
	for _, snap := range snapshots {
		// A verifying client bootstraps from the newest listed snapshot, so
		// an aggregator lists a snapshot only once it is certified.
		if s.cfg.Aggregator != nil && snap.CertificateHash == "" {
			continue
		}
		items = append(items, CardanoDatabaseSnapshotListItem{
			Hash:                    snap.Hash,
			MerkleRoot:              snap.MerkleRoot,
			Beacon:                  snap.Beacon,
			CertificateHash:         snap.CertificateHash,
			TotalDbSizeUncompressed: snap.TotalDbSizeUncompressed,
			CardanoNodeVersion:      snap.CardanoNodeVersion,
			CreatedAt:               snap.CreatedAt,
		})
	}
	writeJSON(w, items)
}

func (s *snapshotServer) handleDetail(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash := r.PathValue("hash")
	if !hashPattern.MatchString(hash) {
		http.NotFound(w, r)
		return
	}
	if s.publicBaseURLErr != nil {
		s.fail(w, fmt.Errorf("invalid public base URL: %w", s.publicBaseURLErr))
		return
	}
	snap, err := readSnapshot(r.Context(), s.cfg.Store, hash)
	if err != nil {
		s.fail(w, err)
		return
	}
	baseURL := *s.publicBaseURL
	baseURL.Path = "/download/" + hash
	base := baseURL.String()
	location := func(uri string) CardanoDatabaseLocation {
		return CardanoDatabaseLocation{
			Type:                 locationTypeCloudStorage,
			URI:                  uri,
			CompressionAlgorithm: compressionZstd,
		}
	}
	snap.Digests.Locations = []CardanoDatabaseLocation{
		location(base + "/" + digestsArchiveName),
	}
	snap.Immutables.Locations = []CardanoDatabaseLocation{{
		Type:                 locationTypeCloudStorage,
		URITemplate:          base + "/" + immutableFileNumberTemplate + ".tar.zst",
		CompressionAlgorithm: compressionZstd,
	}}
	snap.Ancillary.Locations = []CardanoDatabaseLocation{
		location(base + "/" + ancillaryArchiveName),
	}
	writeJSON(w, snap)
}

func (s *snapshotServer) handleDownload(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, name := r.PathValue("hash"), r.PathValue("name")
	if !hashPattern.MatchString(hash) ||
		!downloadNamePattern.MatchString(name) {
		http.NotFound(w, r)
		return
	}
	s.serveObject(w, r, path.Join(hash, name))
}

func (s *snapshotServer) handleCertificate(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash := r.PathValue("hash")
	if !hashPattern.MatchString(hash) {
		http.NotFound(w, r)
		return
	}
	s.serveObject(w, r, path.Join(certificatesPrefix, hash+".json"))
}

// serveObject answers with the object at key: a redirect when one is
// configured, otherwise the object itself with range support.
func (s *snapshotServer) serveObject(
	w http.ResponseWriter,
	r *http.Request,
	key string,
) {
	if s.cfg.RedirectBaseURL != "" {
		// The base is operator configuration and key is built from
		// regex-validated segments, so neither is attacker-chosen.
		http.Redirect( //nolint:gosec // G710
			w, r,
			strings.TrimSuffix(s.cfg.RedirectBaseURL, "/")+"/"+key,
			http.StatusTemporaryRedirect,
		)
		return
	}
	obj, err := s.cfg.Store.Open(r.Context(), key)
	if err != nil {
		s.fail(w, err)
		return
	}
	defer obj.Close()
	if isRemoteStore(s.cfg.Store) && rangeCount(r.Header.Get("Range")) > 1 {
		// ServeContent seeks once per range, and a remote reader opens a new
		// ranged object read after every seek, so one request could fan out
		// into an unbounded number of backend requests. RFC 9110 lets a
		// server ignore Range, which serves the object from a single read.
		r = r.Clone(r.Context())
		r.Header.Del("Range")
	}
	// Without a Content-Type, ServeContent sniffs the type by reading the
	// head of the object and seeking back, which costs a remote store an
	// extra ranged read, and the extension lookup depends on the host's MIME
	// tables. Every served key is a .json certificate or a .tar.zst archive.
	contentType := "application/zstd"
	if strings.HasSuffix(key, ".json") {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	http.ServeContent(w, r, path.Base(key), time.Time{}, obj)
}

// remoteObjectStore is implemented by stores whose readers issue one backend
// request each time a read resumes after a seek.
type remoteObjectStore interface {
	streamsRemoteObjects() bool
}

func isRemoteStore(store ArtifactStore) bool {
	remote, ok := store.(remoteObjectStore)
	return ok && remote.streamsRemoteObjects()
}

// rangeCount is the number of ranges a bytes Range header names, skipping
// the empty and whitespace-only elements net/http also skips.
func rangeCount(header string) int {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0
	}
	n := 0
	for elem := range strings.SplitSeq(spec, ",") {
		if strings.TrimSpace(elem) != "" {
			n++
		}
	}
	return n
}
