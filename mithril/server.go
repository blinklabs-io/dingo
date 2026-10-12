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
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
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

// publicRequestLimit bounds concurrent public requests. One v2 bootstrap
// holds immutableDownloadWorkers archive downloads and the ancillary download
// at once, so the bound admits two of them without shedding a request.
const publicRequestLimit = 2 * (immutableDownloadWorkers + 1)

const (
	certificatesPrefix           = "certificates"
	publicRequestReadTimeout     = 15 * time.Second
	artifactResponseWriteTimeout = 15 * time.Second
)

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
	publicBaseURL        *url.URL
	publicBaseURLErr     error
	publicRequests       chan struct{}
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
	s := &snapshotServer{
		cfg:                  cfg,
		publicBaseURL:        publicBaseURL,
		publicBaseURLErr:     publicBaseURLErr,
		publicRequests:       make(chan struct{}, requestLimit),
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
		s.withPublicRequest(s.handleDownload),
	)
	mux.HandleFunc(
		"GET /certificate/{hash}",
		s.withPublicRequest(s.handleCertificate),
	)
	if cfg.Aggregator != nil {
		cfg.Aggregator.registerRoutes(mux, s.withPublicRequest)
	}
	return mux
}

func (s *snapshotServer) withPublicRequest(
	next http.HandlerFunc,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		select {
		case s.publicRequests <- struct{}{}:
			defer func() { <-s.publicRequests }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "server busy", http.StatusServiceUnavailable)
			return
		}

		bounded := &progressResponseWriter{
			ResponseWriter: w,
			controller:     controller,
			timeout:        s.responseWriteTimeout,
			logger:         s.cfg.Logger,
		}
		next(bounded, r)
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
	controller *http.ResponseController
	timeout    time.Duration
	logger     *slog.Logger
}

func (w *progressResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *progressResponseWriter) Write(p []byte) (int, error) {
	w.setDeadline(time.Now().Add(w.timeout))
	return w.ResponseWriter.Write(p)
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
	snapshots, err := ListSnapshots(r.Context(), s.cfg.Store)
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
	http.ServeContent(w, r, path.Base(key), time.Time{}, obj)
}
