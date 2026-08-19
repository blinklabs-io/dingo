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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package offchainmetadata

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

const (
	// TokenRegistrySyncStateKey is the sync_state key holding the HTTP
	// entity tag of the last successfully applied registry snapshot.
	TokenRegistrySyncStateKey = "token_registry_etag"

	// MainnetTokenRegistryURL and TestnetTokenRegistryURL are the CIP-26
	// registries the Cardano Foundation and IOG publish. Both are served as
	// repository tarballs, which support conditional requests; see SyncOnce.
	MainnetTokenRegistryURL = "https://github.com/cardano-foundation/" +
		"cardano-token-registry/archive/refs/heads/master.tar.gz"
	TestnetTokenRegistryURL = "https://github.com/input-output-hk/" +
		"metadata-registry-testnet/archive/refs/heads/master.tar.gz"

	// defaultTokenRegistryInterval is deliberately far longer than the
	// per-URL fetcher's: the registry is a single bulk artifact that changes
	// on the order of days, and an unchanged one costs only a conditional
	// request at this cadence.
	defaultTokenRegistryInterval = 6 * time.Hour
	// defaultTokenRegistryTimeout bounds the whole download. The mainnet
	// tarball is roughly 240MB, so this is a transfer budget rather than the
	// per-request latency budget the per-URL fetcher uses.
	defaultTokenRegistryTimeout = 15 * time.Minute
	// defaultTokenRegistryMaxBytes caps the compressed download. The mainnet
	// registry sits near 240MB; the headroom here absorbs growth without
	// letting a hostile or misconfigured source stream forever.
	defaultTokenRegistryMaxBytes int64 = 768 << 20
	// defaultTokenRegistryMaxEntryBytes caps a single mapping document. Real
	// mappings run to tens of kilobytes, dominated by base64 logos.
	defaultTokenRegistryMaxEntryBytes int64 = 4 << 20
	// tokenRegistryBatchSize bounds how many parsed entries are held before
	// being flushed to the store, keeping peak memory independent of the
	// roughly 8,000 mappings in the mainnet registry.
	tokenRegistryBatchSize = 500

	// nolint:gosec // G101 matches on "Token" in the name; this is a
	// user agent for the CIP-26 token registry, not a credential.
	defaultTokenRegistryUserAgent = "dingo-token-registry/1" //nolint:gosec
	tokenRegistryMappingsDir      = "mappings"
	tokenRegistryMappingExt       = ".json"
)

// TokenRegistryStore is the persistence surface the registry sync needs.
type TokenRegistryStore interface {
	UpsertTokenRegistryEntries(
		ctx context.Context,
		entries []models.TokenRegistryEntry,
		syncedAt time.Time,
		txn types.Txn,
	) (int, error)
	PruneTokenRegistryEntriesBefore(
		ctx context.Context,
		cutoff time.Time,
		txn types.Txn,
	) (int64, error)
	GetSyncState(key string, txn types.Txn) (string, error)
	SetSyncState(key, value string, txn types.Txn) error
}

// TokenRegistryConfig configures the CIP-26 token registry sync. Zero values
// fall back to the defaults above.
type TokenRegistryConfig struct {
	Logger *slog.Logger
	Store  TokenRegistryStore
	// HTTPClient customizes the download. When private addresses are not
	// allowed, NewTokenRegistrySync clones the client and replaces unsafe
	// transport dial hooks, exactly as the per-URL fetcher does.
	HTTPClient *http.Client
	// SourceURL overrides the network-derived registry source, for operators
	// running a mirror. Empty selects by Network.
	SourceURL string
	// Network selects the default registry source; anything other than
	// "mainnet" uses the IOG testnet registry.
	Network        string
	UserAgent      string
	Interval       time.Duration
	RequestTimeout time.Duration
	// MaxBytes caps the compressed download; MaxEntryBytes caps one mapping.
	MaxBytes      int64
	MaxEntryBytes int64
	// StoreLogos opts into persisting base64 logo payloads, which are
	// roughly 90% of registry bytes. Off by default.
	StoreLogos bool
	// AllowPrivateAddresses permits fetching private, loopback, and
	// link-local addresses. Leave false for the default SSRF guard.
	AllowPrivateAddresses bool
}

// TokenRegistrySync periodically pulls a CIP-26 token registry and upserts its
// mappings, so that GET /assets/{asset} can serve off-chain token metadata
// from local state.
//
// The sync is holdings-agnostic by construction: every node pulls the same
// complete registry, so unlike per-asset lookups against a remote metadata
// server it reveals nothing about which assets a user holds.
type TokenRegistrySync struct {
	logger        *slog.Logger
	store         TokenRegistryStore
	client        *http.Client
	sourceURL     string
	userAgent     string
	interval      time.Duration
	maxBytes      int64
	maxEntryBytes int64
	storeLogos    bool
	allowPrivate  bool
	now           func() time.Time
	mu            sync.Mutex
	cancel        context.CancelFunc
	done          chan struct{}
}

// NewTokenRegistrySync validates cfg and returns a sync that is not yet
// running.
func NewTokenRegistrySync(
	cfg TokenRegistryConfig,
) (*TokenRegistrySync, error) {
	if cfg.Store == nil {
		return nil, errors.New("token registry store is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "token-registry")
	sourceURL := strings.TrimSpace(cfg.SourceURL)
	if sourceURL == "" {
		sourceURL = defaultTokenRegistryURL(cfg.Network)
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultTokenRegistryInterval
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = defaultTokenRegistryTimeout
	}
	client, err := secureHTTPClient(
		cfg.HTTPClient,
		timeout,
		cfg.AllowPrivateAddresses,
	)
	if err != nil {
		return nil, err
	}
	// secureHTTPClient only sets Timeout on a client it constructs itself, so
	// a caller-supplied client with no Timeout would leave the whole-download
	// budget unenforced and let a stalled source hold SyncOnce open. A caller
	// that asked for something stricter meant it, so only widen from unset.
	if client.Timeout <= 0 || client.Timeout > timeout {
		client.Timeout = timeout
	}
	userAgent := cfg.UserAgent
	if userAgent == "" {
		userAgent = defaultTokenRegistryUserAgent
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultTokenRegistryMaxBytes
	}
	maxEntryBytes := cfg.MaxEntryBytes
	if maxEntryBytes <= 0 {
		maxEntryBytes = defaultTokenRegistryMaxEntryBytes
	}
	return &TokenRegistrySync{
		logger:        logger,
		store:         cfg.Store,
		client:        client,
		sourceURL:     sourceURL,
		userAgent:     userAgent,
		interval:      interval,
		maxBytes:      maxBytes,
		maxEntryBytes: maxEntryBytes,
		storeLogos:    cfg.StoreLogos,
		allowPrivate:  cfg.AllowPrivateAddresses,
		now:           time.Now,
	}, nil
}

// defaultTokenRegistryURL picks the registry for a network. Only mainnet has
// its own registry; every test network shares the IOG testnet one.
func defaultTokenRegistryURL(network string) string {
	if strings.EqualFold(strings.TrimSpace(network), "mainnet") {
		return MainnetTokenRegistryURL
	}
	return TestnetTokenRegistryURL
}

// SourceURL returns the resolved registry source.
func (s *TokenRegistrySync) SourceURL() string {
	return s.sourceURL
}

func (s *TokenRegistrySync) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done != nil {
		return errors.New("token registry sync already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	go s.loop(runCtx)
	return nil
}

// Stop signals the worker and waits for it to exit.
//
// The wait is not abandoned when ctx expires. Callers tear the metadata store
// down immediately after Stop returns (node_shutdown.go phase 3,
// node_lifecycle.go's live storage swap), so returning while the worker could
// still reach that store would hand it a closed database. An expired context
// downgrades to a warning and the wait continues, matching
// koiosparity.Observer.Stop, which releases its cache under the same
// constraint. Cancelling the worker aborts any in-flight download, so the
// remaining wait is bounded by one store write rather than by the registry
// transfer.
//
// Stop is idempotent: the startup-failure rollback stack and shutdown() can
// both reach it for the same instance.
func (s *TokenRegistrySync) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.logger.Warn(
			"token registry sync: stop context expired, still waiting for the worker to exit before the store is released",
		)
		<-done
		return nil
	}
}

func (s *TokenRegistrySync) loop(ctx context.Context) {
	defer close(s.done)
	s.runOnce(ctx)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

func (s *TokenRegistrySync) runOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	written, err := s.SyncOnce(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		// A failed sync is not fatal: the node keeps serving whatever the
		// last successful sync produced.
		s.logger.Warn(
			"token registry sync failed",
			"url", s.sourceURL,
			"error", err,
		)
		return
	}
	if written > 0 {
		s.logger.Info(
			"token registry sync complete",
			"url", s.sourceURL,
			"entries", written,
		)
	}
}

// SyncOnce performs a single registry pull and returns the number of entries
// written.
//
// The mainnet registry is roughly 240MB, so an unconditional download every
// interval would be indefensible. SyncOnce sends the entity tag recorded by
// the previous successful sync as If-None-Match; an unchanged registry answers
// 304 and costs one request with no body. The tag is recorded only after the
// whole snapshot has been applied, so an interrupted sync retries in full
// rather than recording progress it did not make.
func (s *TokenRegistrySync) SyncOnce(ctx context.Context) (int, error) {
	previousETag, err := s.store.GetSyncState(TokenRegistrySyncStateKey, nil)
	if err != nil {
		return 0, fmt.Errorf("read token registry sync state: %w", err)
	}
	if err := validateURL(s.sourceURL, s.allowPrivate); err != nil {
		return 0, fmt.Errorf("token registry source URL: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		s.sourceURL,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("build token registry request: %w", err)
	}
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Accept", "application/gzip")
	if previousETag != "" {
		req.Header.Set("If-None-Match", previousETag)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch token registry: %w", err)
	}
	// http.Client.Do documents a non-nil response whenever err is nil, but the
	// per-URL fetcher guards this the same way rather than trusting a
	// substituted client to honor the contract.
	if resp == nil {
		return 0, errors.New("fetch token registry: nil response")
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusNotModified {
		s.logger.Debug(
			"token registry unchanged",
			"url", s.sourceURL,
			"etag", previousETag,
		)
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf(
			"fetch token registry: unexpected status %d",
			resp.StatusCode,
		)
	}
	// One stamp for the whole snapshot: every row it carries gets this
	// value, so anything older afterwards is a subject the snapshot did not
	// carry. Taken before the first write so no row can predate it.
	syncedAt := s.now().UTC()
	written, err := s.applySnapshot(ctx, resp.Body, syncedAt)
	if err != nil {
		return written, err
	}
	// The snapshot applied in full, so it is authoritative: retire subjects
	// it did not carry. An upsert-only sync would keep serving a token the
	// registry has delisted, or one that lost every property, forever. This
	// is deliberately after the error return above -- pruning against a
	// partial snapshot would delete live subjects it never reached.
	pruned, err := s.store.PruneTokenRegistryEntriesBefore(
		ctx,
		syncedAt,
		nil,
	)
	if err != nil {
		return written, fmt.Errorf("prune stale token registry entries: %w", err)
	}
	if pruned > 0 {
		s.logger.Info(
			"token registry entries retired",
			"count", pruned,
		)
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	if etag != "" && etag != previousETag {
		if err := s.store.SetSyncState(
			TokenRegistrySyncStateKey,
			etag,
			nil,
		); err != nil {
			// The snapshot is already applied; failing to record the tag only
			// costs a redundant download next interval.
			s.logger.Warn(
				"recording token registry sync state failed",
				"error", err,
			)
		}
	}
	return written, nil
}

// applySnapshot streams a gzipped tar of the registry, parsing mappings/*.json
// one entry at a time and flushing them to the store in batches. Nothing is
// written to disk and no more than one mapping plus one batch is held in
// memory, so peak usage is independent of the roughly 8,000-file registry.
//
// A mapping that fails to parse is skipped rather than failing the snapshot:
// one bad file out of thousands should not cost the whole sync.
func (s *TokenRegistrySync) applySnapshot(
	ctx context.Context,
	body io.Reader,
	syncedAt time.Time,
) (int, error) {
	limited := io.LimitReader(body, s.maxBytes+1)
	counter := &countingReader{reader: limited}
	gzipReader, err := gzip.NewReader(counter)
	if err != nil {
		if counter.read > s.maxBytes {
			return 0, fmt.Errorf(
				"token registry snapshot exceeds %d bytes",
				s.maxBytes,
			)
		}
		return 0, fmt.Errorf("open token registry snapshot: %w", err)
	}
	defer func() { _ = gzipReader.Close() }()

	tarReader := tar.NewReader(gzipReader)
	batch := make([]models.TokenRegistryEntry, 0, tokenRegistryBatchSize)
	written := 0
	skipped := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		count, err := s.store.UpsertTokenRegistryEntries(
			ctx,
			batch,
			syncedAt,
			nil,
		)
		if err != nil {
			return fmt.Errorf("store token registry entries: %w", err)
		}
		written += count
		batch = batch[:0]
		return nil
	}
	for {
		if ctx.Err() != nil {
			return written, ctx.Err()
		}
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if counter.read > s.maxBytes {
				return written, fmt.Errorf(
					"token registry snapshot exceeds %d bytes",
					s.maxBytes,
				)
			}
			return written, fmt.Errorf("read token registry snapshot: %w", err)
		}
		if !isTokenRegistryMapping(header) {
			continue
		}
		if header.Size > s.maxEntryBytes {
			skipped++
			s.logger.Debug(
				"token registry mapping too large",
				"name", header.Name,
				"size", header.Size,
			)
			continue
		}
		raw, err := readLimited(tarReader, s.maxEntryBytes)
		if err != nil {
			skipped++
			s.logger.Debug(
				"reading token registry mapping failed",
				"name", header.Name,
				"error", err,
			)
			continue
		}
		entry, err := ParseTokenRegistryEntry(raw)
		if err != nil {
			skipped++
			s.logger.Debug(
				"parsing token registry mapping failed",
				"name", header.Name,
				"error", err,
			)
			continue
		}
		if !s.storeLogos {
			entry.Logo = ""
		}
		if entry.IsEmpty() {
			continue
		}
		batch = append(batch, *entry)
		if len(batch) >= tokenRegistryBatchSize {
			if err := flush(); err != nil {
				return written, err
			}
		}
	}
	if err := flush(); err != nil {
		return written, err
	}
	if counter.read > s.maxBytes {
		return written, fmt.Errorf(
			"token registry snapshot exceeds %d bytes",
			s.maxBytes,
		)
	}
	if skipped > 0 {
		s.logger.Info(
			"token registry mappings skipped",
			"count", skipped,
		)
	}
	return written, nil
}

// isTokenRegistryMapping reports whether a tar entry is a registry mapping.
// Archive layouts put everything under one generated top-level directory, so
// the mappings directory is matched relative to that root rather than at an
// absolute position.
func isTokenRegistryMapping(header *tar.Header) bool {
	// archive/tar normalizes the historical TypeRegA to TypeReg on read,
	// so TypeReg alone covers both spellings.
	if header.Typeflag != tar.TypeReg {
		return false
	}
	name := path.Clean(header.Name)
	if !strings.HasSuffix(strings.ToLower(name), tokenRegistryMappingExt) {
		return false
	}
	return path.Base(path.Dir(name)) == tokenRegistryMappingsDir
}

// countingReader tracks how many bytes have been pulled from the response so
// that a snapshot truncated by the size limit is reported as an over-size
// failure rather than as a corrupt archive.
type countingReader struct {
	reader io.Reader
	read   int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.read += int64(n)
	return n, err
}
