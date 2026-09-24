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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"golang.org/x/crypto/blake2b"
)

const (
	maxInt64 = int64(^uint64(0) >> 1)
	// The sync_state keys below describe the snapshot currently *in the
	// table*, not a per-source cache. There is one token_registry_entry
	// table, so there is one set of state, and all three move together on a
	// successful apply.
	//
	// TokenRegistrySyncStateKey holds that snapshot's HTTP entity tag.
	TokenRegistrySyncStateKey = "token_registry_etag"
	// tokenRegistrySnapshotIDKey records which (source, logo mode) pair
	// produced the snapshot the table holds. A validator is only meaningful
	// while the table still holds what that source served: moving to another
	// source and back would otherwise replay the old tag, take a 304, and
	// leave the intervening source's metadata in place. Same for toggling
	// logo storage off and on again.
	tokenRegistrySnapshotIDKey = "token_registry_snapshot_id"
	// tokenRegistryStampKey holds the high-water snapshot stamp. It is
	// persisted because the in-memory sequence resets on restart, and a
	// restart landing in the same wall-clock second as the previous snapshot
	// would otherwise reuse its stamp -- making the prune spare exactly the
	// subjects the new snapshot dropped.
	tokenRegistryStampKey = "token_registry_synced_at"

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
	// minTokenRegistryInterval floors the configured interval. Polling a
	// source that serves a roughly 240MB artifact faster than this is
	// abusive rather than useful, and a sub-second interval is also the
	// only way two snapshots can land in the same wall-clock second --
	// which is what would push stamps ahead of real time and let rows
	// outlive a restart's reset stamp sequence.
	minTokenRegistryInterval = time.Minute
	// defaultTokenRegistryTimeout bounds the whole download. The mainnet
	// tarball is roughly 240MB, so this is a transfer budget rather than the
	// per-request latency budget the per-URL fetcher uses.
	defaultTokenRegistryTimeout = 15 * time.Minute
	// defaultTokenRegistryMaxBytes caps the compressed download. The mainnet
	// registry sits near 240MB; the headroom here absorbs growth without
	// letting a hostile or misconfigured source stream forever.
	defaultTokenRegistryMaxBytes int64 = 768 << 20
	// defaultTokenRegistryMaxDecompressedBytes bounds gzip expansion and all
	// tar content, including files the registry parser does not recognize.
	defaultTokenRegistryMaxDecompressedBytes int64 = 2 << 30
	// defaultTokenRegistryMaxEntryBytes caps a single mapping document. Real
	// mappings run to tens of kilobytes, dominated by base64 logos.
	defaultTokenRegistryMaxEntryBytes int64 = 4 << 20
	// The archive and accepted-entry limits bound header parsing and database
	// work independently of byte limits. The public registries contain fewer
	// than 10,000 mappings.
	defaultTokenRegistryMaxArchiveEntries  = 100_000
	defaultTokenRegistryMaxAcceptedEntries = 50_000
	// Payload strings dominate retained batch memory. This permits the normal
	// 500-entry batch while bounding logo-heavy batches independently of the
	// per-mapping limit.
	defaultTokenRegistryMaxBatchBytes int64 = 64 << 20
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
	Transaction(ctx context.Context) types.Txn
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
	) (int, error)
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
	// MaxBytes caps the compressed download; MaxDecompressedBytes caps all
	// expanded tar content; MaxEntryBytes caps one mapping. MaxArchiveEntries
	// counts every tar header, while MaxAcceptedEntries bounds parsed rows and
	// therefore row-upsert operations. MaxBatchBytes caps retained property
	// payload between database flushes.
	MaxBytes             int64
	MaxDecompressedBytes int64
	MaxEntryBytes        int64
	MaxArchiveEntries    int
	MaxAcceptedEntries   int
	MaxBatchBytes        int64
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
	logger               *slog.Logger
	store                TokenRegistryStore
	client               *http.Client
	sourceURL            string
	userAgent            string
	interval             time.Duration
	maxBytes             int64
	maxDecompressedBytes int64
	maxEntryBytes        int64
	maxArchiveEntries    int
	maxAcceptedEntries   int
	maxBatchBytes        int64
	storeLogos           bool
	allowPrivate         bool
	removeStageFile      func(string) error
	now                  func() time.Time
	lastSyncedAt         time.Time
	mu                   sync.Mutex
	// syncSlot serializes whole snapshot applications. It is a buffered
	// channel rather than a mutex so the wait can be abandoned on context
	// cancellation: an external SyncOnce call holds its own uncancelled
	// context, and a plain mutex would make the worker -- and therefore
	// Stop, which waits for the worker -- block until that call finished.
	// It is also separate from mu, which Stop itself takes.
	syncSlot chan struct{}
	cancel   context.CancelFunc
	done     chan struct{}
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
	if interval < minTokenRegistryInterval {
		interval = minTokenRegistryInterval
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
	maxDecompressedBytes := cfg.MaxDecompressedBytes
	if maxDecompressedBytes <= 0 {
		maxDecompressedBytes = defaultTokenRegistryMaxDecompressedBytes
	}
	maxArchiveEntries := cfg.MaxArchiveEntries
	if maxArchiveEntries <= 0 {
		maxArchiveEntries = defaultTokenRegistryMaxArchiveEntries
	}
	maxAcceptedEntries := cfg.MaxAcceptedEntries
	if maxAcceptedEntries <= 0 {
		maxAcceptedEntries = defaultTokenRegistryMaxAcceptedEntries
	}
	maxBatchBytes := cfg.MaxBatchBytes
	if maxBatchBytes <= 0 {
		maxBatchBytes = defaultTokenRegistryMaxBatchBytes
	}
	return &TokenRegistrySync{
		logger:               logger,
		store:                cfg.Store,
		client:               client,
		sourceURL:            sourceURL,
		userAgent:            userAgent,
		interval:             interval,
		maxBytes:             maxBytes,
		maxDecompressedBytes: maxDecompressedBytes,
		maxEntryBytes:        maxEntryBytes,
		maxArchiveEntries:    maxArchiveEntries,
		maxAcceptedEntries:   maxAcceptedEntries,
		maxBatchBytes:        maxBatchBytes,
		storeLogos:           cfg.StoreLogos,
		allowPrivate:         cfg.AllowPrivateAddresses,
		removeStageFile:      os.Remove,
		now:                  time.Now,
		syncSlot:             make(chan struct{}, 1),
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

// tokenRegistrySnapshotIdentity fingerprints everything that changes what a
// snapshot would produce: the source it came from and whether logos were
// stored. Comparing it against the recorded identity is what tells us whether
// a stored entity tag still describes the table's contents.
//
// Hashed rather than embedded to keep the value bounded and free of any
// credentials a mirror URL might carry.
func tokenRegistrySnapshotIdentity(sourceURL string, storeLogos bool) string {
	material := sourceURL + "|logos=" + strconv.FormatBool(storeLogos)
	sum := blake2b.Sum256([]byte(material))
	return hex.EncodeToString(sum[:8])
}

// SourceURL returns the resolved registry source.
func (s *TokenRegistrySync) SourceURL() string {
	return s.sourceURL
}

// registryLogURL renders a registry source for a log field with every
// component that can carry a credential removed: userinfo, query, and
// fragment. Scheme, host, port, and path are kept, which is what makes a
// log line diagnosable.
//
// A URL that does not parse has no safe parsed form, and neither does an
// opaque one, whose single opaque section is not separable into host and
// path. Both return a fixed placeholder rather than falling back to the
// raw input, so no input can route around the redaction.
func registryLogURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[invalid URL]"
	}
	if u.Opaque != "" {
		return "[invalid URL]"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

// registryRequestError reports that a registry request step failed without
// formatting its cause into the message. URL parser and HTTP client errors
// echo the URL they were given -- including the URL of a redirect target --
// so a source configured with credentials in its userinfo or query would put
// them into any log or API response that rendered the error.
//
// The cause is still reachable through Unwrap, so errors.Is and errors.As
// continue to identify cancellation, deadline, and transport failures.
type registryRequestError struct {
	operation string
	cause     error
}

func (e *registryRequestError) Error() string {
	return e.operation + " failed"
}

func (e *registryRequestError) Unwrap() error {
	return e.cause
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
			"url", registryLogURL(s.sourceURL),
			"error", err,
		)
		return
	}
	if written > 0 {
		s.logger.Info(
			"token registry sync complete",
			"url", registryLogURL(s.sourceURL),
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
func (s *TokenRegistrySync) SyncOnce(
	ctx context.Context,
) (written int, retErr error) {
	// Serialized end to end, and abandonable. SyncOnce is exported and the
	// worker loop calls it, so two applications can overlap; interleaved
	// snapshots would let an older one finish last, overwrite the newer
	// one's properties, reintroduce subjects the newer registry dropped,
	// and record its own stale ETag as current. Queuing is the right
	// behavior for a slow background pass nobody waits on.
	//
	// The wait honors ctx so it cannot outlive a shutdown: an external
	// caller holds its own context, which the node cannot cancel, and a
	// plain mutex would make the worker -- and Stop, which waits for the
	// worker -- block until that caller's whole download finished.
	select {
	case s.syncSlot <- struct{}{}:
		defer func() { <-s.syncSlot }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	identity := tokenRegistrySnapshotIdentity(s.sourceURL, s.storeLogos)
	storedIdentity, err := s.store.GetSyncState(
		tokenRegistrySnapshotIDKey,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("read token registry sync state: %w", err)
	}
	// A stored validator only describes the table while the table still
	// holds what that source served under that logo mode. After a switch
	// away and back, the tag would still match upstream but the table holds
	// the intervening snapshot, so a 304 would leave the wrong metadata in
	// place. Ask unconditionally in that case.
	previousETag := ""
	if storedIdentity == identity {
		previousETag, err = s.store.GetSyncState(
			TokenRegistrySyncStateKey,
			nil,
		)
		if err != nil {
			return 0, fmt.Errorf("read token registry sync state: %w", err)
		}
	}
	persistedStamp, err := s.readPersistedStamp()
	if err != nil {
		return 0, err
	}
	if err := validateURL(s.sourceURL, s.allowPrivate); err != nil {
		return 0, &registryRequestError{
			operation: "validate token registry source URL",
			cause:     err,
		}
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		s.sourceURL,
		nil,
	)
	if err != nil {
		return 0, &registryRequestError{
			operation: "build token registry request",
			cause:     err,
		}
	}
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Accept", "application/gzip")
	if previousETag != "" {
		req.Header.Set("If-None-Match", previousETag)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, &registryRequestError{
			operation: "fetch token registry",
			cause:     err,
		}
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
			"url", registryLogURL(s.sourceURL),
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
	// Truncated to a whole second because MySQL's `datetime` column holds
	// no fractional seconds. A sub-second stamp would be stored rounded
	// while the prune below compared against the unrounded value, so every
	// row the snapshot had just written would look older than the cutoff
	// and be deleted. Truncating at the source makes the written value and
	// the cutoff identical on SQLite, PostgreSQL, and MySQL alike.
	stage, err := s.stageSnapshot(ctx, resp.Body)
	if err != nil {
		return 0, err
	}
	defer func() {
		cleanupErr := stage.close(s.removeStageFile)
		if cleanupErr == nil {
			return
		}
		if retErr != nil {
			s.logger.Warn(
				"cleaning token registry staging file failed after sync error",
				"error", cleanupErr,
			)
			return
		}
		written = 0
		retErr = fmt.Errorf(
			"clean token registry staging file: %w",
			cleanupErr,
		)
	}()
	// An archive carrying no mapping files at all is not evidence that the
	// registry is empty -- it is what an upstream layout change, a
	// truncated artifact, or a mirror serving the wrong repository looks
	// like. Pruning against it would reconcile the whole table to nothing,
	// and recording its ETag would make that stick until the artifact
	// changed again. Keep what we have, retry next interval, and say so.
	if stage.mappings == 0 {
		s.logger.Warn(
			"token registry snapshot contained no usable mappings; keeping existing entries and retrying next interval",
			"url",
			registryLogURL(s.sourceURL),
		)
		return 0, nil
	}

	// Artifact ingestion is complete before the metadata transaction begins.
	// This keeps network and parsing latency outside SQLite's writer lock and
	// outside the corresponding write transaction on every SQL backend.
	syncedAt := s.nextSyncStamp(persistedStamp)
	txn := s.store.Transaction(ctx)
	committed := false
	defer func() {
		if !committed {
			_ = txn.Rollback()
		}
	}()
	// The stamp, rows, reconciliation, and validator state describe one
	// public snapshot and therefore share one metadata transaction. A limit,
	// parse, store, or commit failure leaves the previously served snapshot
	// and all of its state intact.
	if err := s.store.SetSyncState(
		tokenRegistryStampKey,
		syncedAt.UTC().Format(time.RFC3339Nano),
		txn,
	); err != nil {
		return 0, fmt.Errorf("record token registry sync stamp: %w", err)
	}
	written, err = s.applyStagedSnapshot(
		ctx,
		stage,
		syncedAt,
		txn,
	)
	if err != nil {
		return 0, err
	}
	// A skipped mapping is indistinguishable from an absent one at prune
	// time: neither re-stamps its row. Reconciling anyway would let a
	// mapping that was valid yesterday and is malformed or oversized today
	// delete the good metadata still being served for it. Defer instead --
	// skips are rare (all 7,970 mainnet mappings parse), so a later clean
	// snapshot reconciles, and the warning makes a persistent one visible
	// rather than silently destructive.
	if stage.skipped > 0 {
		s.logger.Warn(
			"token registry snapshot had unusable mappings; deferring reconciliation so their stored metadata is not retired",
			"skipped",
			stage.skipped,
			"url",
			registryLogURL(s.sourceURL),
		)
		if err := txn.Commit(); err != nil {
			return 0, fmt.Errorf("commit token registry snapshot: %w", err)
		}
		committed = true
		s.lastSyncedAt = syncedAt
		return written, nil
	}
	// The snapshot applied in full and carried something, so it is
	// authoritative: retire subjects
	// it did not carry. An upsert-only sync would keep serving a token the
	// registry has delisted, or one that lost every property, forever. This
	// is deliberately after the error return above -- pruning against a
	// partial snapshot would delete live subjects it never reached.
	pruned, err := s.store.PruneTokenRegistryEntriesBefore(
		ctx,
		syncedAt,
		txn,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"prune stale token registry entries: %w",
			err,
		)
	}
	// The snapshot is applied, so the recorded state now describes the
	// table. All three move together: recording the tag without the identity
	// would let a later switch-back replay it against the wrong contents.
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	if etag != "" {
		if err := s.store.SetSyncState(
			TokenRegistrySyncStateKey,
			etag,
			txn,
		); err != nil {
			return 0, fmt.Errorf(
				"record token registry entity tag: %w",
				err,
			)
		}
	}
	if err := s.store.SetSyncState(
		tokenRegistrySnapshotIDKey,
		identity,
		txn,
	); err != nil {
		return 0, fmt.Errorf(
			"record token registry snapshot identity: %w",
			err,
		)
	}
	if err := txn.Commit(); err != nil {
		return 0, fmt.Errorf("commit token registry snapshot: %w", err)
	}
	committed = true
	s.lastSyncedAt = syncedAt
	if pruned > 0 {
		s.logger.Info(
			"token registry entries retired",
			"count", pruned,
		)
	}
	return written, nil
}

// readPersistedStamp returns the recorded high-water snapshot stamp, or the
// zero time when none has been recorded or the stored value is unparsable --
// in which case the wall clock alone governs, exactly as before any snapshot
// had been applied.
func (s *TokenRegistrySync) readPersistedStamp() (time.Time, error) {
	raw, err := s.store.GetSyncState(tokenRegistryStampKey, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"read token registry sync stamp: %w",
			err,
		)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	stamp, parseErr := time.Parse(time.RFC3339Nano, raw)
	if parseErr != nil {
		s.logger.Warn(
			"stored token registry sync stamp is unparsable; falling back to the wall clock",
			"value",
			raw,
			"error",
			parseErr,
		)
		return time.Time{}, nil
	}
	return stamp.UTC(), nil
}

// nextSyncStamp returns the stamp for a snapshot: truncated to a whole second
// (see the note on MySQL's fractionless `datetime` below) and strictly
// greater than the previous snapshot's.
//
// Truncation alone is not enough. Two snapshots inside the same wall-clock
// second -- reachable with a sub-second configured interval -- would share a
// stamp, and the prune's `updated_at < cutoff` would then preserve exactly
// the subjects the newer snapshot dropped. Forcing the sequence upward keeps
// reconciliation correct at any interval.
//
// MySQL's `datetime` column carries no fractional seconds, so an unrounded
// stamp would be stored rounded while the prune compared the original, making
// every row the snapshot had just written look stale.
// Called only from SyncOnce, which holds the sync slot. lastSyncedAt advances
// only after the transaction commits, so a rejected snapshot cannot move even
// the process-local high-water mark.
func (s *TokenRegistrySync) nextSyncStamp(persisted time.Time) time.Time {
	stamp := s.now().UTC().Truncate(time.Second)
	// The floor is the later of what this process last used and what the
	// store recorded, so the sequence stays strictly increasing across a
	// restart that resets the in-memory half.
	floor := s.lastSyncedAt
	if persisted.After(floor) {
		floor = persisted
	}
	if !floor.IsZero() && !stamp.After(floor) {
		stamp = floor.Add(time.Second)
	}
	return stamp
}

type tokenRegistryStage struct {
	file     *os.File
	mappings int
	skipped  int
}

// tokenRegistryStageEntry is the private, ephemeral representation written to
// the staging file. Keep it limited to fields the metadata upsert consumes and
// tag every field explicitly so the format does not depend on Go field names.
type tokenRegistryStageEntry struct {
	Decimals    *int   `json:"decimals,omitempty"`
	Subject     string `json:"subject"`
	Name        string `json:"name,omitempty"`
	Ticker      string `json:"ticker,omitempty"`
	Description string `json:"description,omitempty"`
	URL         string `json:"url,omitempty"`
	Logo        string `json:"logo,omitempty"`
}

func newTokenRegistryStageEntry(
	entry *models.TokenRegistryEntry,
) tokenRegistryStageEntry {
	return tokenRegistryStageEntry{
		Decimals:    entry.Decimals,
		Subject:     entry.Subject,
		Name:        entry.Name,
		Ticker:      entry.Ticker,
		Description: entry.Description,
		URL:         entry.URL,
		Logo:        entry.Logo,
	}
}

func (entry tokenRegistryStageEntry) model() models.TokenRegistryEntry {
	return models.TokenRegistryEntry{
		Decimals:    entry.Decimals,
		Subject:     entry.Subject,
		Name:        entry.Name,
		Ticker:      entry.Ticker,
		Description: entry.Description,
		URL:         entry.URL,
		Logo:        entry.Logo,
	}
}

func (s *tokenRegistryStage) close(remove func(string) error) error {
	name := s.file.Name()
	return errors.Join(s.file.Close(), remove(name))
}

// stageSnapshot streams and validates the remote archive into a bounded local
// staging file. Network, decompression, and parsing all finish before the
// final metadata transaction begins; only parsed entries are staged, and both
// their count and encoded bytes are bounded.
//
// A mapping that fails to parse is skipped rather than failing the snapshot:
// one bad file out of thousands should not cost the whole sync.
func (s *TokenRegistrySync) stageSnapshot(
	ctx context.Context,
	body io.Reader,
) (_ *tokenRegistryStage, retErr error) {
	compressed := &countingReader{
		reader: limitReaderPast(body, s.maxBytes),
	}
	gzipReader, err := gzip.NewReader(compressed)
	if err != nil {
		if compressed.read > s.maxBytes {
			return nil, fmt.Errorf(
				"token registry snapshot exceeds %d bytes",
				s.maxBytes,
			)
		}
		return nil, fmt.Errorf(
			"open token registry snapshot: %w",
			err,
		)
	}
	defer func() { _ = gzipReader.Close() }()

	decompressed := &countingReader{
		reader: limitReaderPast(gzipReader, s.maxDecompressedBytes),
	}
	tarReader := tar.NewReader(decompressed)
	stageFile, err := os.CreateTemp("", "dingo-token-registry-*")
	if err != nil {
		return nil, fmt.Errorf("create token registry staging file: %w", err)
	}
	stage := &tokenRegistryStage{file: stageFile}
	defer func() {
		if retErr != nil {
			if cleanupErr := stage.close(s.removeStageFile); cleanupErr != nil {
				s.logger.Warn(
					"cleaning token registry staging file failed after ingestion error",
					"error", cleanupErr,
				)
			}
		}
	}()
	var stagedBytes int64
	archiveEntries := 0
	acceptedEntries := 0
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if decompressed.read > s.maxDecompressedBytes {
				return nil, fmt.Errorf(
					"token registry snapshot exceeds %d decompressed bytes",
					s.maxDecompressedBytes,
				)
			}
			if compressed.read > s.maxBytes {
				return nil, fmt.Errorf(
					"token registry snapshot exceeds %d bytes",
					s.maxBytes,
				)
			}
			return nil, fmt.Errorf(
				"read token registry snapshot: %w",
				err,
			)
		}
		archiveEntries++
		if archiveEntries > s.maxArchiveEntries {
			return nil, fmt.Errorf(
				"token registry snapshot exceeds %d archive entries",
				s.maxArchiveEntries,
			)
		}
		if !isTokenRegistryMapping(header) {
			continue
		}
		// Counted before any per-entry filtering, so this reflects the
		// archive's shape rather than the data's usefulness.
		stage.mappings++
		if header.Size > s.maxEntryBytes {
			stage.skipped++
			s.logger.Debug(
				"token registry mapping too large",
				"name", header.Name,
				"size", header.Size,
			)
			continue
		}
		raw, err := readLimited(tarReader, s.maxEntryBytes)
		if err != nil {
			stage.skipped++
			s.logger.Debug(
				"reading token registry mapping failed",
				"name", header.Name,
				"error", err,
			)
			continue
		}
		entry, err := ParseTokenRegistryEntry(raw)
		if err != nil {
			stage.skipped++
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
		acceptedEntries++
		if acceptedEntries > s.maxAcceptedEntries {
			return nil, fmt.Errorf(
				"token registry snapshot exceeds %d accepted mappings",
				s.maxAcceptedEntries,
			)
		}
		entryBytes := tokenRegistryEntryRetainedBytes(entry)
		if entryBytes > s.maxBatchBytes {
			stage.skipped++
			s.logger.Debug(
				"token registry mapping exceeds retained batch limit",
				"name", header.Name,
				"size", entryBytes,
				"limit", s.maxBatchBytes,
			)
			continue
		}
		encoded, err := json.Marshal(newTokenRegistryStageEntry(entry))
		if err != nil {
			return nil, fmt.Errorf("stage token registry mapping: %w", err)
		}
		encoded = append(encoded, '\n')
		if int64(len(encoded)) > s.maxDecompressedBytes-stagedBytes {
			return nil, fmt.Errorf(
				"token registry staging exceeds %d bytes",
				s.maxDecompressedBytes,
			)
		}
		if _, err := stage.file.Write(encoded); err != nil {
			return nil, fmt.Errorf("write token registry staging file: %w", err)
		}
		stagedBytes += int64(len(encoded))
	}
	if _, err := io.Copy(io.Discard, decompressed); err != nil {
		return nil, fmt.Errorf(
			"read token registry snapshot: %w",
			err,
		)
	}
	if decompressed.read > s.maxDecompressedBytes {
		return nil, fmt.Errorf(
			"token registry snapshot exceeds %d decompressed bytes",
			s.maxDecompressedBytes,
		)
	}
	if compressed.read > s.maxBytes {
		return nil, fmt.Errorf(
			"token registry snapshot exceeds %d bytes",
			s.maxBytes,
		)
	}
	if stage.skipped > 0 {
		s.logger.Info(
			"token registry mappings skipped",
			"count", stage.skipped,
		)
	}
	if _, err := stage.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind token registry staging file: %w", err)
	}
	return stage, nil
}

// applyStagedSnapshot replays validated staged rows in bounded batches inside
// the caller's transaction.
func (s *TokenRegistrySync) applyStagedSnapshot(
	ctx context.Context,
	stage *tokenRegistryStage,
	syncedAt time.Time,
	txn types.Txn,
) (written int, retErr error) {
	decoder := json.NewDecoder(stage.file)
	batch := make([]models.TokenRegistryEntry, 0, tokenRegistryBatchSize)
	var batchBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		count, err := s.store.UpsertTokenRegistryEntries(
			ctx,
			batch,
			syncedAt,
			txn,
		)
		if err != nil {
			return fmt.Errorf("store token registry entries: %w", err)
		}
		written += count
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		var stagedEntry tokenRegistryStageEntry
		if err := decoder.Decode(&stagedEntry); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return 0, fmt.Errorf("read token registry staging file: %w", err)
		}
		entry := stagedEntry.model()
		entryBytes := tokenRegistryEntryRetainedBytes(&entry)
		if entryBytes > s.maxBatchBytes {
			return 0, fmt.Errorf(
				"token registry mapping %q retains %d bytes, exceeding batch limit %d",
				entry.Subject,
				entryBytes,
				s.maxBatchBytes,
			)
		}
		if len(batch) > 0 &&
			(len(batch) >= tokenRegistryBatchSize ||
				batchBytes+entryBytes > s.maxBatchBytes) {
			if err := flush(); err != nil {
				return 0, err
			}
		}
		batch = append(batch, entry)
		batchBytes += entryBytes
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return written, nil
}

// limitReaderPast permits one byte beyond limit so callers can distinguish an
// exhausted budget from a stream whose length is exactly the limit. Saturating
// at MaxInt64 avoids wrapping a caller-supplied maximum into a negative limit.
func limitReaderPast(reader io.Reader, limit int64) io.Reader {
	if limit < maxInt64 {
		limit++
	}
	return io.LimitReader(reader, limit)
}

// tokenRegistryEntryRetainedBytes counts the property payload kept alive by a
// batch. The batch's fixed struct storage is independently bounded by
// tokenRegistryBatchSize; this byte limit targets the variable-size strings
// that can otherwise turn a small entry count into a large allocation.
func tokenRegistryEntryRetainedBytes(entry *models.TokenRegistryEntry) int64 {
	return int64(
		len(entry.Subject) +
			len(entry.Name) +
			len(entry.Ticker) +
			len(entry.Description) +
			len(entry.URL) +
			len(entry.Logo),
	)
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
