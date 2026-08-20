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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

const (
	syncSubjectNut  = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e"
	syncSubjectDjed = "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344"
)

// fakeTokenRegistryStore records what the syncer writes without a database.
type fakeTokenRegistryStore struct {
	mu              sync.Mutex
	entries         map[string]models.TokenRegistryEntry
	syncState       map[string]string
	upserts         int
	prunes          int
	lastPruneCutoff time.Time
	upsertErr       error
	upsertFailFrom  int
	upsertHook      func()
}

func newFakeTokenRegistryStore() *fakeTokenRegistryStore {
	return &fakeTokenRegistryStore{
		entries:   make(map[string]models.TokenRegistryEntry),
		syncState: make(map[string]string),
	}
}

func (f *fakeTokenRegistryStore) UpsertTokenRegistryEntries(
	_ context.Context,
	entries []models.TokenRegistryEntry,
	syncedAt time.Time,
	_ types.Txn,
) (int, error) {
	f.mu.Lock()
	hook := f.upsertHook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.upsertErr != nil {
		return 0, f.upsertErr
	}
	f.upserts++
	if f.upsertFailFrom > 0 && f.upserts >= f.upsertFailFrom {
		return 0, errors.New("simulated store failure mid-snapshot")
	}
	for _, entry := range entries {
		entry.UpdatedAt = syncedAt
		f.entries[entry.Subject] = entry
	}
	return len(entries), nil
}

func (f *fakeTokenRegistryStore) PruneTokenRegistryEntriesBefore(
	_ context.Context,
	cutoff time.Time,
	_ types.Txn,
) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prunes++
	f.lastPruneCutoff = cutoff
	removed := 0
	for subject, entry := range f.entries {
		if entry.UpdatedAt.Before(cutoff) {
			delete(f.entries, subject)
			removed++
		}
	}
	return removed, nil
}

func (f *fakeTokenRegistryStore) GetSyncState(
	key string,
	_ types.Txn,
) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncState[key], nil
}

func (f *fakeTokenRegistryStore) SetSyncState(
	key, value string,
	_ types.Txn,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncState[key] = value
	return nil
}

func (f *fakeTokenRegistryStore) snapshot() map[string]models.TokenRegistryEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]models.TokenRegistryEntry, len(f.entries))
	maps.Copy(out, f.entries)
	return out
}

func (f *fakeTokenRegistryStore) state(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncState[key]
}

// mappingJSON builds a CIP-26 mapping document in the registry's real shape.
func mappingJSON(subject, name, ticker, logo string) string {
	parts := []string{`"subject":"` + subject + `"`}
	if name != "" {
		parts = append(
			parts,
			`"name":{"sequenceNumber":0,"value":"`+name+`","signatures":[]}`,
		)
	}
	if ticker != "" {
		parts = append(
			parts,
			`"ticker":{"sequenceNumber":0,"value":"`+ticker+`","signatures":[]}`,
		)
	}
	if logo != "" {
		parts = append(
			parts,
			`"logo":{"sequenceNumber":0,"value":"`+logo+`","signatures":[]}`,
		)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// tarballOf builds a gzipped tar in the layout codeload serves: every path is
// under a single generated top-level directory.
func tarballOf(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "cardano-token-registry-master/",
		Typeflag: tar.TypeDir,
		Mode:     0o755,
	}))
	for name, body := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     "cardano-token-registry-master/" + name,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Size:     int64(len(body)),
		}))
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

type registryServer struct {
	*httptest.Server
	mu               sync.Mutex
	requests         int
	lastIfNoneMatch  string
	etag             string
	body             []byte
	status           int
	notModifiedOnTag bool
}

func newRegistryServer(t *testing.T, body []byte) *registryServer {
	t.Helper()
	rs := &registryServer{body: body, etag: `"abc123"`, status: http.StatusOK}
	rs.Server = httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rs.mu.Lock()
			rs.requests++
			rs.lastIfNoneMatch = r.Header.Get("If-None-Match")
			etag, body, status := rs.etag, rs.body, rs.status
			conditional := rs.notModifiedOnTag
			rs.mu.Unlock()
			if etag != "" {
				w.Header().Set("ETag", etag)
			}
			if conditional && r.Header.Get("If-None-Match") == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			if status != http.StatusOK {
				w.WriteHeader(status)
				return
			}
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(body)
		}),
	)
	t.Cleanup(rs.Close)
	return rs
}

func (rs *registryServer) setBody(body []byte, etag string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.body = body
	rs.etag = etag
}

func (rs *registryServer) setStatus(status int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.status = status
}

func (rs *registryServer) requestCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.requests
}

func (rs *registryServer) ifNoneMatch() string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastIfNoneMatch
}

func newTestSync(
	t *testing.T,
	store TokenRegistryStore,
	url string,
	mutate func(*TokenRegistryConfig),
) *TokenRegistrySync {
	t.Helper()
	cfg := TokenRegistryConfig{
		Store:     store,
		SourceURL: url,
		Network:   "mainnet",
		// httptest binds loopback, which the SSRF guard blocks by default.
		AllowPrivateAddresses: true,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	sync, err := NewTokenRegistrySync(cfg)
	require.NoError(t, err)
	return sync
}

func TestTokenRegistrySyncStoresMappings(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"README.md": "not a mapping",
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(
			syncSubjectDjed,
			"Djed USD",
			"DJED",
			"",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, 2, written)
	entries := store.snapshot()
	require.Len(t, entries, 2)
	require.Equal(t, "nutcoin", entries[syncSubjectNut].Name)
	require.Equal(t, "NUT", entries[syncSubjectNut].Ticker)
	require.Equal(t, "Djed USD", entries[syncSubjectDjed].Name)
}

func TestTokenRegistrySyncPersistsAndSendsETag(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
	})
	server := newRegistryServer(t, body)
	server.notModifiedOnTag = true
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(
		t,
		`"abc123"`,
		store.state(TokenRegistrySyncStateKey),
	)

	// The registry is ~240MB for mainnet; a second sync against an unchanged
	// registry must cost one conditional request, not another full download.
	written, err = sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Zero(t, written)
	require.Equal(t, `"abc123"`, server.ifNoneMatch())
	require.Equal(t, 2, server.requestCount())
	require.Equal(t, 1, store.upserts, "304 must not write to the store")
}

func TestTokenRegistrySyncDropsLogosByDefault(t *testing.T) {
	// Logos are roughly 90% of registry bytes; storing them is opt-in.
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "iVBORw0KGgoAAAA=",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Empty(t, store.snapshot()[syncSubjectNut].Logo)
}

func TestTokenRegistrySyncStoresLogosWhenEnabled(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "iVBORw0KGgoAAAA=",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.StoreLogos = true
	})

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, "iVBORw0KGgoAAAA=", store.snapshot()[syncSubjectNut].Logo)
}

func TestTokenRegistrySyncIgnoresNonMappingFiles(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"README.md":          "hello",
		"mappings/notes.txt": "not json",
		"nix/default.nix":    "{}",
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"",
			"",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, 1, written)
}

func TestTokenRegistrySyncSkipsMalformedMappings(t *testing.T) {
	// One unparsable file in a 7,900-file registry must not cost the sync.
	body := tarballOf(t, map[string]string{
		"mappings/broken.json":    `{"subject": `,
		"mappings/nosubject.json": `{"name":{"value":"x"}}`,
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"",
			"",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, store.snapshot(), 1)
}

func TestTokenRegistrySyncSkipsSubjectOnlyMappings(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": `{"subject":"` + syncSubjectNut + `"}`,
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Zero(t, written)
}

func TestTokenRegistrySyncRejectsOversizedBody(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"",
			"",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.MaxBytes = 8
	})

	_, err := sync.SyncOnce(t.Context())

	require.Error(t, err)
	require.Empty(t, store.state(TokenRegistrySyncStateKey),
		"a failed sync must not record the ETag")
}

func TestTokenRegistrySyncSkipsOversizedMapping(t *testing.T) {
	huge := strings.Repeat("A", 4096)
	body := tarballOf(t, map[string]string{
		"mappings/huge.json": mappingJSON(
			syncSubjectDjed,
			huge,
			"",
			"",
		),
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"",
			"",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.MaxEntryBytes = 1024
	})

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.NotContains(t, store.snapshot(), syncSubjectDjed)
}

func TestTokenRegistrySyncErrorsOnBadStatus(t *testing.T) {
	server := newRegistryServer(t, nil)
	server.status = http.StatusInternalServerError
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
	require.Empty(t, store.state(TokenRegistrySyncStateKey))
}

func TestTokenRegistrySyncErrorsOnNonGzipBody(t *testing.T) {
	server := newRegistryServer(t, []byte("this is not a gzip stream"))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.Error(t, err)
}

func TestTokenRegistrySyncBlocksPrivateAddressesByDefault(t *testing.T) {
	// The SSRF guard the per-URL fetcher uses applies here too: a registry
	// source URL is operator-supplied, and must not become a way to make the
	// node fetch from its own network.
	server := newRegistryServer(t, nil)
	store := newFakeTokenRegistryStore()
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:     store,
		SourceURL: server.URL,
		Network:   "mainnet",
	})
	require.NoError(t, err)

	_, err = sync.SyncOnce(t.Context())

	require.Error(t, err)
}

func TestNewTokenRegistrySyncRequiresStore(t *testing.T) {
	_, err := NewTokenRegistrySync(TokenRegistryConfig{Network: "mainnet"})

	require.Error(t, err)
}

func TestNewTokenRegistrySyncResolvesSourceByNetwork(t *testing.T) {
	for network, want := range map[string]string{
		"mainnet": "cardano-foundation/cardano-token-registry",
		"preprod": "input-output-hk/metadata-registry-testnet",
		"preview": "input-output-hk/metadata-registry-testnet",
		"devnet":  "input-output-hk/metadata-registry-testnet",
	} {
		t.Run(network, func(t *testing.T) {
			sync, err := NewTokenRegistrySync(TokenRegistryConfig{
				Store:   newFakeTokenRegistryStore(),
				Network: network,
			})
			require.NoError(t, err)
			require.Contains(t, sync.SourceURL(), want)
		})
	}
}

func TestNewTokenRegistrySyncPrefersExplicitSourceURL(t *testing.T) {
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:     newFakeTokenRegistryStore(),
		Network:   "mainnet",
		SourceURL: "https://mirror.example.test/registry.tar.gz",
	})

	require.NoError(t, err)
	require.Equal(
		t,
		"https://mirror.example.test/registry.tar.gz",
		sync.SourceURL(),
	)
}

func TestTokenRegistrySyncStartStop(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"",
			"",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.Interval = time.Hour
	})

	require.NoError(t, sync.Start(t.Context()))
	require.Error(t, sync.Start(t.Context()), "double Start must be refused")

	require.Eventually(t, func() bool {
		return len(store.snapshot()) == 1
	}, 5*time.Second, 10*time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, sync.Stop(stopCtx))
	require.NoError(t, sync.Stop(stopCtx), "Stop must be idempotent")
}

// TestNewTokenRegistrySyncBoundsCallerSuppliedClient covers a caller-supplied
// HTTP client with no Timeout of its own: secureHTTPClient does not set one,
// so without an explicit bound here a stalled source could hold SyncOnce open
// past the configured whole-download budget indefinitely.
func TestNewTokenRegistrySyncBoundsCallerSuppliedClient(t *testing.T) {
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:          newFakeTokenRegistryStore(),
		Network:        "mainnet",
		HTTPClient:     &http.Client{},
		RequestTimeout: 42 * time.Second,
	})

	require.NoError(t, err)
	require.Equal(t, 42*time.Second, sync.client.Timeout)
}

func TestNewTokenRegistrySyncKeepsStricterCallerTimeout(t *testing.T) {
	// A caller who asked for something tighter than the configured budget
	// meant it; do not loosen it.
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:          newFakeTokenRegistryStore(),
		Network:        "mainnet",
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		RequestTimeout: 42 * time.Second,
	})

	require.NoError(t, err)
	require.Equal(t, 5*time.Second, sync.client.Timeout)
}

func TestNewTokenRegistrySyncBoundsDefaultClient(t *testing.T) {
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:          newFakeTokenRegistryStore(),
		Network:        "mainnet",
		RequestTimeout: 42 * time.Second,
	})

	require.NoError(t, err)
	require.Equal(t, 42*time.Second, sync.client.Timeout)
}

// TestTokenRegistrySyncStopWaitsForWorkerAfterContextExpiry is the shutdown
// safety property: node_shutdown.go closes the metadata store immediately
// after Stop returns, so a Stop that gives up on an expired context would
// leave the worker free to use a closed store. Stop must therefore keep
// waiting for the worker to exit even once its context is done.
func TestTokenRegistrySyncStopWaitsForWorkerAfterContextExpiry(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "", "",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	// Block the worker inside a store write, where cancelling its context
	// does not release it. This is the case a context-bounded Stop would
	// return early on.
	release := make(chan struct{})
	entered := make(chan struct{})
	store.upsertHook = func() {
		close(entered)
		<-release
	}
	sync := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.Interval = time.Hour
	})
	require.NoError(t, sync.Start(t.Context()))
	testutil.RequireReceive(
		t,
		entered,
		5*time.Second,
		"worker did not reach the store write",
	)

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- sync.Stop(expired) }()

	select {
	case <-stopped:
		t.Fatal("Stop returned while the worker was still running")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the worker drained")
	}
	// The worker goroutine has actually exited, not merely been signaled.
	select {
	case <-sync.done:
	default:
		t.Fatal("worker goroutine still running after Stop returned")
	}
}

// TestTokenRegistrySyncRetiresSubjectsDroppedUpstream is the reconciliation
// property: an upsert-only sync would serve a delisted token's metadata
// forever, because nothing in a later snapshot mentions it.
func TestTokenRegistrySyncRetiresSubjectsDroppedUpstream(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(
			syncSubjectDjed,
			"Djed USD",
			"DJED",
			"",
		),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 2)

	// The next snapshot no longer carries DJED.
	server.setBody(tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
	}), `"etag2"`)

	_, err = sync.SyncOnce(t.Context())

	require.NoError(t, err)
	entries := store.snapshot()
	require.Contains(t, entries, syncSubjectNut)
	require.NotContains(t, entries, syncSubjectDjed)
}

// TestTokenRegistrySyncRetiresSubjectsThatLoseAllProperties covers the other
// way a subject stops being useful: it stays in the archive but its properties
// are removed, so the parser yields an empty entry the sync skips. Skipping
// alone would leave the old row serving stale metadata.
func TestTokenRegistrySyncRetiresSubjectsThatLoseAllProperties(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 1)

	server.setBody(tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": `{"subject":"` + syncSubjectNut + `"}`,
	}), `"etag2"`)

	_, err = sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.NotContains(t, store.snapshot(), syncSubjectNut)
}

// TestTokenRegistrySyncDoesNotPruneOnFailedSnapshot is the safety half:
// pruning against a snapshot that never finished would delete live subjects
// the failed run simply had not reached yet.
func TestTokenRegistrySyncDoesNotPruneOnFailedSnapshot(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 1)
	// The first, successful sync legitimately reconciles; only the failed one
	// that follows must not.
	prunesAfterSuccess := store.prunes

	server.setStatus(http.StatusInternalServerError)

	_, err = sync.SyncOnce(t.Context())

	require.Error(t, err)
	require.Equal(
		t,
		prunesAfterSuccess,
		store.prunes,
		"a failed snapshot must not prune",
	)
	require.Contains(t, store.snapshot(), syncSubjectNut)
}

// TestTokenRegistrySyncDoesNotPruneOnNotModified: a 304 applies no snapshot,
// so there is nothing to reconcile against.
func TestTokenRegistrySyncDoesNotPruneOnNotModified(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut,
			"nutcoin",
			"NUT",
			"",
		),
	}))
	server.notModifiedOnTag = true
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	prunesAfterFirst := store.prunes

	_, err = sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, prunesAfterFirst, store.prunes)
	require.Contains(t, store.snapshot(), syncSubjectNut)
}

// TestTokenRegistrySyncClearsLogosWhenDisabled documents that the logo column
// needs no special handling: the upsert overwrites every property column, so
// turning storeLogos off clears previously stored logos on the next snapshot.
func TestTokenRegistrySyncClearsLogosWhenDisabled(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "iVBORw0KGgoAAAA=",
		),
	})
	server := newRegistryServer(t, body)
	store := newFakeTokenRegistryStore()
	withLogos := newTestSync(
		t,
		store,
		server.URL,
		func(c *TokenRegistryConfig) {
			c.StoreLogos = true
		},
	)
	_, err := withLogos.SyncOnce(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, store.snapshot()[syncSubjectNut].Logo)

	// Operator restarts with logo storage turned off.
	withoutLogos := newTestSync(t, store, server.URL, nil)
	// second sync must land in a later second than the first
	withoutLogos.now = func() time.Time {
		return time.Date(2026, 8, 20, 12, 1, 0, 0, time.UTC)
	}
	_, err = withoutLogos.SyncOnce(t.Context())

	require.NoError(t, err)
	// Assert presence first: indexing a map with a missing key yields the
	// zero-value entry, whose Logo is also empty, so an Empty check alone
	// would pass if the subject had been deleted outright.
	entries := store.snapshot()
	require.Contains(t, entries, syncSubjectNut)
	require.Empty(t, entries[syncSubjectNut].Logo)
}

// manySubjects builds n distinct, valid registry subjects.
func manySubjects(n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		// 56 hex characters: a policy ID with the index encoded in the tail.
		out = append(out, fmt.Sprintf("%048x%08x", i, i))
	}
	return out
}

// TestTokenRegistrySyncFailureAfterBatchFlushPreservesServedEntries covers a
// snapshot that fails *after* at least one batch has already been written,
// which the HTTP-error test does not reach.
//
// Snapshot application is deliberately not atomic -- see the syncer's
// applySnapshot doc comment -- so earlier batches do persist. What must hold
// is that the failure costs nothing already being served: no entry is lost,
// no properties are cleared, nothing is pruned, and the ETag is not advanced,
// so the next run re-applies the snapshot in full.
func TestTokenRegistrySyncFailureAfterBatchFlushPreservesServedEntries(
	t *testing.T,
) {
	// A subject already served from an earlier, successful snapshot.
	established := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(
			syncSubjectDjed, "Djed USD", "DJED", "",
		),
	})
	server := newRegistryServer(t, established)
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)
	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, "DJED", store.snapshot()[syncSubjectDjed].Ticker)
	prunesBefore := store.prunes
	etagBefore := store.state(TokenRegistrySyncStateKey)

	// The next snapshot spans more than one batch, and the store fails on
	// the second write -- after the first has already been committed.
	files := map[string]string{}
	for i, subject := range manySubjects(tokenRegistryBatchSize + 50) {
		files["mappings/"+subject+".json"] = mappingJSON(
			subject,
			fmt.Sprintf("token %d", i),
			"",
			"",
		)
	}
	server.setBody(tarballOf(t, files), `"etag2"`)
	store.mu.Lock()
	store.upsertFailFrom = store.upserts + 2
	store.mu.Unlock()

	_, err = sync.SyncOnce(t.Context())

	require.Error(t, err)
	entries := store.snapshot()
	require.Contains(
		t,
		entries,
		syncSubjectDjed,
		"a mid-snapshot failure must not drop an already-served subject",
	)
	require.Equal(
		t,
		"DJED",
		entries[syncSubjectDjed].Ticker,
		"an already-served subject must keep its properties",
	)
	require.Equal(
		t,
		prunesBefore,
		store.prunes,
		"a partial snapshot must not prune",
	)
	require.Equal(
		t,
		etagBefore,
		store.state(TokenRegistrySyncStateKey),
		"a partial snapshot must not advance the ETag, so the next run retries it in full",
	)
}

// TestTokenRegistrySyncRecoversAfterPartialSnapshot completes the story: the
// run following a partial snapshot applies the whole thing and reconciles, so
// the partial state is transient rather than sticky.
func TestTokenRegistrySyncRecoversAfterPartialSnapshot(t *testing.T) {
	subjects := manySubjects(tokenRegistryBatchSize + 50)
	files := map[string]string{}
	for i, subject := range subjects {
		files["mappings/"+subject+".json"] = mappingJSON(
			subject,
			fmt.Sprintf("token %d", i),
			"",
			"",
		)
	}
	server := newRegistryServer(t, tarballOf(t, files))
	store := newFakeTokenRegistryStore()
	store.upsertFailFrom = 2
	sync := newTestSync(t, store, server.URL, nil)

	_, err := sync.SyncOnce(t.Context())
	require.Error(t, err)
	require.Len(t, store.snapshot(), tokenRegistryBatchSize)

	// Clear the fault; the retry sees the same registry.
	store.mu.Lock()
	store.upsertFailFrom = 0
	store.mu.Unlock()

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, len(subjects), written)
	require.Len(t, store.snapshot(), len(subjects))
}

// TestTokenRegistrySyncDoesNotPruneEmptySnapshot guards the worst failure
// this design can produce. A 200 carrying a well-formed archive with no
// usable mappings -- an upstream layout change that moves or renames
// mappings/, a truncated-but-valid artifact, a mirror serving the wrong
// repository -- would otherwise reconcile the whole table to nothing and
// record the ETag, leaving the node serving no registry metadata at all
// until the archive changes again.
func TestTokenRegistrySyncDoesNotPruneEmptySnapshot(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)
	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 1)
	prunesBefore := store.prunes
	etagBefore := store.state(TokenRegistrySyncStateKey)

	// Same repository, but nothing the sync recognizes as a mapping.
	server.setBody(tarballOf(t, map[string]string{
		"README.md":           "the layout changed",
		"registry/moved.json": mappingJSON(syncSubjectDjed, "Djed", "", ""),
	}), `"etag-empty"`)

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Zero(t, written)
	require.Equal(
		t,
		prunesBefore,
		store.prunes,
		"an empty snapshot must not reconcile the table to nothing",
	)
	require.Contains(t, store.snapshot(), syncSubjectNut)
	require.Equal(
		t,
		etagBefore,
		store.state(TokenRegistrySyncStateKey),
		"an empty snapshot must not be recorded, so the next run retries it",
	)
}

// TestTokenRegistrySyncEmptyFirstSnapshotIsNotAnError covers a node whose
// very first sync sees nothing usable: there is nothing to protect, so this
// is a quiet no-op rather than a failure.
func TestTokenRegistrySyncEmptyFirstSnapshotIsNotAnError(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"README.md": "no mappings here",
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Zero(t, written)
	require.Empty(t, store.snapshot())
}

// TestTokenRegistrySyncStampsWholeSeconds pins the stamp the prune compares
// against to second resolution. MySQL's `datetime` column carries no
// fractional seconds, so a sub-second stamp would be stored rounded while the
// prune compared against the unrounded value -- deleting the very snapshot
// just written. Truncating at the source keeps written value and cutoff
// identical on every backend.
func TestTokenRegistrySyncStampsWholeSeconds(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)
	sync.now = func() time.Time {
		return time.Date(2026, 8, 20, 12, 0, 0, 123456789, time.UTC)
	}

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	stamp := store.snapshot()[syncSubjectNut].UpdatedAt
	require.Equal(t, stamp.Truncate(time.Second), stamp)
	require.Equal(t, store.lastPruneCutoff, stamp)
}

// TestTokenRegistrySyncIgnoresETagFromADifferentSource covers an operator
// repointing sourceUrl, or a network change. The stored validator belongs to
// the old source; sending it to a new one risks accepting a 304 that means
// "your copy of the *other* registry is current", leaving this node serving
// the wrong network's metadata forever.
func TestTokenRegistrySyncIgnoresETagFromADifferentSource(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	})
	first := newRegistryServer(t, body)
	first.notModifiedOnTag = true
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, first.URL, nil)
	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)

	// A different source that happens to use the same validator value.
	second := newRegistryServer(t, body)
	second.notModifiedOnTag = true
	moved := newTestSync(t, store, second.URL, nil)

	written, err := moved.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(
		t,
		1,
		written,
		"a new source must be fetched in full, not short-circuited by the previous source's ETag",
	)
	require.Empty(
		t,
		second.ifNoneMatch(),
		"the previous source's ETag must not be sent to a new source",
	)
}

// TestTokenRegistrySyncDefersPruneWhenMappingsSkipped protects metadata that
// is already being served from a transient parse problem. A mapping that was
// valid yesterday and is malformed or oversized today is skipped, so it is
// not re-stamped -- and an unconditional prune would then delete the good row
// it still has.
func TestTokenRegistrySyncDefersPruneWhenMappingsSkipped(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json":  mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(syncSubjectDjed, "Djed USD", "DJED", ""),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)
	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 2)
	prunesBefore := store.prunes

	// DJED's mapping goes bad; NUT's is unchanged.
	server.setBody(tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json":  mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
		"mappings/" + syncSubjectDjed + ".json": `{"subject": `,
	}), `"etag2"`)

	_, err = sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(
		t,
		prunesBefore,
		store.prunes,
		"a snapshot with skipped mappings must not reconcile",
	)
	require.Contains(
		t,
		store.snapshot(),
		syncSubjectDjed,
		"a subject whose mapping failed to parse must keep its stored metadata",
	)
}

// TestTokenRegistrySyncKeysETagByLogoMode covers flipping storeLogos on an
// otherwise unchanged registry. The stored validator would otherwise produce
// a 304, applySnapshot would never run, and the new logo setting would never
// take effect: enabling would never backfill logos, and disabling would leave
// them stored indefinitely.
func TestTokenRegistrySyncKeysETagByLogoMode(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "iVBORw0KGgoAAAA=",
		),
	})
	server := newRegistryServer(t, body)
	server.notModifiedOnTag = true
	store := newFakeTokenRegistryStore()

	withLogos := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.StoreLogos = true
	})
	_, err := withLogos.SyncOnce(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, store.snapshot()[syncSubjectNut].Logo)

	// Same source, same registry content, logos now off.
	withoutLogos := newTestSync(t, store, server.URL, nil)
	withoutLogos.now = func() time.Time {
		return time.Date(2026, 8, 20, 12, 5, 0, 0, time.UTC)
	}

	written, err := withoutLogos.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(
		t,
		1,
		written,
		"a logo-mode change must force a full fetch, not be short-circuited by a 304",
	)
	entries := store.snapshot()
	require.Contains(t, entries, syncSubjectNut)
	require.Empty(t, entries[syncSubjectNut].Logo)
}

// TestTokenRegistrySyncStampsAreStrictlyIncreasing keeps reconciliation
// correct when two snapshots land inside the same wall-clock second, which a
// sub-second configured interval makes reachable. Equal stamps would make the
// prune's `updated_at < cutoff` preserve subjects the newer snapshot dropped.
func TestTokenRegistrySyncStampsAreStrictlyIncreasing(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json":  mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(syncSubjectDjed, "Djed USD", "DJED", ""),
	}))
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)
	// A clock frozen inside one second: both snapshots would share a stamp.
	frozen := time.Date(2026, 8, 20, 12, 0, 0, 250000000, time.UTC)
	sync.now = func() time.Time { return frozen }

	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 2)
	first := store.snapshot()[syncSubjectNut].UpdatedAt

	server.setBody(tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
	}), `"etag2"`)

	_, err = sync.SyncOnce(t.Context())

	require.NoError(t, err)
	second := store.snapshot()[syncSubjectNut].UpdatedAt
	require.True(
		t,
		second.After(first),
		"a later snapshot must carry a strictly greater stamp (%s vs %s)",
		second, first,
	)
	require.NotContains(
		t,
		store.snapshot(),
		syncSubjectDjed,
		"reconciliation must still retire a dropped subject within the same second",
	)
}

// TestTokenRegistrySyncSerializesConcurrentCalls covers two SyncOnce calls
// overlapping. Interleaved snapshots let an older one finish last and
// overwrite the newer one's properties, reintroduce subjects the newer
// registry dropped, and record its own stale ETag as current.
//
// SyncOnce is exported, and the previous change already conceded that it can
// run alongside the worker loop by guarding the stamp sequence; serializing
// the whole operation is what that concession actually requires.
func TestTokenRegistrySyncSerializesConcurrentCalls(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	}))
	store := newFakeTokenRegistryStore()
	// Park the first call inside its store write and hold it there.
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var once sync.Once
	store.upsertHook = func() {
		once.Do(func() {
			entered <- struct{}{}
			<-release
		})
	}
	sync := newTestSync(t, store, server.URL, nil)

	firstDone := make(chan error, 1)
	go func() { _, err := sync.SyncOnce(t.Context()); firstDone <- err }()
	testutil.RequireReceive(
		t,
		entered,
		5*time.Second,
		"first sync did not reach the store write",
	)
	requestsDuringFirst := server.requestCount()

	secondDone := make(chan error, 1)
	go func() { _, err := sync.SyncOnce(t.Context()); secondDone <- err }()

	// The second call must not begin fetching while the first holds the sync.
	select {
	case <-secondDone:
		t.Fatal("second SyncOnce completed while the first was still running")
	case <-time.After(150 * time.Millisecond):
	}
	require.Equal(
		t,
		requestsDuringFirst,
		server.requestCount(),
		"an overlapping SyncOnce must not issue its own fetch until the first finishes",
	)

	close(release)
	require.NoError(t, testutil.RequireReceive(
		t, firstDone, 5*time.Second, "first sync did not finish",
	))
	require.NoError(t, testutil.RequireReceive(
		t, secondDone, 5*time.Second, "second sync did not finish",
	))
	require.Contains(t, store.snapshot(), syncSubjectNut)
}

// TestTokenRegistrySyncStopNotBlockedByExternalSync covers a regression the
// serialization in the previous round introduced. An external SyncOnce call
// with its own uncancelled context holds the sync slot; the worker then
// blocks waiting for it. If that wait is not cancellation-aware, Stop -- which
// deliberately waits for the worker to exit -- is held for as long as the
// external sync runs, up to the whole-download timeout.
func TestTokenRegistrySyncStopNotBlockedByExternalSync(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	}))
	store := newFakeTokenRegistryStore()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var once sync.Once
	store.upsertHook = func() {
		once.Do(func() {
			entered <- struct{}{}
			<-release
		})
	}
	registrySync := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.Interval = time.Hour
	})

	// An external caller, with a context the node's shutdown cannot cancel.
	external := make(chan error, 1)
	go func() {
		_, err := registrySync.SyncOnce(context.Background())
		external <- err
	}()
	testutil.RequireReceive(
		t,
		entered,
		5*time.Second,
		"external sync did not reach the store write",
	)

	// The worker starts and immediately queues behind the external call.
	require.NoError(t, registrySync.Start(t.Context()))

	stopped := make(chan error, 1)
	go func() { stopped <- registrySync.Stop(context.Background()) }()

	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Stop blocked behind an external SyncOnce")
	}

	close(release)
	require.NoError(t, testutil.RequireReceive(
		t, external, 5*time.Second, "external sync did not finish",
	))
}

// TestNewTokenRegistrySyncClampsInterval keeps the configured interval at a
// sane floor. A sub-second interval would hammer a source that serves a
// roughly 240MB artifact, and it is also what makes same-second snapshots --
// and therefore stamps forced ahead of wall clock -- reachable at all.
func TestNewTokenRegistrySyncClampsInterval(t *testing.T) {
	for name, configured := range map[string]time.Duration{
		"sub-second": 500 * time.Millisecond,
		"one second": time.Second,
		"negative":   -time.Hour,
	} {
		t.Run(name, func(t *testing.T) {
			sync, err := NewTokenRegistrySync(TokenRegistryConfig{
				Store:    newFakeTokenRegistryStore(),
				Network:  "mainnet",
				Interval: configured,
			})

			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				sync.interval,
				minTokenRegistryInterval,
				"interval must be clamped to the floor",
			)
		})
	}
}

func TestNewTokenRegistrySyncKeepsIntervalAboveFloor(t *testing.T) {
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:    newFakeTokenRegistryStore(),
		Network:  "mainnet",
		Interval: 90 * time.Minute,
	})

	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, sync.interval)
}

// TestTokenRegistrySyncForcesFullApplyWhenSwitchingBack covers a source the
// node used before, moved away from, and later returned to. Per-source ETag
// keys are not enough: the old source's validator is still stored, but the
// table now holds the *other* source's snapshot, so a 304 would skip
// reconciliation and leave the intervening source's metadata in place.
func TestTokenRegistrySyncForcesFullApplyWhenSwitchingBack(t *testing.T) {
	bodyA := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "from source A", "AAA", "",
		),
	})
	bodyB := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(
			syncSubjectDjed, "from source B", "BBB", "",
		),
	})
	sourceA := newRegistryServer(t, bodyA)
	sourceA.notModifiedOnTag = true
	sourceB := newRegistryServer(t, bodyB)
	sourceB.etag = `"b-etag"`
	store := newFakeTokenRegistryStore()
	clock := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	// 1. Sync source A.
	syncA := newTestSync(t, store, sourceA.URL, nil)
	syncA.now = func() time.Time { return clock }
	_, err := syncA.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Contains(t, store.snapshot(), syncSubjectNut)

	// 2. Operator repoints at source B; its snapshot replaces A's.
	syncB := newTestSync(t, store, sourceB.URL, nil)
	syncB.now = func() time.Time { return clock.Add(time.Minute) }
	_, err = syncB.SyncOnce(t.Context())
	require.NoError(t, err)
	entries := store.snapshot()
	require.Contains(t, entries, syncSubjectDjed)
	require.NotContains(t, entries, syncSubjectNut)

	// 3. Operator repoints back at source A, whose content never changed.
	syncBack := newTestSync(t, store, sourceA.URL, nil)
	syncBack.now = func() time.Time { return clock.Add(2 * time.Minute) }
	written, err := syncBack.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(
		t,
		1,
		written,
		"returning to a source must re-apply it in full, not accept a 304 for a snapshot the table no longer holds",
	)
	final := store.snapshot()
	require.Contains(t, final, syncSubjectNut)
	require.NotContains(
		t,
		final,
		syncSubjectDjed,
		"the intervening source's metadata must not survive",
	)
}

// TestTokenRegistrySyncForcesFullApplyWhenLogoModeReturns is the same hazard
// for the logo-storage mode: on, off, then on again against an unchanged
// registry must actually restore logos.
func TestTokenRegistrySyncForcesFullApplyWhenLogoModeReturns(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "iVBORw0KGgoAAAA=",
		),
	})
	server := newRegistryServer(t, body)
	server.notModifiedOnTag = true
	store := newFakeTokenRegistryStore()
	clock := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	withLogos := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.StoreLogos = true
	})
	withLogos.now = func() time.Time { return clock }
	_, err := withLogos.SyncOnce(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, store.snapshot()[syncSubjectNut].Logo)

	withoutLogos := newTestSync(t, store, server.URL, nil)
	withoutLogos.now = func() time.Time { return clock.Add(time.Minute) }
	_, err = withoutLogos.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Empty(t, store.snapshot()[syncSubjectNut].Logo)

	// Back on, same registry bytes.
	logosAgain := newTestSync(t, store, server.URL, func(c *TokenRegistryConfig) {
		c.StoreLogos = true
	})
	logosAgain.now = func() time.Time { return clock.Add(2 * time.Minute) }

	_, err = logosAgain.SyncOnce(t.Context())

	require.NoError(t, err)
	require.NotEmpty(
		t,
		store.snapshot()[syncSubjectNut].Logo,
		"re-enabling logos must re-apply the snapshot rather than accept a 304",
	)
}

// TestTokenRegistrySyncStampSurvivesRestart covers a process or lifecycle
// restart landing in the same wall-clock second as the previous snapshot. The
// stamp sequence lives in memory, so a restart resets it; a fresh snapshot
// could then reuse the previous stamp and the prune's `updated_at < cutoff`
// would spare exactly the subjects the new snapshot dropped.
func TestTokenRegistrySyncStampSurvivesRestart(t *testing.T) {
	server := newRegistryServer(t, tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json":  mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(syncSubjectDjed, "Djed USD", "DJED", ""),
	}))
	store := newFakeTokenRegistryStore()
	// One frozen instant shared by both runs: the restart lands inside the
	// same second as the sync before it.
	frozen := time.Date(2026, 8, 20, 12, 0, 0, 400000000, time.UTC)

	before := newTestSync(t, store, server.URL, nil)
	before.now = func() time.Time { return frozen }
	_, err := before.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, store.snapshot(), 2)

	// Restart: a brand-new instance over the same store, no in-memory state.
	server.setBody(tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
	}), `"etag2"`)
	afterRestart := newTestSync(t, store, server.URL, nil)
	afterRestart.now = func() time.Time { return frozen }

	_, err = afterRestart.SyncOnce(t.Context())

	require.NoError(t, err)
	require.NotContains(
		t,
		store.snapshot(),
		syncSubjectDjed,
		"a restart within the same second must still retire a dropped subject",
	)
	require.Contains(t, store.snapshot(), syncSubjectNut)
}
