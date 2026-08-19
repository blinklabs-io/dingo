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
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

const (
	syncSubjectNut  = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e"
	syncSubjectDjed = "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344"
)

// fakeTokenRegistryStore records what the syncer writes without a database.
type fakeTokenRegistryStore struct {
	mu        sync.Mutex
	entries   map[string]models.TokenRegistryEntry
	syncState map[string]string
	upserts   int
	upsertErr error
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
	_ types.Txn,
) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.upsertErr != nil {
		return 0, f.upsertErr
	}
	f.upserts++
	for _, entry := range entries {
		f.entries[entry.Subject] = entry
	}
	return len(entries), nil
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
		"README.md":                             "not a mapping",
		"mappings/" + syncSubjectNut + ".json":  mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
		"mappings/" + syncSubjectDjed + ".json": mappingJSON(syncSubjectDjed, "Djed USD", "DJED", ""),
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
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "NUT", ""),
	})
	server := newRegistryServer(t, body)
	server.notModifiedOnTag = true
	store := newFakeTokenRegistryStore()
	sync := newTestSync(t, store, server.URL, nil)

	written, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, `"abc123"`, store.state(TokenRegistrySyncStateKey))

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
		"README.md":                            "hello",
		"mappings/notes.txt":                   "not json",
		"nix/default.nix":                      "{}",
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "", ""),
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
		"mappings/broken.json":                 `{"subject": `,
		"mappings/nosubject.json":              `{"name":{"value":"x"}}`,
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "", ""),
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
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "", ""),
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
		"mappings/huge.json":                   mappingJSON(syncSubjectDjed, huge, "", ""),
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "", ""),
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
	require.Equal(t, "https://mirror.example.test/registry.tar.gz", sync.SourceURL())
}

func TestTokenRegistrySyncStartStop(t *testing.T) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(syncSubjectNut, "nutcoin", "", ""),
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
