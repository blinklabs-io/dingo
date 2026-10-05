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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serverFixture is a produced snapshot behind a running handler.
type serverFixture struct {
	srv      *httptest.Server
	store    ArtifactStore
	dir      string
	artifact *CardanoDatabaseSnapshot
	pubKey   string
}

func newServerFixture(t *testing.T, cfg ServerConfig) *serverFixture {
	t.Helper()
	db := newCardanoDB(t, 3)
	pub, key := newSigningKey(t)
	store, dir := newLocalStore(t)
	artifact, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.NoError(t, err)
	cfg.Store = store
	cfg.Logger = slog.New(slog.DiscardHandler)
	srv := httptest.NewServer(NewServerHandler(cfg))
	t.Cleanup(srv.Close)
	return &serverFixture{
		srv: srv, store: store, dir: dir, artifact: artifact,
		pubKey: mithrilJSONHexKey(t, pub),
	}
}

func get(t *testing.T, url string, header ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodGet, url, nil,
	)
	require.NoError(t, err)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func getBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return data
}

func TestServerListsAndDescribesSnapshots(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})

	resp := get(t, f.srv.URL+"/artifact/cardano-database")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var items []CardanoDatabaseSnapshotListItem
	require.NoError(t, json.Unmarshal(getBody(t, resp), &items))
	require.Len(t, items, 1)
	assert.Equal(t, f.artifact.Hash, items[0].Hash)
	assert.Equal(t, f.artifact.MerkleRoot, items[0].MerkleRoot)
	assert.Equal(t, f.artifact.Beacon, items[0].Beacon)

	resp = get(t, f.srv.URL+"/artifact/cardano-database/"+f.artifact.Hash)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var detail CardanoDatabaseSnapshot
	require.NoError(t, json.Unmarshal(getBody(t, resp), &detail))
	assert.Equal(t, f.artifact.ComputeHash(), detail.Hash)
	assert.Equal(t, f.artifact.MerkleRoot, detail.MerkleRoot)

	base := f.srv.URL + "/download/" + f.artifact.Hash
	require.Len(t, detail.Digests.Locations, 1)
	assert.Equal(t, locationTypeCloudStorage, detail.Digests.Locations[0].Type)
	assert.Equal(t, base+"/digests.tar.zst", detail.Digests.Locations[0].URI)
	require.Len(t, detail.Immutables.Locations, 1)
	assert.Equal(
		t,
		base+"/{immutable_file_number}.tar.zst",
		detail.Immutables.Locations[0].URITemplate,
	)
	require.Len(t, detail.Ancillary.Locations, 1)
	assert.Equal(
		t,
		base+"/ancillary.tar.zst",
		detail.Ancillary.Locations[0].URI,
	)
}

func TestServerListsNewestFirstAndSkipsIncompleteSnapshots(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	// A second, newer snapshot of a longer database.
	db := newCardanoDB(t, 4)
	_, key := newSigningKey(t)
	cfg := newSnapshotConfig(t, db, f.store, key)
	cfg.CreatedAt = snapshotCreatedAt.Add(time.Hour)
	newer, err := CreateSnapshot(context.Background(), cfg)
	require.NoError(t, err)
	// A snapshot whose metadata was never written is still being produced.
	require.NoError(t, f.store.Put(
		context.Background(),
		strings.Repeat("a", 64)+"/00000.tar.zst",
		strings.NewReader("partial"),
	))

	resp := get(t, f.srv.URL+"/artifact/cardano-database")
	var items []CardanoDatabaseSnapshotListItem
	require.NoError(t, json.Unmarshal(getBody(t, resp), &items))
	require.Len(t, items, 2)
	assert.Equal(t, newer.Hash, items[0].Hash)
	assert.Equal(t, f.artifact.Hash, items[1].Hash)
}

func TestServerRejectsUnknownAndMalformedPaths(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	hash := f.artifact.Hash
	// Objects that exist but are outside the served shape: a directory that
	// is not a hash, and a certificate under a name that is not one.
	for _, key := range []string{
		"notahash/artifact.json",
		"notahash/digests.tar.zst",
		"certificates/notahash.json",
	} {
		require.NoError(t, f.store.Put(
			context.Background(), key,
			strings.NewReader(`{"hash":"notahash"}`),
		))
	}
	for _, p := range []string{
		"/artifact/cardano-database/notahash",
		"/download/notahash/digests.tar.zst",
		"/certificate/notahash",
	} {
		resp := get(t, f.srv.URL+p)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, p)
	}
	for _, p := range []string{
		"/artifact/cardano-database/" + strings.Repeat("0", 64),
		"/artifact/cardano-database/not-a-hash",
		"/download/" + hash + "/artifact.json",
		"/download/" + hash + "/..%2f" + hash + "%2fartifact.json",
		"/download/" + hash + "/00009.tar.zst",
		"/download/" + hash + "/x.tar.zst",
		"/download/not-a-hash/digests.tar.zst",
		"/certificate/" + strings.Repeat("0", 64),
	} {
		resp := get(t, f.srv.URL+p)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, p)
	}
	// The mux redirects a dot-segment path to its cleaned form; either way it
	// must not reach the store.
	for _, p := range []string{
		"/artifact/cardano-database/..", "/certificate/..",
	} {
		resp := get(t, f.srv.URL+p)
		assert.NotEqual(t, http.StatusOK, resp.StatusCode, p)
	}
}

func TestServerServesByteRanges(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	name := "/download/" + f.artifact.Hash + "/00001.tar.zst"
	full, err := os.ReadFile( //nolint:gosec // test temp dir
		filepath.Join(f.dir, f.artifact.Hash, "00001.tar.zst"),
	)
	require.NoError(t, err)
	require.Greater(t, len(full), 20)

	resp := get(t, f.srv.URL+name)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
	assert.Equal(t, full, getBody(t, resp))

	resp = get(t, f.srv.URL+name, "Range", "bytes=10-19")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(
		t,
		"bytes 10-19/"+itoa(len(full)),
		resp.Header.Get("Content-Range"),
	)
	assert.Equal(t, full[10:20], getBody(t, resp))

	resp = get(t, f.srv.URL+name, "Range", "bytes=10-")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, full[10:], getBody(t, resp))
}

func TestServerRedirectsFilesWhenConfigured(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{
		RedirectBaseURL: "https://cdn.example.net/mithril/",
	})
	resp := get(t, f.srv.URL+"/download/"+f.artifact.Hash+"/ancillary.tar.zst")
	require.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)
	assert.Equal(
		t,
		"https://cdn.example.net/mithril/"+f.artifact.Hash+"/ancillary.tar.zst",
		resp.Header.Get("Location"),
	)
	// Metadata is always answered locally.
	resp = get(t, f.srv.URL+"/artifact/cardano-database/"+f.artifact.Hash)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServerServesStoredCertificates(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	certHash := strings.Repeat("c", 64)
	body := `{"hash":"` + certHash + `"}`
	require.NoError(t, f.store.Put(
		context.Background(),
		"certificates/"+certHash+".json",
		strings.NewReader(body),
	))
	resp := get(t, f.srv.URL+"/certificate/"+certHash)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, body, string(getBody(t, resp)))
}

type blockingListStore struct {
	ArtifactStore
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockingListStore) Subdirs(
	ctx context.Context,
	_ string,
) ([]string, error) {
	if s.calls.Add(1) > 1 {
		return nil, nil
	}
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestServerRejectsArtifactWorkBeyondAdmissionLimit(t *testing.T) {
	t.Parallel()

	store := &blockingListStore{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	handler := newServerHandler(
		ServerConfig{Store: store, Aggregator: &Aggregator{}}, 1, time.Minute,
	)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		handler.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(
				http.MethodGet, "/artifact/cardano-database", nil,
			),
		)
	}()
	testutil.RequireReceive(
		t, store.entered, testutil.AsyncWait, "first artifact request",
	)

	for _, target := range []string{
		"/artifact/cardano-database",
		"/certificate-pending",
		"/artifact/mithril-stake-distributions",
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(
			rec,
			httptest.NewRequest(http.MethodGet, target, nil),
		)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, target)
		assert.Equal(t, "1", rec.Header().Get("Retry-After"), target)
	}

	close(store.release)
	testutil.RequireReceive(
		t, firstDone, testutil.AsyncWait, "admitted artifact request",
	)
}

type deadlineResponseWriter struct {
	header      http.Header
	deadlines   []time.Time
	written     int
	deadlineSet chan time.Time
}

func (w *deadlineResponseWriter) Header() http.Header {
	return w.header
}

func (*deadlineResponseWriter) WriteHeader(int) {}

func (w *deadlineResponseWriter) Write(p []byte) (int, error) {
	w.written += len(p)
	return len(p), nil
}

func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	if w.deadlineSet != nil {
		w.deadlineSet <- deadline
	}
	return nil
}

func TestServerArmsWriteDeadlineOnlyWhenWriting(t *testing.T) {
	t.Parallel()

	store := &blockingListStore{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	handler := newServerHandler(
		ServerConfig{Store: store}, 1, time.Minute,
	)
	w := &deadlineResponseWriter{
		header: make(http.Header), deadlineSet: make(chan time.Time, 1),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(
			w,
			httptest.NewRequest(
				http.MethodGet, "/artifact/cardano-database", nil,
			),
		)
	}()
	testutil.RequireReceive(
		t, store.entered, testutil.AsyncWait, "artifact store read",
	)
	select {
	case <-w.deadlineSet:
		t.Fatal("write deadline armed before the response write")
	default:
	}

	close(store.release)
	testutil.RequireReceive(t, done, testutil.AsyncWait, "artifact response")
	deadline := testutil.RequireReceive(
		t, w.deadlineSet, testutil.AsyncWait, "response write deadline",
	)
	assert.False(t, deadline.IsZero())
}

func TestServerRefreshesWriteDeadlineDuringLargeTransfer(t *testing.T) {
	t.Parallel()

	store, _ := newLocalStore(t)
	hash := strings.Repeat("d", 64)
	body := strings.Repeat("x", 128<<10)
	require.NoError(t, store.Put(
		t.Context(), hash+"/digests.tar.zst", strings.NewReader(body),
	))
	handler := newServerHandler(
		ServerConfig{Store: store}, 1, time.Minute,
	)
	w := &deadlineResponseWriter{header: make(http.Header)}
	handler.ServeHTTP(
		w,
		httptest.NewRequest(
			http.MethodGet,
			"/download/"+hash+"/digests.tar.zst",
			nil,
		),
	)

	assert.Equal(t, len(body), w.written)
	require.GreaterOrEqual(t, len(w.deadlines), 2)
	assert.False(t, w.deadlines[0].IsZero())
	assert.False(t, w.deadlines[len(w.deadlines)-1].IsZero())
}

// TestServerSnapshotBootstrapsThroughClient runs the production download path
// against the handler: resolve the latest artifact, verify the digest merkle
// root, fetch and verify every immutable archive, and extract the ancillary.
func TestServerSnapshotBootstrapsThroughClient(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	result, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:           "preprod",
		Backend:           BackendV2,
		AggregatorURL:     f.srv.URL,
		AllowInsecureHTTP: true,
		DownloadDir:       t.TempDir(),
		Logger:            slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	assert.Equal(t, f.artifact.Hash, result.Snapshot.Digest)
	for num := range 3 {
		for _, ext := range immutableFileExtensions {
			name := itoa5(num) + "." + ext
			got, err := os.ReadFile( //nolint:gosec // test temp dir
				filepath.Join(result.ImmutableDir, name),
			)
			require.NoError(t, err)
			assert.Equal(
				t, []byte("immutable-"+name+"-data"), got, name,
			)
		}
	}
	_, err = os.Stat(
		filepath.Join(result.AncillaryDir, "ledger", "100", "state"),
	)
	require.NoError(t, err)
}

func itoa(n int) string { return strconv.Itoa(n) }

func itoa5(n int) string { return fmt.Sprintf("%05d", n) }

// newSyncableDB writes a cardano-node database directory whose immutable
// blocks and ledger state Sync can import.
func newSyncableDB(t *testing.T) string {
	t.Helper()
	db := t.TempDir()
	files, blockHash := validImmutableFiles(t, 1000)
	for name, content := range files {
		require.NoError(t, os.MkdirAll(
			filepath.Join(db, filepath.Dir(name)), 0o750,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(db, name), content, 0o640,
		))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(db, "ledger", "100"), 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(db, "ledger", "100", "state"),
		minimalLedgerState(t, 1000, blockHash),
		0o640,
	))
	return db
}

// TestServerSnapshotSyncsEndToEnd runs the whole `dingo mithril sync` pipeline
// against a produced snapshot: bootstrap from the handler, import the ledger
// state from the ancillary archive and load the immutable blocks.
func TestServerSnapshotSyncsEndToEnd(t *testing.T) {
	t.Parallel()

	db := newSyncableDB(t)
	_, key := newSigningKey(t)
	store, _ := newLocalStore(t)
	_, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.NoError(t, err)
	srv := httptest.NewServer(NewServerHandler(ServerConfig{Store: store}))
	t.Cleanup(srv.Close)

	result, err := Sync(context.Background(), SyncConfig{
		Network:           "preprod",
		DataDir:           t.TempDir(),
		StorageMode:       "core",
		Backend:           BackendV2,
		AggregatorURL:     srv.URL,
		AllowInsecureHTTP: true,
		VerifyCertChain:   false,
		CleanupAfterLoad:  false,
		Logger:            slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), result.LedgerSlot)
}
