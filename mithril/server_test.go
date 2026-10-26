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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	srv := newMithrilTestServer(t, cfg)
	return &serverFixture{
		srv: srv, store: store, dir: dir, artifact: artifact,
		pubKey: mithrilJSONHexKey(t, pub),
	}
}

func newMithrilTestServer(
	t *testing.T,
	cfg ServerConfig,
) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.NotFoundHandler())
	if cfg.PublicBaseURL == "" {
		cfg.PublicBaseURL = "http://" + srv.Listener.Addr().String()
	}
	srv.Config.Handler = NewServerHandler(cfg)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
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

func TestServerDetailUsesConfiguredPublicBaseURL(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{
		PublicBaseURL: "https://snapshots.example.test",
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/artifact/cardano-database/"+f.artifact.Hash,
		nil,
	)
	req.Host = "attacker.example"
	req.Header.Set("X-Forwarded-Host", "forwarded-attacker.example")
	req.Header.Set("X-Forwarded-Proto", "http")
	resp := httptest.NewRecorder()
	f.srv.Config.Handler.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var detail CardanoDatabaseSnapshot
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &detail))
	wantBase := "https://snapshots.example.test/download/" + f.artifact.Hash
	require.Len(t, detail.Digests.Locations, 1)
	assert.Equal(
		t, wantBase+"/digests.tar.zst", detail.Digests.Locations[0].URI,
	)
	require.Len(t, detail.Immutables.Locations, 1)
	assert.Equal(
		t, wantBase+"/{immutable_file_number}.tar.zst",
		detail.Immutables.Locations[0].URITemplate,
	)
	require.Len(t, detail.Ancillary.Locations, 1)
	assert.Equal(
		t, wantBase+"/ancillary.tar.zst", detail.Ancillary.Locations[0].URI,
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
	entered  chan struct{}
	release  chan struct{}
	calls    atomic.Int32
	blockAll bool
}

func (s *blockingListStore) Subdirs(
	ctx context.Context,
	_ string,
) ([]string, error) {
	if call := s.calls.Add(1); call > 1 && !s.blockAll {
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

func TestServerSignatureAdmissionIsIndependentFromArtifactDownloads(
	t *testing.T,
) {
	t.Parallel()

	aggregator := newAggregatorFixture(t, lotteryParams).aggregator
	store := &blockingListStore{
		entered:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		blockAll: true,
	}
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	handler := NewServerHandler(ServerConfig{
		Store:         store,
		Aggregator:    aggregator,
		PublicBaseURL: "http://127.0.0.1",
	})
	done := make(chan struct{}, 1)
	go func() {
		handler.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(
				http.MethodGet, "/artifact/cardano-database", nil,
			),
		)
		done <- struct{}{}
	}()
	testutil.RequireReceive(
		t, store.entered, testutil.AsyncWait, "admitted artifact request",
	)

	rec := &deadlineResponseWriter{header: make(http.Header)}
	handler.ServeHTTP(
		rec,
		httptest.NewRequest(
			http.MethodPost, "/register-signatures", strings.NewReader(`{}`),
		),
	)
	assert.Equal(t, http.StatusBadRequest, rec.status)
	assert.Empty(t, rec.Header().Get("Retry-After"))
	assert.NotEmpty(t, rec.readDeadlines)

	close(store.release)
	testutil.RequireReceive(
		t, done, testutil.AsyncWait, "admitted artifact response",
	)
}

func TestServerRejectsArtifactWorkBeyondAdmissionLimit(t *testing.T) {
	t.Parallel()

	aggregator := newAggregatorFixture(t, lotteryParams).aggregator
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
		ServerConfig{Store: store, Aggregator: aggregator}, 1, time.Minute,
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

	for _, request := range []struct {
		method string
		target string
		body   string
	}{
		{http.MethodGet, "/artifact/cardano-database", ""},
		{http.MethodGet, "/certificate-pending", ""},
		{http.MethodGet, "/artifact/mithril-stake-distributions", ""},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(
			rec,
			httptest.NewRequest(
				request.method, request.target, strings.NewReader(request.body),
			),
		)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, request.target)
		assert.Equal(t, "1", rec.Header().Get("Retry-After"), request.target)
	}

	close(store.release)
	testutil.RequireReceive(
		t, firstDone, testutil.AsyncWait, "admitted artifact request",
	)
}

func newSignatureAdmissionTestServer(
	readLimit int,
	readsPerClient int,
	verificationLimit int,
) *snapshotServer {
	return &snapshotServer{
		cfg: ServerConfig{
			Logger: slog.New(slog.DiscardHandler),
		},
		signatureReads:       make(chan struct{}, readLimit),
		signatureReadClients: newClientAdmission(readsPerClient),
		signatureRequests:    make(chan struct{}, verificationLimit),
		requestReadTimeout:   time.Minute,
		responseWriteTimeout: time.Minute,
	}
}

func createdSignatureHandler(
	handled *atomic.Int32,
) signatureRequestHandler {
	return func(
		w http.ResponseWriter,
		_ *http.Request,
		_ registerSignatureRequest,
	) {
		if handled != nil {
			handled.Add(1)
		}
		w.WriteHeader(http.StatusCreated)
	}
}

func serveSignatureRequest(
	handler http.Handler,
	remoteAddr string,
	body io.ReadCloser,
) <-chan int {
	done := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/register-signatures", nil)
		req.RemoteAddr = remoteAddr
		req.Body = body
		handler.ServeHTTP(rec, req)
		done <- rec.Code
	}()
	return done
}

func TestServerBoundsSignatureAdmissionIndependently(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	s := newSignatureAdmissionTestServer(2, 2, 1)
	handler := s.withSignatureRequest(
		func(
			w http.ResponseWriter,
			_ *http.Request,
			_ registerSignatureRequest,
		) {
			entered <- struct{}{}
			<-release
			w.WriteHeader(http.StatusCreated)
		},
	)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		handler.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(
				http.MethodPost,
				"/register-signatures",
				strings.NewReader(`{"party_id":"first"}`),
			),
		)
	}()
	testutil.RequireReceive(
		t, entered, testutil.AsyncWait, "admitted signature request",
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(
		rec,
		httptest.NewRequest(
			http.MethodPost,
			"/register-signatures",
			strings.NewReader(`{"party_id":"second"}`),
		),
	)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))

	close(release)
	testutil.RequireReceive(
		t, firstDone, testutil.AsyncWait, "released signature request",
	)
}

type incompleteSignatureBody struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (b *incompleteSignatureBody) Read([]byte) (int, error) {
	b.startedOnce.Do(func() { close(b.started) })
	<-b.release
	return 0, io.ErrUnexpectedEOF
}

func (*incompleteSignatureBody) Close() error { return nil }

type slowTrailingSignatureBody struct {
	prefix      *strings.Reader
	trailing    chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (b *slowTrailingSignatureBody) Read(data []byte) (int, error) {
	if b.prefix.Len() > 0 {
		return b.prefix.Read(data)
	}
	b.startedOnce.Do(func() { close(b.trailing) })
	<-b.release
	return 0, io.ErrUnexpectedEOF
}

func (*slowTrailingSignatureBody) Close() error { return nil }

func TestServerIncompleteSignatureBodyDoesNotHoldVerificationAdmission(
	t *testing.T,
) {
	t.Parallel()

	decodeStarted := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBody) }) }
	defer release()

	s := newSignatureAdmissionTestServer(2, 2, 1)
	handler := s.withSignatureRequest(createdSignatureHandler(nil))
	firstDone := serveSignatureRequest(
		handler,
		"192.0.2.1:1001",
		&incompleteSignatureBody{
			started: decodeStarted,
			release: releaseBody,
		},
	)
	testutil.RequireReceive(
		t, decodeStarted, testutil.AsyncWait, "incomplete body read",
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(
		rec,
		httptest.NewRequest(
			http.MethodPost,
			"/register-signatures",
			strings.NewReader(`{"party_id":"complete"}`),
		),
	)
	if rec.Code != http.StatusCreated {
		t.Fatalf(
			"complete signature body returned %d while incomplete body was decoding",
			rec.Code,
		)
	}
	assert.Empty(t, rec.Header().Get("Retry-After"))

	release()
	assert.Equal(
		t,
		http.StatusBadRequest,
		testutil.RequireReceive(
			t, firstDone, testutil.AsyncWait, "incomplete request response",
		),
	)
}

func TestServerKeepsSignatureReadAdmissionUntilBodyEOF(t *testing.T) {
	t.Parallel()

	trailingRead := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBody) }) }
	defer release()

	s := newSignatureAdmissionTestServer(1, 2, 1)
	var handled atomic.Int32
	handler := s.withSignatureRequest(createdSignatureHandler(&handled))
	firstDone := serveSignatureRequest(
		handler,
		"192.0.2.1:1001",
		&slowTrailingSignatureBody{
			prefix:   strings.NewReader(`{"party_id":"slow"}`),
			trailing: trailingRead,
			release:  releaseBody,
		},
	)
	testutil.RequireReceive(
		t, trailingRead, testutil.AsyncWait, "trailing signature body read",
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/register-signatures",
		strings.NewReader(`{"party_id":"complete"}`),
	)
	req.RemoteAddr = "198.51.100.1:1002"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Zero(t, handled.Load())

	release()
	assert.Equal(
		t,
		http.StatusBadRequest,
		testutil.RequireReceive(
			t, firstDone, testutil.AsyncWait, "slow trailing body response",
		),
	)
}

func TestServerRejectsOversizedTrailingSignatureBody(t *testing.T) {
	t.Parallel()

	s := newSignatureAdmissionTestServer(1, 1, 1)
	var handled atomic.Int32
	handler := s.withSignatureRequest(createdSignatureHandler(&handled))

	body := `{"party_id":"complete"}` +
		strings.Repeat(" ", maxAggregatorRequestBytes)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(
		rec,
		httptest.NewRequest(
			http.MethodPost,
			"/register-signatures",
			strings.NewReader(body),
		),
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Zero(t, handled.Load())
}

type observedSignatureBody struct {
	reader io.Reader
	reads  atomic.Int32
}

func (b *observedSignatureBody) Read(data []byte) (int, error) {
	b.reads.Add(1)
	return b.reader.Read(data)
}

func (*observedSignatureBody) Close() error { return nil }

func TestServerBoundsConcurrentSignatureBodyReads(t *testing.T) {
	t.Parallel()

	decodeStarted := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBody) }) }
	defer release()

	s := newSignatureAdmissionTestServer(1, 2, 1)
	handler := s.withSignatureRequest(createdSignatureHandler(nil))
	firstDone := serveSignatureRequest(
		handler,
		"192.0.2.1:1001",
		&incompleteSignatureBody{
			started: decodeStarted,
			release: releaseBody,
		},
	)
	testutil.RequireReceive(
		t, decodeStarted, testutil.AsyncWait, "first signature body read",
	)

	body := &observedSignatureBody{
		reader: strings.NewReader(`{"party_id":"complete"}`),
	}
	req := httptest.NewRequest(http.MethodPost, "/register-signatures", nil)
	req.RemoteAddr = "198.51.100.1:1002"
	req.Body = body
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Zero(t, body.reads.Load())

	release()
	assert.Equal(
		t,
		http.StatusBadRequest,
		testutil.RequireReceive(
			t, firstDone, testutil.AsyncWait, "incomplete request response",
		),
	)
}

func TestServerSignatureReadAdmissionPreservesOtherClientCapacity(
	t *testing.T,
) {
	t.Parallel()

	decodeStarted := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBody) }) }
	defer release()

	s := newSignatureAdmissionTestServer(2, 1, 1)
	handler := s.withSignatureRequest(createdSignatureHandler(nil))
	firstDone := serveSignatureRequest(
		handler,
		"192.0.2.1:1001",
		&incompleteSignatureBody{
			started: decodeStarted,
			release: releaseBody,
		},
	)
	testutil.RequireReceive(
		t, decodeStarted, testutil.AsyncWait, "first client signature body read",
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/register-signatures",
		strings.NewReader(`{"party_id":"same-client"}`),
	)
	req.RemoteAddr = "192.0.2.1:1002"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))

	req = httptest.NewRequest(
		http.MethodPost,
		"/register-signatures",
		strings.NewReader(`{"party_id":"other-client"}`),
	)
	req.RemoteAddr = "198.51.100.1:1003"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusCreated, rec.Code)

	release()
	assert.Equal(
		t,
		http.StatusBadRequest,
		testutil.RequireReceive(
			t, firstDone, testutil.AsyncWait, "incomplete request response",
		),
	)
}

func requireSlowSignatureConnectionClosed(
	t *testing.T,
	handler http.Handler,
	chunk string,
	wantStatus int,
) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = fmt.Fprintf(
		conn,
		"POST /register-signatures HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Transfer-Encoding: chunked\r\n\r\n"+
			"1\r\n%s\r\n",
		srv.Listener.Addr().String(),
		chunk,
	)
	require.NoError(t, err)

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{
		Method: http.MethodPost,
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, wantStatus, resp.StatusCode)
	assert.True(t, resp.Close)
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}

func TestServerRejectedSignatureBodyClosesSlowConnection(t *testing.T) {
	t.Parallel()

	s := newSignatureAdmissionTestServer(1, 1, 1)
	s.signatureReads <- struct{}{}
	var handled atomic.Int32
	handler := s.withSignatureRequest(createdSignatureHandler(&handled))
	requireSlowSignatureConnectionClosed(
		t, handler, "{", http.StatusServiceUnavailable,
	)
	assert.Zero(t, handled.Load())
}

func TestServerMalformedSignatureBodyClosesSlowConnection(t *testing.T) {
	t.Parallel()

	s := newSignatureAdmissionTestServer(1, 1, 1)
	var handled atomic.Int32
	handler := s.withSignatureRequest(createdSignatureHandler(&handled))
	requireSlowSignatureConnectionClosed(
		t, handler, "]", http.StatusBadRequest,
	)
	assert.Zero(t, handled.Load())
}

type deadlineResponseWriter struct {
	header        http.Header
	status        int
	deadlines     []time.Time
	written       int
	deadlineSet   chan time.Time
	readDeadlines []time.Time
	readArmed     *bool
}

func (w *deadlineResponseWriter) Header() http.Header {
	return w.header
}

func (w *deadlineResponseWriter) WriteHeader(status int) {
	w.status = status
}

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

func (w *deadlineResponseWriter) SetReadDeadline(deadline time.Time) error {
	w.readDeadlines = append(w.readDeadlines, deadline)
	if w.readArmed != nil {
		*w.readArmed = true
	}
	return nil
}

type deadlineCheckingReader struct {
	armed  *bool
	reader *strings.Reader
}

func (r *deadlineCheckingReader) Read(p []byte) (int, error) {
	if !*r.armed {
		return 0, fmt.Errorf("request body read before deadline was armed")
	}
	return r.reader.Read(p)
}

func TestServerArmsSignatureBodyDeadlineBeforeDecode(t *testing.T) {
	t.Parallel()

	f := newAggregatorFixture(t, lotteryParams)
	handler := NewServerHandler(ServerConfig{
		Store:         f.store,
		Aggregator:    f.aggregator,
		PublicBaseURL: "http://127.0.0.1",
	})
	armed := false
	w := &deadlineResponseWriter{
		header: make(http.Header), readArmed: &armed,
	}
	req := httptest.NewRequest(
		http.MethodPost, "/register-signatures", nil,
	)
	req.Body = io.NopCloser(&deadlineCheckingReader{
		armed: &armed, reader: strings.NewReader(`{}`),
	})
	// SetReadDeadline is the boundary the production wrapper must arm before
	// the handler starts decoding an untrusted signature request body.
	handler.ServeHTTP(w, req)
	assert.NotEmpty(t, w.readDeadlines)
	assert.Equal(t, http.StatusBadRequest, w.status)
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
	srv := newMithrilTestServer(t, ServerConfig{Store: store})

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

// Concurrent listing requests share one catalog scan and all receive its
// result after it completes.
func TestServerCoalescesConcurrentCatalogRequests(t *testing.T) {
	t.Parallel()

	const requests = immutableDownloadWorkers + 1
	store := &blockingListStore{
		entered:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		blockAll: true,
	}
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	handler := NewServerHandler(ServerConfig{
		Store:         store,
		PublicBaseURL: "http://127.0.0.1",
	})
	codes := make(chan int, requests)
	for range requests {
		go func() {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(
				rec,
				httptest.NewRequest(
					http.MethodGet, "/artifact/cardano-database", nil,
				),
			)
			codes <- rec.Code
		}()
	}
	testutil.RequireReceive(
		t, store.entered, testutil.AsyncWait, "single catalog scan",
	)
	close(store.release)
	for range requests {
		code := testutil.RequireReceive(
			t, codes, testutil.AsyncWait, "admitted response",
		)
		assert.Equal(t, http.StatusOK, code)
	}
	assert.Equal(t, int32(1), store.calls.Load())
}

// rangeCountingStore reports itself as remote and counts the backend range
// requests an objectReader-style reader would issue: one each time a read
// starts after the position moved.
type rangeCountingStore struct {
	ArtifactStore
	opens atomic.Int64
}

func (s *rangeCountingStore) streamsRemoteObjects() bool { return true }

func (s *rangeCountingStore) Open(
	ctx context.Context,
	key string,
) (io.ReadSeekCloser, error) {
	inner, err := s.ArtifactStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	return &rangeCountingReader{ReadSeekCloser: inner, store: s}, nil
}

type rangeCountingReader struct {
	io.ReadSeekCloser
	store   *rangeCountingStore
	started bool
}

func (r *rangeCountingReader) Read(p []byte) (int, error) {
	if !r.started {
		r.started = true
		r.store.opens.Add(1)
	}
	return r.ReadSeekCloser.Read(p)
}

func (r *rangeCountingReader) Seek(off int64, whence int) (int64, error) {
	cur, err := r.ReadSeekCloser.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	next, err := r.ReadSeekCloser.Seek(off, whence)
	if err == nil && next != cur {
		r.started = false
	}
	return next, err
}

func TestServerLimitsMultipartRangesOnRemoteStores(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	counting := &rangeCountingStore{ArtifactStore: f.store}
	srv := newMithrilTestServer(t, ServerConfig{
		Store: counting, Logger: slog.New(slog.DiscardHandler),
	})
	name := "/download/" + f.artifact.Hash + "/00001.tar.zst"
	full, err := os.ReadFile( //nolint:gosec // test temp dir
		filepath.Join(f.dir, f.artifact.Hash, "00001.tar.zst"),
	)
	require.NoError(t, err)
	require.Greater(t, len(full), 20)

	// A single range, including padded and empty list elements, is served.
	for _, h := range []string{"bytes=10-19", "bytes= 10-19 ,", "bytes=,10-19"} {
		before := counting.opens.Load()
		resp := get(t, srv.URL+name, "Range", h)
		require.Equal(t, http.StatusPartialContent, resp.StatusCode, h)
		assert.Equal(t, "application/zstd", resp.Header.Get("Content-Type"), h)
		assert.Equal(t, full[10:20], getBody(t, resp), h)
		assert.Equal(t, int64(1), counting.opens.Load()-before, h)
	}

	// More than one range is answered as the whole object from one backend
	// read, never as a multipart body.
	for _, h := range []string{
		"bytes=0-0,2-2",
		"bytes=0-0, 2-2",
		"bytes=0-9,5-14,10-19",
		"bytes=0-0" + strings.Repeat(",0-0", 50),
	} {
		before := counting.opens.Load()
		resp := get(t, srv.URL+name, "Range", h)
		require.Equal(t, http.StatusOK, resp.StatusCode, h)
		assert.Empty(t, resp.Header.Get("Content-Range"), h)
		assert.NotContains(t, resp.Header.Get("Content-Type"), "multipart", h)
		assert.Equal(t, full, getBody(t, resp), h)
		assert.Equal(t, int64(1), counting.opens.Load()-before, h)
	}

	// The metadata of a stored certificate is limited the same way.
	certHash := strings.Repeat("c", 64)
	body := `{"hash":"` + certHash + `"}`
	require.NoError(t, counting.Put(
		t.Context(), "certificates/"+certHash+".json", strings.NewReader(body),
	))
	resp := get(t, srv.URL+"/certificate/"+certHash, "Range", "bytes=0-0,2-2")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	assert.Equal(t, body, string(getBody(t, resp)))
}

func TestServerKeepsMultipartRangesOnLocalStores(t *testing.T) {
	t.Parallel()

	f := newServerFixture(t, ServerConfig{})
	resp := get(
		t, f.srv.URL+"/download/"+f.artifact.Hash+"/00001.tar.zst",
		"Range", "bytes=0-0,2-2",
	)
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "multipart/byteranges")
	_ = getBody(t, resp)
}
