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

package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/internal/config"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func TestClassifyRemoteImmutableSource(t *testing.T) {
	t.Parallel()
	for source, want := range map[string]bool{
		"https://cdn.example/immutable/":  true,
		"http://127.0.0.1:8080/immutable": true,
		"/var/lib/cardano/immutable":      false,
		"database/immutable/testdata":     false,
		`C:\cardano\immutable`:            false,
		"ftp://cdn.example/immutable":     false,
		"https:///immutable":              false,
	} {
		got, err := classifyRemoteImmutableSource(source)
		if source == "https:///immutable" {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err, source)
		require.Equal(t, want, got, source)
	}
}

func TestClassifyRemoteImmutableSourceRejectsInvalidOrPlaintextRemoteHost(
	t *testing.T,
) {
	t.Parallel()
	for _, source := range []string{
		"http://cdn.example/immutable",
		"http://cdn.example/%zz",
		"http:cdn.example/immutable",
	} {
		_, err := classifyRemoteImmutableSource(source)
		require.Error(t, err, source)
	}
	for _, source := range []string{
		"https://cdn.example/immutable",
		"http://localhost:8080/immutable",
		"http://127.0.0.1:8080/immutable",
		"/var/lib/cardano/immutable",
	} {
		_, err := classifyRemoteImmutableSource(source)
		require.NoError(t, err, source)
	}
}

func TestRemoteImmutableRedirectRejectsHTTPSDowngrade(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(
		http.MethodGet,
		"http://127.0.0.1:8081/immutable/00000.chunk",
		nil,
	)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		checkRemoteImmutableRedirect(req, []*http.Request{
			{URL: &url.URL{Scheme: "http", Host: "localhost:8080"}},
			{URL: &url.URL{Scheme: "https", Host: "cdn.example"}},
		}),
		"requires HTTPS",
	)
	req.URL.Scheme = "https"
	require.NoError(t, checkRemoteImmutableRedirect(req, nil))
}

func TestRemoteImmutableRedirectPreservesLimitAndLoopbackHTTP(t *testing.T) {
	t.Parallel()
	req := &http.Request{URL: &url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:8080",
	}}
	via := []*http.Request{{URL: &url.URL{
		Scheme: "http",
		Host:   "localhost:8080",
	}}}
	require.NoError(t, checkRemoteImmutableRedirect(req, via))

	for len(via) < 10 {
		via = append(via, via[0])
	}
	require.ErrorContains(t, checkRemoteImmutableRedirect(req, via), "10 redirects")
}

// remoteRoot serves chunk triads copied from the immutable testdata, plus a
// tip.json naming the tip of the last published chunk.
type remoteRoot struct {
	dir string
	tip remoteImmutableTip
	// handle, when set, serves a request instead of the file server; it
	// returns false to fall through to it.
	handle func(w http.ResponseWriter, r *http.Request) bool
	mu     sync.Mutex
	seen   []string
}

func newRemoteRoot(t *testing.T, published uint64) *remoteRoot {
	t.Helper()
	root := &remoteRoot{dir: t.TempDir()}
	source := filepath.Join("..", "..", "database", "immutable", "testdata")
	for chunk := range published {
		for _, ext := range []string{".chunk", ".primary", ".secondary"} {
			name := immutable.ChunkName(chunk) + ext
			data, err := os.ReadFile(filepath.Join(source, name))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(
				filepath.Join(root.dir, name), data, 0o600,
			))
		}
	}
	imm, err := immutable.New(root.dir)
	require.NoError(t, err)
	tip, err := imm.GetTip()
	require.NoError(t, err)
	root.tip = remoteImmutableTip{
		Slot: tip.Slot,
		Hash: hex.EncodeToString(tip.Hash),
	}
	return root
}

func (r *remoteRoot) serve(t *testing.T) string {
	t.Helper()
	files := http.FileServer(http.Dir(r.dir))
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			r.mu.Lock()
			r.seen = append(r.seen, strings.TrimPrefix(req.URL.Path, "/"))
			handle := r.handle
			r.mu.Unlock()
			if handle != nil && handle(w, req) {
				return
			}
			if req.URL.Path == "/tip.json" {
				_ = json.NewEncoder(w).Encode(r.tip)
				return
			}
			files.ServeHTTP(w, req)
		},
	))
	t.Cleanup(server.Close)
	return server.URL + "/"
}

func (r *remoteRoot) requested(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.seen, name)
}

// recordingHandler keeps the messages of the log records it handles.
type recordingHandler struct {
	mu       sync.Mutex
	messages []string
}

func (h *recordingHandler) Enabled(
	context.Context,
	slog.Level,
) bool {
	return true
}

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

func (h *recordingHandler) count(message string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.messages {
		if m == message {
			n++
		}
	}
	return n
}

type remoteLoad struct {
	chain    *chain.Chain
	cacheDir string
	log      *recordingHandler
}

func newRemoteLoad(t *testing.T) *remoteLoad {
	t.Helper()
	cm, err := chain.NewManager(newTestDB(t), nil)
	require.NoError(t, err)
	return &remoteLoad{
		chain:    cm.PrimaryChain(),
		cacheDir: t.TempDir(),
		log:      &recordingHandler{},
	}
}

func (l *remoteLoad) run(
	ctx context.Context,
	rootURL string,
) (int, error) {
	batches := make(chan []gledger.Block)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range batches { //nolint:revive // drain only
		}
	}()
	copied, _, err := copyBlocksRemote(
		ctx, slog.New(l.log), rootURL, l.cacheDir, l.chain, batches,
	)
	close(batches)
	<-drained
	return copied, err
}

func (l *remoteLoad) readyChunks(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(l.cacheDir, "ready"))
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".chunk" {
			names = append(names, entry.Name())
		}
	}
	return names
}

func localBlockCount(t *testing.T, dir string) int {
	t.Helper()
	cm, err := chain.NewManager(newTestDB(t), nil)
	require.NoError(t, err)
	batches := make(chan []gledger.Block)
	go func() {
		for range batches { //nolint:revive // drain only
		}
	}()
	copied, _, err := copyBlocksDirect(
		context.Background(),
		slog.New(slog.DiscardHandler),
		dir,
		cm.PrimaryChain(),
		batches,
	)
	close(batches)
	require.NoError(t, err)
	return copied
}

func TestCopyBlocksRemoteLoadsToTheRemoteTip(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 3)
	// The root also publishes a chunk past its tip.json, which is not loaded
	// once the chain reaches the tip.
	extra := newRemoteRoot(t, 4)
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		data, err := os.ReadFile(filepath.Join(extra.dir, "00003"+ext))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(root.dir, "00003"+ext), data, 0o600,
		))
	}
	want := localBlockCount(t, newRemoteRoot(t, 3).dir)

	load := newRemoteLoad(t)
	copied, err := load.run(context.Background(), root.serve(t))
	require.NoError(t, err)
	require.Equal(t, want, copied)
	require.Equal(t, root.tip.Slot, load.chain.Tip().Point.Slot)
	require.Equal(
		t,
		root.tip.Hash,
		hex.EncodeToString(load.chain.Tip().Point.Hash),
	)
	require.Equal(t, []string{"00002.chunk"}, load.readyChunks(t))
	require.Equal(t, 3, load.log.count("loaded remote ImmutableDB chunk"))
}

func TestCopyBlocksRemoteStopsAtTheLastPublishedChunk(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	root.tip.Slot += 1_000_000
	load := newRemoteLoad(t)
	copied, err := load.run(context.Background(), root.serve(t))
	require.NoError(t, err)
	require.Equal(t, localBlockCount(t, newRemoteRoot(t, 2).dir), copied)
	require.Equal(
		t, 1, load.log.count("remote ImmutableDB publishes no further chunks"),
	)
}

// TestCopyBlocksRemoteHoldsChunksCompletedOutOfOrder holds chunk 0 until
// chunk 2 has downloaded, and chunk 1 until chunk 0 is copied, so chunk 2 is
// complete while chunk 1 is not. Copying must not see chunk 2 before chunk 1.
func TestCopyBlocksRemoteHoldsChunksCompletedOutOfOrder(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 3)
	load := newRemoteLoad(t)
	chunk2Done := func() bool {
		matches, _ := filepath.Glob(
			filepath.Join(load.cacheDir, "*", "00002.secondary"),
		)
		return len(matches) > 0
	}
	chunk0Copied := func() bool {
		return load.log.count("loaded remote ImmutableDB chunk") > 0
	}
	waitFor := func(cond func() bool) {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.After(5 * time.Second)
		for !cond() {
			select {
			case <-ticker.C:
			case <-deadline:
				return
			}
		}
	}
	root.handle = func(_ http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/00000.chunk":
			waitFor(chunk2Done)
		case "/00001.chunk":
			waitFor(chunk0Copied)
		}
		return false
	}
	copied, err := load.run(context.Background(), root.serve(t))
	require.NoError(t, err)
	require.True(t, chunk2Done())
	require.Equal(t, localBlockCount(t, newRemoteRoot(t, 3).dir), copied)
	require.Equal(t, root.tip.Slot, load.chain.Tip().Point.Slot)
}

func TestCopyBlocksRemoteRejectsATipHashMismatch(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	root.tip.Hash = strings.Repeat("00", 32)
	load := newRemoteLoad(t)
	_, err := load.run(context.Background(), root.serve(t))
	require.ErrorContains(t, err, "remote ImmutableDB tip mismatch")
}

func TestCopyBlocksRemoteRejectsAMissingIndex(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 3)
	require.NoError(t, os.Remove(filepath.Join(root.dir, "00001.primary")))
	load := newRemoteLoad(t)
	_, err := load.run(context.Background(), root.serve(t))
	require.ErrorContains(t, err, "remote chunk 00001 is missing .primary")
}

func TestCopyBlocksRemoteRejectsACorruptChunk(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"00001.chunk", "00001.secondary"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			root := newRemoteRoot(t, 3)
			path := filepath.Join(root.dir, file)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			for i := range data {
				data[i] ^= 0xff
			}
			require.NoError(t, os.WriteFile(path, data, 0o600))
			load := newRemoteLoad(t)
			_, err = load.run(context.Background(), root.serve(t))
			require.ErrorContains(t, err, "chunk 00001")
		})
	}
}

func TestCopyBlocksRemoteResumesAPartialFile(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	full, err := os.ReadFile(filepath.Join(root.dir, "00000.chunk"))
	require.NoError(t, err)
	load := newRemoteLoad(t)
	staging := filepath.Join(load.cacheDir, "staging")
	require.NoError(t, os.MkdirAll(staging, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(staging, "00000.chunk.part"), full[:len(full)/2], 0o600,
	))
	var ranges []string
	var mu sync.Mutex
	root.handle = func(_ http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/00000.chunk" {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		return false
	}
	copied, err := load.run(context.Background(), root.serve(t))
	require.NoError(t, err)
	require.Equal(t, localBlockCount(t, newRemoteRoot(t, 2).dir), copied)
	require.Equal(
		t,
		[]string{"bytes=" + strconv.Itoa(len(full)/2) + "-"},
		ranges,
	)
}

func TestCopyBlocksRemoteRetriesAnInterruptedTransfer(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	full, err := os.ReadFile(filepath.Join(root.dir, "00001.chunk"))
	require.NoError(t, err)
	var once sync.Once
	root.handle = func(w http.ResponseWriter, r *http.Request) bool {
		cut := false
		if r.URL.Path == "/00001.chunk" {
			once.Do(func() { cut = true })
		}
		if !cut {
			return false
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(full)))
		_, _ = w.Write(full[:len(full)/3])
		panic(http.ErrAbortHandler)
	}
	load := newRemoteLoad(t)
	copied, err := load.run(context.Background(), root.serve(t))
	require.NoError(t, err)
	require.Equal(t, localBlockCount(t, newRemoteRoot(t, 2).dir), copied)
}

func TestCopyBlocksRemoteResumesFromTheChunkHoldingTheTip(t *testing.T) {
	t.Parallel()
	load := newRemoteLoad(t)
	first := newRemoteRoot(t, 2)
	_, err := load.run(context.Background(), first.serve(t))
	require.NoError(t, err)

	second := newRemoteRoot(t, 3)
	_, err = load.run(context.Background(), second.serve(t))
	require.NoError(t, err)
	require.Equal(t, second.tip.Slot, load.chain.Tip().Point.Slot)
	require.False(t, second.requested("00000.chunk"))
	require.True(t, second.requested("00002.chunk"))
}

func TestCopyBlocksRemoteStopsOnCancellation(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	root.handle = func(_ http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/00000.chunk" {
			cancel()
			<-r.Context().Done()
			return true
		}
		return false
	}
	load := newRemoteLoad(t)
	done := make(chan error, 1)
	go func() {
		_, err := load.run(ctx, root.serve(t))
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("copyBlocksRemote did not return after cancellation")
	}
}

func TestCopyBlocksRemoteRequiresChunkZero(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 1)
	require.NoError(t, os.Remove(filepath.Join(root.dir, "00000.chunk")))
	load := newRemoteLoad(t)
	_, err := load.run(context.Background(), root.serve(t))
	require.ErrorIs(t, err, errRemoteChunkNotPublished)
}

// TestLoadWithDBLoadsFromARemoteImmutableRoot drives the load entry point
// with an HTTP root in place of a directory.
func TestLoadWithDBLoadsFromARemoteImmutableRoot(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	db := newTestDB(t)
	err := LoadWithDB(
		context.Background(),
		&config.Config{Network: "preview", DatabasePath: t.TempDir()},
		slog.New(slog.DiscardHandler),
		root.serve(t),
		db,
	)
	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, root.tip.Slot, tip.Point.Slot)
}

func TestLoadWithDBRemoteRootRequiresADatabasePath(t *testing.T) {
	t.Parallel()
	err := LoadWithDB(
		context.Background(),
		&config.Config{Network: "preview"},
		slog.New(slog.DiscardHandler),
		"https://cdn.example/immutable/",
		newTestDB(t),
	)
	require.ErrorContains(t, err, "requires databasePath")
}
