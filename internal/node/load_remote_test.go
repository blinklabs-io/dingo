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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	"github.com/blinklabs-io/dingo/internal/netguard"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/stretchr/testify/require"
)

type remoteImmutableRoundTripFunc func(*http.Request) (*http.Response, error)

func (f remoteImmutableRoundTripFunc) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	return f(req)
}

func newRemoteImmutableTestClientAndBudget(
	t *testing.T,
	rootURL string,
	dir string,
	limits remoteImmutableLimits,
) (*http.Client, *remoteImmutableBudget) {
	t.Helper()
	client, err := newRemoteImmutableHTTPClient(rootURL)
	require.NoError(t, err)
	budget, err := newRemoteImmutableBudget(dir, limits)
	require.NoError(t, err)
	return client, budget
}

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
		(remoteImmutableNetworkPolicy{}).checkRedirect(req, []*http.Request{
			{URL: &url.URL{Scheme: "http", Host: "localhost:8080"}},
			{URL: &url.URL{Scheme: "https", Host: "cdn.example"}},
		}),
		"requires HTTPS",
	)
	req.URL = &url.URL{Scheme: "https", Host: "cdn.example"}
	require.NoError(t, (remoteImmutableNetworkPolicy{}).checkRedirect(req, nil))
}

func TestRemoteImmutableRedirectRejectsPrivateHTTPS(t *testing.T) {
	t.Parallel()
	req := &http.Request{URL: &url.URL{
		Scheme: "https",
		Host:   "127.0.0.1",
	}}
	err := (remoteImmutableNetworkPolicy{}).checkRedirect(req, []*http.Request{{URL: &url.URL{
		Scheme: "https",
		Host:   "cdn.example",
	}}})
	require.ErrorIs(t, err, netguard.ErrBlockedDestination)
}

func TestRemoteImmutableDialRejectsPrivateDNSAnswer(t *testing.T) {
	t.Parallel()
	called := false
	policy := remoteImmutableNetworkPolicy{
		dial: func(context.Context, string, string) (net.Conn, error) {
			called = true
			return nil, errors.New("unexpected dial")
		},
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		},
	}
	_, err := policy.dialContext(
		context.Background(), "tcp", "cdn.example:443",
	)
	require.ErrorIs(t, err, netguard.ErrBlockedDestination)
	require.False(t, called)
}

func TestRemoteImmutableClientDisablesProxy(t *testing.T) {
	t.Parallel()
	client, err := newRemoteImmutableHTTPClient("https://cdn.example/immutable")
	require.NoError(t, err)
	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, transport.Proxy)
	require.NotNil(t, transport.DialContext)
}

func TestRemoteImmutableAggregateLimitsFollowRequestedRange(t *testing.T) {
	t.Parallel()
	limits := remoteImmutableLimits{
		primary: 2, secondary: 3, chunk: 5,
		concurrent: 2, chunkSlots: 10,
	}
	got, err := limits.withImportRange(1, 29)
	require.NoError(t, err)
	require.Equal(t, int64(120), got.imported)
	require.Equal(t, int64(30), got.cache)
	require.Equal(t, int64(remoteImmutableStagingOverhead), got.minFree)
}

func TestRemoteImmutableBudgetPreservesFreeSpace(t *testing.T) {
	t.Parallel()
	budget := &remoteImmutableBudget{
		files: make(map[string]int64),
		limits: remoteImmutableLimits{
			imported: 10, cache: 10, minFree: 5, concurrent: 1,
		},
		freeDisk: func(string) (uint64, error) { return 5, nil },
	}
	err := budget.reserveWrite(filepath.Join(t.TempDir(), "part"), 1)
	require.ErrorIs(t, err, errRemoteImmutableFreeSpace)
	require.Zero(t, budget.total)
	require.Zero(t, budget.active)
}

func TestRemoteImmutableBudgetReservesFreeSpaceForOutstandingWrites(
	t *testing.T,
) {
	t.Parallel()
	budget := &remoteImmutableBudget{
		files: make(map[string]int64),
		limits: remoteImmutableLimits{
			imported: 10, cache: 10, minFree: 5, concurrent: 2,
		},
		freeDisk: func(string) (uint64, error) { return 9, nil },
	}
	path := filepath.Join(t.TempDir(), "part")
	require.NoError(t, budget.reserveWrite(path, 4))
	require.EqualValues(t, 4, budget.reserved)
	require.ErrorIs(t, budget.reserveWrite(path+"-other", 1), errRemoteImmutableFreeSpace)
	require.EqualValues(t, 4, budget.reserved)

	budget.completeWrite(path, 4, 4)
	require.Zero(t, budget.reserved)
	require.EqualValues(t, 4, budget.active)
}

func TestRemoteImmutableBudgetRenameReplacesExistingAccounting(t *testing.T) {
	t.Parallel()
	budget := &remoteImmutableBudget{
		active: 10,
		total:  10,
		files: map[string]int64{
			"staging/part": 4,
			"ready/chunk":  6,
		},
	}

	budget.rename("staging/part", "ready/chunk")

	require.EqualValues(t, 4, budget.active)
	require.EqualValues(t, 10, budget.total)
	require.EqualValues(t, 4, budget.files["ready/chunk"])
	_, exists := budget.files["staging/part"]
	require.False(t, exists)
}

func TestRemoteImmutableBudgetKeepsFailedTruncateAccounted(t *testing.T) {
	t.Parallel()
	partPath := t.TempDir()
	info, err := os.Stat(partPath)
	require.NoError(t, err)
	require.Positive(t, info.Size())

	client := &http.Client{Transport: remoteImmutableRoundTripFunc(func(
		req *http.Request,
	) (*http.Response, error) {
		return &http.Response{
			Status:        http.StatusText(http.StatusOK),
			StatusCode:    http.StatusOK,
			Body:          http.NoBody,
			ContentLength: 0,
			Request:       req,
		}, nil
	})}

	budget := &remoteImmutableBudget{
		total:  info.Size(),
		active: info.Size(),
		files:  map[string]int64{partPath: info.Size()},
		limits: remoteImmutableLimits{
			imported: 1 << 20, cache: 1 << 20, concurrent: 1,
		},
		freeDisk: func(string) (uint64, error) { return 1 << 20, nil },
	}
	err = fetchRemoteFileOnceWithClient(
		context.Background(), client, budget,
		"https://cdn.example/chunk", partPath, 1<<20,
	)
	require.Error(t, err)
	require.Equal(t, info.Size(), budget.total)
	require.Equal(t, info.Size(), budget.active)
	require.Equal(t, info.Size(), budget.files[partPath])
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
	policy := remoteImmutableNetworkPolicy{allowLoopback: true}
	require.NoError(t, policy.checkRedirect(req, via))

	for len(via) < 10 {
		via = append(via, via[0])
	}
	require.ErrorContains(t, policy.checkRedirect(req, via), "10 redirects")
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
	source   string
	limits   remoteImmutableLimits
	log      *recordingHandler
}

func newRemoteLoad(t *testing.T) *remoteLoad {
	t.Helper()
	cm, err := chain.NewManager(context.Background(), newTestDB(t), nil)
	require.NoError(t, err)
	limits, err := remoteImmutableLimitsForSlots(4_320, true)
	require.NoError(t, err)
	return &remoteLoad{
		chain:    cm.PrimaryChain(),
		cacheDir: t.TempDir(),
		limits:   limits,
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
	l.source = rootURL
	copied, _, err := copyBlocksRemote(
		ctx, slog.New(l.log), rootURL, l.cacheDir, l.limits, l.chain, batches,
	)
	close(batches)
	<-drained
	return copied, err
}

func (l *remoteLoad) readyChunks(t *testing.T) []string {
	t.Helper()
	sourceCache, err := remoteImmutableSourceCache(l.cacheDir, l.source)
	require.NoError(t, err)
	entries, err := os.ReadDir(filepath.Join(sourceCache, "ready"))
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
	cm, err := chain.NewManager(context.Background(), newTestDB(t), nil)
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

func TestCopyBlocksRemoteEnforcesAggregateImportBudgetAndCleansStaging(
	t *testing.T,
) {
	t.Parallel()
	root := newRemoteRoot(t, 2)
	triadSize := func(chunk uint64) int64 {
		var total int64
		for _, ext := range []string{".primary", ".secondary", ".chunk"} {
			info, err := os.Stat(filepath.Join(
				root.dir, immutable.ChunkName(chunk)+ext,
			))
			require.NoError(t, err)
			total += info.Size()
		}
		return total
	}
	load := newRemoteLoad(t)
	load.limits.concurrent = 2
	limits, err := load.limits.withChunkCount(2)
	require.NoError(t, err)
	limits.imported = max(triadSize(0), triadSize(1)) + 1
	load.limits = limits
	rootURL := root.serve(t)
	_, err = load.run(context.Background(), rootURL)
	require.ErrorIs(t, err, errRemoteImmutableImportBudget)

	sourceCache, cacheErr := remoteImmutableSourceCache(load.cacheDir, rootURL)
	require.NoError(t, cacheErr)
	_, statErr := os.Stat(filepath.Join(sourceCache, "staging"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestCopyBlocksRemoteCleansStagingWhenBudgetInitializationFails(
	t *testing.T,
) {
	t.Parallel()
	root := newRemoteRoot(t, 1)
	load := newRemoteLoad(t)
	load.limits.imported = 1
	load.limits.cache = 1
	rootURL := root.serve(t)
	sourceCache, err := remoteImmutableSourceCache(load.cacheDir, rootURL)
	require.NoError(t, err)
	stagingDir := filepath.Join(sourceCache, "staging")
	require.NoError(t, os.MkdirAll(stagingDir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(stagingDir, "stale.part"), []byte("stale"), 0o600,
	))

	_, err = load.run(context.Background(), rootURL)
	require.ErrorIs(t, err, errRemoteImmutableCacheBudget)
	_, statErr := os.Stat(stagingDir)
	require.ErrorIs(t, statErr, os.ErrNotExist)
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
			filepath.Join(load.cacheDir, "*", "*", "00002.secondary"),
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
	var (
		probeMu    sync.Mutex
		probeRange string
	)
	root.handle = func(_ http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == "/00001.chunk" {
			probeMu.Lock()
			probeRange = req.Header.Get("Range")
			probeMu.Unlock()
		}
		return false
	}
	load := newRemoteLoad(t)
	_, err := load.run(context.Background(), root.serve(t))
	require.ErrorContains(t, err, "remote chunk 00001 is missing .primary")
	probeMu.Lock()
	gotProbeRange := probeRange
	probeMu.Unlock()
	require.Equal(t, "bytes=0-0", gotProbeRange)
}

func TestFetchRemoteChunkValidatesIndexesBeforeChunkBody(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 1)
	primary := filepath.Join(root.dir, "00000.primary")
	data, err := os.ReadFile(primary)
	require.NoError(t, err)
	data[0] = 2
	require.NoError(t, os.WriteFile(primary, data, 0o600))
	limits, err := remoteImmutableLimitsForSlots(4_320, true)
	require.NoError(t, err)
	rootURL := strings.TrimRight(root.serve(t), "/")
	limits, err = limits.withChunkCount(1)
	require.NoError(t, err)
	dir := t.TempDir()
	client, budget := newRemoteImmutableTestClientAndBudget(
		t, rootURL, dir, limits,
	)
	err = fetchRemoteChunkWithClient(
		context.Background(), client, budget, rootURL, dir, 0, limits,
	)
	require.ErrorContains(t, err, "unsupported primary index version")
	require.False(t, root.requested("00000.chunk"))
}

func TestFetchRemoteChunkRejectsOversizedBlockSpan(t *testing.T) {
	t.Parallel()
	root := newRemoteRoot(t, 1)
	chunkPath := filepath.Join(root.dir, "00000.chunk")
	chunkInfo, err := os.Stat(chunkPath)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(
		chunkPath,
		chunkInfo.Size()+int64(blockfetch.StreamingMaxPendingMessageBytes)+1,
	))
	limits, err := remoteImmutableLimitsForSlots(4_320, true)
	require.NoError(t, err)
	limits, err = limits.withChunkCount(1)
	require.NoError(t, err)
	rootURL := strings.TrimRight(root.serve(t), "/")
	dir := t.TempDir()
	client, budget := newRemoteImmutableTestClientAndBudget(
		t, rootURL, dir, limits,
	)
	err = fetchRemoteChunkWithClient(
		context.Background(), client, budget, rootURL, dir, 0, limits,
	)
	require.ErrorIs(t, err, immutable.ErrInvalidChunkOffset)
	require.ErrorContains(t, err, "exceeds maximum")
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
	rootURL := root.serve(t)
	sourceCache, err := remoteImmutableSourceCache(load.cacheDir, rootURL)
	require.NoError(t, err)
	staging := filepath.Join(sourceCache, "staging")
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
	copied, err := load.run(context.Background(), rootURL)
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
	root := newRemoteRoot(t, 2)
	rootURL := root.serve(t)
	_, err := load.run(context.Background(), rootURL)
	require.NoError(t, err)

	extended := newRemoteRoot(t, 3)
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		data, readErr := os.ReadFile(filepath.Join(extended.dir, "00002"+ext))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(
			filepath.Join(root.dir, "00002"+ext), data, 0o600,
		))
	}
	root.mu.Lock()
	root.tip = extended.tip
	root.seen = nil
	root.mu.Unlock()
	_, err = load.run(context.Background(), rootURL)
	require.NoError(t, err)
	require.Equal(t, root.tip.Slot, load.chain.Tip().Point.Slot)
	require.False(t, root.requested("00000.chunk"))
	require.True(t, root.requested("00002.chunk"))
}

func TestCopyBlocksRemoteDoesNotReuseAnotherSourceCache(t *testing.T) {
	t.Parallel()
	load := newRemoteLoad(t)
	first := newRemoteRoot(t, 2)
	_, err := load.run(context.Background(), first.serve(t))
	require.NoError(t, err)

	second := newRemoteRoot(t, 3)
	_, err = load.run(context.Background(), second.serve(t))
	require.NoError(t, err)
	require.True(t, second.requested("00000.chunk"))
}

func TestFetchRemoteFileOnceRejectsBodyPastFormatLimit(t *testing.T) {
	t.Parallel()
	const limit = int64(16)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			flusher := w.(http.Flusher)
			w.WriteHeader(http.StatusOK)
			flusher.Flush()
			_, _ = w.Write(bytes.Repeat([]byte{0xaa}, int(limit+1)))
		},
	))
	t.Cleanup(server.Close)
	partPath := filepath.Join(t.TempDir(), "00000.chunk.part")
	client, budget := newRemoteImmutableTestClientAndBudget(
		t, server.URL, filepath.Dir(partPath), remoteImmutableLimits{
			imported: limit + 1, cache: limit + 1, concurrent: 1,
		},
	)
	err := fetchRemoteFileOnceWithClient(
		context.Background(), client, budget,
		server.URL+"/00000.chunk", partPath, limit,
	)
	require.ErrorIs(t, err, errRemoteImmutableFileTooLarge)
	_, statErr := os.Stat(partPath)
	require.True(t, errors.Is(statErr, os.ErrNotExist))
}

func TestFetchRemoteFileOnceRedactsURLCredentials(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "no", http.StatusUnauthorized)
		},
	))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL + "/00000.chunk")
	require.NoError(t, err)
	u.User = url.UserPassword("archive-user", "archive-secret")
	partPath := filepath.Join(t.TempDir(), "00000.chunk.part")
	client, budget := newRemoteImmutableTestClientAndBudget(
		t, server.URL, filepath.Dir(partPath), remoteImmutableLimits{
			imported: 17, cache: 17, concurrent: 1,
		},
	)
	err = fetchRemoteFileOnceWithClient(
		context.Background(), client, budget, u.String(), partPath, 16,
	)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "archive-user")
	require.NotContains(t, err.Error(), "archive-secret")
}

func TestRemoteImmutableRequestErrorRedactsURLCredentials(t *testing.T) {
	t.Parallel()
	const rawURL = "https://archive-user:archive-secret@example.invalid/tip.json"
	err := remoteImmutableRequestError(
		"fetching remote ImmutableDB tip",
		rawURL,
		&url.Error{
			Op:  "Get",
			URL: rawURL,
			Err: errors.New("connection refused"),
		},
	)
	require.NotContains(t, err.Error(), "archive-user")
	require.NotContains(t, err.Error(), "archive-secret")
	require.ErrorContains(t, err, "connection refused")
}

func TestRemoteImmutableMalformedURLsRedactCredentials(t *testing.T) {
	t.Parallel()
	const malformed = "https://archive-user:archive-secret@example.invalid/%zz"
	_, classifyErr := classifyRemoteImmutableSource(malformed)
	_, canonicalErr := canonicalRemoteImmutableRoot(malformed)
	_, clientErr := newRemoteImmutableHTTPClient(malformed)
	for _, err := range []error{classifyErr, canonicalErr, clientErr} {
		require.Error(t, err)
		require.NotContains(t, err.Error(), "archive-user")
		require.NotContains(t, err.Error(), "archive-secret")
	}
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

func TestExpectedRemotePrefetchCleanupError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "canceled", err: context.Canceled, want: true},
		{
			name: "unpublished chunk and cancellation",
			err:  errors.Join(errRemoteChunkNotPublished, context.Canceled),
			want: true,
		},
		{
			name: "network failure and cancellation",
			err:  errors.Join(errors.New("connection reset"), context.Canceled),
		},
		{
			name: "unpublished chunk and network failure",
			err:  errors.Join(errRemoteChunkNotPublished, errors.New("HTTP 500")),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isExpectedRemotePrefetchCleanupError(tc.err))
		})
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

func TestRedactRemoteImmutableURLDropsQueryAndFragment(t *testing.T) {
	t.Parallel()
	got := redactRemoteImmutableURL(
		"https://archive-user:archive-secret@example.invalid/tip.json" +
			"?token=query-secret&x=1#frag-secret",
	)
	require.Equal(t, "https://example.invalid/tip.json", got)
	got = redactRemoteImmutableURL("https://example.invalid/tip.json?#frag")
	require.Equal(t, "https://example.invalid/tip.json", got)
}

func TestRemoteImmutableRequestErrorRedactsQueryCredentials(t *testing.T) {
	t.Parallel()
	const rawURL = "https://example.invalid/00000.chunk?token=query-secret"
	for name, cause := range map[string]error{
		"url error": &url.Error{
			Op: "Get", URL: rawURL, Err: errors.New("connection refused"),
		},
		"nested url in cause": &url.Error{
			Op: "Get", URL: rawURL,
			Err: fmt.Errorf(
				"redirect to %q failed: connection refused",
				"https://other.invalid/x?token=query-secret#frag-secret",
			),
		},
		"bare cause": fmt.Errorf("dial %s: connection refused", rawURL),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := remoteImmutableRequestError("fetching", rawURL, cause)
			require.NotContains(t, err.Error(), "query-secret")
			require.NotContains(t, err.Error(), "frag-secret")
			require.ErrorContains(t, err, "connection refused")
		})
	}
}

func TestFetchRemoteFileOnceRedactsQueryCredentials(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// A redirect target that cannot be parsed makes the client
			// quote the Location value in its error.
			w.Header().Set("Location", "http://[::1/x?token=query-secret")
			w.WriteHeader(http.StatusFound)
		},
	))
	t.Cleanup(server.Close)
	partPath := filepath.Join(t.TempDir(), "00000.chunk.part")
	client, budget := newRemoteImmutableTestClientAndBudget(
		t, server.URL, filepath.Dir(partPath), remoteImmutableLimits{
			imported: 17, cache: 17, concurrent: 1,
		},
	)
	err := fetchRemoteFileOnceWithClient(
		context.Background(), client, budget,
		server.URL+"/00000.chunk?token=query-secret", partPath, 16,
	)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "query-secret")
}

func TestFetchRemoteFileOnceRedactsUnparseableRedirectLocations(t *testing.T) {
	t.Parallel()
	for name, location := range map[string]string{
		"relative bad escape": "/x%zz?token=query-secret",
		"non-http scheme":     "ftp://[::1/x?token=query-secret",
		"scheme relative":     "//[::1/x?token=query-secret",
		"apostrophe":          "http://[::1/x?token=ab'query-secret",
		"fragment only":       "/x%zz#frag-secret",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Location", location)
					w.WriteHeader(http.StatusFound)
				},
			))
			t.Cleanup(server.Close)
			partPath := filepath.Join(t.TempDir(), "00000.chunk.part")
			client, budget := newRemoteImmutableTestClientAndBudget(
				t, server.URL, filepath.Dir(partPath),
				remoteImmutableLimits{imported: 17, cache: 17, concurrent: 1},
			)
			err := fetchRemoteFileOnceWithClient(
				context.Background(), client, budget,
				server.URL+"/00000.chunk?token=query-secret", partPath, 16,
			)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "query-secret")
			require.NotContains(t, err.Error(), "frag-secret")
		})
	}
}

func TestRemoteImmutableRequestErrorScrubsRequestSecrets(t *testing.T) {
	t.Parallel()
	const rawURL = "https://archive-user:archive-secret@example.invalid/f" +
		"?token=query-secret#frag-secret"
	err := remoteImmutableRequestError(
		"fetching", rawURL,
		errors.New("bad token query-secret for archive-secret"),
	)
	for _, s := range []string{
		"query-secret", "frag-secret", "archive-secret", "archive-user",
	} {
		require.NotContains(t, err.Error(), s)
	}
}

func TestRemoteImmutableRequestErrorAsHidesRawURL(t *testing.T) {
	t.Parallel()
	const rawURL = "https://example.invalid/f?token=query-secret"
	err := remoteImmutableRequestError(
		"fetching", rawURL,
		&url.Error{Op: "Get", URL: rawURL, Err: errors.New("boom")},
	)
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		require.NotContains(t, urlErr.Error(), "query-secret")
	}
	bare := remoteImmutableRequestError(
		"fetching", rawURL, &url.Error{Op: "Get", URL: rawURL},
	)
	urlErr = nil
	if errors.As(bare, &urlErr) {
		require.NotContains(t, urlErr.Error(), "query-secret")
	}
}

func TestFetchRemoteFileOnceRedactsQuotedRedirectQuery(t *testing.T) {
	t.Parallel()
	// The Location credential differs from the request URL, so only the
	// quoted-literal scrub can remove it. A double quote or space inside the
	// query must not split the credential out of the redacted text.
	for name, location := range map[string]string{
		"plain":        "/x%zz?sig=location-secret",
		"double quote": `/x%zz?sig="location-secret`,
		"space":        "/x%zz?sig=ab location-secret",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Location", location)
					w.WriteHeader(http.StatusFound)
				},
			))
			t.Cleanup(server.Close)
			partPath := filepath.Join(t.TempDir(), "00000.chunk.part")
			client, budget := newRemoteImmutableTestClientAndBudget(
				t, server.URL, filepath.Dir(partPath),
				remoteImmutableLimits{imported: 17, cache: 17, concurrent: 1},
			)
			err := fetchRemoteFileOnceWithClient(
				context.Background(), client, budget,
				server.URL+"/00000.chunk", partPath, 16,
			)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "location-secret")
		})
	}
}
