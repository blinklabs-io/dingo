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

package dingo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startSignalWriter is a fakeIndexerWriter that closes startedCh when the node
// starts it, so a test can block on the start instead of polling for it.
type startSignalWriter struct {
	fakeIndexerWriter
	startedCh chan struct{}
	startOnce sync.Once
}

func newStartSignalWriter() *startSignalWriter {
	return &startSignalWriter{
		fakeIndexerWriter: fakeIndexerWriter{wakes: make(chan struct{}, 8)},
		startedCh:         make(chan struct{}),
	}
}

func (w *startSignalWriter) Start(ctx context.Context) error {
	err := w.fakeIndexerWriter.Start(ctx)
	w.startOnce.Do(func() { close(w.startedCh) })
	return err
}

// requireWriterStarted blocks until Run starts the writer, and fails with
// Run's own error if Run returns first.
//
// The first data directory a test process opens migrates the shared metadata
// template inside Run, before the writer starts, and that takes tens of
// seconds on a loaded host. The wait therefore follows the two events that
// end it rather than a budget for that work; AsyncWait only reports a Run
// that does neither.
func requireWriterStarted(
	t *testing.T,
	writer *startSignalWriter,
	runErr <-chan error,
) {
	t.Helper()
	select {
	case <-writer.startedCh:
	case err := <-runErr:
		t.Fatalf("node run ended before the IndexerDB writer started: %v", err)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("node run neither started the IndexerDB writer nor returned")
	}
}

func freeLoopbackPort(t *testing.T) uint {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return uint(l.Addr().(*net.TCPAddr).Port) // #nosec G115 -- a TCP port
}

// TestNodeServesEveryAPIFromTheIndexerDB runs a node with the real
// built-in API providers and checks that each answers a request from the
// IndexerDB rather than refusing it.
func TestNodeServesEveryAPIFromTheIndexerDB(t *testing.T) {
	t.Parallel()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	utxorpcPort := freeLoopbackPort(t)
	blockfrostPort := freeLoopbackPort(t)
	meshPort := freeLoopbackPort(t)
	writer := newStartSignalWriter()
	var indexerDB atomic.Pointer[database.Database]
	var slotTime atomic.Pointer[func(uint64) (time.Time, error)]
	dbPath := t.TempDir()
	n, err := New(NewConfig(
		WithPluginSelection(plugin.CapabilityStorageBlob, plugin.Selection{
			Provider: "badger", Config: map[string]any{"memTableSize": int64(8 << 20)},
		}),
		WithDatabasePath(dbPath),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		// API storage mode keeps the consensus store in core mode; the API
		// servers must still start.
		WithStorageMode(StorageModeAPI),
		WithIndexerWriterFactory(
			func(deps IndexerWriterDeps) (IndexerWriter, error) {
				indexerDB.Store(deps.Indexer)
				slotTime.Store(&deps.SlotTime)
				return writer, nil
			},
		),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithBindAddr("127.0.0.1"),
		WithNetwork("preview"),
		WithUtxorpcPort(utxorpcPort),
		WithBlockfrostPort(blockfrostPort),
		WithMeshPort(meshPort),
		WithShutdownTimeout(5*time.Second),
	))
	require.NoError(t, err)
	n.config.listeners = nil
	kupoPort := freeLoopbackPort(t)
	n.config.pluginSelections[plugin.CapabilityAPIKupo] = plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": kupoPort},
	}

	runErr := make(chan error, 1)
	runDone := make(chan struct{})
	go func() {
		runErr <- n.Run(context.Background())
		close(runDone)
	}()
	t.Cleanup(func() {
		require.NoError(t, n.Stop())
		testutil.RequireReceive(t, runDone, 30*time.Second, "node run returned")
	})
	requireWriterStarted(t, writer, runErr)
	requireStartupComplete(t, n)

	clock := slotTime.Load()
	require.NotNil(t, clock)
	require.NotNil(t, *clock, "the writer gets the ledger slot clock")
	at, err := (*clock)(100)
	require.NoError(t, err)
	later, err := (*clock)(200)
	require.NoError(t, err)
	require.Equal(
		t,
		100*time.Second,
		later.Sub(at),
		"preview slots are one second",
	)

	idx := indexerDB.Load()
	require.NotNil(t, idx, "the writer factory received the IndexerDB")
	require.DirExists(t, filepath.Join(dbPath, "indexer"))
	indexed := indexerdb.Tip{
		Slot:        4242,
		BlockNumber: 7,
		Hash:        bytes.Repeat([]byte{0xcd}, 32),
	}
	// The position names a block no store holds, so it is recorded under the
	// tip key alone. Store.SetTip also writes the metadata tip row, which
	// Kupo's checkpoint snapshot resolves to a stored block.
	txn := idx.Transaction(t.Context(), true)
	require.NoError(t, idx.SetSyncState(
		indexerdb.TipSyncKey,
		fmt.Sprintf(
			"%d:%d:%x", indexed.Slot, indexed.BlockNumber, indexed.Hash,
		),
		txn,
	))
	require.NoError(t, txn.Commit())

	// do retries until the listener accepts, then returns the response; a
	// Run that ended early fails with its own error.
	do := func(method, url, contentType, payload string) (int, string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			select {
			case err := <-runErr:
				t.Fatalf("node run ended early: %v", err)
			default:
			}
			req, err := http.NewRequestWithContext(
				t.Context(), method, url, strings.NewReader(payload),
			)
			require.NoError(t, err)
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}
			resp, err := http.DefaultClient.Do(req) //nolint:gosec
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				return resp.StatusCode, string(b)
			}
			require.True(t, time.Now().Before(deadline), "%s: %v", url, err)
			time.Sleep(50 * time.Millisecond)
		}
	}
	base := func(port uint) string {
		return "http://127.0.0.1:" + strconv.FormatUint(uint64(port), 10)
	}

	status, body := do(http.MethodGet, base(kupoPort)+"/checkpoints", "", "")
	require.Equal(t, http.StatusOK, status, body)

	status, body = do(
		http.MethodGet,
		base(blockfrostPort)+"/api/v0/epochs/latest",
		"",
		"",
	)
	require.NotEqual(t, http.StatusServiceUnavailable, status, body)
	require.NotEqual(t, http.StatusNotFound, status, body)

	status, body = do(
		http.MethodPost, base(meshPort)+"/network/list",
		"application/json", `{"metadata":{}}`,
	)
	require.Equal(t, http.StatusOK, status, body)

	status, body = do(
		http.MethodPost,
		base(utxorpcPort)+"/utxorpc.v1alpha.query.QueryService/SearchUtxos",
		"application/json", `{"maxItems":1}`,
	)
	require.Equal(t, http.StatusOK, status, body)
	require.Contains(t, body, "4242",
		"UTxO RPC reports the IndexerDB tip, not the ledger tip")
}

// runIndexerNode starts a node over dbPath and returns when its IndexerDB
// writer is running, with a function that stops it.
func runIndexerNode(t *testing.T, dbPath string) func() {
	t.Helper()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	writer := newStartSignalWriter()
	n, err := New(NewConfig(
		WithPluginSelection(plugin.CapabilityStorageBlob, plugin.Selection{
			Provider: "badger", Config: map[string]any{"memTableSize": int64(8 << 20)},
		}),
		WithDatabasePath(dbPath),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithIndexerWriterFactory(
			func(IndexerWriterDeps) (IndexerWriter, error) {
				return writer, nil
			},
		),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
		WithBindAddr("127.0.0.1"),
		WithNetwork("preview"),
		WithShutdownTimeout(5*time.Second),
	))
	require.NoError(t, err)
	n.config.listeners = nil
	runErr := make(chan error, 1)
	runDone := make(chan struct{})
	runCtx, cancelRun := context.WithCancel(context.Background())
	go func() {
		defer close(runDone)
		runErr <- n.Run(runCtx)
	}()
	var stopOnce sync.Once
	stop := func() {
		t.Helper()
		stopOnce.Do(func() {
			cancelRun()
			assert.NoError(t, n.Stop())
			testutil.RequireReceive(t, runDone, 30*time.Second, "node run returned")
		})
	}
	t.Cleanup(stop)
	requireWriterStarted(t, writer, runErr)
	requireStartupComplete(t, n)
	return stop
}

// requireStartupComplete waits for Run to finish starting. The writer starts
// partway through startup, and Run holds startupLifecycleMu until the rest
// (the genesis snapshot among it) is done, so a Stop issued at that point
// waits out the remainder inside its shutdown timeout, which a loaded machine
// can exceed. Startup is complete once the mutex is free.
func requireStartupComplete(t *testing.T, n *Node) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !n.startupLifecycleMu.TryLock() {
			return false
		}
		n.startupLifecycleMu.Unlock()
		return true
	}, testutil.AsyncWait, 10*time.Millisecond, "node startup completes")
}

// A node persists its consensus store in core mode, which a second
// start over the same data directory must accept, after a clean stop and
// after the directory is copied from under a running node as a crash would
// leave it.
func TestNodeRestartsOverItsDataDirectory(t *testing.T) {
	t.Parallel()
	dbPath := testutil.OwnerOnlyTempDir(t)
	stop := runIndexerNode(t, dbPath)
	crashImage := filepath.Join(testutil.OwnerOnlyTempDir(t), "image")
	require.NoError(t, os.CopyFS(crashImage, os.DirFS(dbPath)))
	require.NoError(t, os.Chmod(crashImage, 0o700))
	stop()

	stopAgain := runIndexerNode(t, dbPath)
	stopAgain()

	stopImage := runIndexerNode(t, crashImage)
	stopImage()
}
