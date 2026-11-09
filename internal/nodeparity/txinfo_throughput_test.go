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

package nodeparity

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestRunFromGenesisTxInfoThroughput times a UTxO reconstruction against a
// Koios fake shaped like the mainnet mirror, scaled down 100x: a /tx_info
// request answers in 60ms, and one chunk in txInfoThroughputStallEvery
// stalls past the client's header timeout on every attempt of its first
// fetch and answers when fetched again. It only measures, so it runs only
// with NODEPARITY_TXINFO_THROUGHPUT set, and logs the wall time, the
// requests made, and the most ever in flight.
//
// Not t.Parallel: it shortens txInfoIndexWaits and the Koios client's retry
// backoff, both package-level.
func TestRunFromGenesisTxInfoThroughput(t *testing.T) {
	if os.Getenv("NODEPARITY_TXINFO_THROUGHPUT") == "" {
		t.Skip("set NODEPARITY_TXINFO_THROUGHPUT to measure")
	}
	const (
		magic       = 42
		blocks      = 801
		perEpoch    = 200
		batch       = 5
		concurrency = 4
		latency     = 60 * time.Millisecond
		// The client gives up on a request after 15s without headers; at
		// 1/100 scale a stalled attempt costs 150ms.
		stall = 150 * time.Millisecond
	)
	prevWaits := txInfoIndexWaits
	txInfoIndexWaits = []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
	}
	koiosparity.SetRetryBackoff5xxForTesting(20 * time.Millisecond)
	t.Cleanup(func() {
		txInfoIndexWaits = prevWaits
		koiosparity.SetRetryBackoff5xxForTesting(2 * time.Second)
	})

	server := newGenesisFakeServerWithTransactions(t, blocks)
	server.ungated = true
	server.epochBySlot = map[uint64]int{}
	for i, p := range server.chain.Points {
		server.epochBySlot[p.Slot] = 1 + i/perEpoch
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	var (
		mu          sync.Mutex
		stalled     = map[string]int{}
		requests    atomic.Int64
		inFlight    atomic.Int64
		maxInFlight atomic.Int64
	)
	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/tx_info" {
				http.NotFound(w, r)
				return
			}
			requests.Add(1)
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				m := maxInFlight.Load()
				if n <= m || maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}
			var body struct {
				TxHashes []string `json:"_tx_hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
				len(body.TxHashes) == 0 {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			h := fnv.New32a()
			_, _ = h.Write([]byte(body.TxHashes[0]))
			if h.Sum32()%txInfoThroughputStallEvery == 0 {
				mu.Lock()
				attempt := stalled[body.TxHashes[0]]
				stalled[body.TxHashes[0]]++
				mu.Unlock()
				// The client's three attempts of the first fetch.
				if attempt < 3 {
					time.Sleep(stall)
					http.Error(w, "timeout", http.StatusGatewayTimeout)
					return
				}
			}
			time.Sleep(latency)
			items := make([]map[string]any, len(body.TxHashes))
			for i, hash := range body.TxHashes {
				items[i] = map[string]any{"tx_hash": hash}
			}
			_ = json.NewEncoder(w).Encode(items)
		},
	))
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)
	require.NoError(t, koios.SetTxInfoLimits(batch, concurrency))

	results := make(chan EpochResult, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
			WithBoundaryCaptureDir(t.TempDir()),
			withBoundaryFollower(func(context.Context, *boundaryCaptures) {}),
		)
	}()
	last := uint64(1 + (blocks-1)/perEpoch)
	for {
		r := testutil.RequireReceive(t, results, 2*time.Minute, "epoch result")
		require.NoError(t, r.UTxOErr, "epoch %d", r.Epoch)
		if r.Epoch == last {
			break
		}
	}
	elapsed := time.Since(start)
	cancel()
	err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
	require.True(t, err == nil || errors.Is(err, context.Canceled), "%v", err)
	t.Logf(
		"%d txs in %s (%s tx/s): %d requests, at most %d in flight",
		blocks-1, elapsed.Round(time.Millisecond),
		strconv.FormatFloat(float64(blocks-1)/elapsed.Seconds(), 'f', 0, 64),
		requests.Load(), maxInFlight.Load(),
	)
	require.LessOrEqual(t, maxInFlight.Load(), int64(concurrency))
}

// txInfoThroughputStallEvery is the share of chunks that stall: one in this
// many, about the 2 to 7 of every 64 a mainnet flush saw time out.
const txInfoThroughputStallEvery = 16
