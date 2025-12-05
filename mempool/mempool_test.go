// Copyright 2025 Blink Labs Software
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

package mempool

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMempool_Stop(t *testing.T) {
	// Create a mempool with minimal config
	m := NewMempool(MempoolConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     event.NewEventBus(nil, nil),
		PromRegistry: prometheus.NewRegistry(),
	})

	// Add a consumer to test cleanup
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:1")
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
	consumer := m.AddConsumer(connId)
	if consumer == nil {
		t.Fatal("failed to add consumer")
	}

	// Verify consumer was added
	m.consumersMutex.Lock()
	if len(m.consumers) != 1 {
		t.Fatalf("expected 1 consumer, got %d", len(m.consumers))
	}
	m.consumersMutex.Unlock()

	// Add a mock transaction to test cleanup
	m.Lock()
	m.transactions = []*MempoolTransaction{
		{
			Hash:     "test-hash",
			Cbor:     []byte("test-cbor"),
			Type:     0,
			LastSeen: time.Now(),
		},
	}
	m.metrics.txsInMempool.Set(1)
	m.metrics.mempoolBytes.Set(100)
	m.Unlock()

	// Verify transaction was added
	m.RLock()
	if len(m.transactions) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(m.transactions))
	}
	m.RUnlock()

	// Verify metrics are set
	if testutil.ToFloat64(m.metrics.txsInMempool) != 1 {
		t.Fatalf(
			"expected txsInMempool to be 1, got %f",
			testutil.ToFloat64(m.metrics.txsInMempool),
		)
	}
	if testutil.ToFloat64(m.metrics.mempoolBytes) != 100 {
		t.Fatalf(
			"expected mempoolBytes to be 100, got %f",
			testutil.ToFloat64(m.metrics.mempoolBytes),
		)
	}

	// Stop the mempool
	ctx := context.Background()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}

	// Verify consumers were cleared
	m.consumersMutex.Lock()
	if len(m.consumers) != 0 {
		t.Fatalf("expected 0 consumers after stop, got %d", len(m.consumers))
	}
	m.consumersMutex.Unlock()

	// Verify transactions were cleared
	m.RLock()
	if len(m.transactions) != 0 {
		t.Fatalf(
			"expected 0 transactions after stop, got %d",
			len(m.transactions),
		)
	}
	m.RUnlock()

	// Verify metrics were reset
	if testutil.ToFloat64(m.metrics.txsInMempool) != 0 {
		t.Fatalf(
			"expected txsInMempool to be 0 after stop, got %f",
			testutil.ToFloat64(m.metrics.txsInMempool),
		)
	}
	if testutil.ToFloat64(m.metrics.mempoolBytes) != 0 {
		t.Fatalf(
			"expected mempoolBytes to be 0 after stop, got %f",
			testutil.ToFloat64(m.metrics.mempoolBytes),
		)
	}
}
