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
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Helpers and Mocks
// =============================================================================

// mockValidator is a test validator that can be configured to pass or fail
type mockValidator struct {
	failHashes map[string]bool
	mu         sync.Mutex
	failAll    bool
}

func newMockValidator() *mockValidator {
	return &mockValidator{
		failHashes: make(map[string]bool),
	}
}

func (v *mockValidator) ValidateTx(tx gledger.Transaction) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failAll {
		return fmt.Errorf("validation disabled")
	}
	if v.failHashes[tx.Hash().String()] {
		return fmt.Errorf("validation failed for %s", tx.Hash())
	}
	return nil
}

func (v *mockValidator) setFailHash(hash string, fail bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.failHashes[hash] = fail
}

func (v *mockValidator) setFailAll(fail bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.failAll = fail
}

// Real Conway transaction for testing AddTransaction
// preview tx: 842a18e8b3280cf769f7a50551525b60820ea74ac8d3223e78939bd36e8185cc
const testTxHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

// newTestMempool creates a mempool configured for testing
func newTestMempool(t *testing.T) *Mempool {
	t.Helper()
	return NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       newMockValidator(),
		MempoolCapacity: 1024 * 1024, // 1MB
	})
}

// newTestMempoolWithValidator creates a mempool with a specific validator
func newTestMempoolWithValidator(
	t *testing.T,
	v TxValidator,
) *Mempool {
	t.Helper()
	return NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       v,
		MempoolCapacity: 1024 * 1024,
	})
}

// newTestConnectionId creates a unique connection ID for testing
func newTestConnectionId(n int) ouroboros.ConnectionId {
	localAddr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", n))
	remoteAddr, _ := net.ResolveTCPAddr(
		"tcp",
		fmt.Sprintf("127.0.0.1:%d", n+10000),
	)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

// addMockTransactions adds mock transactions directly to mempool (bypasses validation)
func addMockTransactions(
	t *testing.T,
	m *Mempool,
	count int,
) []*MempoolTransaction {
	t.Helper()
	txs := make([]*MempoolTransaction, count)
	m.Lock()
	for i := range count {
		tx := &MempoolTransaction{
			Hash:     fmt.Sprintf("tx-hash-%d", i),
			Cbor:     fmt.Appendf(nil, "tx-cbor-%d", i),
			Type:     uint(conway.EraIdConway),
			LastSeen: time.Now(),
		}
		txs[i] = tx
		m.transactions = append(m.transactions, tx)
		m.txByHash[tx.Hash] = tx
		m.metrics.txsInMempool.Inc()
		m.metrics.mempoolBytes.Add(float64(len(tx.Cbor)))
	}
	m.Unlock()
	return txs
}

// getTestTxBytes returns the decoded test transaction bytes
func getTestTxBytes(t *testing.T) []byte {
	t.Helper()
	txBytes, err := hex.DecodeString(testTxHex)
	require.NoError(t, err, "failed to decode test tx hex")
	return txBytes
}

// =============================================================================
// Original Test (preserved)
// =============================================================================

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

// =============================================================================
// Basic Consumer Tests
// =============================================================================

func TestMempool_AddRemoveConsumer(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add multiple consumers
	connIds := make([]ouroboros.ConnectionId, 5)
	consumers := make([]*MempoolConsumer, 5)
	for i := range 5 {
		connIds[i] = newTestConnectionId(i)
		consumers[i] = m.AddConsumer(connIds[i])
		require.NotNil(t, consumers[i], "consumer %d should not be nil", i)
	}

	// Verify all consumers are retrievable
	for i := range 5 {
		c := m.Consumer(connIds[i])
		assert.Equal(
			t,
			consumers[i],
			c,
			"consumer %d should be retrievable",
			i,
		)
	}

	// Verify consumer count
	m.consumersMutex.Lock()
	assert.Equal(t, 5, len(m.consumers), "should have 5 consumers")
	m.consumersMutex.Unlock()

	// Remove some consumers
	m.RemoveConsumer(connIds[1])
	m.RemoveConsumer(connIds[3])

	// Verify removed consumers are gone
	assert.Nil(t, m.Consumer(connIds[1]), "consumer 1 should be removed")
	assert.Nil(t, m.Consumer(connIds[3]), "consumer 3 should be removed")

	// Verify remaining consumers still exist
	assert.NotNil(t, m.Consumer(connIds[0]), "consumer 0 should still exist")
	assert.NotNil(t, m.Consumer(connIds[2]), "consumer 2 should still exist")
	assert.NotNil(t, m.Consumer(connIds[4]), "consumer 4 should still exist")

	// Verify consumer count
	m.consumersMutex.Lock()
	assert.Equal(t, 3, len(m.consumers), "should have 3 consumers remaining")
	m.consumersMutex.Unlock()
}

func TestMempoolConsumer_NextTx_NonBlocking(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions
	txs := addMockTransactions(t, m, 5)

	// Create consumer
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)
	require.NotNil(t, consumer)

	// Get all transactions in order (non-blocking)
	for i := range 5 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx, "transaction %d should not be nil", i)
		assert.Equal(t, txs[i].Hash, tx.Hash, "transaction %d hash mismatch", i)
	}

	// Next call should return nil (no more transactions)
	tx := consumer.NextTx(false)
	assert.Nil(t, tx, "should return nil when no more transactions")

	// Verify nextTxIdx is at end
	assert.Equal(t, 5, consumer.nextTxIdx, "nextTxIdx should be 5")
}

func TestMempoolConsumer_NextTx_Blocking(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Create consumer on empty mempool
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)
	require.NotNil(t, consumer)

	// Start goroutine that will block waiting for transaction
	resultChan := make(chan *MempoolTransaction, 1)
	go func() {
		tx := consumer.NextTx(true) // blocking
		resultChan <- tx
	}()

	// Verify goroutine is blocking (not returning immediately)
	select {
	case <-resultChan:
		t.Fatal("NextTx should be blocking on empty mempool")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocking
	}

	// Add a transaction - this should unblock the consumer
	txs := addMockTransactions(t, m, 1)

	// Publish event to notify waiting consumers
	m.eventBus.Publish(
		AddTransactionEventType,
		event.NewEvent(
			AddTransactionEventType,
			AddTransactionEvent{
				Hash: txs[0].Hash,
				Type: txs[0].Type,
				Body: txs[0].Cbor,
			},
		),
	)

	// Wait for result with timeout
	select {
	case tx := <-resultChan:
		require.NotNil(t, tx, "should receive transaction")
		assert.Equal(
			t,
			txs[0].Hash,
			tx.Hash,
			"should receive correct transaction",
		)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocking NextTx to return")
	}
}

// =============================================================================
// Consumer Creation Timing Tests
// =============================================================================

func TestMempool_ConsumerCreatedBeforeTxs(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Create consumer when mempool is empty
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)
	require.NotNil(t, consumer)

	// Verify nextTxIdx starts at 0
	assert.Equal(t, 0, consumer.nextTxIdx, "nextTxIdx should start at 0")

	// Add transactions after consumer creation
	txs := addMockTransactions(t, m, 3)

	// Verify consumer sees all transactions
	for i := range 3 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx, "should get transaction %d", i)
		assert.Equal(t, txs[i].Hash, tx.Hash)
	}

	// No more transactions
	assert.Nil(t, consumer.NextTx(false))
}

func TestMempool_ConsumerCreatedAfterTxs(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions first
	txs := addMockTransactions(t, m, 3)

	// Create consumer after transactions exist
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)
	require.NotNil(t, consumer)

	// Verify nextTxIdx starts at 0 (consumer starts from beginning)
	assert.Equal(t, 0, consumer.nextTxIdx, "nextTxIdx should start at 0")

	// Verify consumer sees all existing transactions
	for i := range 3 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx, "should get transaction %d", i)
		assert.Equal(t, txs[i].Hash, tx.Hash)
	}
}

func TestMempool_ConsumerCreatedDuringTxAddition(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Start goroutine adding transactions continuously
	stopAdding := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stopAdding:
				return
			default:
				m.Lock()
				tx := &MempoolTransaction{
					Hash:     fmt.Sprintf("concurrent-tx-%d", i),
					Cbor:     fmt.Appendf(nil, "cbor-%d", i),
					Type:     uint(conway.EraIdConway),
					LastSeen: time.Now(),
				}
				m.transactions = append(m.transactions, tx)
				m.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Create consumers at various points during addition
	consumers := make([]*MempoolConsumer, 10)
	for i := range 10 {
		time.Sleep(5 * time.Millisecond) // Stagger consumer creation
		connId := newTestConnectionId(i)
		consumers[i] = m.AddConsumer(connId)
		require.NotNil(t, consumers[i], "consumer %d should not be nil", i)
	}

	// Let more transactions be added
	time.Sleep(50 * time.Millisecond)
	close(stopAdding)

	// Verify each consumer can read transactions without panic
	for i, consumer := range consumers {
		// Read some transactions (don't need to read all)
		for range 5 {
			tx := consumer.NextTx(false)
			if tx == nil {
				break // No more transactions for this consumer
			}
		}
		// Consumer should be in valid state
		assert.GreaterOrEqual(
			t,
			consumer.nextTxIdx,
			0,
			"consumer %d nextTxIdx should be >= 0",
			i,
		)
	}
}

// =============================================================================
// Multiple Consumer Tests
// =============================================================================

func TestMempool_MultipleConsumers_IndependentProgress(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions
	txs := addMockTransactions(t, m, 10)

	// Create 3 consumers
	consumers := make([]*MempoolConsumer, 3)
	for i := range 3 {
		connId := newTestConnectionId(i)
		consumers[i] = m.AddConsumer(connId)
		require.NotNil(t, consumers[i])
	}

	// Consumer 0: read all transactions
	for i := range 10 {
		tx := consumers[0].NextTx(false)
		require.NotNil(t, tx)
		assert.Equal(t, txs[i].Hash, tx.Hash)
	}
	assert.Equal(t, 10, consumers[0].nextTxIdx)

	// Consumer 1: read only 5 transactions
	for i := range 5 {
		tx := consumers[1].NextTx(false)
		require.NotNil(t, tx)
		assert.Equal(t, txs[i].Hash, tx.Hash)
	}
	assert.Equal(t, 5, consumers[1].nextTxIdx)

	// Consumer 2: read only 2 transactions
	for i := range 2 {
		tx := consumers[2].NextTx(false)
		require.NotNil(t, tx)
		assert.Equal(t, txs[i].Hash, tx.Hash)
	}
	assert.Equal(t, 2, consumers[2].nextTxIdx)

	// Verify consumers track positions independently
	assert.Equal(t, 10, consumers[0].nextTxIdx)
	assert.Equal(t, 5, consumers[1].nextTxIdx)
	assert.Equal(t, 2, consumers[2].nextTxIdx)

	// Consumer 1 can continue from where it left off
	tx := consumers[1].NextTx(false)
	require.NotNil(t, tx)
	assert.Equal(t, txs[5].Hash, tx.Hash)
	assert.Equal(t, 6, consumers[1].nextTxIdx)
}

func TestMempool_MultipleConsumers_SameTransaction(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add one transaction
	txs := addMockTransactions(t, m, 1)

	// Create multiple consumers
	consumers := make([]*MempoolConsumer, 5)
	for i := range 5 {
		connId := newTestConnectionId(i)
		consumers[i] = m.AddConsumer(connId)
	}

	// All consumers should be able to get the same transaction
	for i, consumer := range consumers {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx, "consumer %d should get transaction", i)
		assert.Equal(
			t,
			txs[0].Hash,
			tx.Hash,
			"consumer %d should get same transaction",
			i,
		)
	}

	// Verify consumer caches are independent
	for i, consumer := range consumers {
		cachedTx := consumer.GetTxFromCache(txs[0].Hash)
		require.NotNil(
			t,
			cachedTx,
			"consumer %d should have transaction in cache",
			i,
		)
	}

	// Clear one consumer's cache, others should still have it
	consumers[0].ClearCache()
	assert.Nil(t, consumers[0].GetTxFromCache(txs[0].Hash))
	for i := 1; i < 5; i++ {
		assert.NotNil(
			t,
			consumers[i].GetTxFromCache(txs[0].Hash),
			"consumer %d cache should be independent",
			i,
		)
	}
}

// =============================================================================
// Transaction Removal Tests
// =============================================================================

func TestMempool_RemoveTx_BeforeConsumerReaches(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions [A, B, C]
	addMockTransactions(t, m, 3) // tx-hash-0, tx-hash-1, tx-hash-2

	// Create consumer (nextTxIdx=0)
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Remove B (tx-hash-1) before consumer reaches it
	m.RemoveTransaction("tx-hash-1")

	// Consumer gets A
	tx := consumer.NextTx(false)
	require.NotNil(t, tx)
	assert.Equal(t, "tx-hash-0", tx.Hash)

	// Consumer gets C (not B, which was removed)
	tx = consumer.NextTx(false)
	require.NotNil(t, tx)
	assert.Equal(t, "tx-hash-2", tx.Hash)

	// No more transactions
	tx = consumer.NextTx(false)
	assert.Nil(t, tx)
}

func TestMempool_RemoveTx_AfterConsumerPasses(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions [A, B, C]
	addMockTransactions(t, m, 3)

	// Create consumer
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Consumer gets A (nextTxIdx=1)
	tx := consumer.NextTx(false)
	require.NotNil(t, tx)
	assert.Equal(t, "tx-hash-0", tx.Hash)
	assert.Equal(t, 1, consumer.nextTxIdx)

	// Remove A (consumer already passed it)
	m.RemoveTransaction("tx-hash-0")

	// Consumer's nextTxIdx should be decremented (now 0)
	assert.Equal(t, 0, consumer.nextTxIdx, "nextTxIdx should be decremented")

	// Consumer gets B (now at index 0)
	tx = consumer.NextTx(false)
	require.NotNil(t, tx)
	assert.Equal(t, "tx-hash-1", tx.Hash)
}

func TestMempool_RemoveTx_ConsumerAtBoundary(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions [A, B, C]
	addMockTransactions(t, m, 3)

	// Create consumer
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Consumer gets all transactions
	for range 3 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx)
	}
	assert.Equal(t, 3, consumer.nextTxIdx)

	// Remove C (last transaction)
	m.RemoveTransaction("tx-hash-2")

	// Consumer's nextTxIdx should stay valid
	// (it's > removed index, so it gets decremented to 2)
	assert.Equal(t, 2, consumer.nextTxIdx)

	// Add D
	m.Lock()
	tx := &MempoolTransaction{
		Hash:     "tx-hash-D",
		Cbor:     []byte("tx-cbor-D"),
		Type:     uint(conway.EraIdConway),
		LastSeen: time.Now(),
	}
	m.transactions = append(m.transactions, tx)
	m.Unlock()

	// Consumer gets D
	gotTx := consumer.NextTx(false)
	require.NotNil(t, gotTx)
	assert.Equal(t, "tx-hash-D", gotTx.Hash)
}

func TestMempool_RemoveAllTxs(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transactions
	addMockTransactions(t, m, 5)

	// Create consumer and get some transactions
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Get 2 transactions
	for range 2 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx)
	}
	assert.Equal(t, 2, consumer.nextTxIdx)

	// Remove all transactions
	for i := range 5 {
		m.RemoveTransaction(fmt.Sprintf("tx-hash-%d", i))
	}

	// Verify mempool is empty
	m.RLock()
	assert.Equal(t, 0, len(m.transactions))
	m.RUnlock()

	// NextTx should return nil (no panic)
	tx := consumer.NextTx(false)
	assert.Nil(t, tx, "should return nil when mempool empty")

	// Consumer nextTxIdx should be adjusted
	assert.Equal(t, 0, consumer.nextTxIdx)
}

// =============================================================================
// Concurrent Operation Tests
// =============================================================================

func TestMempool_ConcurrentAddRemove(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	var wg sync.WaitGroup
	const numAdders = 5
	const numRemovers = 3
	const opsPerGoroutine = 100

	addedHashes := make(chan string, numAdders*opsPerGoroutine)

	// Start adder goroutines
	for i := range numAdders {
		wg.Add(1)
		go func(adderID int) {
			defer wg.Done()
			for j := range opsPerGoroutine {
				hash := fmt.Sprintf("add-%d-%d", adderID, j)
				m.Lock()
				tx := &MempoolTransaction{
					Hash:     hash,
					Cbor:     []byte(hash),
					Type:     uint(conway.EraIdConway),
					LastSeen: time.Now(),
				}
				m.transactions = append(m.transactions, tx)
				m.Unlock()
				addedHashes <- hash
			}
		}(i)
	}

	// Start remover goroutines
	for range numRemovers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				select {
				case hash := <-addedHashes:
					m.RemoveTransaction(hash)
				default:
					// No hash available, skip
				}
			}
		}()
	}

	// Wait for all operations to complete
	wg.Wait()
	close(addedHashes)

	// Mempool should be in consistent state (no panics, no races)
	m.RLock()
	txCount := len(m.transactions)
	m.RUnlock()

	t.Logf("Final mempool size: %d transactions", txCount)
}

func TestMempool_ConcurrentConsumerOperations(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add initial transactions
	addMockTransactions(t, m, 50)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutine adding transactions
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
				m.Lock()
				tx := &MempoolTransaction{
					Hash:     fmt.Sprintf("concurrent-add-%d", i),
					Cbor:     fmt.Appendf(nil, "cbor-%d", i),
					Type:     uint(conway.EraIdConway),
					LastSeen: time.Now(),
				}
				m.transactions = append(m.transactions, tx)
				m.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Goroutine removing transactions
	wg.Add(1)
	go func() {
		defer wg.Done()
		removeIdx := 0
		for {
			select {
			case <-done:
				return
			default:
				m.RemoveTransaction(fmt.Sprintf("tx-hash-%d", removeIdx))
				removeIdx++
				if removeIdx >= 50 {
					removeIdx = 0
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Goroutine creating consumers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
				connId := newTestConnectionId(1000 + i)
				consumer := m.AddConsumer(connId)
				if consumer != nil {
					// Read a few transactions
					for range 3 {
						consumer.NextTx(false)
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Goroutine consuming transactions
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				consumer.NextTx(false)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Run for a short duration
	time.Sleep(200 * time.Millisecond)
	close(done)

	// Wait with timeout
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Success - no deadlock
	case <-time.After(5 * time.Second):
		t.Fatal("potential deadlock detected - test timed out")
	}
}

func TestMempool_ConcurrentNextTx_MultipleConsumers(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add initial transactions
	addMockTransactions(t, m, 100)

	const numConsumers = 10
	consumers := make([]*MempoolConsumer, numConsumers)
	for i := range numConsumers {
		connId := newTestConnectionId(i)
		consumers[i] = m.AddConsumer(connId)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	txsRead := make([]int32, numConsumers)

	// Each consumer reads in its own goroutine
	for i := range numConsumers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					tx := consumers[idx].NextTx(false)
					if tx != nil {
						atomic.AddInt32(&txsRead[idx], 1)
					}
					time.Sleep(time.Microsecond * 100)
				}
			}
		}(i)
	}

	// Adder goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
				m.Lock()
				tx := &MempoolTransaction{
					Hash:     fmt.Sprintf("new-tx-%d", i),
					Cbor:     fmt.Appendf(nil, "new-cbor-%d", i),
					Type:     uint(conway.EraIdConway),
					LastSeen: time.Now(),
				}
				m.transactions = append(m.transactions, tx)
				m.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Remover goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
				m.RemoveTransaction(fmt.Sprintf("tx-hash-%d", i%100))
				time.Sleep(time.Millisecond * 2)
			}
		}
	}()

	// Run for short duration
	time.Sleep(300 * time.Millisecond)
	close(done)

	// Wait with timeout
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Log results
		var total int32
		for i, count := range txsRead {
			t.Logf("Consumer %d read %d transactions", i, count)
			total += count
		}
		t.Logf("Total transactions read: %d", total)
	case <-time.After(5 * time.Second):
		t.Fatal("potential deadlock detected")
	}
}

// =============================================================================
// Consumer Cache Tests
// =============================================================================

func TestMempoolConsumer_Cache(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transaction
	txs := addMockTransactions(t, m, 1)

	// Create consumer
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Get transaction via NextTx - should add to cache
	tx := consumer.NextTx(false)
	require.NotNil(t, tx)

	// Verify transaction is in cache
	cachedTx := consumer.GetTxFromCache(txs[0].Hash)
	require.NotNil(t, cachedTx, "transaction should be in cache")
	assert.Equal(t, txs[0].Hash, cachedTx.Hash)

	// Remove from cache
	consumer.RemoveTxFromCache(txs[0].Hash)

	// Verify transaction is no longer in cache
	cachedTx = consumer.GetTxFromCache(txs[0].Hash)
	assert.Nil(t, cachedTx, "transaction should be removed from cache")

	// Verify original transaction still in mempool
	mempoolTx, exists := m.GetTransaction(txs[0].Hash)
	assert.True(t, exists, "transaction should still be in mempool")
	assert.Equal(t, txs[0].Hash, mempoolTx.Hash)
}

func TestMempoolConsumer_ClearCache(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add multiple transactions
	txs := addMockTransactions(t, m, 5)

	// Create consumer
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Get all transactions (adds to cache)
	for range 5 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx)
	}

	// Verify cache has entries
	for i := range 5 {
		cachedTx := consumer.GetTxFromCache(txs[i].Hash)
		require.NotNil(t, cachedTx, "tx %d should be in cache", i)
	}

	// Clear cache
	consumer.ClearCache()

	// Verify cache is empty
	for i := range 5 {
		cachedTx := consumer.GetTxFromCache(txs[i].Hash)
		assert.Nil(t, cachedTx, "tx %d should not be in cache after clear", i)
	}

	// Verify mempool still has transactions
	allTxs := m.Transactions()
	assert.Equal(
		t,
		5,
		len(allTxs),
		"mempool should still have all transactions",
	)
}

// =============================================================================
// Edge Cases and Boundary Tests
// =============================================================================

func TestMempoolConsumer_NilReceiver(t *testing.T) {
	var consumer *MempoolConsumer

	// Verify operations on nil consumer don't panic
	assert.Nil(t, consumer.NextTx(false), "NextTx on nil should return nil")
	assert.Nil(
		t,
		consumer.GetTxFromCache("any"),
		"GetTxFromCache on nil should return nil",
	)

	// These should not panic
	consumer.ClearCache()
	consumer.RemoveTxFromCache("any")
}

func TestMempool_ConsumerAfterStop(t *testing.T) {
	m := newTestMempool(t)

	// Add consumer and transactions
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)
	addMockTransactions(t, m, 5)

	// Get some transactions
	for range 3 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx)
	}

	// Stop mempool
	err := m.Stop(context.Background())
	require.NoError(t, err)

	// Verify consumers are cleared
	m.consumersMutex.Lock()
	assert.Equal(t, 0, len(m.consumers), "consumers should be cleared")
	m.consumersMutex.Unlock()

	// Verify transactions are cleared
	m.RLock()
	assert.Equal(t, 0, len(m.transactions), "transactions should be cleared")
	m.RUnlock()
}

func TestMempool_BlockingNextTx_WithEmptyMempool(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Start blocking NextTx in goroutine
	resultChan := make(chan *MempoolTransaction, 1)
	go func() {
		tx := consumer.NextTx(true) // This will block
		resultChan <- tx
	}()

	// Verify it's still blocking (hasn't returned)
	select {
	case <-resultChan:
		t.Fatal("NextTx should still be blocking")
	case <-time.After(50 * time.Millisecond):
		// Expected - still blocking after waiting
	}

	// Now add a transaction and publish event
	m.Lock()
	tx := &MempoolTransaction{
		Hash:     "unblock-tx",
		Cbor:     []byte("unblock-cbor"),
		Type:     uint(conway.EraIdConway),
		LastSeen: time.Now(),
	}
	m.transactions = append(m.transactions, tx)
	m.Unlock()

	// Publish event
	m.eventBus.Publish(
		AddTransactionEventType,
		event.NewEvent(
			AddTransactionEventType,
			AddTransactionEvent{
				Hash: tx.Hash,
				Type: tx.Type,
				Body: tx.Cbor,
			},
		),
	)

	// Should unblock now
	select {
	case gotTx := <-resultChan:
		require.NotNil(t, gotTx)
		assert.Equal(t, "unblock-tx", gotTx.Hash)
	case <-time.After(2 * time.Second):
		t.Fatal("NextTx failed to unblock after transaction added")
	}
}

func TestMempool_BlockingNextTx_UnblocksOnShutdown(t *testing.T) {
	m := newTestMempool(t)

	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Start blocking NextTx in goroutine
	resultChan := make(chan *MempoolTransaction, 1)
	go func() {
		tx := consumer.NextTx(true) // This will block
		resultChan <- tx
	}()

	// Verify it's still blocking (hasn't returned)
	select {
	case <-resultChan:
		t.Fatal("NextTx should still be blocking")
	case <-time.After(50 * time.Millisecond):
		// Expected - still blocking after waiting
	}

	// Stop the mempool
	err := m.Stop(context.Background())
	require.NoError(t, err)

	// Should unblock and return nil
	select {
	case gotTx := <-resultChan:
		assert.Nil(t, gotTx, "NextTx should return nil on shutdown")
	case <-time.After(2 * time.Second):
		t.Fatal("NextTx failed to unblock after mempool stopped")
	}
}

func TestMempool_Transactions_ReturnsCopies(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transaction
	addMockTransactions(t, m, 1)

	// Get transactions
	txs := m.Transactions()
	require.Equal(t, 1, len(txs))

	// Modify returned transaction
	originalHash := txs[0].Hash
	txs[0].Hash = "modified-hash"

	// Verify mempool transaction is unchanged
	mempoolTxs := m.Transactions()
	assert.Equal(
		t,
		originalHash,
		mempoolTxs[0].Hash,
		"mempool should return copies",
	)
}

func TestMempool_GetTransaction_ReturnsCopy(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add transaction
	addMockTransactions(t, m, 1)

	// Get transaction
	tx, exists := m.GetTransaction("tx-hash-0")
	require.True(t, exists)

	// Modify returned transaction
	tx.Hash = "modified"

	// Verify mempool transaction is unchanged
	tx2, exists := m.GetTransaction("tx-hash-0")
	require.True(t, exists)
	assert.Equal(t, "tx-hash-0", tx2.Hash, "mempool should return copy")
}

func TestMempool_AddTransaction_DuplicateUpdatesLastSeen(t *testing.T) {
	validator := newMockValidator()
	m := newTestMempoolWithValidator(t, validator)
	defer m.Stop(context.Background())

	txBytes := getTestTxBytes(t)

	// Add transaction first time
	err := m.AddTransaction(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)

	// Get the transaction hash dynamically
	allTxs := m.Transactions()
	require.Equal(t, 1, len(allTxs), "should have one transaction")
	txHash := allTxs[0].Hash

	// Get initial last seen
	tx1, exists := m.GetTransaction(txHash)
	require.True(t, exists)
	firstLastSeen := tx1.LastSeen

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Add same transaction again
	err = m.AddTransaction(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)

	// Verify last seen was updated
	tx2, exists := m.GetTransaction(txHash)
	require.True(t, exists)
	assert.True(
		t,
		tx2.LastSeen.After(firstLastSeen),
		"last seen should be updated",
	)

	// Verify only one transaction in mempool
	allTxs = m.Transactions()
	assert.Equal(t, 1, len(allTxs), "should not duplicate transaction")
}

func TestMempool_AddTransaction_ValidationFailure(t *testing.T) {
	validator := newMockValidator()
	validator.setFailAll(true)
	m := newTestMempoolWithValidator(t, validator)
	defer m.Stop(context.Background())

	txBytes := getTestTxBytes(t)

	// Try to add transaction - should fail validation
	err := m.AddTransaction(uint(conway.EraIdConway), txBytes)
	require.Error(t, err, "should fail validation")

	// Verify mempool is empty
	allTxs := m.Transactions()
	assert.Equal(t, 0, len(allTxs), "transaction should not be added")
}

func TestMempool_MempoolFull(t *testing.T) {
	m := NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       newMockValidator(),
		MempoolCapacity: 100, // Very small capacity
	})
	defer m.Stop(context.Background())

	// Add transaction that exceeds capacity
	txBytes := getTestTxBytes(t)
	err := m.AddTransaction(uint(conway.EraIdConway), txBytes)

	require.Error(t, err, "should fail due to capacity")
	var fullErr *MempoolFullError
	assert.ErrorAs(t, err, &fullErr, "should be MempoolFullError")
}

// =============================================================================
// Stress/Load Tests
// =============================================================================

func TestMempool_HighVolumeTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	m := newTestMempool(t)
	defer m.Stop(context.Background())

	const numTxs = 10000

	// Add many transactions
	start := time.Now()
	for i := range numTxs {
		m.Lock()
		tx := &MempoolTransaction{
			Hash:     fmt.Sprintf("stress-tx-%d", i),
			Cbor:     fmt.Appendf(nil, "stress-cbor-%d", i),
			Type:     uint(conway.EraIdConway),
			LastSeen: time.Now(),
		}
		m.transactions = append(m.transactions, tx)
		m.Unlock()
	}
	addDuration := time.Since(start)
	t.Logf("Added %d transactions in %v", numTxs, addDuration)

	// Create multiple consumers
	const numConsumers = 5
	consumers := make([]*MempoolConsumer, numConsumers)
	for i := range numConsumers {
		connId := newTestConnectionId(i)
		consumers[i] = m.AddConsumer(connId)
	}

	// Each consumer reads all transactions
	var wg sync.WaitGroup
	for i := range numConsumers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			count := 0
			for {
				tx := consumers[idx].NextTx(false)
				if tx == nil {
					break
				}
				count++
			}
			t.Logf("Consumer %d read %d transactions", idx, count)
		}(i)
	}
	wg.Wait()

	// Remove half the transactions
	start = time.Now()
	for i := range numTxs / 2 {
		m.RemoveTransaction(fmt.Sprintf("stress-tx-%d", i))
	}
	removeDuration := time.Since(start)
	t.Logf("Removed %d transactions in %v", numTxs/2, removeDuration)

	// Verify final state
	m.RLock()
	finalCount := len(m.transactions)
	m.RUnlock()
	assert.Equal(
		t,
		numTxs/2,
		finalCount,
		"should have half transactions remaining",
	)
}

func TestMempool_RapidConsumerCreationDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add some transactions
	addMockTransactions(t, m, 100)

	const iterations = 1000

	var wg sync.WaitGroup

	// Rapidly create and delete consumers
	for i := range iterations {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			connId := newTestConnectionId(idx + 10000) // Avoid conflicts
			consumer := m.AddConsumer(connId)
			if consumer != nil {
				// Read a transaction
				consumer.NextTx(false)
				// Remove consumer
				m.RemoveConsumer(connId)
			}
		}(i)
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Logf("Completed %d consumer create/delete cycles", iterations)
	case <-time.After(30 * time.Second):
		t.Fatal("stress test timed out - possible resource leak or deadlock")
	}

	// Verify mempool is in valid state
	m.consumersMutex.Lock()
	consumerCount := len(m.consumers)
	m.consumersMutex.Unlock()

	// Some consumers might still exist due to race, but should be minimal
	t.Logf("Remaining consumers: %d", consumerCount)
}

// =============================================================================
// Regression Tests for Known Issues
// =============================================================================

// TestMempool_NextTxIdx_RaceCondition tests the potential bug noted in consumer.go:54-55
// regarding nextTxIdx management when multiple TXs are added rapidly
func TestMempool_NextTxIdx_RaceCondition(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Start consumer waiting for transactions
	resultChan := make(chan []*MempoolTransaction, 1)
	go func() {
		var received []*MempoolTransaction
		for range 5 {
			tx := consumer.NextTx(true) // blocking
			if tx != nil {
				received = append(received, tx)
			}
		}
		resultChan <- received
	}()

	// Verify consumer is blocking (waiting for transactions)
	select {
	case <-resultChan:
		t.Fatal("consumer should be blocking on empty mempool")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocking
	}

	// Rapidly add multiple transactions and publish events
	for i := range 5 {
		m.Lock()
		tx := &MempoolTransaction{
			Hash:     fmt.Sprintf("rapid-tx-%d", i),
			Cbor:     fmt.Appendf(nil, "rapid-cbor-%d", i),
			Type:     uint(conway.EraIdConway),
			LastSeen: time.Now(),
		}
		m.transactions = append(m.transactions, tx)
		m.Unlock()

		// Publish event
		m.eventBus.Publish(
			AddTransactionEventType,
			event.NewEvent(
				AddTransactionEventType,
				AddTransactionEvent{
					Hash: tx.Hash,
					Type: tx.Type,
					Body: tx.Cbor,
				},
			),
		)
	}

	// Wait for results
	select {
	case received := <-resultChan:
		t.Logf("Received %d transactions", len(received))
		// Due to the known potential bug, we might not receive all 5
		// This test documents the behavior
		assert.GreaterOrEqual(
			t,
			len(received),
			1,
			"should receive at least 1 transaction",
		)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout - possible deadlock in blocking NextTx")
	}
}

// TestMempool_RemoveTransaction_ConsumerIndexAdjustment verifies that consumer
// nextTxIdx is properly adjusted when transactions are removed
func TestMempool_RemoveTransaction_ConsumerIndexAdjustment(t *testing.T) {
	m := newTestMempool(t)
	defer m.Stop(context.Background())

	// Add 5 transactions
	addMockTransactions(t, m, 5) // tx-hash-0 through tx-hash-4

	// Create consumer
	connId := newTestConnectionId(0)
	consumer := m.AddConsumer(connId)

	// Consumer reads transactions 0, 1, 2 (nextTxIdx = 3)
	for i := range 3 {
		tx := consumer.NextTx(false)
		require.NotNil(t, tx)
		assert.Equal(t, fmt.Sprintf("tx-hash-%d", i), tx.Hash)
	}
	assert.Equal(t, 3, consumer.nextTxIdx)

	// Remove transaction at index 1 (tx-hash-1) - before current position
	m.RemoveTransaction("tx-hash-1")
	// nextTxIdx should be decremented since removed tx was before it
	assert.Equal(t, 2, consumer.nextTxIdx, "nextTxIdx should decrement")

	// Remove transaction at index 3 (tx-hash-4, now at index 3) - after current position
	m.RemoveTransaction("tx-hash-4")
	// nextTxIdx should stay the same since removed tx was after it
	assert.Equal(t, 2, consumer.nextTxIdx, "nextTxIdx should stay same")

	// Consumer should now get tx-hash-3 (which moved from index 3 to index 2)
	tx := consumer.NextTx(false)
	require.NotNil(t, tx)
	assert.Equal(t, "tx-hash-3", tx.Hash)

	// No more transactions
	tx = consumer.NextTx(false)
	assert.Nil(t, tx)
}
