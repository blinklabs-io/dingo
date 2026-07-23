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

package mempool

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type backendContractFactory func(t *testing.T, config MempoolConfig) Pool

func fifoContractFactory(t *testing.T, config MempoolConfig) Pool {
	t.Helper()
	config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	config.PromRegistry = prometheus.NewRegistry()
	pool, err := New(ImplementationFIFO, config)
	require.NoError(t, err)
	return pool
}

func newBackendContractPool(
	t *testing.T,
	factory backendContractFactory,
	capacity int64,
) Pool {
	t.Helper()
	return factory(t, MempoolConfig{
		EventBus:        event.NewEventBus(nil, nil),
		Validator:       newMockValidator(),
		MempoolCapacity: capacity,
	})
}

func getDependentTestTxBytes(t *testing.T) ([]byte, []byte, string, string) {
	t.Helper()
	parentBytes := getTestTxBytes(t)
	parent, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		parentBytes,
	)
	require.NoError(t, err)
	parentHash := parent.Hash().String()
	require.NotEmpty(t, parent.Produced())
	parentOutputHash := parent.Produced()[0].Id.Id().String()
	parentOutputHashBytes, err := hex.DecodeString(parentOutputHash)
	require.NoError(t, err)

	const originalInputHash = "0c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8"
	originalInputHashBytes, err := hex.DecodeString(originalInputHash)
	require.NoError(t, err)
	inputOffset := bytes.Index(parentBytes, originalInputHashBytes)
	require.NotEqual(t, -1, inputOffset)

	childBytes := bytes.Clone(parentBytes)
	copy(childBytes[inputOffset:], parentOutputHashBytes)
	child, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		childBytes,
	)
	require.NoError(t, err)
	return parentBytes, childBytes, parentHash, child.Hash().String()
}

// runBackendContract is intentionally backend-neutral so any Pool
// implementation must pass the same suite.
func runBackendContract(t *testing.T, factory backendContractFactory) {
	t.Helper()
	txBytes := getTestTxBytes(t)

	t.Run("admission snapshot removal and lifecycle", func(t *testing.T) {
		pool := newBackendContractPool(t, factory, 1<<20)
		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), txBytes),
		)

		txs := pool.Transactions()
		require.Len(t, txs, 1)
		hash := txs[0].Hash
		txs[0].Cbor[0] ^= 0xff
		stored, ok := pool.GetTransaction(hash)
		require.True(t, ok)
		assert.Equal(t, txBytes, stored.Cbor, "snapshots must be immutable")

		pool.RemoveTxsByHash([]string{hash})
		assert.Empty(t, pool.Transactions())
		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), txBytes),
		)
		pool.RemoveTransaction(hash)
		assert.Empty(t, pool.Transactions())
		require.NoError(t, pool.Stop(context.Background()))
		require.NoError(t, pool.Stop(context.Background()))
		require.ErrorIs(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), txBytes),
			ErrMempoolStopped,
		)
		assert.Nil(t, pool.AddConsumer(newTestConnectionId(99)))
	})

	t.Run("relay cursor and advertised cache", func(t *testing.T) {
		pool := newBackendContractPool(t, factory, 1<<20)
		t.Cleanup(func() { _ = pool.Stop(context.Background()) })
		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), txBytes),
		)
		consumer := pool.AddConsumer(newTestConnectionId(1))
		require.NotNil(t, consumer)
		tx := consumer.NextTx(false)
		require.NotNil(t, tx)
		assert.Nil(t, consumer.NextTx(false))
		cached := consumer.GetTxFromCache(tx.Hash)
		require.NotNil(t, cached)
		cached.Cbor[0] ^= 0xff
		assert.Equal(t, tx.Cbor, consumer.GetTxFromCache(tx.Hash).Cbor)
		consumer.RemoveTxFromCache(tx.Hash)
		assert.Nil(t, consumer.GetTxFromCache(tx.Hash))
	})

	t.Run("dependent relay removal and events", func(t *testing.T) {
		parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(
			t,
		)
		const originalInputHash = "0c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8"
		baseInput := buildMockInput(t, originalInputHash, 1)
		bus := event.NewEventBus(nil, nil)
		pool := factory(t, MempoolConfig{
			EventBus: bus,
			Validator: newOverlayValidator(map[string]lcommon.Utxo{
				originalInputHash + ":1": {
					Id:     baseInput,
					Output: buildMockOutput(t, 50_000_000),
				},
			}),
			MempoolCapacity: 1 << 20,
		})
		t.Cleanup(func() { _ = pool.Stop(context.Background()) })
		_, addEvents := bus.Subscribe(AddTransactionEventType)
		_, removeEvents := bus.Subscribe(RemoveTransactionEventType)

		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
		)
		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), childBytes),
		)
		for _, wantHash := range []string{parentHash, childHash} {
			evt := dingotestutil.RequireReceive(
				t,
				addEvents,
				time.Second,
				"add transaction event",
			)
			addEvt, ok := evt.Data.(AddTransactionEvent)
			require.True(t, ok)
			assert.Equal(t, wantHash, addEvt.Hash)
		}

		consumer := pool.AddConsumer(newTestConnectionId(2))
		require.NotNil(t, consumer)
		parentRelay := consumer.NextTx(false)
		require.NotNil(t, parentRelay)
		assert.Equal(t, parentHash, parentRelay.Hash)
		childRelay := consumer.NextTx(false)
		require.NotNil(t, childRelay)
		assert.Equal(t, childHash, childRelay.Hash)
		assert.Nil(t, consumer.NextTx(false))

		pool.RemoveTransaction(parentHash)
		assert.Empty(t, pool.Transactions())
		removed := make(map[string]struct{}, 2)
		for range 2 {
			evt := dingotestutil.RequireReceive(
				t,
				removeEvents,
				time.Second,
				"remove transaction event",
			)
			removeEvt, ok := evt.Data.(RemoveTransactionEvent)
			require.True(t, ok)
			removed[removeEvt.Hash] = struct{}{}
		}
		assert.Equal(t, map[string]struct{}{
			parentHash: {},
			childHash:  {},
		}, removed)
	})

	t.Run("capacity rejection", func(t *testing.T) {
		pool := newBackendContractPool(t, factory, int64(len(txBytes)-1))
		t.Cleanup(func() { _ = pool.Stop(context.Background()) })
		assert.Equal(t, int64(len(txBytes)-1), pool.CapacityBytes())
		err := pool.AddTransaction(uint(conway.EraIdConway), txBytes)
		var fullErr *MempoolFullError
		require.ErrorAs(t, err, &fullErr)
		assert.Empty(t, pool.Transactions())
	})

	t.Run("concurrent duplicate admission", func(t *testing.T) {
		pool := newBackendContractPool(t, factory, 1<<20)
		t.Cleanup(func() { _ = pool.Stop(context.Background()) })
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		for range 16 {
			wg.Go(func() {
				errs <- pool.AddTransaction(uint(conway.EraIdConway), txBytes)
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assert.Len(t, pool.Transactions(), 1)
	})
}

func TestFIFOBackendContract(t *testing.T) {
	runBackendContract(t, fifoContractFactory)

	t.Run("revalidation", func(t *testing.T) {
		const inputHash = "0c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8"
		inputKey := inputHash + ":1"
		input := buildMockInput(t, inputHash, 1)
		validator := newOverlayValidator(map[string]lcommon.Utxo{
			inputKey: {Id: input, Output: buildMockOutput(t, 50_000_000)},
		})
		pool, err := NewFIFO(MempoolConfig{
			Validator:       validator,
			MempoolCapacity: 1 << 20,
			PromRegistry:    prometheus.NewRegistry(),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = pool.Stop(context.Background()) })
		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), getTestTxBytes(t)),
		)
		validator.removeBaseUtxo(inputKey)
		require.NoError(t, pool.rebuildOverlay())
		assert.Empty(t, pool.Transactions())
	})
}

func TestBackendFactorySelection(t *testing.T) {
	config := MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	}

	pool, err := New("", config)
	require.NoError(t, err)
	assert.Equal(t, ImplementationFIFO, pool.Implementation())
	require.NoError(t, pool.Stop(context.Background()))

	pool, err = New(Implementation("priority"), config)
	assert.Nil(t, pool)
	assert.ErrorContains(t, err, "unknown mempool implementation")
}

func TestFIFOBackendIdentityMetric(t *testing.T) {
	registry := prometheus.NewRegistry()
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 20,
		PromRegistry:    registry,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_metrics_mempool_info" {
			continue
		}
		require.Len(t, family.Metric, 1)
		labels := family.Metric[0].Label
		require.Len(t, labels, 1)
		assert.Equal(t, "implementation", labels[0].GetName())
		assert.Equal(t, "fifo", labels[0].GetValue())
		assert.Equal(t, float64(1), family.Metric[0].Gauge.GetValue())
		return
	}
	t.Fatal("dingo_metrics_mempool_info was not registered")
}
