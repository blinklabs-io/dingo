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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type oneShotBlockingValidator struct {
	blockNext        atomic.Bool
	advanceEveryCall atomic.Bool
	generation       atomic.Uint64
	calls            atomic.Uint64
	started          chan struct{}
	release          chan struct{}
	startOnce        sync.Once
}

type countingFailValidator struct {
	failHash string
	calls    int
}

func (v *countingFailValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (v *countingFailValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[string]struct{},
	_ map[string]lcommon.Utxo,
) error {
	v.calls++
	if tx.Hash().String() == v.failHash {
		return errors.New("rejected for test")
	}
	return nil
}

func newOneShotBlockingValidator() *oneShotBlockingValidator {
	return &oneShotBlockingValidator{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (v *oneShotBlockingValidator) arm() {
	v.blockNext.Store(true)
}

func (v *oneShotBlockingValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (v *oneShotBlockingValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[string]struct{},
	map[string]lcommon.Utxo,
) error {
	v.calls.Add(1)
	if v.advanceEveryCall.Load() {
		v.generation.Add(1)
	}
	if v.blockNext.CompareAndSwap(true, false) {
		v.startOnce.Do(func() { close(v.started) })
		<-v.release
	}
	return nil
}

func (v *oneShotBlockingValidator) WithTxValidationSession(
	fn func(
		validate func(
			gledger.Transaction,
			map[string]struct{},
			map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error,
) error {
	generation := v.generation.Load()
	return fn(
		v.ValidateTxWithOverlay,
		func() bool {
			return v.generation.Load() == generation
		},
	)
}

func graphTx(hash string, inputs []string, outputs ...string) appliedTx {
	created := make(map[string]lcommon.Utxo, len(outputs))
	for _, output := range outputs {
		created[output] = lcommon.Utxo{}
	}
	return appliedTx{
		hash:     hash,
		consumed: inputs,
		created:  created,
	}
}

func TestTransactionDAGTopologicalOrderAndDescendants(t *testing.T) {
	graph := newTransactionDAG()
	graph.add(graphTx("parent", []string{"base:0"}, "parent:0", "parent:1"))
	graph.add(graphTx("independent", []string{"base:1"}, "independent:0"))
	graph.add(graphTx("left", []string{"parent:0"}, "left:0"))
	graph.add(graphTx("right", []string{"parent:1"}, "right:0"))
	graph.add(graphTx("grandchild", []string{"left:0"}, "grandchild:0"))

	assert.Equal(
		t,
		[]string{"parent", "independent", "left", "right", "grandchild"},
		graph.topologicalOrder(),
	)
	assert.Equal(t, map[string]struct{}{
		"parent":     {},
		"left":       {},
		"right":      {},
		"grandchild": {},
	}, graph.descendants(map[string]struct{}{"parent": {}}))
	assert.Equal(t, "parent", graph.producerByUtxo["parent:0"])
	assert.Equal(t, "left", graph.spenderByUtxo["parent:0"])
}

func TestTransactionDAGConfirmedRemovalPreservesDescendants(t *testing.T) {
	graph := newTransactionDAG()
	graph.add(graphTx("parent", []string{"base:0"}, "parent:0"))
	graph.add(graphTx("child", []string{"parent:0"}, "child:0"))

	graph.remove(map[string]struct{}{"parent": {}})

	assert.Equal(t, []string{"child"}, graph.topologicalOrder())
	child := graph.nodes["child"]
	require.NotNil(t, child)
	assert.Empty(t, child.parents)
	assert.Equal(
		t,
		map[string]struct{}{"child": {}},
		graph.descendants(map[string]struct{}{"child": {}}),
	)
}

func TestDAGTracksAdmittedTransactionDependencies(t *testing.T) {
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	const originalInputHash = "0c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8"
	baseInput := buildMockInput(t, originalInputHash, 1)
	pool, err := NewDAG(MempoolConfig{
		Validator: newOverlayValidator(map[string]lcommon.Utxo{
			originalInputHash + ":1": {
				Id:     baseInput,
				Output: buildMockOutput(t, 50_000_000),
			},
		}),
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
	)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), childBytes),
	)

	require.NotNil(t, pool.dag)
	parentNode := pool.dag.nodes[parentHash]
	require.NotNil(t, parentNode)
	childNode := pool.dag.nodes[childHash]
	require.NotNil(t, childNode)
	require.Contains(t, parentNode.children, childHash)
	require.Contains(t, childNode.parents, parentHash)
	txs := pool.Transactions()
	require.Len(t, txs, 2)
	assert.Equal(t, parentHash, txs[0].Hash)
	assert.Equal(t, childHash, txs[1].Hash)

	pool.RemoveTxsByHash([]string{parentHash})
	txs = pool.Transactions()
	require.Len(t, txs, 1)
	assert.Equal(t, childHash, txs[0].Hash)
	childNode = pool.dag.nodes[childHash]
	require.NotNil(t, childNode)
	assert.Empty(t, childNode.parents)
}

func TestDAGDoesNotWatermarkEvict(t *testing.T) {
	parentBytes, childBytes, _, _ := getDependentTestTxBytes(t)
	const originalInputHash = "0c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8"
	totalSize := int64(len(parentBytes) + len(childBytes))
	capacity := totalSize
	for totalSize > int64(float64(capacity)*DefaultRejectionWatermark) ||
		totalSize <= int64(float64(capacity)*DefaultEvictionWatermark) {
		capacity++
	}
	pool, err := NewDAG(MempoolConfig{
		Validator: newOverlayValidator(map[string]lcommon.Utxo{
			originalInputHash + ":1": {
				Id:     buildMockInput(t, originalInputHash, 1),
				Output: buildMockOutput(t, 50_000_000),
			},
		}),
		MempoolCapacity: capacity,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
	)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), childBytes),
	)
	assert.Len(t, pool.Transactions(), 2)

	pool.Lock()
	pool.consumersMutex.Lock()
	assert.Empty(t, pool.evictOldestLocked(0))
	pool.consumersMutex.Unlock()
	pool.Unlock()
	assert.Len(t, pool.Transactions(), 2)
}

func TestDAGAdmissionHeadroomWaitsForRemoval(t *testing.T) {
	pool, err := NewDAG(MempoolConfig{
		Validator:          newMockValidator(),
		MempoolCapacity:    100,
		RejectionWatermark: 1,
		PromRegistry:       prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	pool.Lock()
	pool.currentSizeBytes = pool.admissionLimitBytes()
	pool.Unlock()
	assert.Zero(t, pool.AdmissionHeadroomBytes())
	assert.Equal(t, int64(100), pool.MaxAdmissionHeadroomBytes())

	entered := make(chan struct{})
	result := make(chan bool, 1)
	connectionDone := make(chan error)
	go func() {
		close(entered)
		result <- pool.WaitForAdmissionHeadroom(1, connectionDone)
	}()
	dingotestutil.RequireReceive(
		t,
		entered,
		time.Second,
		"headroom waiter start",
	)

	pool.Lock()
	pool.currentSizeBytes--
	pool.notifyHeadroomChangedLocked()
	pool.Unlock()
	assert.True(t, dingotestutil.RequireReceive(
		t,
		result,
		time.Second,
		"headroom waiter result",
	))
}

func TestDAGConcurrentSnapshotsAndMutations(t *testing.T) {
	txBytes := getTestTxBytes(t)
	decoded, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txBytes,
	)
	require.NoError(t, err)
	txHash := decoded.Hash().String()
	pool, err := NewDAG(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 8 {
		wg.Go(func() {
			for range 200 {
				for _, tx := range pool.Transactions() {
					_, _ = pool.GetTransaction(tx.Hash)
				}
			}
		})
	}
	wg.Go(func() {
		for range 200 {
			errs <- pool.AddTransaction(uint(conway.EraIdConway), txBytes)
			pool.RemoveTransaction(txHash)
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestDAGAdmissionContinuesDuringRevalidation(t *testing.T) {
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	validator := newOneShotBlockingValidator()
	pool, err := NewDAG(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
	)

	validator.arm()
	rebuildDone := make(chan error, 1)
	go func() { rebuildDone <- pool.rebuildOverlay() }()
	dingotestutil.RequireReceive(
		t,
		validator.started,
		time.Second,
		"DAG revalidation start",
	)

	addDone := make(chan error, 1)
	go func() {
		addDone <- pool.AddTransaction(
			uint(conway.EraIdConway),
			childBytes,
		)
	}()
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		addDone,
		time.Second,
		"admission during DAG revalidation",
	))

	close(validator.release)
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		rebuildDone,
		time.Second,
		"DAG revalidation completion",
	))
	txs := pool.Transactions()
	require.Len(t, txs, 2)
	assert.Equal(t, parentHash, txs[0].Hash)
	assert.Equal(t, childHash, txs[1].Hash)
}

func TestDAGRemovalContinuesDuringRevalidation(t *testing.T) {
	parentBytes, childBytes, parentHash, _ := getDependentTestTxBytes(t)
	validator := newOneShotBlockingValidator()
	pool, err := NewDAG(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
	)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), childBytes),
	)

	validator.arm()
	rebuildDone := make(chan error, 1)
	go func() { rebuildDone <- pool.rebuildOverlay() }()
	dingotestutil.RequireReceive(
		t,
		validator.started,
		time.Second,
		"DAG revalidation start",
	)

	removeDone := make(chan struct{}, 1)
	go func() {
		pool.RemoveTransaction(parentHash)
		removeDone <- struct{}{}
	}()
	dingotestutil.RequireReceive(
		t,
		removeDone,
		time.Second,
		"removal during DAG revalidation",
	)

	close(validator.release)
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		rebuildDone,
		time.Second,
		"DAG revalidation completion",
	))
	assert.Empty(t, pool.Transactions())
}

func TestDAGRevalidationRetriesAfterLedgerGenerationChange(t *testing.T) {
	txBytes := getTestTxBytes(t)
	validator := newOneShotBlockingValidator()
	pool, err := NewDAG(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), txBytes),
	)
	callsBeforeRevalidation := validator.calls.Load()

	validator.arm()
	rebuildDone := make(chan error, 1)
	go func() { rebuildDone <- pool.rebuildOverlay() }()
	dingotestutil.RequireReceive(
		t,
		validator.started,
		time.Second,
		"DAG revalidation start",
	)
	validator.generation.Add(1)
	close(validator.release)

	require.NoError(t, dingotestutil.RequireReceive(
		t,
		rebuildDone,
		time.Second,
		"DAG revalidation retry completion",
	))
	assert.GreaterOrEqual(
		t,
		validator.calls.Load()-callsBeforeRevalidation,
		uint64(2),
		"transaction should be validated again after the ledger generation changes",
	)
	assert.Len(t, pool.Transactions(), 1)
}

func TestDAGRevalidationBoundsLedgerGenerationRetries(t *testing.T) {
	txBytes := getTestTxBytes(t)
	validator := newOneShotBlockingValidator()
	pool, err := NewDAG(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), txBytes),
	)

	validator.advanceEveryCall.Store(true)
	err = pool.rebuildOverlay()
	require.ErrorIs(t, err, errValidationSnapshotChanged)
	assert.Len(t, pool.Transactions(), 1)
	assert.False(t, pool.journalActive)
}

func TestDAGRevalidationJournalOverflowLeavesLiveStateUntouched(t *testing.T) {
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	validator := newOneShotBlockingValidator()
	pool, err := NewDAG(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
	)
	pool.revalidationJournalCap = 1

	validator.arm()
	rebuildDone := make(chan error, 1)
	go func() { rebuildDone <- pool.rebuildOverlay() }()
	dingotestutil.RequireReceive(
		t,
		validator.started,
		time.Second,
		"DAG revalidation start",
	)
	pool.RemoveTransaction(parentHash)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), childBytes),
	)

	close(validator.release)
	require.ErrorIs(
		t,
		dingotestutil.RequireReceive(
			t,
			rebuildDone,
			time.Second,
			"DAG revalidation completion",
		),
		errRevalidationJournalOverflow,
	)
	txs := pool.Transactions()
	require.Len(t, txs, 1)
	assert.Equal(t, childHash, txs[0].Hash)
	assert.Contains(t, pool.dag.nodes, childHash)
	assert.NotContains(t, pool.dag.nodes, parentHash)
	assert.False(t, pool.journalActive)
}

func TestDAGRevalidationSkipsInvalidDescendantValidation(t *testing.T) {
	parentBytes, childBytes, parentHash, _ := getDependentTestTxBytes(t)
	validator := &countingFailValidator{}
	pool, err := NewDAG(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), parentBytes),
	)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), childBytes),
	)

	validator.failHash = parentHash
	validator.calls = 0
	require.NoError(t, pool.rebuildOverlay())

	assert.Equal(t, 1, validator.calls)
	assert.Empty(t, pool.Transactions())
	assert.Empty(t, pool.dag.nodes)
}

func TestMempoolProvidersIncludeFIFOAndDAG(t *testing.T) {
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	require.NoError(t, RegisterFIFOProvider(host))
	require.NoError(t, RegisterDAGProvider(host))

	descriptors := host.Providers()
	names := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.Capability == plugin.CapabilityMempool {
			names = append(names, descriptor.Name)
		}
	}
	assert.ElementsMatch(t, []string{"default", "fifo", "dag"}, names)

	service, err := plugin.Resolve[Service](
		context.Background(),
		host,
		plugin.CapabilityMempool,
		"dag",
		map[string]any{"capacity": 1 << 20},
		ProviderDependencies{
			PromRegistry: prometheus.NewRegistry(),
			Validator:    newMockValidator(),
		},
	)
	require.NoError(t, err)
	dagPool, ok := service.(*DAG)
	require.True(t, ok)
	assert.Equal(t, ImplementationDAG, dagPool.implementation)
	require.NoError(t, host.Stop(context.Background()))
}
