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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/plugin"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	benchmarkCapacity       = int64(1 << 30)
	benchmarkFanoutWidth    = 512
	benchmarkNormalChainGap = uint64(10)
)

var errBenchmarkDoubleSpend = errors.New(
	"benchmark transaction double-spends a pending input",
)

type benchmarkTopology string

const (
	benchmarkTopologyNormal      benchmarkTopology = "normal"
	benchmarkTopologyDeepChain   benchmarkTopology = "deep-chain"
	benchmarkTopologyWideFanout  benchmarkTopology = "wide-fanout"
	benchmarkTopologyConflicting benchmarkTopology = "conflicting"
)

type benchmarkWorkload struct {
	name     string
	topology benchmarkTopology
	prefill  int
}

type benchmarkValidator struct {
	applyCPU time.Duration
}

func (v benchmarkValidator) ValidateTx(tx gledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil)
}

func (v benchmarkValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	consumed map[string]struct{},
	_ map[string]lcommon.Utxo,
) error {
	for _, input := range tx.Inputs() {
		key := fmt.Sprintf(
			"%s:%d",
			input.Id().String(),
			input.Index(),
		)
		if _, ok := consumed[key]; ok {
			return fmt.Errorf("%w: %s", errBenchmarkDoubleSpend, key)
		}
	}
	if v.applyCPU <= 0 {
		return nil
	}
	start := time.Now()
	for time.Since(start) < v.applyCPU {
		runtime.Gosched()
	}
	return nil
}

type benchmarkPluginPool struct {
	host *plugin.Host
	pool Pool
	core *Mempool
}

type benchmarkScenarioResult struct {
	elapsed        time.Duration
	maxRead        time.Duration
	syncTotal      time.Duration
	added          uint64
	rejected       uint64
	reads          uint64
	syncs          uint64
	finalOccupancy int
}

type benchmarkTxGenerator struct {
	topology     benchmarkTopology
	adderID      int
	sequence     uint64
	previousHash string
	fanoutHash   string
	fanoutTips   []string
}

func TestBenchmarkTransactionShapes(t *testing.T) {
	deep := newBenchmarkTxGenerator(benchmarkTopologyDeepChain, 0)
	parentBytes, err := deep.next()
	if err != nil {
		t.Fatalf("create parent transaction: %v", err)
	}
	childBytes, err := deep.next()
	if err != nil {
		t.Fatalf("create child transaction: %v", err)
	}
	parent := decodeBenchmarkTransaction(t, parentBytes)
	child := decodeBenchmarkTransaction(t, childBytes)
	if len(child.Inputs()) == 0 {
		t.Fatal("generated child transaction has no inputs")
	}
	if len(parent.Produced()) == 0 {
		t.Fatal("generated parent transaction has no outputs")
	}
	if got, want := parent.Produced()[0].Id.Id().String(), parent.Hash().String(); got != want {
		t.Fatalf(
			"parent output transaction ID = %s, want parent hash %s",
			got,
			want,
		)
	}
	if got, want := child.Inputs()[0].Id().String(), parent.Hash().String(); got != want {
		t.Fatalf(
			"child input transaction ID = %s, want parent hash %s",
			got,
			want,
		)
	}

	fanout := newBenchmarkTxGenerator(benchmarkTopologyWideFanout, 1)
	fanoutParentBytes, fanoutHash, err := buildBenchmarkTransaction(
		benchmarkSeedHash(1, 0),
		0,
		1,
		benchmarkFanoutWidth,
	)
	if err != nil {
		t.Fatalf("create fanout parent transaction: %v", err)
	}
	fanout.fanoutHash = fanoutHash
	fanout.fanoutTips = make([]string, benchmarkFanoutWidth)
	firstBytes, err := fanout.next()
	if err != nil {
		t.Fatalf("create first fanout child: %v", err)
	}
	secondBytes, err := fanout.next()
	if err != nil {
		t.Fatalf("create second fanout child: %v", err)
	}
	fanoutParent := decodeBenchmarkTransaction(t, fanoutParentBytes)
	first := decodeBenchmarkTransaction(t, firstBytes)
	second := decodeBenchmarkTransaction(t, secondBytes)
	if got, want := len(fanoutParent.Outputs()), benchmarkFanoutWidth; got != want {
		t.Fatalf("fanout parent outputs = %d, want %d", got, want)
	}
	firstInputs := first.Inputs()
	secondInputs := second.Inputs()
	if len(firstInputs) == 0 || len(secondInputs) == 0 {
		t.Fatal("generated fanout child has no inputs")
	}
	if got, want := firstInputs[0].Id().String(), fanoutParent.Hash().String(); got != want {
		t.Fatalf(
			"first fanout input transaction ID = %s, want parent hash %s",
			got,
			want,
		)
	}
	if firstInputs[0].Index() == secondInputs[0].Index() {
		t.Fatal("fanout children consume the same parent output")
	}

	pool, err := NewDAG(MempoolConfig{
		Validator:       benchmarkValidator{},
		MempoolCapacity: benchmarkCapacity,
		PromRegistry:    prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("create DAG pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	if err := pool.AddTransaction(
		uint(conway.EraIdConway),
		parentBytes,
	); err != nil {
		t.Fatalf("add parent transaction: %v", err)
	}
	if err := pool.AddTransaction(
		uint(conway.EraIdConway),
		childBytes,
	); err != nil {
		t.Fatalf("add child transaction: %v", err)
	}
	descendants := pool.dag.descendants(
		map[string]struct{}{parent.Hash().String(): {}},
	)
	if got, want := len(descendants), 2; got != want {
		t.Fatalf(
			"DAG descendants = %d, want %d",
			got,
			want,
		)
	}
	for _, item := range []struct {
		name    string
		txBytes []byte
	}{
		{name: "fanout parent", txBytes: fanoutParentBytes},
		{name: "first fanout child", txBytes: firstBytes},
		{name: "second fanout child", txBytes: secondBytes},
	} {
		if err := pool.AddTransaction(
			uint(conway.EraIdConway),
			item.txBytes,
		); err != nil {
			t.Fatalf("add %s: %v", item.name, err)
		}
	}
	fanoutDescendants := pool.dag.descendants(
		map[string]struct{}{fanoutParent.Hash().String(): {}},
	)
	if got, want := len(fanoutDescendants), 3; got != want {
		t.Fatalf("fanout DAG descendants = %d, want %d", got, want)
	}

	firstConflict := newBenchmarkTxGenerator(
		benchmarkTopologyConflicting,
		10,
	)
	secondConflict := newBenchmarkTxGenerator(
		benchmarkTopologyConflicting,
		11,
	)
	firstConflictBytes, err := firstConflict.next()
	if err != nil {
		t.Fatalf("create first conflicting transaction: %v", err)
	}
	secondConflictBytes, err := secondConflict.next()
	if err != nil {
		t.Fatalf("create second conflicting transaction: %v", err)
	}
	if err := pool.AddTransaction(
		uint(conway.EraIdConway),
		firstConflictBytes,
	); err != nil {
		t.Fatalf("add first conflicting transaction: %v", err)
	}
	err = pool.AddTransaction(
		uint(conway.EraIdConway),
		secondConflictBytes,
	)
	if !errors.Is(err, errBenchmarkDoubleSpend) {
		t.Fatalf("second conflicting transaction error = %v", err)
	}
}

// BenchmarkMempoolPlugins is Dingo's counterpart to the concurrent
// mempool-state-bench introduced by ouroboros-consensus PR #2148. It drives
// the actual FIFO and DAG plugin providers with concurrent adders, snapshot
// readers, and overlay rebuilds over the same offered-load/peer-count matrix.
//
// Override the one-second scenario duration for sustained runs, for example:
//
//	DINGO_MEMPOOL_BENCH_DURATION=20s make bench-mempool
//
// Use bench-mempool-normal or bench-mempool-degenerate to run only one suite.
func BenchmarkMempoolPlugins(b *testing.B) {
	config := readMempoolBenchmarkConfig(b)
	workload := benchmarkWorkload{
		name:     "normal",
		topology: benchmarkTopologyNormal,
	}
	for _, peers := range []int{2, 20, 100} {
		for _, targetTPS := range []int{100, 1_000, 10_000} {
			workloadName := fmt.Sprintf(
				"peers=%d/tps=%d",
				peers,
				targetTPS,
			)
			b.Run(workloadName, func(b *testing.B) {
				for _, implementation := range []Implementation{
					ImplementationFIFO,
					ImplementationDAG,
				} {
					b.Run(string(implementation), func(b *testing.B) {
						var aggregate benchmarkScenarioResult
						for range b.N {
							result := runMempoolBenchmarkScenario(
								b,
								implementation,
								workload,
								peers,
								targetTPS,
								config,
							)
							aggregate.elapsed += result.elapsed
							aggregate.maxRead = max(
								aggregate.maxRead,
								result.maxRead,
							)
							aggregate.syncTotal += result.syncTotal
							aggregate.added += result.added
							aggregate.rejected += result.rejected
							aggregate.reads += result.reads
							aggregate.syncs += result.syncs
							aggregate.finalOccupancy +=
								result.finalOccupancy
						}
						reportMempoolBenchmarkMetrics(b, aggregate)
					})
				}
			})
		}
	}
}

// BenchmarkMempoolPluginsDegenerate compares pathological dependency shapes
// at an intentionally oversubscribed offered load. The large-occupancy case
// starts from a prefilled pool, so setup is excluded from the timed interval.
func BenchmarkMempoolPluginsDegenerate(b *testing.B) {
	config := readMempoolBenchmarkConfig(b)
	prefill := benchmarkEnvInt(
		b,
		"DINGO_MEMPOOL_BENCH_PREFILL",
		5_000,
	)
	workloads := []benchmarkWorkload{
		{name: "deep-chain", topology: benchmarkTopologyDeepChain},
		{name: "wide-fanout", topology: benchmarkTopologyWideFanout},
		{name: "conflicting", topology: benchmarkTopologyConflicting},
		{
			name:     "large-occupancy",
			topology: benchmarkTopologyNormal,
			prefill:  prefill,
		},
	}
	const (
		degeneratePeers = 20
		degenerateTPS   = 10_000
	)
	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			for _, implementation := range []Implementation{
				ImplementationFIFO,
				ImplementationDAG,
			} {
				b.Run(string(implementation), func(b *testing.B) {
					var aggregate benchmarkScenarioResult
					for range b.N {
						result := runMempoolBenchmarkScenario(
							b,
							implementation,
							workload,
							degeneratePeers,
							degenerateTPS,
							config,
						)
						aggregate.elapsed += result.elapsed
						aggregate.maxRead = max(
							aggregate.maxRead,
							result.maxRead,
						)
						aggregate.syncTotal += result.syncTotal
						aggregate.added += result.added
						aggregate.rejected += result.rejected
						aggregate.reads += result.reads
						aggregate.syncs += result.syncs
						aggregate.finalOccupancy +=
							result.finalOccupancy
					}
					reportMempoolBenchmarkMetrics(b, aggregate)
				})
			}
		})
	}
	b.Run("removal-cascade", func(b *testing.B) {
		for _, implementation := range []Implementation{
			ImplementationFIFO,
			ImplementationDAG,
		} {
			b.Run(string(implementation), func(b *testing.B) {
				benchmarkRemovalCascade(
					b,
					implementation,
					prefill,
					config.applyCPU,
				)
			})
		}
	})
}

type mempoolBenchmarkConfig struct {
	duration   time.Duration
	syncPeriod time.Duration
	readPeriod time.Duration
	applyCPU   time.Duration
}

func runMempoolBenchmarkScenario(
	b *testing.B,
	implementation Implementation,
	workload benchmarkWorkload,
	peers int,
	targetTPS int,
	config mempoolBenchmarkConfig,
) benchmarkScenarioResult {
	b.Helper()
	b.StopTimer()
	pluginPool := newBenchmarkPluginPool(
		b,
		implementation,
		config.applyCPU,
	)
	adders := peers + 1
	if workload.topology == benchmarkTopologyDeepChain {
		adders = 1
	}
	generators, err := prepareBenchmarkGenerators(
		pluginPool.pool,
		workload.topology,
		adders,
	)
	if err != nil {
		_ = pluginPool.host.Stop(context.Background())
		b.Fatalf("prepare %s benchmark: %v", workload.name, err)
	}
	if err := prefillBenchmarkPool(
		pluginPool.pool,
		workload.prefill,
	); err != nil {
		_ = pluginPool.host.Stop(context.Background())
		b.Fatalf("prefill %s benchmark: %v", workload.name, err)
	}
	preparedTxs, err := prepareBenchmarkTransactions(
		generators,
		targetTPS,
		config.duration,
	)
	if err != nil {
		_ = pluginPool.host.Stop(context.Background())
		b.Fatalf(
			"generate %s benchmark transactions: %v",
			workload.name,
			err,
		)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		config.duration,
	)
	defer cancel()
	var (
		added     atomic.Uint64
		rejected  atomic.Uint64
		reads     atomic.Uint64
		syncs     atomic.Uint64
		maxReadNS atomic.Int64
		syncNS    atomic.Int64
		wg        sync.WaitGroup
	)
	errCh := make(chan error, peers+2)
	b.StartTimer()
	start := time.Now()

	for range peers {
		wg.Go(func() {
			runBenchmarkReader(
				ctx,
				pluginPool.pool,
				config.readPeriod,
				&reads,
				&maxReadNS,
			)
		})
	}

	wg.Go(func() {
		runBenchmarkSyncer(
			ctx,
			pluginPool.core,
			config.syncPeriod,
			&syncs,
			&syncNS,
			errCh,
		)
	})

	for adderID := range adders {
		wg.Go(func() {
			runBenchmarkAdder(
				ctx,
				pluginPool.pool,
				preparedTxs[adderID],
				adders,
				targetTPS,
				&added,
				&rejected,
				errCh,
			)
		})
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	finalOccupancy := len(pluginPool.pool.Transactions())
	var workloadErr error
	select {
	case workloadErr = <-errCh:
	default:
	}
	if err := pluginPool.host.Stop(context.Background()); err != nil {
		b.Fatalf("stop plugin host: %v", err)
	}
	if workloadErr != nil {
		b.Fatalf("%s benchmark workload: %v", implementation, workloadErr)
	}
	return benchmarkScenarioResult{
		elapsed:        elapsed,
		maxRead:        time.Duration(maxReadNS.Load()),
		syncTotal:      time.Duration(syncNS.Load()),
		added:          added.Load(),
		rejected:       rejected.Load(),
		reads:          reads.Load(),
		syncs:          syncs.Load(),
		finalOccupancy: finalOccupancy,
	}
}

func newBenchmarkPluginPool(
	b *testing.B,
	implementation Implementation,
	applyCPU time.Duration,
) benchmarkPluginPool {
	b.Helper()
	host := plugin.NewHost()
	if err := RegisterFIFOProvider(host); err != nil {
		b.Fatalf("register FIFO provider: %v", err)
	}
	if err := RegisterDAGProvider(host); err != nil {
		b.Fatalf("register DAG provider: %v", err)
	}
	service, err := plugin.Resolve[Service](
		context.Background(),
		host,
		plugin.CapabilityMempool,
		string(implementation),
		map[string]any{
			"capacity":           benchmarkCapacity,
			"evictionWatermark":  1,
			"rejectionWatermark": 1,
		},
		ProviderDependencies{
			PromRegistry: prometheus.NewRegistry(),
			Validator: benchmarkValidator{
				applyCPU: applyCPU,
			},
		},
	)
	if err != nil {
		b.Fatalf("resolve %s mempool provider: %v", implementation, err)
	}
	switch pool := service.(type) {
	case *FIFO:
		return benchmarkPluginPool{host: host, pool: pool, core: pool.Mempool}
	case *DAG:
		return benchmarkPluginPool{host: host, pool: pool, core: pool.Mempool}
	default:
		b.Fatalf(
			"resolve %s mempool provider returned %T",
			implementation,
			service,
		)
		return benchmarkPluginPool{}
	}
}

func runBenchmarkAdder(
	ctx context.Context,
	pool Pool,
	transactions [][]byte,
	numAdders int,
	targetTPS int,
	added *atomic.Uint64,
	rejected *atomic.Uint64,
	errCh chan<- error,
) {
	var interval time.Duration
	if targetTPS > 0 {
		interval = time.Duration(
			float64(time.Second) * float64(numAdders) / float64(targetTPS),
		)
	}
	for _, txBytes := range transactions {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if addErr := pool.AddTransaction(
			uint(conway.EraIdConway),
			txBytes,
		); addErr != nil {
			if errors.Is(addErr, errBenchmarkDoubleSpend) {
				rejected.Add(1)
			} else {
				reportBenchmarkError(errCh, addErr)
				return
			}
		} else {
			added.Add(1)
		}
		if interval > 0 && !benchmarkWait(ctx, interval) {
			return
		}
	}
	select {
	case <-ctx.Done():
	default:
		reportBenchmarkError(
			errCh,
			errors.New("prepared transaction workload exhausted"),
		)
	}
}

func runBenchmarkReader(
	ctx context.Context,
	pool Pool,
	readPeriod time.Duration,
	reads *atomic.Uint64,
	maxReadNS *atomic.Int64,
) {
	for {
		start := time.Now()
		snapshot := pool.Transactions()
		elapsed := time.Since(start)
		runtime.KeepAlive(snapshot)
		reads.Add(1)
		updateAtomicMax(maxReadNS, elapsed.Nanoseconds())
		if !benchmarkWait(ctx, readPeriod) {
			return
		}
	}
}

func runBenchmarkSyncer(
	ctx context.Context,
	pool *Mempool,
	syncPeriod time.Duration,
	syncs *atomic.Uint64,
	syncNS *atomic.Int64,
	errCh chan<- error,
) {
	for benchmarkWait(ctx, syncPeriod) {
		start := time.Now()
		if err := pool.rebuildOverlay(); err != nil {
			reportBenchmarkError(errCh, err)
			return
		}
		syncNS.Add(time.Since(start).Nanoseconds())
		syncs.Add(1)
	}
}

func newBenchmarkTxGenerator(
	topology benchmarkTopology,
	adderID int,
) *benchmarkTxGenerator {
	return &benchmarkTxGenerator{
		topology: topology,
		adderID:  adderID,
	}
}

func prepareBenchmarkGenerators(
	pool Pool,
	topology benchmarkTopology,
	count int,
) ([]*benchmarkTxGenerator, error) {
	generators := make([]*benchmarkTxGenerator, count)
	for adderID := range count {
		generator := newBenchmarkTxGenerator(topology, adderID)
		if topology == benchmarkTopologyWideFanout {
			parentBytes, parentHash, err := buildBenchmarkTransaction(
				benchmarkSeedHash(adderID, 0),
				0,
				uint64(adderID+1),
				benchmarkFanoutWidth,
			)
			if err != nil {
				return nil, err
			}
			if err := pool.AddTransaction(
				uint(conway.EraIdConway),
				parentBytes,
			); err != nil {
				return nil, fmt.Errorf(
					"add fanout parent %d: %w",
					adderID,
					err,
				)
			}
			generator.fanoutHash = parentHash
			generator.fanoutTips = make(
				[]string,
				benchmarkFanoutWidth,
			)
		}
		generators[adderID] = generator
	}
	return generators, nil
}

func prepareBenchmarkTransactions(
	generators []*benchmarkTxGenerator,
	targetTPS int,
	duration time.Duration,
) ([][][]byte, error) {
	total := int(
		float64(targetTPS)*duration.Seconds()*1.10,
	) + len(generators)*2
	perAdder := max(1, (total+len(generators)-1)/len(generators))
	prepared := make([][][]byte, len(generators))
	for i, generator := range generators {
		prepared[i] = make([][]byte, perAdder)
		for j := range perAdder {
			txBytes, err := generator.next()
			if err != nil {
				return nil, fmt.Errorf(
					"generate transaction %d for adder %d: %w",
					j,
					i,
					err,
				)
			}
			prepared[i][j] = txBytes
		}
	}
	return prepared, nil
}

func prefillBenchmarkPool(pool Pool, count int) error {
	if count == 0 {
		return nil
	}
	generator := newBenchmarkTxGenerator(
		benchmarkTopologyNormal,
		1_000_000,
	)
	for i := range count {
		txBytes, err := generator.next()
		if err != nil {
			return fmt.Errorf("generate prefill transaction %d: %w", i, err)
		}
		if err := pool.AddTransaction(
			uint(conway.EraIdConway),
			txBytes,
		); err != nil {
			return fmt.Errorf("add prefill transaction %d: %w", i, err)
		}
	}
	return nil
}

func (g *benchmarkTxGenerator) next() ([]byte, error) {
	var (
		inputHash  string
		inputIndex uint32
	)
	switch g.topology {
	case benchmarkTopologyNormal:
		if g.sequence%benchmarkNormalChainGap == 1 &&
			g.previousHash != "" {
			inputHash = g.previousHash
		} else {
			inputHash = benchmarkSeedHash(g.adderID, g.sequence+1)
		}
	case benchmarkTopologyDeepChain:
		inputHash = g.previousHash
		if inputHash == "" {
			inputHash = benchmarkSeedHash(g.adderID, 0)
		}
	case benchmarkTopologyWideFanout:
		if g.fanoutHash == "" {
			return nil, errors.New("wide-fanout generator has no parent")
		}
		slot := int(g.sequence % benchmarkFanoutWidth)
		if g.fanoutTips[slot] == "" {
			inputHash = g.fanoutHash
			inputIndex = uint32(slot)
		} else {
			inputHash = g.fanoutTips[slot]
		}
	case benchmarkTopologyConflicting:
		inputHash = benchmarkSeedHash(2_000_000, 0)
	default:
		return nil, fmt.Errorf(
			"unknown benchmark topology %q",
			g.topology,
		)
	}
	nonce := uint64(g.adderID+1)*1_000_000 + g.sequence
	txBytes, txHash, err := buildBenchmarkTransaction(
		inputHash,
		inputIndex,
		nonce,
		1,
	)
	if err != nil {
		return nil, err
	}
	if g.topology == benchmarkTopologyWideFanout {
		slot := int(g.sequence % benchmarkFanoutWidth)
		g.fanoutTips[slot] = txHash
	}
	g.previousHash = txHash
	g.sequence++
	return txBytes, nil
}

func buildBenchmarkTransaction(
	inputHash string,
	inputIndex uint32,
	nonce uint64,
	outputCount int,
) ([]byte, string, error) {
	template, err := hex.DecodeString(testTxHex)
	if err != nil {
		return nil, "", fmt.Errorf(
			"decode benchmark transaction: %w",
			err,
		)
	}
	decoded, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		template,
	)
	if err != nil {
		return nil, "", fmt.Errorf(
			"decode benchmark transaction template: %w",
			err,
		)
	}
	tx, ok := decoded.(*conway.ConwayTransaction)
	if !ok {
		return nil, "", fmt.Errorf(
			"benchmark transaction template decoded as %T",
			decoded,
		)
	}
	tx.Body.TxInputs.SetItems([]shelley.ShelleyTransactionInput{
		shelley.NewShelleyTransactionInput(
			inputHash,
			int(inputIndex),
		),
	})
	if len(tx.Body.TxOutputs) == 0 {
		return nil, "", errors.New(
			"benchmark transaction template has no outputs",
		)
	}
	output := tx.Body.TxOutputs[0]
	tx.Body.TxOutputs = tx.Body.TxOutputs[:0]
	for range outputCount {
		tx.Body.TxOutputs = append(tx.Body.TxOutputs, output)
	}
	tx.Body.TxFee = 1_000_000 + nonce
	tx.TxIsValid = true
	// Decoded ledger values retain their original CBOR. Clear both caches so
	// the mutated body and transaction are actually encoded.
	tx.Body.SetCbor(nil)
	tx.SetCbor(nil)
	txBytes, err := tx.MarshalCBOR()
	if err != nil {
		return nil, "", fmt.Errorf(
			"encode generated benchmark transaction: %w",
			err,
		)
	}
	generated, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txBytes,
	)
	if err != nil {
		return nil, "", fmt.Errorf(
			"decode generated benchmark transaction: %w",
			err,
		)
	}
	return txBytes, generated.Hash().String(), nil
}

func benchmarkSeedHash(adderID int, sequence uint64) string {
	var seed [16]byte
	binary.BigEndian.PutUint64(seed[:8], uint64(adderID))
	binary.BigEndian.PutUint64(seed[8:], sequence)
	hash := sha256.Sum256(seed[:])
	return hex.EncodeToString(hash[:])
}

func benchmarkRemovalCascade(
	b *testing.B,
	implementation Implementation,
	count int,
	applyCPU time.Duration,
) {
	b.Helper()
	var cascadeTotal time.Duration
	for range b.N {
		pluginPool := newBenchmarkPluginPool(
			b,
			implementation,
			applyCPU,
		)
		generator := newBenchmarkTxGenerator(
			benchmarkTopologyDeepChain,
			0,
		)
		var rootHash string
		for i := range count {
			txBytes, err := generator.next()
			if err != nil {
				b.Fatalf(
					"generate cascade transaction %d: %v",
					i,
					err,
				)
			}
			if err := pluginPool.pool.AddTransaction(
				uint(conway.EraIdConway),
				txBytes,
			); err != nil {
				b.Fatalf("add cascade transaction %d: %v", i, err)
			}
			if i == 0 {
				rootHash = generator.previousHash
			}
		}
		start := time.Now()
		pluginPool.pool.RemoveTransaction(rootHash)
		cascadeTotal += time.Since(start)
		if remaining := len(pluginPool.pool.Transactions()); remaining != 0 {
			b.Fatalf(
				"%s cascade left %d of %d transactions",
				implementation,
				remaining,
				count,
			)
		}
		b.StopTimer()
		if err := pluginPool.host.Stop(context.Background()); err != nil {
			b.Fatalf("stop plugin host: %v", err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(count), "txs/cascade")
	if cascadeTotal > 0 {
		b.ReportMetric(
			float64(count*b.N)/cascadeTotal.Seconds(),
			"removed-tx/s",
		)
		b.ReportMetric(
			float64(cascadeTotal.Nanoseconds())/float64(b.N),
			"cascade-ns/avg",
		)
	}
}

func benchmarkWait(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func reportBenchmarkError(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func updateAtomicMax(value *atomic.Int64, candidate int64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func reportMempoolBenchmarkMetrics(
	b *testing.B,
	result benchmarkScenarioResult,
) {
	b.Helper()
	elapsedSeconds := result.elapsed.Seconds()
	if elapsedSeconds > 0 {
		b.ReportMetric(float64(result.added)/elapsedSeconds, "tx/s")
		b.ReportMetric(
			float64(result.added+result.rejected)/elapsedSeconds,
			"attempts/s",
		)
		b.ReportMetric(
			float64(result.rejected)/elapsedSeconds,
			"rejected/s",
		)
		b.ReportMetric(float64(result.reads)/elapsedSeconds, "reads/s")
	}
	b.ReportMetric(float64(result.maxRead.Nanoseconds()), "read-ns/max")
	if result.syncs > 0 {
		b.ReportMetric(
			float64(result.syncTotal.Nanoseconds())/float64(result.syncs),
			"sync-ns/avg",
		)
	}
	b.ReportMetric(
		float64(result.finalOccupancy)/float64(b.N),
		"txs-final",
	)
}

func readMempoolBenchmarkConfig(b *testing.B) mempoolBenchmarkConfig {
	b.Helper()
	return mempoolBenchmarkConfig{
		duration: benchmarkEnvDuration(
			b,
			"DINGO_MEMPOOL_BENCH_DURATION",
			time.Second,
		),
		syncPeriod: benchmarkEnvDuration(
			b,
			"DINGO_MEMPOOL_BENCH_SYNC_PERIOD",
			250*time.Millisecond,
		),
		readPeriod: benchmarkEnvDuration(
			b,
			"DINGO_MEMPOOL_BENCH_READ_PERIOD",
			150*time.Millisecond,
		),
		applyCPU: benchmarkEnvDuration(
			b,
			"DINGO_MEMPOOL_BENCH_APPLY_CPU",
			0,
		),
	}
}

func benchmarkEnvDuration(
	b *testing.B,
	name string,
	defaultValue time.Duration,
) time.Duration {
	b.Helper()
	value := defaultValue
	if envValue, ok := os.LookupEnv(name); ok {
		parsed, err := time.ParseDuration(envValue)
		if err != nil {
			b.Fatalf("parse %s=%q: %v", name, envValue, err)
		}
		value = parsed
	}
	if value < 0 {
		b.Fatalf("%s must not be negative", name)
	}
	return value
}

func benchmarkEnvInt(
	b *testing.B,
	name string,
	defaultValue int,
) int {
	b.Helper()
	value := defaultValue
	if envValue, ok := os.LookupEnv(name); ok {
		parsed, err := strconv.Atoi(envValue)
		if err != nil {
			b.Fatalf("parse %s=%q: %v", name, envValue, err)
		}
		value = parsed
	}
	if value < 1 {
		b.Fatalf("%s must be positive", name)
	}
	return value
}

func decodeBenchmarkTransaction(
	t *testing.T,
	txBytes []byte,
) gledger.Transaction {
	t.Helper()
	tx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txBytes,
	)
	if err != nil {
		t.Fatalf("decode generated benchmark transaction: %v", err)
	}
	return tx
}
