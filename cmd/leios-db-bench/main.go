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

// Command leios-db-bench measures the latency of Dingo's LeiosDB storage
// steps, mirroring the ouroboros-consensus LeiosDB-bench latency benchmark.
//
// Every measured endorser block (EB) goes through:
//
//   - Fetch: its Tx chunks arrive from the upstream peers and the complete EB is
//     committed with one SetLeiosEB call, as the EB persistence writer does.
//   - Vote read: once fetched, one transaction-closure read.
//   - Serve: once fetched, every downstream peer reads the manifest, then the
//     transactions request by request, each request reloading the stored
//     transaction list as the historical serving path does.
//   - Apply read: once served, one transaction-closure read.
//
// The measured EBs go one at a time. Optional background bursts of EBs are
// fetched, voted on and served concurrently and push the measured latencies
// where they overlap. Pruning keeps the stored EB window at -volatile-ebs.
package main

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const maxLeiosTxSizeBytes = 1<<16 - 1

type benchConfig struct {
	volatileEbs     int
	txsPerEb        int
	txSizeBytes     int
	requestBytes    int
	jobBytes        int
	upstreamPeers   int
	downstreamPeers int
	samples         int
	gcEvery         int
	burst           bool
	burstSize       int
	burstInterval   time.Duration
	cachedReads     bool
	cpuProfile      string
	heapProfile     string
}

type metric int

const (
	metricFetch metric = iota
	metricFetchChunks
	metricFetchCommit
	metricServe
	metricServeBody
	metricServeTxs
	metricVoteRead
	metricApplyRead
	metricCount
)

var metricLabels = [metricCount]string{
	"Fetch", "  Chunks", "  Commit", "Serve", "  Body", "  Txs request",
	"Vote read", "Apply read",
}

// benchEb is one EB generated ahead of its fetch. Its slot is assigned when it
// is fetched, in fetch order.
type benchEb struct {
	idx       int
	hash      []byte
	manifest  []byte
	txs       []cbor.RawMessage
	measured  bool
	readCache *ebReadCache
}

type txRange struct{ start, end int }

type recorder struct {
	mu      sync.Mutex
	samples [metricCount][]time.Duration
	missing int
	err     error
}

func (r *recorder) record(m metric, d time.Duration) {
	r.mu.Lock()
	r.samples[m] = append(r.samples[m], d)
	r.mu.Unlock()
}

func (r *recorder) fail(err error) {
	r.mu.Lock()
	r.err = errors.Join(r.err, err)
	r.mu.Unlock()
}

func (r *recorder) miss() {
	r.mu.Lock()
	r.missing++
	r.mu.Unlock()
}

func (r *recorder) failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err != nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	config := parseConfig()
	if err := validateConfig(config); err != nil {
		return err
	}

	tmpDir, err := os.MkdirTemp("", "dingo-leios-db-bench-")
	if err != nil {
		return fmt.Errorf("create benchmark directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			runErr = errors.Join(runErr,
				fmt.Errorf("remove benchmark directory: %w", err))
		}
	}()
	printBenchInfo(config, tmpDir)

	dbDir := filepath.Join(tmpDir, "db")
	logger := slog.New(slog.DiscardHandler)
	rt, err := plugins.OpenDatabase(
		context.Background(),
		&database.Config{
			DataDir:        dbDir,
			StorageMode:    "core",
			BlobPlugin:     "badger",
			MetadataPlugin: "sqlite",
			Logger:         logger,
			AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
				nil, "", "mainnet",
			),
		},
		plugins.StorageSelections{
			Blob:     plugin.Selection{Provider: "badger"},
			Metadata: plugin.Selection{Provider: "sqlite"},
		},
		plugins.StorageDependencies{
			DataDir:     dbDir,
			RunMode:     "leios",
			StorageMode: "core",
			Logger:      logger,
		},
	)
	if err != nil {
		return fmt.Errorf("open Dingo database: %w", err)
	}
	defer func() {
		if err := rt.Close(context.Background()); err != nil {
			runErr = errors.Join(runErr,
				fmt.Errorf("close Dingo database: %w", err))
		}
	}()
	if err := rt.RecoveryError(); err != nil {
		return fmt.Errorf("open Dingo database recovery state: %w", err)
	}

	if config.heapProfile != "" {
		stopMemoryProfile := startMemoryProfile(config.heapProfile)
		defer func() {
			runErr = errors.Join(runErr, stopMemoryProfile())
		}()
	}
	if err := populate(rt.Database, config); err != nil {
		return err
	}
	if config.cpuProfile != "" {
		file, err := os.Create(config.cpuProfile)
		if err != nil {
			return fmt.Errorf("create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			return errors.Join(err, file.Close())
		}
		defer func() {
			pprof.StopCPUProfile()
			runErr = errors.Join(runErr, file.Close())
		}()
	}
	return runBench(rt.Database, config)
}

func parseConfig() benchConfig {
	config := benchConfig{}
	flag.IntVar(&config.volatileEbs, "volatile-ebs", 1000,
		"EBs stored before timing and kept stored by pruning")
	flag.IntVar(&config.txsPerEb, "txs-per-eb", 2857,
		"number of transactions in each EB")
	flag.IntVar(&config.txSizeBytes, "tx-size-bytes", 350,
		"serialized transaction item size in bytes")
	flag.IntVar(&config.requestBytes, "request-bytes", 500_000,
		"upper bound on the transaction bytes of one request")
	flag.IntVar(&config.jobBytes, "job-bytes", 65_536,
		"transaction bytes per job, the unit a request is made of")
	flag.IntVar(&config.upstreamPeers, "upstream-peers", 20,
		"peers the transaction chunks of every EB are fetched from")
	flag.IntVar(&config.downstreamPeers, "downstream-peers", 100,
		"peers each reading every EB once it is complete")
	flag.IntVar(&config.samples, "samples", 100,
		"measured EBs, processed one at a time")
	flag.IntVar(
		&config.gcEvery,
		"gc-every",
		10,
		"prune beyond the volatile window every this many fetched EBs; the stored window can exceed -volatile-ebs by up to this many minus one",
	)
	flag.BoolVar(&config.burst, "burst", false,
		"fetch and serve bursts of background EBs while measuring")
	flag.IntVar(&config.burstSize, "burst-size", 10,
		"background EBs started at once per burst")
	flag.DurationVar(&config.burstInterval, "burst-interval", 5*time.Second,
		"time between background bursts")
	flag.BoolVar(
		&config.cachedReads,
		"cached-reads",
		false,
		"share persisted manifest and closure reads per EB; clone returned transactions",
	)
	flag.StringVar(&config.cpuProfile, "cpuprofile", "",
		"write a CPU profile of the measured workload, excluding prepopulation")
	flag.StringVar(
		&config.heapProfile,
		"heapprofile",
		"",
		"write an unforced final heap profile and sample runtime memory; diagnostic runs only",
	)
	flag.Parse()
	return config
}

func validateConfig(config benchConfig) error {
	if config.volatileEbs < 1 || config.txsPerEb < 1 ||
		config.txSizeBytes < 32 || config.txSizeBytes > maxLeiosTxSizeBytes ||
		config.requestBytes < 1 || config.jobBytes < 1 ||
		config.upstreamPeers < 1 || config.downstreamPeers < 1 ||
		config.samples < 1 || config.gcEvery < 1 ||
		config.burstSize < 1 || config.burstInterval <= 0 {
		return errors.New(
			"invalid benchmark configuration: counts and durations must be " +
				"positive and transaction sizes must be between 32 and " +
				"65535 bytes",
		)
	}
	if _, err := txPayloadSize(config.txSizeBytes); err != nil {
		return fmt.Errorf("invalid benchmark configuration: %w", err)
	}
	if minRequest := minRequestBytes(config); config.requestBytes < minRequest {
		return fmt.Errorf(
			"invalid benchmark configuration: -request-bytes %d is below one job (%d bytes)",
			config.requestBytes,
			minRequest,
		)
	}
	return nil
}

// minRequestBytes is the size of one whole job, the smallest request.
func minRequestBytes(config benchConfig) int {
	return config.txSizeBytes * max(1, config.jobBytes/config.txSizeBytes)
}

// fetchRanges splits an EB's transactions into one contiguous chunk per
// upstream peer, fewer when the EB has fewer transactions than peers.
func fetchRanges(config benchConfig) []txRange {
	count := min(config.upstreamPeers, config.txsPerEb)
	ranges := make([]txRange, count)
	for i := range ranges {
		ranges[i] = txRange{
			start: i * config.txsPerEb / count,
			end:   (i + 1) * config.txsPerEb / count,
		}
	}
	return ranges
}

// pruneGuard assigns fetch slots and computes prune cutoffs that never pass a
// slot whose readers are still running.
type pruneGuard struct {
	mu       sync.Mutex
	next     uint64
	inFlight map[uint64]struct{}
	window   uint64
	every    uint64
}

func newPruneGuard(window, every int) *pruneGuard {
	return &pruneGuard{
		next:     uint64(window), //nolint:gosec // validated positive
		inFlight: map[uint64]struct{}{},
		window:   uint64(window), //nolint:gosec // validated positive
		every:    uint64(every),  //nolint:gosec // validated positive
	}
}

// begin assigns the next slot and marks it in flight. When this slot is due a
// prune it returns the cutoff, held back to the oldest in-flight slot.
func (g *pruneGuard) begin() (slot, cutoff uint64, prune bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	slot = g.next
	g.next++
	g.inFlight[slot] = struct{}{}
	if slot%g.every != 0 || slot+1 <= g.window {
		return slot, 0, false
	}
	cutoff = slot + 1 - g.window
	for s := range g.inFlight {
		cutoff = min(cutoff, s)
	}
	return slot, cutoff, true
}

func (g *pruneGuard) end(slot uint64) {
	g.mu.Lock()
	delete(g.inFlight, slot)
	g.mu.Unlock()
}

func printBenchInfo(config benchConfig, dir string) {
	background := "None (-burst to turn on)"
	if config.burst {
		background = fmt.Sprintf(
			"%d EBs every %s (%s)",
			config.burstSize,
			config.burstInterval,
			showBytes(config.burstSize*config.txsPerEb*config.txSizeBytes),
		)
	}
	fmt.Println("Dingo LeiosDB latency benchmark")
	fmt.Println()
	fmt.Printf(
		"  Database          : %d volatile EBs (Badger blob store, core defaults)\n",
		config.volatileEbs,
	)
	fmt.Printf("  EB                : %d Txs of %s (closure %s)\n",
		config.txsPerEb, showBytes(config.txSizeBytes),
		showBytes(config.txsPerEb*config.txSizeBytes))
	fmt.Printf("  Requests          : %s of %d KiB jobs\n",
		showBytes(config.requestBytes), config.jobBytes/1024)
	fmt.Printf("  Schedule          : %d EBs, one at a time\n", config.samples)
	fmt.Printf(
		"  Fetch             : from %d upstream peers\n",
		config.upstreamPeers,
	)
	fmt.Printf(
		"  Serve             : to %d downstream peers\n",
		config.downstreamPeers,
	)
	fmt.Printf("  Background load   : %s\n", background)
	fmt.Printf("  Runtime           : GOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))
	if config.cachedReads {
		fmt.Println(
			"  Reads             : per-EB read-through cache (first read loads storage)",
		)
	} else {
		fmt.Println("  Reads             : database (application cache bypassed)")
	}
	fmt.Println()
	fmt.Printf("Database in %s (TMPDIR to change)\n", dir)
	fmt.Println()
	fmt.Println("Comparison notes:")
	fmt.Println(
		"  Haskell writes each Tx chunk to SQLite as it arrives; Dingo assembles the",
	)
	fmt.Println("  chunks in memory and commits the EB once with SetLeiosEB.")
	fmt.Println(
		"  Default Txs requests reload the stored transaction list, then slice it.",
	)
	fmt.Println(
		"  -cached-reads shares the first persisted read per EB; later reads reuse it.",
	)
	fmt.Println(
		"  This models cached storage reads, not the protocol cache or wire encoding.",
	)
	fmt.Println()
}

func populate(db *database.Database, config benchConfig) error {
	fmt.Print("Inserting EBs: ")
	step := max(config.volatileEbs/10, 1)
	for i := range config.volatileEbs {
		eb, err := genBenchEb(i, config, false)
		if err != nil {
			return fmt.Errorf("generate EB %d: %w", i, err)
		}
		slot, err := nonNegativeUint64(i)
		if err != nil {
			return err
		}
		if err := db.SetLeiosEB(slot, eb.hash, eb.manifest, eb.txs); err != nil {
			return fmt.Errorf("prepopulate EB %d: %w", i, err)
		}
		if (i+1)%step == 0 {
			fmt.Printf("%d ", i+1)
		}
	}
	fmt.Println("done")
	return nil
}

type peerItem struct {
	eb        *benchEb
	slot      uint64
	completed time.Time
	onDone    func()
}

type chunkJob struct {
	eb       *benchEb
	assembly []cbor.RawMessage
	rng      txRange
	done     chan<- struct{}
}

func runBench(db *database.Database, config benchConfig) error {
	rec := &recorder{}
	fmt.Print("Generating EBs: ")
	signal := make([]*benchEb, config.samples)
	for i := range signal {
		eb, err := genBenchEb(signalIdx+i, config, true)
		if err != nil {
			return fmt.Errorf("generate EB %d: %w", signalIdx+i, err)
		}
		signal[i] = eb
	}
	fmt.Println("done")

	upstream := make([]chan chunkJob, config.upstreamPeers)
	var upstreamWg sync.WaitGroup
	for i := range upstream {
		upstream[i] = make(chan chunkJob, 1<<12)
		upstreamWg.Go(func() {
			for job := range upstream[i] {
				// A delivered chunk lands in the EB's in-memory assembly.
				copy(job.assembly[job.rng.start:job.rng.end],
					job.eb.txs[job.rng.start:job.rng.end])
				job.done <- struct{}{}
			}
		})
	}
	voteQ := make(chan peerItem, 1<<12)
	var voterWg sync.WaitGroup
	voterWg.Go(func() {
		for item := range voteQ {
			closureRead(db, item.eb, item.slot, metricVoteRead, config, rec)
			item.onDone()
		}
	})
	peerQs := make([]chan peerItem, config.downstreamPeers)
	var peersWg sync.WaitGroup
	for seed := range peerQs {
		peerQs[seed] = make(chan peerItem, 1<<12)
		peersWg.Go(func() {
			for item := range peerQs[seed] {
				servePeer(db, seed+1, item, config, rec)
				item.onDone()
			}
		})
	}

	guard := newPruneGuard(config.volatileEbs, config.gcEvery)
	fetchEb := func(eb *benchEb, onDone func()) uint64 {
		slot, cutoff, prune := guard.begin()
		var readers atomic.Int64
		readers.Store(int64(1 + config.downstreamPeers))
		release := onDone
		onDone = func() {
			if readers.Add(-1) == 0 {
				guard.end(slot)
			}
			release()
		}
		if prune {
			if _, err := db.PruneLeiosEBBeforeSlot(
				context.Background(), cutoff,
			); err != nil {
				rec.fail(fmt.Errorf("prune before slot %d: %w", cutoff, err))
			}
		}
		started := time.Now()
		assembly := make([]cbor.RawMessage, len(eb.txs))
		ranges := fetchRanges(config)
		dones := make(chan struct{}, len(ranges))
		for k, rng := range ranges {
			upstream[(int(slot)+k)%config.upstreamPeers] <- chunkJob{ //nolint:gosec // slot fits int
				eb: eb, assembly: assembly, rng: rng, done: dones,
			}
		}
		for range ranges {
			<-dones
		}
		chunksDone := time.Now()
		if err := db.SetLeiosEB(slot, eb.hash, eb.manifest, assembly); err != nil {
			rec.fail(fmt.Errorf("write EB %d: %w", eb.idx, err))
		}
		if config.cachedReads {
			eb.readCache = newEBReadCache(db, eb.hash, slot)
		}
		completed := time.Now()
		if eb.measured {
			rec.record(metricFetch, completed.Sub(started))
			rec.record(metricFetchChunks, chunksDone.Sub(started))
			rec.record(metricFetchCommit, completed.Sub(chunksDone))
		}
		item := peerItem{
			eb:        eb,
			slot:      slot,
			completed: completed,
			onDone:    onDone,
		}
		voteQ <- item
		for _, q := range peerQs {
			q <- item
		}
		return slot
	}

	var fetched atomic.Int64
	var stopped atomic.Bool
	stop := make(chan struct{})
	t0 := time.Now()
	progressDone := make(chan struct{})
	progressStopped := make(chan struct{})
	go func() {
		defer close(progressStopped)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			state := ""
			if stopped.Load() {
				state = ", draining"
			}
			fmt.Printf("\rRunning: %s, %d/%d EBs done%s   ",
				showTime(time.Since(t0)), fetched.Load(), config.samples, state)
			select {
			case <-progressDone:
				return
			case <-ticker.C:
			}
		}
	}()

	var burstWg sync.WaitGroup
	if config.burst {
		burstWg.Go(
			func() { burstLoad(config, func(eb *benchEb) { fetchEb(eb, func() {}) }, stop, t0, rec) },
		)
	}
	for _, eb := range signal {
		if rec.failed() {
			break
		}
		var readers sync.WaitGroup
		readers.Add(1 + config.downstreamPeers)
		slot := fetchEb(eb, readers.Done)
		readers.Wait()
		closureRead(db, eb, slot, metricApplyRead, config, rec)
		// All readers have finished, including apply; release this EB's
		// retained closure before the next measured EB starts.
		eb.readCache = nil
		fetched.Add(1)
	}
	stopped.Store(true)
	close(stop)
	burstWg.Wait()
	for _, q := range upstream {
		close(q)
	}
	upstreamWg.Wait()
	close(voteQ)
	voterWg.Wait()
	for _, q := range peerQs {
		close(q)
	}
	peersWg.Wait()
	close(progressDone)
	<-progressStopped
	fmt.Println()
	if rec.err != nil {
		return rec.err
	}
	report(rec)
	return nil
}

// signalIdx and burstIdx keep the generated EB hashes of measured and
// background EBs apart from each other and from the preloaded ones.
const (
	signalIdx = 1_000_000
	burstIdx  = 2_000_000
)

// burstLoad starts a burst of EBs every burst interval from t0, whether or not
// earlier ones are done, until stopped; then it waits for those in flight.
func burstLoad(
	config benchConfig,
	fetchEb func(*benchEb),
	stop <-chan struct{},
	t0 time.Time,
	rec *recorder,
) {
	var inFlight sync.WaitGroup
	defer inFlight.Wait()
	for b := 0; ; b++ {
		ebs := make([]*benchEb, config.burstSize)
		for i := range ebs {
			eb, err := genBenchEb(burstIdx+b*config.burstSize+i, config, false)
			if err != nil {
				rec.fail(fmt.Errorf("generate background EB: %w", err))
				return
			}
			ebs[i] = eb
		}
		timer := time.NewTimer(
			time.Until(t0.Add(time.Duration(b) * config.burstInterval)),
		)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		if rec.failed() {
			return
		}
		for _, eb := range ebs {
			inFlight.Go(func() { fetchEb(eb) })
		}
	}
}

// closureRead resolves a completed EB's transaction closure, as the voter and
// block application do.
func closureRead(
	db *database.Database,
	eb *benchEb,
	slot uint64,
	m metric,
	config benchConfig,
	rec *recorder,
) {
	started := time.Now()
	txs, err := readEBTxs(db, eb, slot, txRange{end: -1})
	elapsed := time.Since(started)
	switch {
	case err != nil:
		rec.fail(fmt.Errorf("read closure of EB %d: %w", eb.idx, err))
	case len(txs) != config.txsPerEb:
		rec.miss()
	case eb.measured:
		rec.record(m, elapsed)
	}
}

// servePeer is a downstream peer reading a completed EB in full, one request at
// a time: the manifest, then the transaction requests in the order of its seed.
func servePeer(
	db *database.Database,
	seed int,
	item peerItem,
	config benchConfig,
	rec *recorder,
) {
	eb := item.eb
	started := time.Now()
	var manifest []byte
	var err error
	if eb.readCache != nil {
		manifest, err = eb.readCache.manifest()
	} else {
		manifest, err = db.GetLeiosEBManifest(eb.hash, item.slot)
	}
	elapsed := time.Since(started)
	switch {
	case err != nil:
		rec.fail(fmt.Errorf("read manifest of EB %d: %w", eb.idx, err))
		return
	case len(manifest) == 0:
		rec.miss()
	case eb.measured:
		rec.record(metricServeBody, elapsed)
	}
	for _, rng := range requests(seed, eb.idx, config) {
		started := time.Now()
		served, err := readEBTxs(db, eb, item.slot, rng)
		elapsed := time.Since(started)
		switch {
		case err != nil:
			rec.fail(fmt.Errorf("read transactions of EB %d: %w", eb.idx, err))
			return
		case len(served) != rng.end-rng.start:
			rec.miss()
		case eb.measured:
			rec.record(metricServeTxs, elapsed)
		}
	}
	if eb.measured {
		rec.record(metricServe, time.Since(item.completed))
	}
}

// requests splits an EB's transaction offsets into requests of contiguous
// offsets, whole jobs at a time and at most the request size, in the order of
// the given seed.
func requests(seed, ebIdx int, config benchConfig) []txRange {
	txsPerJob := max(1, config.jobBytes/config.txSizeBytes)
	txsPerRequest := txsPerJob *
		max(1, config.requestBytes/(txsPerJob*config.txSizeBytes))
	count := (config.txsPerEb + txsPerRequest - 1) / txsPerRequest
	ranges := make([]txRange, count)
	for i := range ranges {
		ranges[i] = txRange{
			start: i * txsPerRequest,
			end:   min(config.txsPerEb, (i+1)*txsPerRequest),
		}
	}
	slices.SortFunc(ranges, func(a, b txRange) int {
		return cmp.Compare(
			mix(seed, ebIdx, a.start/txsPerRequest),
			mix(seed, ebIdx, b.start/txsPerRequest),
		)
	})
	return ranges
}

// mix is a deterministic shuffle key.
func mix(seed, ebIdx, j int) int64 {
	h := (int64(j)*0x9E3779B1 + int64(ebIdx)*0x85EBCA77 + int64(seed)*0xC2B2AE3D) * 0x27D4EB2F
	return h ^ (h >> 29)
}

func genBenchEb(
	ebIdx int,
	config benchConfig,
	measured bool,
) (*benchEb, error) {
	txs, err := genTxs(ebIdx, config)
	if err != nil {
		return nil, err
	}
	manifest, err := genManifest(txs)
	if err != nil {
		return nil, err
	}
	return &benchEb{
		idx:      ebIdx,
		hash:     genHash(fmt.Sprintf("ebHash:%d", ebIdx)),
		manifest: manifest,
		txs:      txs,
		measured: measured,
	}, nil
}

func report(rec *recorder) {
	fmt.Println()
	fmt.Printf(
		"%-18s%11s%11s%11s%11s\n",
		"Latency",
		"p50",
		"p99",
		"Max",
		"Count",
	)
	for m := range metricCount {
		ts := slices.Clone(rec.samples[m])
		slices.Sort(ts)
		fmt.Printf("%-18s%11s%11s%11s%11d\n",
			metricLabels[m],
			percentile(ts, 0.5), percentile(ts, 0.99), percentile(ts, 1),
			len(ts))
	}
	if rec.missing > 0 {
		fmt.Printf(
			"WARNING: %d reads found their EB incomplete or missing (pruned too early?)\n",
			rec.missing,
		)
	}
}

func percentile(sorted []time.Duration, q float64) string {
	n := len(sorted)
	if n == 0 {
		return "-"
	}
	idx := min(n-1, max(0, int(math.Ceil(q*float64(n)))-1))
	return showTime(sorted[idx])
}

func showTime(elapsed time.Duration) string {
	switch {
	case elapsed < time.Millisecond:
		return fmt.Sprintf(
			"%d μs",
			elapsed.Round(time.Microsecond).Microseconds(),
		)
	case elapsed < time.Second:
		return fmt.Sprintf(
			"%d ms",
			elapsed.Round(time.Millisecond).Milliseconds(),
		)
	default:
		return fmt.Sprintf("%.2f s", elapsed.Seconds())
	}
}

func showBytes(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.4g MB", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.4g KB", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func genTxs(ebIdx int, config benchConfig) ([]cbor.RawMessage, error) {
	txs := make([]cbor.RawMessage, config.txsPerEb)
	payloadSize, err := txPayloadSize(config.txSizeBytes)
	if err != nil {
		return nil, err
	}
	for txIdx := range config.txsPerEb {
		payload := make([]byte, payloadSize)
		copy(payload, genTxHash(ebIdx, txIdx))
		txRaw := appendCborHead(nil, 4, 1)
		txRaw = appendCborBytes(txRaw, payload)
		if len(txRaw) != config.txSizeBytes {
			return nil, fmt.Errorf(
				"encoded transaction %d in EB %d has %d bytes, want %d",
				txIdx,
				ebIdx,
				len(txRaw),
				config.txSizeBytes,
			)
		}
		txs[txIdx] = cbor.RawMessage(txRaw)
	}
	return txs, nil
}

func genManifest(txs []cbor.RawMessage) ([]byte, error) {
	count, err := nonNegativeUint64(len(txs))
	if err != nil {
		return nil, err
	}
	manifest := appendCborHead(nil, 5, count)
	for _, tx := range txs {
		if len(tx) > maxLeiosTxSizeBytes {
			return nil, fmt.Errorf(
				"serialized transaction size %d exceeds Leios limit %d",
				len(tx),
				maxLeiosTxSizeBytes,
			)
		}
		txSize, err := nonNegativeUint64(len(tx))
		if err != nil {
			return nil, err
		}
		txHash := lcommon.Blake2b256Hash(tx)
		manifest = appendCborBytes(manifest, txHash.Bytes())
		manifest = appendCborHead(manifest, 0, txSize)
	}
	return manifest, nil
}

func txPayloadSize(serializedSize int) (int, error) {
	const transactionArrayHeadSize = 1
	for _, byteStringHeadSize := range []int{1, 2, 3, 5, 9} {
		payloadSize := serializedSize - transactionArrayHeadSize - byteStringHeadSize
		if payloadSize < 0 {
			continue
		}
		if cborHeadSize(uint64(payloadSize)) == byteStringHeadSize {
			return payloadSize, nil
		}
	}
	return 0, fmt.Errorf(
		"transaction size %d cannot be encoded as a CBOR array with one byte string",
		serializedSize,
	)
}

func cborHeadSize(value uint64) int {
	switch {
	case value < 24:
		return 1
	case value <= 0xff:
		return 2
	case value <= 0xffff:
		return 3
	case value <= 0xffffffff:
		return 5
	default:
		return 9
	}
}

func nonNegativeUint64(value int) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("cannot encode negative integer %d as CBOR", value)
	}
	return uint64(value), nil
}

func genTxHash(ebIdx, txIdx int) []byte {
	return genHash(fmt.Sprintf("txHash:%d:%d", ebIdx, txIdx))
}

func genHash(tag string) []byte {
	hash := make([]byte, 32)
	copy(hash, tag)
	return hash
}

func appendCborBytes(dst, value []byte) []byte {
	dst = appendCborHead(dst, 2, uint64(len(value)))
	return append(dst, value...)
}

func appendCborHead(dst []byte, major byte, value uint64) []byte {
	prefix := major << 5
	switch {
	case value < 24:
		return append(dst, prefix|byte(value))
	case value <= 0xff:
		return append(dst, prefix|24, byte(value))
	case value <= 0xffff:
		dst = append(dst, prefix|25, 0, 0)
		binary.BigEndian.PutUint16(dst[len(dst)-2:], uint16(value))
		return dst
	case value <= 0xffffffff:
		dst = append(dst, prefix|26, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(dst[len(dst)-4:], uint32(value))
		return dst
	default:
		dst = append(dst, prefix|27, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(dst[len(dst)-8:], value)
		return dst
	}
}
