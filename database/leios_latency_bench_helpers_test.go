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

package database_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestTxPayloadSizeHandlesCBORHeaderBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		serializedSize int
		wantPayload    int
		wantErr        bool
	}{
		{name: "byte-string short form", serializedSize: 258, wantPayload: 255},
		{name: "first unrepresentable size", serializedSize: 259, wantErr: true},
		{name: "byte-string uint16 form", serializedSize: 260, wantPayload: 256},
		{name: "last uint16 form", serializedSize: 65539, wantPayload: 65535},
		{name: "first uint32 gap", serializedSize: 65540, wantErr: true},
		{name: "second uint32 gap", serializedSize: 65541, wantErr: true},
		{name: "byte-string uint32 form", serializedSize: 65542, wantPayload: 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := txPayloadSize(tt.serializedSize)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("txPayloadSize(%d) succeeded with payload %d", tt.serializedSize, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("txPayloadSize(%d): %v", tt.serializedSize, err)
			}
			if got != tt.wantPayload {
				t.Fatalf("txPayloadSize(%d) = %d, want %d", tt.serializedSize, got, tt.wantPayload)
			}
		})
	}
}

func TestValidateConfigRejectsUnrepresentableTransactionSize(t *testing.T) {
	config := validTestConfig()
	config.txSizeBytes = 259
	if err := validateConfig(config); err == nil || !strings.Contains(err.Error(), "cannot be encoded") {
		t.Fatalf("validateConfig(%d-byte transaction) error = %v, want CBOR size error", config.txSizeBytes, err)
	}
}

func TestGenManifestReferencesSerializedTransactions(t *testing.T) {
	config := benchConfig{txsPerEb: 3, txSizeBytes: 16_384}
	txs, err := genTxs(42, config)
	if err != nil {
		t.Fatalf("genTxs: %v", err)
	}
	manifest, err := genManifest(txs)
	if err != nil {
		t.Fatalf("genManifest: %v", err)
	}
	var block lcommon.LeiosEndorserBlock
	if bytesRead, err := cbor.Decode(manifest, &block); err != nil {
		t.Fatalf("decode manifest: %v", err)
	} else if bytesRead != len(manifest) {
		t.Fatalf("manifest consumed %d bytes, want %d", bytesRead, len(manifest))
	}
	if err := block.Validate(); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	if len(block.TransactionReferences) != len(txs) {
		t.Fatalf("manifest has %d references, want %d", len(block.TransactionReferences), len(txs))
	}
	for i, tx := range txs {
		ref := block.TransactionReferences[i]
		if got, want := int(ref.TransactionSize), len(tx); got != want {
			t.Errorf("transaction %d reference size = %d, want %d", i, got, want)
		}
		if got, want := ref.TransactionHash, lcommon.Blake2b256Hash(tx); got != want {
			t.Errorf("transaction %d reference hash does not match serialized transaction", i)
		}
		var items []cbor.RawMessage
		if bytesRead, err := cbor.Decode(tx, &items); err != nil {
			t.Errorf("transaction %d is not a CBOR envelope: %v", i, err)
		} else if bytesRead != len(tx) || len(items) != 1 {
			t.Errorf("transaction %d decoded %d bytes and %d items; want %d bytes and one item", i, bytesRead, len(items), len(tx))
		}
	}
}

func validTestConfig() benchConfig {
	return benchConfig{
		volatileEbs:     1,
		txsPerEb:        1,
		txSizeBytes:     350,
		requestBytes:    500_000,
		jobBytes:        65_536,
		upstreamPeers:   1,
		downstreamPeers: 1,
		samples:         1,
		gcEvery:         1,
		burstSize:       1,
		burstInterval:   time.Second,
	}
}

func TestRequestsCoverEveryOffsetOnceInSeedOrder(t *testing.T) {
	config := validTestConfig()
	config.txsPerEb = 2857
	// 187 txs per job and 7 jobs per request give 1309 txs per request.
	want := []txRange{{0, 1309}, {1309, 2618}, {2618, 2857}}
	for seed := range 8 {
		got := requests(seed, 7, config)
		if len(got) != len(want) {
			t.Fatalf("seed %d: %d requests, want %d", seed, len(got), len(want))
		}
		sorted := slices.Clone(got)
		slices.SortFunc(sorted, func(a, b txRange) int { return a.start - b.start })
		if !slices.Equal(sorted, want) {
			t.Fatalf("seed %d: requests %v do not cover the offsets as %v", seed, got, want)
		}
	}
	orders := map[string]bool{}
	for seed := range 16 {
		orders[fmt.Sprint(requests(seed, 7, config))] = true
	}
	if len(orders) < 2 {
		t.Fatal("every seed produced the same request order")
	}
}

func TestValidateConfigRejectsNonPositiveCounts(t *testing.T) {
	for name, mutate := range map[string]func(*benchConfig){
		"samples":    func(c *benchConfig) { c.samples = 0 },
		"downstream": func(c *benchConfig) { c.downstreamPeers = 0 },
		"gc":         func(c *benchConfig) { c.gcEvery = 0 },
		"interval":   func(c *benchConfig) { c.burstInterval = 0 },
	} {
		config := validTestConfig()
		mutate(&config)
		if err := validateConfig(config); err == nil {
			t.Errorf("%s: validateConfig accepted %+v", name, config)
		}
	}
	if err := validateConfig(validTestConfig()); err != nil {
		t.Fatalf("validateConfig(valid) = %v", err)
	}
}

func TestValidateConfigRejectsRequestBelowOneJob(t *testing.T) {
	config := validTestConfig()
	// One job is 187 transactions of 350 bytes.
	config.requestBytes = 187*350 - 1
	if err := validateConfig(config); err == nil {
		t.Fatalf("validateConfig accepted %+v", config)
	}
	config.requestBytes = 187 * 350
	if err := validateConfig(config); err != nil {
		t.Fatalf("validateConfig(one job) = %v", err)
	}
}

func TestFetchRangesSpreadOverUpstreamPeers(t *testing.T) {
	config := validTestConfig()
	config.txsPerEb = 2857
	config.upstreamPeers = 20
	got := fetchRanges(config)
	if len(got) != 20 {
		t.Fatalf("%d fetch chunks, want 20", len(got))
	}
	next := 0
	for _, rng := range got {
		if rng.start != next || rng.end <= rng.start {
			t.Fatalf("chunk %v does not continue at %d", rng, next)
		}
		next = rng.end
	}
	if next != config.txsPerEb {
		t.Fatalf("chunks end at %d, want %d", next, config.txsPerEb)
	}
	config.txsPerEb = 3
	if got := fetchRanges(config); len(got) != 3 {
		t.Fatalf("%d chunks for 3 txs, want 3", len(got))
	}
}

func TestPruneGuardHoldsBackToInFlightSlots(t *testing.T) {
	g := newPruneGuard(10, 5)
	first, _, _ := g.begin() // slot 10, still in flight
	var cutoff uint64
	var prune bool
	for range 20 {
		_, cutoff, prune = g.begin()
	}
	if !prune || cutoff != first {
		t.Fatalf("cutoff = %d (prune %v), want held back to in-flight slot %d", cutoff, prune, first)
	}
	g.end(first)
	for {
		slot, c, p := g.begin()
		if p {
			if c <= first {
				t.Fatalf("cutoff %d did not advance past released slot %d", c, first)
			}
			break
		}
		g.end(slot)
	}
}

func TestBurstLoadStopsWhileWaitingForNextBurst(t *testing.T) {
	config := validTestConfig()
	config.burstInterval = time.Hour
	stop := make(chan struct{})
	var started atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		burstLoad(config, func(*benchEb) { started.Add(1) }, stop, time.Now(), &recorder{})
	}()
	deadline := time.After(5 * time.Second)
	for started.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("first burst never started")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("burstLoad kept sleeping after stop")
	}
}

type countedEBReader struct {
	reads atomic.Int64
	err   error
}

func (r *countedEBReader) GetLeiosEBManifest(
	hash []byte,
	slot uint64,
) ([]byte, error) {
	r.reads.Add(1)
	return []byte{hash[0], byte(slot)}, r.err
}

func (r *countedEBReader) GetLeiosEBTxs(
	hash []byte,
	slot uint64,
) ([]cbor.RawMessage, error) {
	r.reads.Add(1)
	return []cbor.RawMessage{{hash[0]}, {byte(slot)}, {0x82, 1, 2}}, r.err
}

func TestCachedReadsShareConcurrentStorageLoads(t *testing.T) {
	t.Parallel()
	reader := &countedEBReader{}
	hash := []byte{42}
	cache := newEBReadCache(reader, hash, 7)
	hash[0] = 99
	eb := &benchEb{readCache: cache}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			manifest, err := cache.manifest()
			if err != nil || !bytes.Equal(manifest, []byte{42, 7}) {
				t.Errorf(
					"manifest = %v, %v; want original occurrence",
					manifest,
					err,
				)
			}
			txs, err := readEBTxs(nil, eb, 7, txRange{1, 3})
			if err != nil || len(txs) != 2 || !bytes.Equal(txs[0], []byte{7}) {
				t.Errorf("selected transactions = %v, %v", txs, err)
			}
		})
	}
	wg.Wait()
	if got := reader.reads.Load(); got != 2 {
		t.Fatalf(
			"concurrent cache readers issued %d storage reads, want one manifest and one closure read",
			got,
		)
	}
}

func TestCachedReadsPreserveReturnedOwnershipAndOccurrence(t *testing.T) {
	t.Parallel()
	reader := &countedEBReader{}
	eb := &benchEb{readCache: newEBReadCache(reader, []byte{42}, 7)}
	txs, err := readEBTxs(nil, eb, 7, txRange{1, 3})
	if err != nil || len(txs) != 2 {
		t.Fatalf("selected closure = %v, %v", txs, err)
	}
	txs[0][0] = 99
	txs[1] = nil
	again, err := readEBTxs(nil, eb, 7, txRange{end: -1})
	if err != nil || len(again) != 3 || !bytes.Equal(again[1], []byte{7}) ||
		!bytes.Equal(again[2], []byte{0x82, 1, 2}) {
		t.Fatalf("returned mutation changed cached closure: %v, %v", again, err)
	}
	other := &benchEb{readCache: newEBReadCache(reader, []byte{42}, 8)}
	txs, err = readEBTxs(nil, other, 8, txRange{1, 2})
	if err != nil || len(txs) != 1 || !bytes.Equal(txs[0], []byte{8}) {
		t.Fatalf("different occurrence reused old closure: %v, %v", txs, err)
	}
}

func TestCachedReadsDoNotHideStorageFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("storage read failed")
	eb := &benchEb{
		readCache: newEBReadCache(&countedEBReader{err: want}, []byte{42}, 7),
	}
	if _, err := readEBTxs(nil, eb, 7, txRange{0, 1}); !errors.Is(err, want) {
		t.Fatalf("closure error = %v, want storage error", err)
	}
	if _, err := eb.readCache.manifest(); !errors.Is(err, want) {
		t.Fatalf("manifest error = %v, want storage error", err)
	}
}

func TestCachedReadPackedTransactionsKeepIndependentCapacity(t *testing.T) {
	t.Parallel()
	eb := &benchEb{readCache: &ebReadCache{
		txs: func() ([]cbor.RawMessage, error) {
			return []cbor.RawMessage{nil, {}, {1}, {2}}, nil
		},
	}}
	txs, err := readEBTxs(nil, eb, 0, txRange{end: -1})
	if err != nil || len(txs) != 4 {
		t.Fatalf("packed transactions = %v, %v", txs, err)
	}
	if txs[0] != nil || txs[1] == nil || len(txs[1]) != 0 {
		t.Fatalf("nil and empty transaction distinction lost: %v", txs)
	}
	appended := append(txs[2], 99)
	if !bytes.Equal(appended, []byte{1, 99}) ||
		!bytes.Equal(txs[3], []byte{2}) {
		t.Fatalf("append overwrote adjacent transaction: %v", txs)
	}
}

// countingLeiosReads counts the reads the benchmark makes of the Leios store,
// where endorser blocks live.
type countingLeiosReads struct {
	leiosdb.Store
	reads atomic.Int64
}

func (s *countingLeiosReads) Get(eb leiosdb.Ref) (leiosdb.Content, error) {
	s.reads.Add(1)
	return s.Store.Get(eb)
}

func TestRunBenchCachedReadsReduceActualStorageRequests(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "database", true: "cached"}[cached],
			func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(
					t,
					&database.Config{DataDir: t.TempDir()},
				)
				if err != nil {
					t.Fatal(err)
				}
				base, err := db.LeiosDB()
				if err != nil {
					t.Fatal(err)
				}
				counter := &countingLeiosReads{Store: base}
				db.SetLeiosDB(counter)
				config := validTestConfig()
				const peers = 5
				config.downstreamPeers = peers
				config.cachedReads = cached
				if err := runBench(db, config); err != nil {
					t.Fatal(err)
				}
				// one manifest and closure read per peer, vote and apply
				want := int64(2*peers + 2)
				if cached {
					want = 2
				}
				if got := counter.reads.Load(); got != want {
					t.Fatalf(
						"cached=%v issued %d Leios store reads, want %d",
						cached,
						got,
						want,
					)
				}
			},
		)
	}
}

func TestMemoryProfileWritesCompressedProfile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "heap.pprof")
	stop := startMemoryProfile(path)
	require.NoError(t, stop())
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close() //nolint:errcheck
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close() //nolint:errcheck
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestMemoryProfileReportsCreateError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "absent", "heap.pprof")
	stop := startMemoryProfile(path)
	require.ErrorIs(t, stop(), os.ErrNotExist)
}
