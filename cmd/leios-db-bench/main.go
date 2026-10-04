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

// Command leios-db-bench runs the Dingo workload corresponding to the
// ouroboros-consensus leios-db-bench entrypoint.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
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
	prePopulatedEbs int
	txsPerEb        int
	txSizeBytes     int
	fetchClients    int
	ebsPerClient    int
	fetchServers    int
	chainSelReads   int
	gcTicks         int
	runs            int
}

type benchPoint struct {
	slot uint64
	hash []byte
}

type benchEnv struct {
	db        *database.Database
	points    []benchPoint
	nextEbIdx int
	config    benchConfig
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
	printBenchInfo(config)

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

	dbDir := filepath.Join(tmpDir, "db")
	logger := slog.New(slog.DiscardHandler)
	runtime, err := plugins.OpenDatabase(
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
		if err := runtime.Close(context.Background()); err != nil {
			runErr = errors.Join(runErr,
				fmt.Errorf("close Dingo database: %w", err))
		}
	}()
	if err := runtime.RecoveryError(); err != nil {
		return fmt.Errorf("open Dingo database recovery state: %w", err)
	}

	env := &benchEnv{
		db:     runtime.Database,
		points: make([]benchPoint, 0, config.prePopulatedEbs),
		config: config,
	}
	if err := setupBenchEnv(env); err != nil {
		return err
	}
	return runBench(env)
}

func parseConfig() benchConfig {
	config := benchConfig{}
	flag.IntVar(&config.prePopulatedEbs, "pre-populated-ebs", 500,
		"number of complete EBs inserted before timing")
	flag.IntVar(&config.txsPerEb, "txs-per-eb", 200,
		"number of transactions in each EB")
	flag.IntVar(&config.txSizeBytes, "tx-size-bytes", 16_384,
		"serialized transaction item size in bytes")
	flag.IntVar(&config.fetchClients, "fetch-clients", 3,
		"number of concurrent EB writers")
	flag.IntVar(&config.ebsPerClient, "ebs-per-client", 20,
		"new EBs written by each fetch client per iteration")
	flag.IntVar(&config.fetchServers, "fetch-servers", 3,
		"number of concurrent fetch readers")
	flag.IntVar(&config.chainSelReads, "chain-sel-reads", 50,
		"closure reads per iteration")
	flag.IntVar(&config.gcTicks, "gc-ticks", 3,
		"GC ticker calls per iteration")
	flag.IntVar(&config.runs, "runs", 5,
		"timed workload repetitions after the warmup")
	flag.Parse()
	return config
}

func validateConfig(config benchConfig) error {
	if config.prePopulatedEbs < 1 || config.txsPerEb < 1 ||
		config.txSizeBytes < 32 || config.txSizeBytes > maxLeiosTxSizeBytes ||
		config.fetchClients < 1 ||
		config.ebsPerClient < 1 || config.fetchServers < 1 ||
		config.chainSelReads < 1 || config.gcTicks < 0 || config.runs < 1 {
		return errors.New(
			"invalid benchmark configuration: counts must be positive, " +
				"GC ticks may be zero, and transaction sizes must be " +
				"between 32 and 65535 bytes",
		)
	}
	if _, err := txPayloadSize(config.txSizeBytes); err != nil {
		return fmt.Errorf("invalid benchmark configuration: %w", err)
	}
	return nil
}

func printBenchInfo(config benchConfig) {
	fmt.Println("Dingo LeiosDB concurrent benchmark")
	fmt.Println()
	fmt.Println("Database setup:")
	fmt.Printf("  EBs pre-populated : %d\n", config.prePopulatedEbs)
	fmt.Printf("  TXs per EB        : %d\n", config.txsPerEb)
	fmt.Printf("  Total TXs         : %d\n", config.prePopulatedEbs*config.txsPerEb)
	fmt.Printf("  TX bytes per item : %d\n", config.txSizeBytes)
	fmt.Println("  Blob backend      : Badger (core defaults)")
	fmt.Println()
	fmt.Println("Concurrent workload per iteration:")
	fmt.Printf("  Fetch clients   (×%d): %d SetLeiosEB calls each\n",
		config.fetchClients, config.ebsPerClient)
	fmt.Printf("  Fetch servers   (×%d): 30 manifest reads + 10 transaction batches each\n",
		config.fetchServers)
	fmt.Printf("  Chain-sel reader(×1): %d transaction-closure reads\n",
		config.chainSelReads)
	fmt.Printf("  GC ticker       (×1): %d no-op calls\n", config.gcTicks)
	fmt.Println()
	fmt.Printf("Runs: 1 warmup + %d timed\n", config.runs)
	fmt.Println()
	fmt.Println("Comparison notes:")
	fmt.Println("  Haskell uses SQLite; Dingo stores Leios EB data in its Badger blob store.")
	fmt.Println("  Dingo's SQLite metadata store is not touched by these Leios operations.")
	fmt.Println("  Dingo reads the stored transaction list before selecting batch offsets.")
	fmt.Println("  Dingo manifest refs bind each CBOR body's Blake2b-256 hash and serialized size.")
	fmt.Println("  The reference fixture uses synthetic hashes and 200-byte manifest sizes.")
	fmt.Println("  GC ticks match the reference's current no-op call; Badger's periodic GC remains enabled.")
	fmt.Println()
}

func setupBenchEnv(env *benchEnv) error {
	config := env.config
	fmt.Printf("Inserting EBs: ")
	for i := range config.prePopulatedEbs {
		point, err := insertOneEb(env.db, i, config)
		if err != nil {
			return fmt.Errorf("prepopulate EB %d: %w", i, err)
		}
		env.points = append(env.points, point)
		step := max(config.prePopulatedEbs/10, 1)
		if (i+1)%step == 0 || i+1 == config.prePopulatedEbs {
			fmt.Printf("%d ", i+1)
		}
	}
	env.nextEbIdx = config.prePopulatedEbs
	fmt.Println("done")
	return nil
}

func runBench(env *benchEnv) error {
	config := env.config
	if err := benchConcurrentAll(env); err != nil {
		return fmt.Errorf("warmup workload: %w", err)
	}
	times := make([]time.Duration, config.runs)
	for i := range config.runs {
		started := time.Now()
		if err := benchConcurrentAll(env); err != nil {
			return fmt.Errorf("timed workload %d: %w", i+1, err)
		}
		times[i] = time.Since(started)
		fmt.Printf("  run %d/%d: %s\n", i+1, config.runs, showTime(times[i]))
	}
	var total time.Duration
	minimum := times[0]
	maximum := times[0]
	for _, elapsed := range times {
		total += elapsed
		minimum = min(minimum, elapsed)
		maximum = max(maximum, elapsed)
	}
	average := total / time.Duration(len(times))
	fmt.Printf("  => min=%s  avg=%s  max=%s\n",
		showTime(minimum), showTime(average), showTime(maximum))
	return nil
}

func benchConcurrentAll(env *benchEnv) error {
	config := env.config
	startIdx := env.nextEbIdx
	env.nextEbIdx += config.fetchClients * config.ebsPerClient

	workerCount := config.fetchClients + config.fetchServers + 2
	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	start := func(work func() error) {
		wg.Go(func() {
			errCh <- work()
		})
	}

	start(func() error { return chainSelReader(env) })
	start(func() error { return gcTicker(config.gcTicks) })
	for client := range config.fetchClients {
		first := startIdx + client*config.ebsPerClient
		start(func() error {
			return fetchClient(env, first, config.ebsPerClient)
		})
	}
	for server := range config.fetchServers {
		start(func() error { return fetchServer(env, server) })
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func fetchClient(env *benchEnv, firstEb, count int) error {
	for i := range count {
		idx := firstEb + i
		if _, err := insertOneEb(env.db, idx, env.config); err != nil {
			return fmt.Errorf("write EB %d: %w", idx, err)
		}
	}
	return nil
}

func chainSelReader(env *benchEnv) error {
	for i := range env.config.chainSelReads {
		point := env.points[i%len(env.points)]
		if _, err := env.db.GetLeiosEBTxs(point.hash, point.slot); err != nil {
			return fmt.Errorf("read transaction closure at slot %d: %w", point.slot, err)
		}
	}
	return nil
}

func gcTicker(ticks int) error {
	for i := 1; i <= ticks; i++ {
		leiosGarbageCollect(uint64(i * 10))
	}
	return nil
}

// Dingo has no explicit Leios garbage-collection call. The reference
// benchmark's current backend implementation is also a no-op; Badger's own
// periodic value-log collection stays enabled through the production provider.
func leiosGarbageCollect(_ uint64) {}

func fetchServer(env *benchEnv, server int) error {
	points := env.points
	for i := range 30 {
		point := points[(server*30+i)%len(points)]
		if _, err := env.db.GetLeiosEBManifest(point.hash, point.slot); err != nil {
			return fmt.Errorf("read EB body at slot %d: %w", point.slot, err)
		}
	}
	for i := range 10 {
		point := points[(server*10+i)%len(points)]
		txs, err := env.db.GetLeiosEBTxs(point.hash, point.slot)
		if err != nil {
			return fmt.Errorf("read transaction batch at slot %d: %w", point.slot, err)
		}
		for offset := 0; offset < env.config.txsPerEb; offset += 10 {
			if offset >= len(txs) {
				return fmt.Errorf("transaction batch at slot %d has %d entries, need offset %d",
					point.slot, len(txs), offset)
			}
			_ = txs[offset]
		}
	}
	return nil
}

func insertOneEb(
	db *database.Database,
	ebIdx int,
	config benchConfig,
) (benchPoint, error) {
	point, err := genPoint(ebIdx)
	if err != nil {
		return benchPoint{}, err
	}
	txs, err := genTxs(ebIdx, config)
	if err != nil {
		return benchPoint{}, err
	}
	manifest, err := genManifest(txs)
	if err != nil {
		return benchPoint{}, err
	}
	if err := db.SetLeiosEB(point.slot, point.hash, manifest, txs); err != nil {
		return benchPoint{}, err
	}
	return point, nil
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

func genPoint(ebIdx int) (benchPoint, error) {
	slot, err := nonNegativeUint64(ebIdx)
	if err != nil {
		return benchPoint{}, err
	}
	return benchPoint{
		slot: slot,
		hash: genHash(fmt.Sprintf("ebHash:%d", ebIdx)),
	}, nil
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

func showTime(elapsed time.Duration) string {
	switch {
	case elapsed < time.Microsecond:
		return fmt.Sprintf("%d ns", elapsed.Nanoseconds())
	case elapsed < time.Millisecond:
		return fmt.Sprintf("%d μs", elapsed.Round(time.Microsecond).Microseconds())
	case elapsed < time.Second:
		return fmt.Sprintf("%d ms", elapsed.Round(time.Millisecond).Milliseconds())
	default:
		return fmt.Sprintf("%.3f s", elapsed.Seconds())
	}
}
