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

package integration

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	blobbadger "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The storage scale benchmarks measure SQLite UTxO lookups and writes, Badger
// block volumes, and explicit-snapshot commit pause at a caller-chosen
// cardinality. They are not run at scale by "go test -bench=."; see the
// bench-storage-scale target and the scale runbook in DATABASE.md.
//
// DINGO_BENCH_SCALE is a comma-separated list of UTxO counts.
// DINGO_BENCH_BLOCKS is the block count, set independently because historical
// block volume does not follow from live UTxO cardinality.

const (
	scaleSeedBatch    = 100_000
	scaleWriteBatch   = 1_000
	scaleDefaultSamp  = 20_000
	scaleBlobTxnBytes = 512 << 10
)

var scaleDefault = []int{10_000}

const scaleDefaultBlocks = 1_000

type scaleConfig struct {
	scales           []int
	blocks           int
	blockBytes       int
	latencyLimit     int
	dataDir          string
	productionBadger bool
}

func loadScaleConfig(tb testing.TB) scaleConfig {
	tb.Helper()
	cfg, err := scaleConfigFrom(os.Getenv)
	require.NoError(tb, err)
	return cfg
}

func scaleConfigFrom(getenv func(string) string) (scaleConfig, error) {
	var cfg scaleConfig
	var err error
	if cfg.scales, err = envScales(getenv, envBenchScale, scaleDefault); err != nil {
		return cfg, err
	}
	blocks, err := envScales(getenv, envBenchBlocks, []int{scaleDefaultBlocks})
	if err != nil {
		return cfg, err
	}
	if len(blocks) != 1 {
		return cfg, fmt.Errorf(
			"%s takes one block count, got %q", envBenchBlocks, getenv(envBenchBlocks),
		)
	}
	cfg.blocks = blocks[0]
	if cfg.blockBytes, err = envIntAtLeast(getenv, envBenchBlockBytes, 32<<10, 8); err != nil {
		return cfg, err
	}
	if cfg.latencyLimit, err = envInt(getenv, envBenchLatencySamps, scaleDefaultSamp); err != nil {
		return cfg, err
	}
	cfg.dataDir = getenv(envBenchDataDir)
	cfg.productionBadger = getenv(envBenchScale) != "" || getenv(envBenchBlocks) != ""
	return cfg, nil
}

// newScaleDB opens a file-backed database. Scale runs need real files so the
// page cache, WAL and LSM behave as they would on a node; DINGO_BENCH_DATADIR
// selects the volume to measure.
func (c scaleConfig) newScaleDB(
	tb testing.TB,
	reg prometheus.Registerer,
) *database.Database {
	tb.Helper()
	dir := tb.TempDir()
	if c.dataDir != "" {
		require.NoError(tb, os.MkdirAll(c.dataDir, 0o750))
		var err error
		dir, err = os.MkdirTemp(c.dataDir, "scale-*")
		require.NoError(tb, err)
		tb.Cleanup(func() { _ = os.RemoveAll(dir) })
	}
	opts := dbtest.Options{Config: &database.Config{DataDir: dir, PromRegistry: reg}}
	if c.productionBadger {
		opts.Blob.Config = map[string]any{
			"valueLogFileSize": uint64(blobbadger.DefaultValueLogFileSize),
			"memTableSize":     uint64(blobbadger.DefaultMemTableSize),
		}
	}
	db, err := dbtest.NewDatabaseWithOptions(tb, opts)
	require.NoError(tb, err)
	return db
}

func scaleTxID(index int) []byte {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], uint64(index)+1) // #nosec G115 -- non-negative benchmark index
	return txID
}

// seedScaleUtxos inserts rows [from, to) in large transactions and returns
// how long the inserts took.
func seedScaleUtxos(tb testing.TB, raw *sql.DB, from, to int) time.Duration {
	tb.Helper()
	start := time.Now()
	for lo := from; lo < to; lo += scaleSeedBatch {
		hi := min(lo+scaleSeedBatch, to)
		insertScaleUtxos(tb, raw, lo, hi)
	}
	return time.Since(start)
}

func insertScaleUtxos(tb testing.TB, raw *sql.DB, lo, hi int) {
	tb.Helper()
	tx, err := raw.BeginTx(context.Background(), nil)
	require.NoError(tb, err)
	stmt, err := tx.PrepareContext(
		context.Background(),
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount) "+
			"VALUES (?, 0, ?, 0, '1000000')",
	)
	require.NoError(tb, err)
	for i := lo; i < hi; i++ {
		_, err = stmt.ExecContext(
			context.Background(), scaleTxID(i), int64(i+1),
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

func dirBytes(tb testing.TB, dir string) int64 {
	tb.Helper()
	var total int64
	require.NoError(tb, filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			total += fi.Size()
		}
		return nil
	}))
	return total
}

func reportRSS(b *testing.B) {
	b.Helper()
	if rss, ok := rssBytes(); ok {
		b.ReportMetric(float64(rss), "rss-bytes")
	}
}

// BenchmarkStorageScaleUtxo measures SQLite metadata UTxO point lookups and
// batched writes against a table seeded to each scale.
func BenchmarkStorageScaleUtxo(b *testing.B) {
	cfg := loadScaleConfig(b)
	for _, n := range cfg.scales {
		b.Run("utxos="+scaleLabel(n), func(b *testing.B) {
			db := cfg.newScaleDB(b, nil)
			raw, err := dbtest.RawSQLiteMetadata(b, db)
			require.NoError(b, err)
			seedTime := seedScaleUtxos(b, raw, 0, n)

			b.Run("lookup", func(b *testing.B) {
				rng := rand.New(rand.NewPCG(1, uint64(n))) // #nosec G404 -- deterministic benchmark keys
				rec := newLatencyRecorder(cfg.latencyLimit)
				meta := db.Metadata()
				b.ResetTimer()
				for b.Loop() {
					id := scaleTxID(rng.IntN(n))
					start := time.Now()
					utxo, err := meta.GetUtxo(id, 0, nil)
					rec.record(time.Since(start))
					if err != nil || utxo == nil {
						b.Fatalf("lookup failed: utxo=%v err=%v", utxo, err)
					}
				}
				b.StopTimer()
				rec.report(b, "lookup")
				b.ReportMetric(float64(n), "utxos")
				b.ReportMetric(
					float64(n)/seedTime.Seconds(), "seed-rows/s",
				)
				b.ReportMetric(float64(dirBytes(b, db.DataDir())), "disk-bytes")
				reportRSS(b)
			})

			b.Run("write", func(b *testing.B) {
				rec := newLatencyRecorder(cfg.latencyLimit)
				next := n
				b.ResetTimer()
				for b.Loop() {
					start := time.Now()
					insertScaleUtxos(b, raw, next, next+scaleWriteBatch)
					rec.record(time.Since(start))
					next += scaleWriteBatch
				}
				b.StopTimer()
				rec.report(b, "write-batch")
				b.ReportMetric(float64(scaleWriteBatch), "rows/batch")
				b.ReportMetric(float64(dirBytes(b, db.DataDir())), "disk-bytes")
				reportRSS(b)
			})
		})
	}
}

func scaleLabel(n int) string { return strconv.Itoa(n) }

func seedScaleBlocks(
	tb testing.TB,
	store blob.BlobStore,
	from, to, blockBytes int,
) {
	tb.Helper()
	payload := make([]byte, blockBytes)
	// Bounded by bytes: a Badger transaction has a size budget that a fixed
	// block count would exceed at large block sizes.
	perTxn := min(max(scaleBlobTxnBytes/blockBytes, 1), 1000)
	for lo := from; lo < to; lo += perTxn {
		hi := min(lo+perTxn, to)
		txn := store.NewTransaction(true)
		for i := lo; i < hi; i++ {
			// Distinct bytes per block keep Badger's compression honest.
			rng := rand.New(rand.NewPCG(uint64(i), uint64(i)+1)) // #nosec G115 -- non-negative benchmark index
			for offset := 0; offset < len(payload); offset += 8 {
				var chunk [8]byte
				binary.LittleEndian.PutUint64(chunk[:], rng.Uint64())
				copy(payload[offset:], chunk[:])
			}
			if err := store.SetBlock(
				txn, uint64(i)*20, scaleTxID(i), payload,
				uint64(i)+1, 1, uint64(i)+1, nil,
			); err != nil { // #nosec G115 -- non-negative benchmark index
				_ = txn.Rollback()
				tb.Fatalf("write block %d: %v", i, err)
			}
		}
		require.NoError(tb, txn.Commit())
	}
}

// BenchmarkStorageScaleBlobBlocks measures Badger block writes and reads
// at DINGO_BENCH_BLOCKS blocks of DINGO_BENCH_BLOCK_BYTES, and reports the
// blob directory size and table count before, and its size and duration
// after, a full compaction.
func BenchmarkStorageScaleBlobBlocks(b *testing.B) {
	cfg := loadScaleConfig(b)
	blocks := cfg.blocks
	b.Run("blocks="+scaleLabel(blocks), func(b *testing.B) {
		db := cfg.newScaleDB(b, nil)
		store := db.Blob()
		writeStart := time.Now()
		seedScaleBlocks(b, store, 0, blocks, cfg.blockBytes)
		writeTime := time.Since(writeStart)

		rng := rand.New(rand.NewPCG(2, uint64(blocks))) // #nosec G404 -- deterministic benchmark keys
		rec := newLatencyRecorder(cfg.latencyLimit)
		b.ResetTimer()
		for b.Loop() {
			i := rng.IntN(blocks)
			txn := store.NewTransaction(false)
			start := time.Now()
			data, _, err := store.GetBlock(txn, uint64(i)*20, scaleTxID(i)) // #nosec G115 -- non-negative benchmark index
			rec.record(time.Since(start))
			_ = txn.Rollback()
			if err != nil || len(data) != cfg.blockBytes {
				b.Fatalf("read block %d: len=%d err=%v", i, len(data), err)
			}
		}
		b.StopTimer()
		rec.report(b, "read")
		b.ReportMetric(float64(blocks)/writeTime.Seconds(), "write-blocks/s")
		b.ReportMetric(float64(blocks)*float64(cfg.blockBytes), "block-volume-bytes")
		// Walk the directory: Badger's Size() is refreshed only
		// periodically and reads zero right after a short run.
		blobDir := filepath.Join(db.DataDir(), "blob")
		b.ReportMetric(float64(dirBytes(b, blobDir)), "blob-dir-bytes")
		if bs, ok := store.(interface{ DB() *badgerdb.DB }); ok {
			b.ReportMetric(float64(len(bs.DB().Tables())), "sst-tables")
			start := time.Now()
			require.NoError(b, bs.DB().Flatten(2))
			b.ReportMetric(time.Since(start).Seconds(), "flatten-s")
			b.ReportMetric(
				float64(dirBytes(b, blobDir)), "blob-dir-bytes-after-flatten",
			)
		}
		reportRSS(b)
	})
}

// BenchmarkStorageScaleSnapshotPause measures how long an explicit snapshot
// holds the commit barrier with both stores seeded to each scale, using the
// dingo_snapshot_commit_pause_seconds histogram Snapshot records.
func BenchmarkStorageScaleSnapshotPause(b *testing.B) {
	cfg := loadScaleConfig(b)
	blocks := cfg.blocks
	for _, n := range cfg.scales {
		b.Run("utxos="+scaleLabel(n)+",blocks="+scaleLabel(blocks), func(b *testing.B) {
			reg := prometheus.NewRegistry()
			db := cfg.newScaleDB(b, reg)
			raw, err := dbtest.RawSQLiteMetadata(b, db)
			require.NoError(b, err)
			seedScaleUtxos(b, raw, 0, n)
			seedScaleBlocks(b, db.Blob(), 0, blocks, cfg.blockBytes)

			snapRoot := cfg.dataDir
			if snapRoot == "" {
				snapRoot = b.TempDir()
			}
			var last lifecycle.Manifest
			i := 0
			b.ResetTimer()
			lastDir := ""
			for b.Loop() {
				b.StopTimer()
				if lastDir != "" {
					require.NoError(b, os.RemoveAll(lastDir))
				}
				b.StartTimer()
				dir := filepath.Join(snapRoot, "snap-"+strconv.Itoa(n)+"-"+strconv.Itoa(i))
				i++
				var err error
				last, err = lifecycle.Snapshot(
					context.Background(), db, dir, lifecycle.TriggerManual,
					"bench", "badger", "sqlite",
				)
				require.NoError(b, err)
				lastDir = dir
			}
			b.StopTimer()
			if lastDir != "" {
				b.Cleanup(func() { _ = os.RemoveAll(lastDir) })
			}

			families, err := reg.Gather()
			require.NoError(b, err)
			pauseObserved := false
			for _, f := range families {
				if f.GetName() != "dingo_snapshot_commit_pause_seconds" {
					continue
				}
				for _, metric := range f.GetMetric() {
					isSuccessful := false
					for _, label := range metric.GetLabel() {
						if label.GetName() == "result" && label.GetValue() == "ok" {
							isSuccessful = true
							break
						}
					}
					if !isSuccessful {
						continue
					}
					h := metric.GetHistogram()
					require.NotNil(b, h)
					require.Positive(
						b, h.GetSampleCount(),
						"successful snapshots must record commit pause",
					)
					b.ReportMetric(
						h.GetSampleSum()/float64(h.GetSampleCount()),
						"commit-pause-s",
					)
					pauseObserved = true
				}
			}
			require.True(b, pauseObserved, "commit-pause histogram has no ok series")
			b.ReportMetric(float64(last.BlobBytes), "snapshot-blob-bytes")
			b.ReportMetric(float64(last.MetadataBytes), "snapshot-metadata-bytes")
			reportRSS(b)
		})
	}
}
