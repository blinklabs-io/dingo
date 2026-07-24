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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
)

// loadBlockData loads real block data from testdata chunks for benchmarking
func loadBlockData(numBlocks int) ([][]byte, error) {
	var blocks [][]byte
	// Use absolute path to testdata directory by going up from the current package
	// internal/integration -> internal -> root -> database/immutable/testdata
	testdataDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)

	// Open immutable database to parse chunks
	imm, err := immutable.New(testdataDir)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open immutable DB at %s: %v",
			testdataDir,
			err,
		)
	}

	// Create iterator from origin (slot 0) to get all blocks
	origin := ocommon.NewPoint(0, make([]byte, 32))
	iter, err := imm.BlocksFromPoint(origin)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create block iterator from %s: %v",
			testdataDir,
			err,
		)
	}
	defer iter.Close()

	// Extract blocks
	for len(blocks) < numBlocks {
		block, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to read block: %v", err)
		}
		if block == nil {
			break
		}

		blocks = append(blocks, block.Cbor)
	}

	if len(blocks) == 0 {
		return nil, fmt.Errorf("no blocks found in testdata")
	}

	if len(blocks) < numBlocks {
		// If we don't have enough blocks, duplicate the ones we have
		for len(blocks) < numBlocks {
			for _, block := range blocks {
				if len(blocks) >= numBlocks {
					break
				}
				blocks = append(blocks, block)
			}
		}
	}

	return blocks[:numBlocks], nil
}

// storageBenchBackend is one storage backend under benchmark: a display name,
// the dbtest options that compose its database, and whether it is a local
// on-disk backend that should receive a fresh directory for each run.
type storageBenchBackend struct {
	name      string
	opts      dbtest.Options
	localDisk bool
}

// getTestBackends returns the storage backends to benchmark. Memory and disk
// (Badger) are always present; cloud backends are appended only in
// dingo_extra_plugins builds when the matching credentials are configured.
func getTestBackends(diskDataDir, benchName string) []storageBenchBackend {
	backends := []storageBenchBackend{
		{
			name: "memory",
			opts: dbtest.Options{Config: &database.Config{DataDir: ""}},
		},
		{
			name: "disk",
			opts: dbtest.Options{
				Config: &database.Config{DataDir: diskDataDir},
			},
			localDisk: true,
		},
	}
	return append(
		backends,
		cloudStorageBenchmarkBackends(diskDataDir, benchName)...,
	)
}

// BenchmarkStorageBackends benchmarks different storage backends
func BenchmarkStorageBackends(b *testing.B) {
	for _, backend := range getTestBackends(b.TempDir(), b.Name()) {
		b.Run(backend.name, func(b *testing.B) {
			benchmarkStorageBackend(b, backend)
		})
	}
}

// BenchmarkTestLoad benchmarks the equivalent of loading the first 200 blocks
func BenchmarkTestLoad(b *testing.B) {
	for _, backend := range getTestBackends(b.TempDir(), b.Name()) {
		b.Run(backend.name, func(b *testing.B) {
			benchmarkTestLoad(b, backend)
		})
	}
}

func benchmarkStorageBackend(
	b *testing.B,
	backend storageBenchBackend,
) {
	opts := backend.opts
	// Give a local on-disk backend a fresh directory for this run.
	if backend.localDisk && opts.Config.DataDir != "" {
		tempDir, err := os.MkdirTemp(
			"",
			fmt.Sprintf("dingo-bench-%s-", backend.name),
		)
		if err != nil {
			b.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)
		cfg := *opts.Config
		cfg.DataDir = filepath.Join(tempDir, "data")
		opts.Config = &cfg
	}

	// Create database with the specified backend
	db, err := dbtest.NewDatabaseWithOptions(b, opts)
	if err != nil {
		b.Fatalf(
			"failed to create database with %s backend: %v",
			backend.name,
			err,
		)
	}
	defer dbtest.CloseDatabase(db)

	// Pre-populate with 10 real blocks
	blocks, err := loadBlockData(10)
	if err != nil {
		b.Fatalf("failed to load block data: %v", err)
	}

	for i := range 10 {
		txn := db.Transaction(true)
		key := fmt.Appendf(nil, "block-%d", i)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		if err := blob.Set(txn.Blob(), key, blocks[i]); err != nil {
			txn.Rollback()
			b.Fatalf("failed to set block %d: %v", i, err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("failed to commit block %d: %v", i, err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		// Process 10 blocks of data
		txn := db.Transaction(false)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		for blockNum := range 10 {
			key := fmt.Appendf(nil, "block-%d", blockNum)
			_, err := blob.Get(txn.Blob(), key)
			if err != nil {
				txn.Rollback()
				b.Fatalf("failed to get block %d: %v", blockNum, err)
			}
		}
		txn.Rollback()
	}
}

func benchmarkTestLoad(
	b *testing.B,
	backend storageBenchBackend,
) {
	opts := backend.opts
	// Give a local on-disk backend a fresh directory for this run.
	if backend.localDisk && opts.Config.DataDir != "" {
		tempDir, err := os.MkdirTemp(
			"",
			fmt.Sprintf("dingo-testload-%s-", backend.name),
		)
		if err != nil {
			b.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)
		cfg := *opts.Config
		cfg.DataDir = filepath.Join(tempDir, "data")
		opts.Config = &cfg
	}

	// Create database with the specified backend
	db, err := dbtest.NewDatabaseWithOptions(b, opts)
	if err != nil {
		b.Fatalf(
			"failed to create database with %s backend: %v",
			backend.name,
			err,
		)
	}
	defer dbtest.CloseDatabase(db)

	// Pre-populate with 200 real blocks
	blocks, err := loadBlockData(200)
	if err != nil {
		b.Fatalf("failed to load block data: %v", err)
	}

	for i := range 200 {
		txn := db.Transaction(true)
		key := fmt.Appendf(nil, "block-%d", i)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		if err := blob.Set(txn.Blob(), key, blocks[i]); err != nil {
			txn.Rollback()
			b.Fatalf("failed to set block %d: %v", i, err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("failed to commit block %d: %v", i, err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		// Load first 200 blocks
		txn := db.Transaction(false)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		for blockNum := range 200 {
			key := fmt.Appendf(nil, "block-%d", blockNum)
			_, err := blob.Get(txn.Blob(), key)
			if err != nil {
				txn.Rollback()
				b.Fatalf("failed to get block %d: %v", blockNum, err)
			}
		}
		txn.Rollback()
	}
}
