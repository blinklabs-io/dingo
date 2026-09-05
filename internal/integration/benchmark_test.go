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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
)

// loadImmutableBlocks loads numBlocks real blocks -- hash, type, slot, and
// CBOR together, so a caller that needs a block whose (hash, type, slot)
// genuinely match its own CBOR (e.g. for a cloud backend's content
// verification, which independently re-derives all three from the bytes)
// can use them as a set instead of only the raw CBOR loadBlockData exposes.
func loadImmutableBlocks(numBlocks int) ([]immutable.Block, error) {
	var blocks []immutable.Block
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

		blocks = append(blocks, *block)
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

// loadBlockData loads real block CBOR from testdata chunks for benchmarking.
func loadBlockData(numBlocks int) ([][]byte, error) {
	blocks, err := loadImmutableBlocks(numBlocks)
	if err != nil {
		return nil, err
	}
	cbors := make([][]byte, len(blocks))
	for i, block := range blocks {
		cbors[i] = block.Cbor
	}
	return cbors, nil
}

// verifyBlockSelfConsistent checks the same things S3/GCS's own
// blockverify.Hash does (database/plugin/blob/internal/blockverify, which
// this mirrors without importing across that internal package boundary):
// decoding block.Cbor under block.Type must reproduce block.Hash and
// block.Slot, and -- since NewBlockFromCbor treats Type as a decode hint,
// and for Shelley and later eras the block hash covers only the header,
// which adjacent eras share the layout of, so decoding under an
// adjacent-but-wrong era can still succeed and reproduce the same hash and
// slot -- the era independently re-derived from the decoded header
// (Byron exempt, matching blockverify.checkEra: its hash already covers
// the block-type byte) must also match block.Type.
func verifyBlockSelfConsistent(block immutable.Block) error {
	decoded, err := gledger.NewBlockFromCbor(block.Type, block.Cbor)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	gotHash := decoded.Hash()
	if !bytes.Equal(gotHash[:], block.Hash) {
		return fmt.Errorf(
			"hash mismatch: got %x, recorded %x",
			gotHash[:],
			block.Hash,
		)
	}
	if decoded.SlotNumber() != block.Slot {
		return fmt.Errorf(
			"slot mismatch: got %d, recorded %d",
			decoded.SlotNumber(),
			block.Slot,
		)
	}
	if block.Type == gledger.BlockTypeByronEbb ||
		block.Type == gledger.BlockTypeByronMain {
		return nil
	}
	header := decoded.Header()
	if header == nil {
		return errors.New("block has no header to derive the era from")
	}
	derived, err := gledger.DetermineBlockType(header.Cbor())
	if err != nil {
		return fmt.Errorf("derive era from header: %w", err)
	}
	if derived != block.Type {
		return fmt.Errorf(
			"era mismatch: header derives %d, recorded %d",
			derived,
			block.Type,
		)
	}
	return nil
}

// loadMigrationFixtureBlock returns a real testdata block that passes
// verifyBlockSelfConsistent, for a caller (storage_migration_test.go's
// blob dataset) that needs one whose (hash, type, slot) as a set are what
// blockverify.Hash would accept -- not just one that decodes successfully
// under its own recorded Type, which verifyBlockSelfConsistent's own doc
// comment explains is not the same thing.
//
// A plain loadImmutableBlocks(1) is not enough on its own: this testdata
// set's first ~20 blocks decode fine under their recorded Type and
// reproduce the right hash/slot, but carry a protocol-version field
// DetermineBlockType can't place in any known era's range ("unknown proto
// major 7 for Shelley-like") -- evidently placeholder data left over from
// however this fixture set was originally generated, not a genuine era
// disagreement. Scanning forward for the first block that passes every
// check, rather than assuming the first block in iteration order will,
// is what keeps this independent of exactly which testdata blocks happen
// to carry that placeholder.
func loadMigrationFixtureBlock() (immutable.Block, error) {
	const scanLimit = 50
	blocks, err := loadImmutableBlocks(scanLimit)
	if err != nil {
		return immutable.Block{}, err
	}
	for _, block := range blocks {
		if err := verifyBlockSelfConsistent(block); err == nil {
			return block, nil
		}
	}
	return immutable.Block{}, fmt.Errorf(
		"no self-consistent block found in the first %d testdata blocks",
		scanLimit,
	)
}

// TestLoadImmutableBlocksAreSelfConsistent proves loadMigrationFixtureBlock
// actually returns a block whose (hash, type, slot) blockverify.Hash would
// accept -- see verifyBlockSelfConsistent's own doc comment for what that
// means and why checking hash and slot alone would not be enough.
func TestLoadImmutableBlocksAreSelfConsistent(t *testing.T) {
	block, err := loadMigrationFixtureBlock()
	require.NoError(t, err)
	require.NoError(t, verifyBlockSelfConsistent(block))
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
