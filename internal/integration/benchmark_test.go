package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

// getTestBackends returns a slice of test backends for benchmarking
func getTestBackends(b *testing.B, diskDataDir string) []struct {
	name   string
	config *database.Config
} {
	backends := []struct {
		name   string
		config *database.Config
	}{
		{
			name: "memory",
			config: &database.Config{
				BlobPlugin:     "badger",
				DataDir:        "",
				MetadataPlugin: "sqlite",
			},
		},
		{
			name: "disk",
			config: &database.Config{
				BlobPlugin:     "badger",
				DataDir:        diskDataDir,
				MetadataPlugin: "sqlite",
			},
		},
	}

	// Add cloud backends if credentials are available
	if hasGCSCredentials() {
		testBucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
		if testBucket == "" {
			testBucket = "dingo-test-bucket"
		}
		// Use path prefix for isolation instead of unique bucket names
		testPrefix := strings.ReplaceAll(b.Name(), "/", "-")
		backends = append(backends, struct {
			name   string
			config *database.Config
		}{
			name: "GCS",
			config: &database.Config{
				BlobPlugin:     "gcs",
				DataDir:        "gcs://" + testBucket + "/" + testPrefix,
				MetadataPlugin: "sqlite",
			},
		})
	}

	if hasS3Credentials() {
		testBucket := os.Getenv("DINGO_TEST_S3_BUCKET")
		if testBucket == "" {
			testBucket = "dingo-test-bucket"
		}
		// Use path prefix for isolation instead of unique bucket names
		testPrefix := strings.ReplaceAll(b.Name(), "/", "-")
		backends = append(backends, struct {
			name   string
			config *database.Config
		}{
			name: "S3",
			config: &database.Config{
				BlobPlugin:     "s3",
				DataDir:        "s3://" + testBucket + "/" + testPrefix,
				MetadataPlugin: "sqlite",
			},
		})
	}

	return backends
}

// BenchmarkStorageBackends benchmarks different storage backends
func BenchmarkStorageBackends(b *testing.B) {
	backends := getTestBackends(b, b.TempDir())

	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			benchmarkStorageBackend(b, backend.name, backend.config)
		})
	}
}

// BenchmarkTestLoad benchmarks the equivalent of loading the first 200 blocks
func BenchmarkTestLoad(b *testing.B) {
	backends := getTestBackends(b, b.TempDir())

	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			benchmarkTestLoad(b, backend.name, backend.config)
		})
	}
}

func benchmarkStorageBackend(
	b *testing.B,
	backendName string,
	config *database.Config,
) {
	var tempDir string
	var err error
	// Create temporary directory for on-disk local backends
	if config.BlobPlugin == "badger" && config.DataDir != "" {
		tempDir, err = os.MkdirTemp(
			"",
			fmt.Sprintf("dingo-bench-%s-", backendName),
		)
		if err != nil {
			b.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)
		config.DataDir = filepath.Join(tempDir, "data")
	}

	// Create database with the specified backend
	db, err := database.New(config)
	if err != nil {
		b.Fatalf(
			"failed to create database with %s backend: %v",
			backendName,
			err,
		)
	}
	defer db.Close()

	// Pre-populate with 10 real blocks
	blocks, err := loadBlockData(10)
	if err != nil {
		b.Fatalf("failed to load block data: %v", err)
	}

	for i := range 10 {
		txn := db.Blob().NewTransaction(true)
		key := fmt.Appendf(nil, "block-%d", i)
		if err := txn.Set(key, blocks[i]); err != nil {
			txn.Discard()
			b.Fatalf("failed to set block %d: %v", i, err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("failed to commit block %d: %v", i, err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		// Process 10 blocks of data
		txn := db.Blob().NewTransaction(false)
		for blockNum := range 10 {
			key := fmt.Appendf(nil, "block-%d", blockNum)
			_, err := txn.Get(key)
			if err != nil {
				b.Fatalf("failed to get block %d: %v", blockNum, err)
			}
		}
		txn.Discard()
	}
}

func benchmarkTestLoad(
	b *testing.B,
	backendName string,
	config *database.Config,
) {
	var tempDir string
	var err error
	// Create temporary directory for on-disk local backends
	if config.BlobPlugin == "badger" && config.DataDir != "" {
		tempDir, err = os.MkdirTemp(
			"",
			fmt.Sprintf("dingo-testload-%s-", backendName),
		)
		if err != nil {
			b.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)
		config.DataDir = filepath.Join(tempDir, "data")
	}

	// Create database with the specified backend
	db, err := database.New(config)
	if err != nil {
		b.Fatalf(
			"failed to create database with %s backend: %v",
			backendName,
			err,
		)
	}
	defer db.Close()

	// Pre-populate with 200 real blocks
	blocks, err := loadBlockData(200)
	if err != nil {
		b.Fatalf("failed to load block data: %v", err)
	}

	for i := range 200 {
		txn := db.Blob().NewTransaction(true)
		key := fmt.Appendf(nil, "block-%d", i)
		if err := txn.Set(key, blocks[i]); err != nil {
			txn.Discard()
			b.Fatalf("failed to set block %d: %v", i, err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("failed to commit block %d: %v", i, err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		// Load first 200 blocks
		txn := db.Blob().NewTransaction(false)
		for blockNum := range 200 {
			key := fmt.Appendf(nil, "block-%d", blockNum)
			_, err := txn.Get(key)
			if err != nil {
				b.Fatalf("failed to get block %d: %v", blockNum, err)
			}
		}
		txn.Discard()
	}
}
