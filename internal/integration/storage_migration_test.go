//go:build dingo_extra_plugins

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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	aws "github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	gcs "github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// cloudStorageBenchmarkBackends returns GCS and S3 storage backends for the
// storage benchmarks when their credentials are configured. Cloud blob stores
// read the bucket (and S3 prefix) from provider config, so DataDir is consumed
// only by the sqlite metadata store and must be a local filesystem path.
func cloudStorageBenchmarkBackends(
	diskDataDir, benchName string,
) []storageBenchBackend {
	var backends []storageBenchBackend
	if hasGCSBackend() {
		bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
		if bucket == "" {
			bucket = "dingo-test-bucket"
		}
		backends = append(backends, storageBenchBackend{
			name: "GCS",
			opts: dbtest.Options{
				Config: &database.Config{
					DataDir: filepath.Join(diskDataDir, "gcs-metadata"),
				},
				Blob: dbtest.StorageProvider{
					Name:     "gcs",
					Config:   map[string]any{"bucket": bucket},
					Register: gcs.RegisterProvider,
				},
			},
		})
	}
	if hasS3Credentials() {
		bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
		if bucket == "" {
			bucket = "dingo-test-bucket"
		}
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
		s3Config := map[string]any{
			"bucket": bucket,
			// A path prefix isolates concurrent benchmark runs within one
			// bucket instead of requiring a unique bucket per run.
			"prefix": strings.ReplaceAll(benchName, "/", "-") + "/",
			"region": region,
		}
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			s3Config["endpoint"] = endpoint
		}
		backends = append(backends, storageBenchBackend{
			name: "S3",
			opts: dbtest.Options{
				Config: &database.Config{
					DataDir: filepath.Join(diskDataDir, "s3-metadata"),
				},
				Blob: dbtest.StorageProvider{
					Name:     "s3",
					Config:   s3Config,
					Register: aws.RegisterProvider,
				},
			},
		})
	}
	return backends
}

func hasGCSBackend() bool {
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" ||
		os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		adcPath := filepath.Join(
			home,
			".config",
			"gcloud",
			"application_default_credentials.json",
		)
		if _, err := os.Stat(adcPath); err == nil {
			return true
		}
	}
	return false
}

type gcsMigrationDataset struct {
	blockSlot     uint64
	blockHash     []byte
	blockCbor     []byte
	blockID       uint64
	blockType     uint
	blockHeight   uint64
	blockPrevHash []byte
	txID          []byte
	outputIdx     uint32
	utxoCbor      []byte
	txHash        []byte
	txData        []byte
}

func seedGCSMigrationDataset(
	t *testing.T,
	store blob.BlobStore,
) gcsMigrationDataset {
	t.Helper()
	blocks, err := loadImmutableBlocks(1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	block := blocks[0]
	dataset := gcsMigrationDataset{
		blockSlot:     block.Slot,
		blockHash:     block.Hash,
		blockCbor:     block.Cbor,
		blockID:       99,
		blockType:     block.Type,
		blockHeight:   4_200_000,
		blockPrevHash: []byte("gcs-migration-prev-hash"),
		txID:          []byte("gcs-migration-tx-id"),
		outputIdx:     2,
		utxoCbor:      []byte{0x81, 0x03},
		txHash:        []byte("gcs-migration-tx-hash"),
		txData:        []byte{0x04, 0x05, 0x06},
	}
	txn := store.NewTransaction(true)
	require.NoError(t, store.SetBlock(
		txn, dataset.blockSlot, dataset.blockHash, dataset.blockCbor,
		dataset.blockID, dataset.blockType, dataset.blockHeight,
		dataset.blockPrevHash,
	))
	require.NoError(
		t,
		store.SetUtxo(txn, dataset.txID, dataset.outputIdx, dataset.utxoCbor),
	)
	require.NoError(t, store.SetTx(txn, dataset.txHash, dataset.txData))
	require.NoError(t, txn.Commit())
	return dataset
}

func migrateGCSBlobDataset(
	t *testing.T,
	src, dest blob.BlobStore,
	dataset gcsMigrationDataset,
) {
	t.Helper()
	readTxn := src.NewTransaction(false)
	blockCbor, blockMeta, err := src.GetBlock(
		readTxn, dataset.blockSlot, dataset.blockHash,
	)
	require.NoError(t, err)
	utxoCbor, err := src.GetUtxo(readTxn, dataset.txID, dataset.outputIdx)
	require.NoError(t, err)
	txData, err := src.GetTx(readTxn, dataset.txHash)
	require.NoError(t, err)
	require.NoError(t, readTxn.Rollback())

	writeTxn := dest.NewTransaction(true)
	require.NoError(t, dest.SetBlock(
		writeTxn, dataset.blockSlot, dataset.blockHash, blockCbor,
		blockMeta.ID, blockMeta.Type, blockMeta.Height, blockMeta.PrevHash,
	))
	require.NoError(
		t,
		dest.SetUtxo(writeTxn, dataset.txID, dataset.outputIdx, utxoCbor),
	)
	require.NoError(t, dest.SetTx(writeTxn, dataset.txHash, txData))
	require.NoError(t, writeTxn.Commit())
}

func requireGCSMigrationDataset(
	t *testing.T,
	store blob.BlobStore,
	dataset gcsMigrationDataset,
) {
	t.Helper()
	txn := store.NewTransaction(false)
	defer func() { require.NoError(t, txn.Rollback()) }()
	gotBlock, gotMeta, err := store.GetBlock(
		txn, dataset.blockSlot, dataset.blockHash,
	)
	require.NoError(t, err)
	require.Equal(t, dataset.blockCbor, gotBlock)
	require.Equal(t, dataset.blockID, gotMeta.ID)
	require.Equal(t, dataset.blockType, gotMeta.Type)
	require.Equal(t, dataset.blockHeight, gotMeta.Height)
	require.Equal(t, dataset.blockPrevHash, gotMeta.PrevHash)
	gotUtxo, err := store.GetUtxo(txn, dataset.txID, dataset.outputIdx)
	require.NoError(t, err)
	require.Equal(t, dataset.utxoCbor, gotUtxo)
	gotTx, err := store.GetTx(txn, dataset.txHash)
	require.NoError(t, err)
	require.Equal(t, dataset.txData, gotTx)
}

func cleanupGCSMigrationDataset(
	t *testing.T,
	store blob.BlobStore,
	dataset gcsMigrationDataset,
) {
	t.Helper()
	txn := store.NewTransaction(true)
	require.NoError(t, store.DeleteBlock(
		txn, dataset.blockSlot, dataset.blockHash, dataset.blockID,
	))
	require.NoError(t, store.DeleteUtxo(txn, dataset.txID, dataset.outputIdx))
	require.NoError(t, store.DeleteTx(txn, dataset.txHash))
	require.NoError(t, txn.Commit())
}

func TestBlobStoreMigrationGCS(t *testing.T) {
	t.Parallel()
	required := os.Getenv("DINGO_REQUIRE_GCS_TESTS") == "1"
	if !hasGCSBackend() {
		if required {
			t.Fatal("GCS migration is required but no backend is configured")
		}
		t.Skip("GCS backend not configured")
	}
	bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if bucket == "" {
		if required {
			t.Fatal(
				"GCS migration is required but DINGO_TEST_GCS_BUCKET is empty",
			)
		}
		t.Skip("DINGO_TEST_GCS_BUCKET is not set")
	}

	srcDB, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: filepath.Join(t.TempDir(), "src"),
	})
	require.NoError(t, err)
	destDB, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: filepath.Join(t.TempDir(), "dest")},
		Blob: dbtest.StorageProvider{
			Name:     "gcs",
			Config:   map[string]any{"bucket": bucket},
			Register: gcs.RegisterProvider,
		},
	})
	require.NoError(t, err)
	dataset := seedGCSMigrationDataset(t, srcDB.Blob())
	t.Cleanup(func() {
		cleanupGCSMigrationDataset(t, destDB.Blob(), dataset)
	})
	migrateGCSBlobDataset(t, srcDB.Blob(), destDB.Blob(), dataset)
	requireGCSMigrationDataset(t, destDB.Blob(), dataset)
}

func hasS3Credentials() bool {
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" &&
		os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		if _, err := os.Stat(filepath.Join(home, ".aws", "credentials")); err == nil {
			return true
		}
	}
	return false
}
