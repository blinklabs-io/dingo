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

//go:build dingo_extra_plugins

package integration

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// blobMigrationDataset is a small, fixed set of blob-store rows written
// through the public blob.BlobStore API and replayed from one backend into
// another, proving a migration transfers data without loss rather than just
// that both backends independently pass conformance. blockCbor is a real
// block loaded from database/immutable/testdata/ (see loadBlockData in
// benchmark_test.go) rather than a synthetic placeholder, so the migration
// actually moves realistic-sized, realistic-shaped bytes.
type blobMigrationDataset struct {
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

func seedBlobMigrationDataset(
	t *testing.T,
	store blob.BlobStore,
) blobMigrationDataset {
	t.Helper()
	blocks, err := loadBlockData(1)
	require.NoError(t, err)

	dataset := blobMigrationDataset{
		blockSlot:     4200,
		blockHash:     []byte("storagetest-migration-block-hash"),
		blockCbor:     blocks[0],
		blockID:       99,
		blockType:     6,
		blockHeight:   4_200_000,
		blockPrevHash: []byte("storagetest-migration-prev-hash"),
		txID:          []byte("storagetest-migration-tx-id"),
		outputIdx:     2,
		utxoCbor:      []byte{0x81, 0x03},
		txHash:        []byte("storagetest-migration-tx-hash"),
		txData:        []byte{0x04, 0x05, 0x06},
	}
	txn := store.NewTransaction(true)
	require.NoError(t, store.SetBlock(
		txn,
		dataset.blockSlot,
		dataset.blockHash,
		dataset.blockCbor,
		dataset.blockID,
		dataset.blockType,
		dataset.blockHeight,
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

// migrateBlobDataset reads dataset's rows from src and writes the exact
// retrieved bytes into dest, the same shape a real migration tool would use.
func migrateBlobDataset(
	t *testing.T,
	src, dest blob.BlobStore,
	dataset blobMigrationDataset,
) {
	t.Helper()
	readTxn := src.NewTransaction(false)
	blockCbor, blockMeta, err := src.GetBlock(
		readTxn,
		dataset.blockSlot,
		dataset.blockHash,
	)
	require.NoError(t, err)
	utxoCbor, err := src.GetUtxo(readTxn, dataset.txID, dataset.outputIdx)
	require.NoError(t, err)
	txData, err := src.GetTx(readTxn, dataset.txHash)
	require.NoError(t, err)
	require.NoError(t, readTxn.Rollback())

	writeTxn := dest.NewTransaction(true)
	require.NoError(t, dest.SetBlock(
		writeTxn,
		dataset.blockSlot,
		dataset.blockHash,
		blockCbor,
		blockMeta.ID,
		blockMeta.Type,
		blockMeta.Height,
		blockMeta.PrevHash,
	))
	require.NoError(
		t,
		dest.SetUtxo(writeTxn, dataset.txID, dataset.outputIdx, utxoCbor),
	)
	require.NoError(t, dest.SetTx(writeTxn, dataset.txHash, txData))
	require.NoError(t, writeTxn.Commit())
}

func requireBlobDatasetMatches(
	t *testing.T,
	store blob.BlobStore,
	dataset blobMigrationDataset,
) {
	t.Helper()
	txn := store.NewTransaction(false)
	defer func() { require.NoError(t, txn.Rollback()) }()

	gotBlockCbor, gotBlockMeta, err := store.GetBlock(
		txn,
		dataset.blockSlot,
		dataset.blockHash,
	)
	require.NoError(t, err)
	require.Equal(t, dataset.blockCbor, gotBlockCbor)
	require.Equal(t, dataset.blockID, gotBlockMeta.ID)
	require.Equal(t, dataset.blockType, gotBlockMeta.Type)
	require.Equal(t, dataset.blockHeight, gotBlockMeta.Height)
	require.Equal(t, dataset.blockPrevHash, gotBlockMeta.PrevHash)

	gotUtxoCbor, err := store.GetUtxo(txn, dataset.txID, dataset.outputIdx)
	require.NoError(t, err)
	require.Equal(t, dataset.utxoCbor, gotUtxoCbor)

	gotTxData, err := store.GetTx(txn, dataset.txHash)
	require.NoError(t, err)
	require.Equal(t, dataset.txData, gotTxData)
}

// cleanupBlobMigrationDataset deletes exactly the rows
// seedBlobMigrationDataset/migrateBlobDataset write, so a migration test run
// against a real, persistent bucket does not leave them behind.
func cleanupBlobMigrationDataset(
	t *testing.T,
	store blob.BlobStore,
	dataset blobMigrationDataset,
) {
	t.Helper()
	txn := store.NewTransaction(true)
	_ = store.DeleteBlock(
		txn,
		dataset.blockSlot,
		dataset.blockHash,
		dataset.blockID,
	)
	_ = store.DeleteUtxo(txn, dataset.txID, dataset.outputIdx)
	_ = store.DeleteTx(txn, dataset.txHash)
	_ = txn.Commit()
}

// TestBlobStoreMigration migrates a small dataset from the always-available
// Badger backend into every cloud blob backend this environment has
// credentials for, reusing cloudStorageBenchmarkBackends's existing
// credential/bucket/prefix resolution (see benchmark_test.go and
// cloud_test.go) instead of re-deriving it. It skips entirely when neither S3
// (MinIO in CI) nor GCS (real bucket + ADC only, no local emulator exists)
// is configured.
func TestBlobStoreMigration(t *testing.T) {
	destinations := cloudStorageBenchmarkBackends(t.TempDir(), t.Name())
	if len(destinations) == 0 {
		t.Skip(
			"no cloud blob backend configured (S3 via MinIO/AWS_* or GCS " +
				"via Application Default Credentials), skipping test",
		)
	}
	for _, destination := range destinations {
		t.Run(destination.name, func(t *testing.T) {
			srcDB, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
				Config: &database.Config{
					DataDir: filepath.Join(t.TempDir(), "src"),
				},
			})
			require.NoError(t, err)

			destDB, err := dbtest.NewDatabaseWithOptions(t, destination.opts)
			require.NoError(t, err)

			dataset := seedBlobMigrationDataset(t, srcDB.Blob())
			// Registered before dbtest's own Close cleanup runs (t.Cleanup
			// is LIFO, and dbtest.NewDatabaseWithOptions already registered
			// its Close via t.Cleanup above), so the delete happens while
			// destDB is still open. The S3 destination is isolated by its
			// own unique-per-run prefix (see cloudStorageBenchmarkBackends)
			// so this is defense in depth there, but GCS has no prefix
			// option at all: every migration run without this would leave
			// this dataset's keys at the bucket root, where they would
			// then fail every other GCS test's "bucket must be empty"
			// precondition (see gcs.newTestGCSStore) until an operator
			// noticed and emptied the bucket by hand.
			t.Cleanup(func() {
				cleanupBlobMigrationDataset(t, destDB.Blob(), dataset)
			})

			migrateBlobDataset(t, srcDB.Blob(), destDB.Blob(), dataset)
			requireBlobDatasetMatches(t, destDB.Blob(), dataset)
		})
	}
}

// metadataMigrationDataset is a small, fixed set of metadata-store rows
// written through the public metadata.MetadataStore API and replayed from
// one backend into another.
type metadataMigrationDataset struct {
	commitTimestamp int64
	network         string
	gateName        string
	gateValue       string
}

func seedMetadataMigrationDataset(
	t *testing.T,
	store metadata.MetadataStore,
) metadataMigrationDataset {
	t.Helper()
	dataset := metadataMigrationDataset{
		commitTimestamp: 987654321,
		network:         "storagetest-migration",
		gateName:        "storagetest-migration-gate",
		gateValue:       "enabled",
	}
	txn := store.Transaction()
	require.NoError(t, store.SetCommitTimestamp(dataset.commitTimestamp, txn))
	require.NoError(t, txn.Commit())
	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     dataset.network,
	}))
	require.NoError(t, store.SetNodeSettingsGates(
		nodesettings.Values{dataset.gateName: dataset.gateValue},
		1,
		10,
	))
	return dataset
}

// migrateMetadataDataset reads dataset's rows from src and writes the exact
// retrieved values into dest, the same shape a real migration tool would use
// -- there is no generic raw-row copy across dialects with different SQL
// schemas, so replaying through the typed store API is the only sound
// approach.
func migrateMetadataDataset(
	t *testing.T,
	src, dest metadata.MetadataStore,
) {
	t.Helper()
	timestamp, err := src.GetCommitTimestamp()
	require.NoError(t, err)
	settings, err := src.GetNodeSettings()
	require.NoError(t, err)
	gates, err := src.GetNodeSettingsGates()
	require.NoError(t, err)

	txn := dest.Transaction()
	require.NoError(t, dest.SetCommitTimestamp(timestamp, txn))
	require.NoError(t, txn.Commit())
	require.NoError(t, dest.SetNodeSettings(settings))
	require.NoError(t, dest.SetNodeSettingsGates(gates, 1, 10))
}

func requireMetadataDatasetMatches(
	t *testing.T,
	store metadata.MetadataStore,
	dataset metadataMigrationDataset,
) {
	t.Helper()
	timestamp, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, dataset.commitTimestamp, timestamp)

	settings, err := store.GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, dataset.network, settings.Network)
	require.Equal(t, types.StorageModeCore, settings.StorageMode)

	gates, err := store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, dataset.gateValue, gates[dataset.gateName])
}

// TestMetadataStoreMigrationSQLiteToPostgres migrates a small dataset from
// the always-available SQLite backend into Postgres, skipping when Postgres
// is not configured -- matching the credential convention
// database/plugin/metadata/postgres/conformance_test.go and
// internal/test/conformance use, so this runs automatically in CI.
func TestMetadataStoreMigrationSQLiteToPostgres(t *testing.T) {
	if os.Getenv("POSTGRES_PASSWORD") == "" && os.Getenv("POSTGRES_DSN") == "" {
		t.Skip(
			"Skipping postgres migration test: postgres not configured " +
				"(set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	dsn := postgresMigrationDSN()
	// Unique per run (rather than a fixed, predictable name): two runs
	// against the same server at the same time -- a real possibility for
	// `go test ./...`, which runs different packages as separate concurrent
	// processes -- must not race on CREATE/DROP of the same schema, and a
	// schema that can never have existed before this run needs no
	// preexistence check before an unconditional drop in cleanup.
	schema := fmt.Sprintf("storage_migration_%d", time.Now().UnixNano())

	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})

	srcDB, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: filepath.Join(t.TempDir(), "src"),
	})
	require.NoError(t, err)

	destDB, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: filepath.Join(t.TempDir(), "dest-blob"),
		},
		Metadata: dbtest.StorageProvider{
			Name: "postgres",
			Config: map[string]any{
				"dsn": postgresMigrationDSNWithSearchPath(dsn, schema),
			},
			Register: postgres.RegisterProvider,
		},
	})
	require.NoError(t, err)

	dataset := seedMetadataMigrationDataset(t, srcDB.Metadata())
	migrateMetadataDataset(t, srcDB.Metadata(), destDB.Metadata())
	requireMetadataDatasetMatches(t, destDB.Metadata(), dataset)
}

// escapeLibpqValue quotes and backslash-escapes a value for a libpq
// keyword/value connection string, so a password containing a space,
// single quote, or backslash -- all legal in a Postgres password --
// produces a well-formed DSN instead of breaking the conninfo parse.
func escapeLibpqValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}

func postgresMigrationDSN() string {
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	host := "localhost"
	if v := os.Getenv("POSTGRES_HOST"); v != "" {
		host = v
	}
	port := "5432"
	if v := os.Getenv("POSTGRES_PORT"); v != "" {
		port = v
	}
	user := "postgres"
	if v := os.Getenv("POSTGRES_USER"); v != "" {
		user = v
	}
	dbName := "dingo_test"
	if v := os.Getenv("POSTGRES_DATABASE"); v != "" {
		dbName = v
	}
	sslMode := "disable"
	if v := os.Getenv("POSTGRES_SSLMODE"); v != "" {
		sslMode = v
	}
	return "host=" + escapeLibpqValue(host) +
		" port=" + escapeLibpqValue(port) +
		" user=" + escapeLibpqValue(user) +
		" password=" + escapeLibpqValue(os.Getenv("POSTGRES_PASSWORD")) +
		" dbname=" + escapeLibpqValue(dbName) +
		" sslmode=" + escapeLibpqValue(sslMode)
}

// postgresMigrationDSNWithSearchPath is dsn with the connection's
// search_path pinned to schema, the same URL-vs-keyword/value handling
// database/plugin/metadata/postgres/conformance_test.go's
// postgresConformanceDSN uses: postgresMigrationDSN returns POSTGRES_DSN
// verbatim when that is set, and an operator may legitimately set it to a
// URL (postgres://user:pass@host/db) rather than keyword/value form --
// appending " options='...'" text to a URL produces a malformed DSN.
func postgresMigrationDSNWithSearchPath(dsn, schema string) string {
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err == nil {
			query := parsed.Query()
			query.Set("options", "-csearch_path="+schema)
			parsed.RawQuery = query.Encode()
			return parsed.String()
		}
	}
	return dsn + " options='-csearch_path=" + schema + "'"
}

// TestMetadataStoreMigrationSQLiteToMySQL migrates a small dataset from the
// always-available SQLite backend into MySQL, skipping when MySQL is not
// configured for admin access -- matching the credential convention
// database/plugin/metadata/mysql/conformance_test.go and
// internal/test/conformance use, so this runs automatically in CI.
func TestMetadataStoreMigrationSQLiteToMySQL(t *testing.T) {
	if os.Getenv("MYSQL_ROOT_PASSWORD") == "" && os.Getenv("MYSQL_DSN") == "" {
		t.Skip(
			"Skipping mysql migration test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
	// Unique per run (rather than a fixed, predictable name): two runs
	// against the same server at the same time -- a real possibility for
	// `go test ./...`, which runs different packages as separate concurrent
	// processes -- must not race on CREATE/DROP of the same database, and a
	// database that can never have existed before this run needs no
	// preexistence check before an unconditional drop in cleanup.
	dbName := fmt.Sprintf("storage_migration_test_%d", time.Now().UnixNano())
	rootDSN := mysqlMigrationRootDSN()

	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec("CREATE DATABASE `" + dbName + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + dbName + "`")
		_ = admin.Close()
	})

	parsed, err := mysqldriver.ParseDSN(rootDSN)
	require.NoError(t, err)
	parsed.DBName = dbName

	srcDB, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: filepath.Join(t.TempDir(), "src"),
	})
	require.NoError(t, err)

	destDB, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: filepath.Join(t.TempDir(), "dest-blob"),
		},
		Metadata: dbtest.StorageProvider{
			Name:     "mysql",
			Config:   map[string]any{"dsn": parsed.FormatDSN()},
			Register: mysql.RegisterProvider,
		},
	})
	require.NoError(t, err)

	dataset := seedMetadataMigrationDataset(t, srcDB.Metadata())
	migrateMetadataDataset(t, srcDB.Metadata(), destDB.Metadata())
	requireMetadataDatasetMatches(t, destDB.Metadata(), dataset)
}

func mysqlMigrationRootDSN() string {
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		return dsn
	}
	host := "localhost"
	if v := os.Getenv("MYSQL_HOST"); v != "" {
		host = v
	}
	port := "3306"
	if v := os.Getenv("MYSQL_PORT"); v != "" {
		port = v
	}
	cfg := mysqldriver.Config{
		User:                 "root",
		Passwd:               os.Getenv("MYSQL_ROOT_PASSWORD"),
		Net:                  "tcp",
		Addr:                 host + ":" + port,
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	return cfg.FormatDSN()
}
