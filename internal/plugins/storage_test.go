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

package plugins

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func localStorageSelections() StorageSelections {
	return StorageSelections{
		Blob: plugin.Selection{
			Provider: "badger",
			Config:   map[string]any{},
		},
		Metadata: plugin.Selection{
			Provider: "sqlite",
			Config:   map[string]any{},
		},
	}
}

func TestOpenDatabaseErrorDoesNotReturnLiveRuntime(t *testing.T) {
	t.Parallel()
	selections := localStorageSelections()
	selections.Blob.Provider = "missing"
	runtime, err := OpenDatabase(
		context.Background(),
		&database.Config{DataDir: t.TempDir()},
		selections,
		StorageDependencies{DataDir: t.TempDir()},
	)
	require.Error(t, err)
	require.Nil(t, runtime)
	require.ErrorContains(
		t,
		err,
		"plugin provider not found: storage.blob/missing",
	)
}

func openIndexerPair(
	t *testing.T,
	reg prometheus.Registerer,
) (*DatabaseRuntime, *database.Database, StorageDependencies, string) {
	t.Helper()
	dataDir := t.TempDir()
	deps := StorageDependencies{
		DataDir: dataDir, StorageMode: "core", PromRegistry: reg,
	}
	selections := localStorageSelections()
	dbConfig := &database.Config{
		DataDir: dataDir, PromRegistry: reg, IndexerLayout: "split",
	}
	runtime, err := OpenDatabase(t.Context(), dbConfig, selections, deps)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, runtime.Close(context.WithoutCancel(t.Context())))
	})
	indexer, err := OpenIndexerDatabase(
		t.Context(), runtime.Host, runtime.Database, dbConfig, selections, deps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, indexer.Close()) })
	return runtime, indexer, deps, dataDir
}

func TestOpenIndexerDatabaseIsASeparateAPIStoreOverTheSharedBlobs(t *testing.T) {
	t.Parallel()
	runtime, indexer, _, dataDir := openIndexerPair(t, nil)

	require.Equal(t, "core", runtime.Database.StorageMode())
	require.Equal(t, "api", indexer.StorageMode())
	require.FileExists(t, filepath.Join(dataDir, IndexerDirName, "metadata.sqlite"))

	// The indexer sees the ledger's blocks through the shared blob store.
	require.NoError(t, runtime.Database.BlockCreate(models.Block{
		Slot: 7, Hash: bytes.Repeat([]byte{7}, 32), Number: 1, Type: 1,
		Cbor: []byte{0x80},
	}, nil))
	blk, err := database.BlockByHash(t.Context(), indexer, bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.Equal(t, uint64(7), blk.Slot)

}

func TestOpenIndexerDatabaseRegistersMetricsUnderTheIndexerRole(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	openIndexerPair(t, reg)
	families, err := reg.Gather()
	require.NoError(t, err)
	var indexerSeries, ledgerSeries int
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), IndexerMetricsPrefix) {
			indexerSeries++
		}
		for _, m := range f.GetMetric() {
			role := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "role" {
					role = l.GetValue()
				}
			}
			if role == IndexerMetricsRole {
				require.True(t, strings.HasPrefix(f.GetName(), IndexerMetricsPrefix),
					"%s carries the indexer role without the indexer prefix", f.GetName())
			} else {
				ledgerSeries++
				require.False(t, strings.HasPrefix(f.GetName(), IndexerMetricsPrefix))
			}
		}
	}
	require.NotZero(t, indexerSeries, "the indexer's metrics carry the role label")
	require.NotZero(t, ledgerSeries, "the ledger database's metrics stay unlabelled")
}

func TestIndexerMetadataSelection(t *testing.T) {
	t.Parallel()
	base := StorageSelections{Metadata: plugin.Selection{
		Provider: "sqlite", Config: map[string]any{"dataDir": "/elsewhere"},
	}}
	sel, dir, err := IndexerMetadataSelection(base, "/data")
	require.NoError(t, err)
	require.Equal(t, filepath.Join("/data", IndexerDirName), dir)
	require.NotContains(t, sel.Config, "dataDir",
		"the consensus store's directory override must not be inherited")
	_, dir, err = IndexerMetadataSelection(base, "")
	require.NoError(t, err)
	require.Empty(t, dir, "an ephemeral IndexerDB must not create a relative directory")

	pg := StorageSelections{Metadata: plugin.Selection{
		Provider: "postgres", Config: map[string]any{"dsn": "postgres://a/ledger"},
	}}
	_, _, err = IndexerMetadataSelection(pg, "/data")
	require.ErrorContains(t, err, "its own namespace",
		"a server store without the consensus namespace would share its tables")
	pg.IndexerDB = true
	sel, _, err = IndexerMetadataSelection(pg, "/data")
	require.NoError(t, err)
	require.Equal(t, "postgres://a/ledger", sel.Config["dsn"],
		"the released database is the IndexerDB")
}

func TestConfiguredStorageSelectionsSelectsTheConsensusNamespace(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{StorageMode: "api"}
	cfg.Plugins.Storage.Metadata = plugin.Selection{Provider: "postgres"}
	require.True(t, ConfiguredStorageSelections(cfg).IndexerDB)
	cfg.StorageMode = "core"
	require.False(t, ConfiguredStorageSelections(cfg).IndexerDB)
	cfg.ServerCoreNamespace = true
	require.True(t, ConfiguredStorageSelections(cfg).IndexerDB)
}

// Opening an API IndexerDB over a pending address-history rebuild reads a
// closure transaction's body through the ledger's Leios store while the
// database is still being constructed. The store belongs to the ledger's
// database, so the IndexerDB must neither open the directory a second time nor
// close it.
func TestOpenIndexerDatabaseRebuildReadsClosureThroughConsensusLeiosStore(
	t *testing.T,
) {
	t.Parallel()
	dataDir := t.TempDir()
	deps := StorageDependencies{DataDir: dataDir, StorageMode: "core"}
	selections := localStorageSelections()
	dbConfig := &database.Config{DataDir: dataDir, IndexerLayout: "split"}
	runtime, err := OpenDatabase(t.Context(), dbConfig, selections, deps)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, runtime.Close(context.WithoutCancel(t.Context())))
	})
	consensus := runtime.Database

	// A closure transaction body that produces one output.
	body, err := cbor.Encode(map[uint]any{
		0: []any{},
		1: []any{map[uint]any{
			0: append([]byte{0x60}, bytes.Repeat([]byte{0x51}, 28)...),
			1: uint64(1_000_000),
		}},
		2: uint64(5),
	})
	require.NoError(t, err)
	txHash := lcommon.Blake2b256Hash(body).Bytes()
	const ebSlot, rbSlot = uint64(90), uint64(100)
	ebHash := bytes.Repeat([]byte{0xee}, 32)
	rbHash := bytes.Repeat([]byte{0xbb}, 32)
	leios, err := consensus.LeiosDB()
	require.NoError(t, err)
	require.NoError(t, leios.PutClosure(
		leiosdb.Ref{Slot: ebSlot, Hash: ebHash}, body,
	))

	// First open: creates the schema, with no rebuild pending.
	indexer, err := OpenIndexerDatabase(
		t.Context(), runtime.Host, consensus, dbConfig, selections, deps,
	)
	require.NoError(t, err)
	var ebHashArr [32]byte
	copy(ebHashArr[:], ebHash)
	blobTxn := indexer.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *database.Txn) error {
		return indexer.Blob().SetTx(
			txn.Blob(), txHash, database.EncodeTxOffset(&database.CborOffset{
				BlockSlot:  ebSlot,
				BlockHash:  ebHashArr,
				ByteLength: uint32(len(body)), //nolint:gosec // small test body
			}),
		)
	}))
	require.NoError(t, indexer.Close())

	// Leave the legacy rebuild pending for the next open.
	raw, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(dataDir, IndexerDirName, "metadata.sqlite")+
			"?_pragma=busy_timeout(30000)",
	)
	require.NoError(t, err)
	result, err := testutil.ExecTransactionInsert(raw, `
INSERT INTO "transaction" (hash, block_hash, slot, type, block_index, valid)
VALUES (?, ?, ?, 0, 0, TRUE)`, txHash, rbHash, int64(rbSlot))
	require.NoError(t, err)
	id, err := result.LastInsertId()
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), `
INSERT INTO address_transaction_rebuild_source (transaction_id) VALUES (?)`, id)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	indexer, err = OpenIndexerDatabase(
		t.Context(), runtime.Host, consensus, dbConfig, selections, deps,
	)
	require.NoError(t, err)
	require.NoError(t, indexer.Close())

	// The ledger's Leios store is still open and still serves the closure.
	got, err := leios.GetClosure(leiosdb.Ref{Slot: ebSlot, Hash: ebHash})
	require.NoError(t, err)
	require.Equal(t, body, got)
}
