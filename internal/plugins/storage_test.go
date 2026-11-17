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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/plugin"
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

func TestOpenDatabaseReturnsRecoveryErrorOnRuntime(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	deps := StorageDependencies{DataDir: dataDir}
	runtime, err := OpenDatabase(
		context.Background(),
		&database.Config{DataDir: dataDir},
		localStorageSelections(),
		deps,
	)
	require.NoError(t, err)
	firstRuntime := runtime
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, firstRuntime.Close(ctx))
	})

	metaTxn := runtime.Database.Metadata().Transaction(t.Context())
	require.NoError(
		t,
		runtime.Database.Metadata().SetCommitTimestamp(123456789, metaTxn),
	)
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, runtime.Close(context.Background()))

	runtime, err = OpenDatabase(
		context.Background(),
		&database.Config{DataDir: dataDir},
		localStorageSelections(),
		deps,
	)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	t.Cleanup(func() {
		require.NoError(t, runtime.Close(context.Background()))
	})
	var timestampErr database.CommitTimestampError
	require.ErrorAs(t, runtime.RecoveryError(), &timestampErr)
	require.Equal(t, int64(123456789), timestampErr.MetadataTimestamp)
	require.Zero(t, timestampErr.BlobTimestamp)
}

func TestOpenDatabaseErrorDoesNotReturnLiveRuntime(t *testing.T) {
	t.Parallel()
	selections := localStorageSelections()
	selections.Metadata.Provider = "missing"
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
		"plugin provider not found: storage.metadata/missing",
	)
}

func openSplitPair(
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
	runtime, indexer, _, dataDir := openSplitPair(t, nil)

	require.Equal(t, "core", runtime.Database.StorageMode())
	require.Equal(t, "api", indexer.StorageMode())
	require.FileExists(t, filepath.Join(dataDir, "metadata.sqlite"))
	require.FileExists(t, filepath.Join(dataDir, IndexerDirName, "metadata.sqlite"))

	// A row written through one store is not visible through the other.
	require.NoError(t, indexer.SetSyncState("only_in_indexer", "x", nil))
	got, err := runtime.Database.GetSyncState("only_in_indexer", nil)
	require.NoError(t, err)
	require.Empty(t, got)

	// The indexer sees the ledger's blocks through the shared blob store.
	require.NoError(t, runtime.Database.BlockCreate(models.Block{
		Slot: 7, Hash: bytes.Repeat([]byte{7}, 32), Number: 1, Type: 1,
		Cbor: []byte{0x80},
	}, nil))
	blk, err := database.BlockByHash(t.Context(), indexer, bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.Equal(t, uint64(7), blk.Slot)

	// Committing in the indexer must not move the ledger's commit timestamp
	// pairing: the consensus database reopens cleanly with no mismatch.
	before, err := runtime.Database.Blob().GetCommitTimestamp()
	require.NoError(t, err)
	txn := indexer.Transaction(t.Context(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return indexer.SetSyncState("k", "v", txn)
	}))
	after, err := runtime.Database.Blob().GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, before, after, "an indexer commit must not write the ledger's commit timestamp")
}

func TestOpenIndexerDatabaseRegistersMetricsUnderTheIndexerRole(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	openSplitPair(t, reg)
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

	base.IndexerMetadataConfig = map[string]any{"dataDir": "/indexer-disk"}
	sel, _, err = IndexerMetadataSelection(base, "/data")
	require.NoError(t, err)
	require.Equal(t, "/indexer-disk", sel.Config["dataDir"])

	pg := StorageSelections{Metadata: plugin.Selection{
		Provider: "postgres", Config: map[string]any{"dsn": "postgres://a/ledger"},
	}}
	_, _, err = IndexerMetadataSelection(pg, "/data")
	require.ErrorContains(t, err, "separate postgres database",
		"a client/server store with no override would share the consensus tables")
	pg.IndexerMetadataConfig = map[string]any{"dsn": "postgres://a/ledger"}
	_, _, err = IndexerMetadataSelection(pg, "/data")
	require.ErrorContains(t, err, "separate postgres database",
		"an override naming the same database is no override")
	pg.IndexerMetadataConfig = map[string]any{"dsn": "postgres://a/indexer"}
	sel, _, err = IndexerMetadataSelection(pg, "/data")
	require.NoError(t, err)
	require.Equal(t, "postgres://a/indexer", sel.Config["dsn"])
}
