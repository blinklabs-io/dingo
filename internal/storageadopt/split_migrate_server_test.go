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

package storageadopt

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/immutable/chunkfile"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// serverMigrationHost uses real SQLite SQL stores in separate files to cover
// the server adoption sequence without requiring a live SQL-server campaign.
func serverMigrationHost(
	t *testing.T,
	provider string,
) func() (*plugin.Host, error) {
	t.Helper()
	return func() (*plugin.Host, error) {
		host := plugin.NewHost()
		if err := badger.RegisterProvider(host); err != nil {
			return nil, err
		}
		if err := chunkfile.RegisterProvider(host); err != nil {
			return nil, err
		}
		err := plugin.Register(
			host,
			plugin.Descriptor{
				Capability: plugin.CapabilityStorageMetadata,
				Name:       provider,
			},
			func() struct{} { return struct{}{} },
			func(_ context.Context, _ struct{}, deps metadata.ProviderDependencies) (*sqlstore.Store, plugin.Instance, error) {
				dir := deps.DataDir
				if deps.CoreNamespace {
					dir = filepath.Join(dir, "core-metadata")
				}
				if _, err := os.Stat(filepath.Join(dir, "metadata.sqlite")); errors.Is(
					err,
					os.ErrNotExist,
				) {
					raw, err := dbtest.MetadataTemplateBytes()
					if err != nil {
						return nil, nil, err
					}
					if err := dbtest.SeedMetadataTemplateBytes(dir, raw); err != nil {
						return nil, nil, err
					}
				}
				store, err := sqlite.NewSQLStore(
					sqlite.Config{DataDir: dir},
					deps,
				)
				if err != nil {
					return nil, nil, err
				}
				return store, plugin.Lifecycle{
					StartFunc: store.Start,
					StopFunc:  store.CloseContext,
				}, nil
			},
		)
		if err != nil {
			return nil, err
		}
		return host, nil
	}
}

// resolveReleasedStores resolves the stores of a v0.94.2 database on host:
// ResolveStorage's and, when it does not resolve one, the metadata store
// that database keeps its state in.
func resolveReleasedStores(
	t *testing.T,
	host *plugin.Host,
	selections internalplugins.StorageSelections,
	deps internalplugins.StorageDependencies,
) database.Stores {
	t.Helper()
	stores, err := internalplugins.ResolveStorage(
		t.Context(), host, selections, deps,
	)
	require.NoError(t, err)
	if stores.Metadata == nil {
		stores.Metadata, err = internalplugins.ResolveMetadata(
			t.Context(), host, selections, deps,
		)
		require.NoError(t, err)
	}
	return stores
}

func seedServerMigration(
	t *testing.T,
	provider string,
) (*splitMigration, ocommon.Point) {
	t.Helper()
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "api",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Indexer:      config.IndexerConfig{MaxReadyLagBlocks: 10},
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob: plugin.Selection{
					Provider: "badger",
					Config:   testutil.BadgerBlobConfig(),
				},
				Metadata: plugin.Selection{Provider: provider},
			},
		},
	}
	m := &splitMigration{
		cfg:         cfg,
		logger:      migrateTestLogger(),
		hostFactory: serverMigrationHost(t, provider),
	}
	host, err := m.newHost()
	require.NoError(t, err)
	deps, err := m.dependencies("api")
	require.NoError(t, err)
	// The released layout: one API store on the configured database.
	released := m.selections()
	released.SplitIndexer = false
	stores := resolveReleasedStores(t, host, released, deps)
	db, err := database.New(t.Context(), m.databaseConfig("api", ""), stores)
	require.NoError(t, err)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	blocks, err := testfixtures.GenerateBabbageChainAt(1, 2, 3)
	require.NoError(t, err)
	var last ocommon.Point
	for _, b := range blocks {
		require.NoError(t, cm.PrimaryChain().AddBlock(t.Context(), b, nil))
		last = ocommon.Point{Slot: b.SlotNumber(), Hash: b.Hash().Bytes()}
	}
	require.NoError(
		t,
		db.Metadata().SetTip(
			ochainsync.Tip{Point: last, BlockNumber: 3}, nil,
		),
	)
	require.NoError(
		t,
		db.Metadata().SetSyncState("server-migration-probe", "kept", nil),
	)
	require.NoError(t, db.Close())
	require.NoError(t, host.Stop(t.Context()))
	raw, err := sql.Open(
		"sqlite",
		filepath.Join(cfg.DatabasePath, "metadata.sqlite"),
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO account(staking_key,credential_tag,reward,active) VALUES(?,0,'123',TRUE)`,
		bytes.Repeat([]byte{7}, 28),
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO datum(hash,raw_datum,added_slot) VALUES(?,x'01',?)`,
		bytes.Repeat([]byte{8}, 32), blocks[0].SlotNumber(),
	)
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	return m, last
}

func TestServerMigrationResumesAndKeepsLedgerStateInPlace(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"postgres", "mysql"} {
		for _, step := range []string{"source-latched", "consensus-batch:account", "consensus-batch:sync_state", "consensus-copied", "consensus-ready"} {
			t.Run(provider+"/"+step, func(t *testing.T) {
				m, last := seedServerMigration(t, provider)
				interrupted := errors.New("interrupted migration")
				m.afterStep = func(got string) error {
					if got == step {
						return interrupted
					}
					return nil
				}
				require.ErrorIs(t, m.run(t.Context()), interrupted)
				m.afterStep = nil
				require.NoError(t, m.run(t.Context()))
				source, err := sql.Open(
					"sqlite",
					filepath.Join(m.cfg.DatabasePath, "metadata.sqlite"),
				)
				require.NoError(t, err)
				defer source.Close()
				target, err := sql.Open(
					"sqlite",
					filepath.Join(
						m.cfg.DatabasePath,
						"core-metadata",
						"metadata.sqlite",
					),
				)
				require.NoError(t, err)
				defer target.Close()
				// The IndexerDB keeps its rows, and the consensus namespace
				// gets the ledger state with the rest: the node reverts it
				// there before moving it into the LedgerDB.
				for table, copied := range map[string]int{"account": 1, "datum": 1} {
					var oldCount, newCount int
					require.NoError(
						t,
						source.QueryRow("SELECT count(*) FROM "+table).
							Scan(&oldCount),
					)
					require.NoError(
						t,
						target.QueryRow("SELECT count(*) FROM "+table).
							Scan(&newCount),
					)
					require.Equal(t, 1, oldCount)
					require.Equal(t, copied, newCount, table)
				}
				// The adopted IndexerDB holds the datum as an offset into the
				// block it was seeded at, not a copy of its bytes.
				var datumColumn []byte
				require.NoError(
					t,
					source.QueryRow("SELECT raw_datum FROM datum").
						Scan(&datumColumn),
				)
				datumOffset, err := types.DecodeCborOffset(datumColumn)
				require.NoError(t, err, "the datum column holds an offset")
				require.Equal(t, last.Slot >= datumOffset.BlockSlot, true)
				var mode string
				require.NoError(
					t,
					source.QueryRow("SELECT storage_mode FROM node_settings WHERE id=1").
						Scan(&mode),
				)
				require.Equal(t, "api", mode)
				require.NoError(
					t,
					target.QueryRow("SELECT storage_mode FROM node_settings WHERE id=1").
						Scan(&mode),
				)
				require.Equal(t, "core", mode)
				var hash []byte
				require.NoError(
					t,
					target.QueryRow("SELECT hash FROM tip WHERE id=1").
						Scan(&hash),
				)
				require.Equal(t, last.Hash, hash)
				_, err = os.Stat(utxoset.DirFor(m.cfg.DatabasePath))
				require.ErrorIs(
					t, err, os.ErrNotExist,
					"the node builds the LedgerDB, from the reverted namespace",
				)
				var gate int
				require.NoError(t, target.QueryRow(
					"SELECT count(*) FROM sync_state WHERE sync_key = ?",
					database.UtxoSetOnlySyncKey,
				).Scan(&gate))
				require.Zero(t, gate)
				_, err = target.Exec("UPDATE tip SET slot=slot+10 WHERE id=1")
				require.NoError(t, err)
				m.cfg.StorageMode = "core"
				m.cfg.ServerCoreNamespace = false
				require.NoError(t, m.run(t.Context()))
				require.True(
					t,
					m.cfg.ServerCoreNamespace,
					"an adopted API database keeps selecting its core namespace in core mode",
				)
				var slot uint64
				require.NoError(
					t,
					target.QueryRow("SELECT slot FROM tip WHERE id=1").
						Scan(&slot),
				)
				require.Equal(
					t,
					last.Slot+10,
					slot,
					"a completed migration must never recopy the lagging IndexerDB over the core tip",
				)
			})
		}
	}
}

// TestServerAdoptionCarriesCoreModeReads adopts a released API store and reads
// back, from its consensus namespace, the rows core-mode block and epoch
// processing read from SQL: the producer of an input and the endorser
// transactions a Leios block deduplicates, and the MIR certificates an epoch
// boundary applies.
func TestServerAdoptionCarriesCoreModeReads(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"postgres", "mysql"} {
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			m, _ := seedServerMigration(t, provider)
			raw, err := sql.Open("sqlite", filepath.Join(m.cfg.DatabasePath, "metadata.sqlite"))
			require.NoError(t, err)
			txHash := bytes.Repeat([]byte{0x51}, 32)
			res, err := raw.Exec(`INSERT INTO "transaction"(hash) VALUES(?)`, txHash)
			require.NoError(t, err)
			txID, err := res.LastInsertId()
			require.NoError(t, err)
			_, err = raw.Exec(
				`INSERT INTO applied_transaction(tx_id,block_hash,slot,type,fee,block_index,valid) VALUES(?,?,2,6,'170000',0,TRUE)`,
				txID, bytes.Repeat([]byte{0x52}, 32),
			)
			require.NoError(t, err)
			_, err = raw.Exec(
				`INSERT INTO move_instantaneous_rewards(pot,certificate_id,added_slot,other_pot) VALUES(1,0,2,'25')`,
			)
			require.NoError(t, err)
			require.NoError(t, raw.Close())

			require.NoError(t, m.run(t.Context()))

			host, err := m.newHost()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
			core, err := plugin.Resolve[metadata.MetadataStore](
				t.Context(), host, plugin.CapabilityStorageMetadata, provider, nil,
				metadata.ProviderDependencies{
					DataDir: m.cfg.DatabasePath, StorageMode: "core", CoreNamespace: true,
				},
			)
			require.NoError(t, err)
			txs, err := core.GetTransactionsByHashes([][]byte{txHash}, nil)
			require.NoError(t, err)
			require.Len(t, txs, 1, "the producer and dedup lookups read the copied transaction")
			effects, err := core.GetMIRCertsInSlotRange(0, 100, nil)
			require.NoError(t, err)
			require.Len(t, effects, 1, "the epoch boundary reads the copied MIR certificate")
			require.EqualValues(t, 25, effects[0].OtherPot)
		})
	}
}

// TestServerAdoptionRefusesALostConsensusNamespace adopts a store, removes its
// consensus namespace and starts again: adopting the IndexerDB a second time
// would rebuild consensus state from its tip.
func TestServerAdoptionRefusesALostConsensusNamespace(t *testing.T) {
	t.Parallel()
	m, _ := seedServerMigration(t, "postgres")
	require.NoError(t, m.run(t.Context()))
	require.NoError(t, os.RemoveAll(filepath.Join(m.cfg.DatabasePath, "core-metadata")))
	require.ErrorContains(t, m.run(t.Context()), "holds no completed consensus state")
	target, err := sql.Open("sqlite", filepath.Join(m.cfg.DatabasePath, "core-metadata", "metadata.sqlite"))
	require.NoError(t, err)
	defer target.Close()
	var count int
	require.NoError(t, target.QueryRow("SELECT count(*) FROM tip").Scan(&count))
	require.Zero(t, count, "the refusal copies nothing")
}

// A server API database whose v0.94.2 backfill did not finish stays in place
// as the IndexerDB, so adopting it would serve a history that stops at the
// backfill's last block.
func TestServerMigrationRefusesAnInterruptedBackfill(t *testing.T) {
	t.Parallel()
	m, _ := seedServerMigration(t, "postgres")
	markInterruptedBackfill(t, m.cfg.DatabasePath)

	require.ErrorIs(t, m.run(t.Context()), errInterruptedBackfill)
	require.NoDirExists(
		t, filepath.Join(m.cfg.DatabasePath, "core-metadata"),
		"the refusal derives no consensus namespace",
	)
}

// TestCheckServerAdoptedRefusesAReleasedStore covers a Node started without
// the dingo command's startup adoption: a released API store is refused, an
// adopted one, an empty one and an IndexerDB the node built itself pass, and
// an interrupted adoption is refused.
func TestCheckServerAdoptedRefusesAReleasedStore(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, m *splitMigration) error {
		t.Helper()
		host, err := m.newHost()
		require.NoError(t, err)
		defer func() { require.NoError(t, host.Stop(context.Background())) }()
		return CheckServerAdopted(
			t.Context(), host, m.cfg.Plugins.Storage.Metadata,
			m.cfg.DatabasePath, false,
		)
	}
	t.Run("released", func(t *testing.T) {
		t.Parallel()
		m, _ := seedServerMigration(t, "postgres")
		require.ErrorIs(t, check(t, m), ErrServerAPINotAdopted)
		require.NoError(t, m.run(t.Context()))
		require.NoError(t, check(t, m))
	})
	t.Run("interrupted adoption", func(t *testing.T) {
		t.Parallel()
		m, _ := seedServerMigration(t, "mysql")
		interrupted := errors.New("interrupted")
		m.afterStep = func(step string) error {
			if step == "consensus-batch:sync_state" {
				return interrupted
			}
			return nil
		}
		require.ErrorIs(t, m.run(t.Context()), interrupted)
		require.ErrorIs(t, check(t, m), ErrServerAPINotAdopted)
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		cfg := &config.Config{
			StorageMode: "api", DatabasePath: t.TempDir(),
			Plugins: config.PluginsConfig{Storage: config.StoragePluginsConfig{
				Metadata: plugin.Selection{Provider: "postgres"},
			}},
		}
		m := &splitMigration{cfg: cfg, hostFactory: serverMigrationHost(t, "postgres")}
		require.NoError(t, check(t, m))
	})
}

// A v0.94.2 API store interrupted during its metadata backfill is refused by
// the first open of its provider, before the schema is migrated: the server
// store the dingo command adopts, and a restored store adopted from a staging
// directory. Each is left as v0.94.2 wrote it, which that release still opens.
func TestAdoptionRefusesAReleasedInterruptedBackfillUntouched(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		adopt func(t *testing.T, dir string) error
	}{
		{name: "postgres", adopt: func(t *testing.T, dir string) error {
			return releasedServerMigration(t, "postgres", dir).run(t.Context())
		}},
		{name: "mysql", adopt: func(t *testing.T, dir string) error {
			return releasedServerMigration(t, "mysql", dir).run(t.Context())
		}},
		{name: "staged sqlite", adopt: func(t *testing.T, dir string) error {
			cfg := releasedServerMigration(t, "sqlite", t.TempDir()).cfg
			return AdoptStagedAPI(
				t.Context(), *cfg, migrateTestLogger(), nil, dir,
			)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			require.NoError(t, dbtest.SeedReleasedMetadata(dir, "backfill"))
			before, err := dbtest.MetadataFingerprint(dir)
			require.NoError(t, err)

			err = tc.adopt(t, dir)
			require.ErrorIs(t, err, metadata.ErrInterruptedBackfill)
			require.ErrorContains(t, err, "start dingo v0.94.2")

			after, err := dbtest.MetadataFingerprint(dir)
			require.NoError(t, err)
			require.Equal(t, before, after,
				"the refusal changed the released store")
			require.NoDirExists(t, filepath.Join(dir, "core-metadata"))
			require.NoDirExists(
				t, filepath.Join(dir, internalplugins.IndexerDirName),
			)
			status, err := dbtest.ReleasedMetadataSyncStatus(dir)
			require.NoError(t, err, "v0.94.2 no longer opens the store")
			require.Equal(t, "backfill", status)
		})
	}
}

// releasedServerMigration is a migration of the API store in dir, with a
// SQLite store registered under provider.
func releasedServerMigration(
	t *testing.T,
	provider, dir string,
) *splitMigration {
	t.Helper()
	return &splitMigration{
		cfg: &config.Config{
			RunMode:      config.RunModeServe,
			StorageMode:  "api",
			Network:      "preview",
			DatabasePath: dir,
			Plugins: config.PluginsConfig{
				Storage: config.StoragePluginsConfig{
					Blob: plugin.Selection{
						Provider: "badger",
						Config:   testutil.BadgerBlobConfig(),
					},
					Metadata: plugin.Selection{Provider: provider},
				},
			},
		},
		logger:      migrateTestLogger(),
		hostFactory: serverMigrationHost(t, provider),
	}
}

// With an archive configured, the migration reads the IndexerDB's blocks
// through the archive store the configuration builds, so a block that expired
// locally can be fetched to compute its offsets.
func TestMigrationReadsBlocksThroughTheConfiguredArchive(t *testing.T) {
	// Not t.Parallel: ArchiveStore is a package-level seam, and the migration
	// reads it.
	var wrapped atomic.Int32
	previous := ArchiveStore
	ArchiveStore = func(
		cfg *config.Config,
		upstream blob.BlobStore,
	) (blob.BlobStore, error) {
		require.Equal(t, "https://archive.example", cfg.BarkBaseUrl)
		wrapped.Add(1)
		return upstream, nil
	}
	t.Cleanup(func() { ArchiveStore = previous })

	m, _ := seedServerMigration(t, "postgres")
	m.cfg.BarkBaseUrl = "https://archive.example"
	require.NoError(t, m.run(t.Context()))
	require.NotZero(t, wrapped.Load(), "the archive store was not used")
}
