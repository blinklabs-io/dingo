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
	"archive/tar"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// releasedAPIArchive is the data directory the released version wrote for
// preview in API storage mode.
var releasedAPIArchive = filepath.Join(
	"..", "node", "testdata", "v0.94.2-api-preview", "database.tar.zst",
)

func unpackReleasedDatabase(t *testing.T) string {
	t.Helper()
	dir := testutil.OwnerOnlyTempDir(t)
	f, err := os.Open(releasedAPIArchive)
	require.NoError(t, err)
	defer f.Close()
	zr, err := zstd.NewReader(f)
	require.NoError(t, err)
	defer zr.Close()
	// The root refuses an entry that names a path outside dir.
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch hdr.Typeflag {
		case tar.TypeDir:
			require.NoError(t, root.MkdirAll(hdr.Name, 0o700))
		case tar.TypeReg:
			out, err := root.OpenFile(
				hdr.Name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600,
			)
			require.NoError(t, err)
			_, err = io.CopyN(out, tr, hdr.Size)
			require.NoError(t, errors.Join(err, out.Close()), hdr.Name)
		default:
			t.Fatalf("unexpected archive entry %q", hdr.Name)
		}
	}
	return dir
}

func sqliteTipSlot(t *testing.T, path string) uint64 {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer raw.Close()
	var slot uint64
	require.NoError(
		t, raw.QueryRow("SELECT slot FROM tip WHERE id=1").Scan(&slot),
	)
	return slot
}

// A released PostgreSQL or MySQL API store is adopted as the IndexerDB, and
// the node that then starts on its consensus namespace follows a fork that
// leaves the chain below the adopted tip: the namespace was reverted below
// the tip before its ledger state moved into the LedgerDB, so the blocks up
// to the tip were applied through the set, which journals each. The adopted
// IndexerDB is not reverted.
func TestAdoptedServerStoreRollsBackBelowItsAdoptedTip(t *testing.T) {
	t.Parallel()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	require.Positive(t, nodeCfg.ShelleyGenesis().SecurityParam)
	k := uint64(nodeCfg.ShelleyGenesis().SecurityParam) //nolint:gosec // checked positive above
	for _, provider := range []string{"postgres", "mysql"} {
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			logger := migrateTestLogger()
			dir := unpackReleasedDatabase(t)
			cfg := &config.Config{
				RunMode:      config.RunModeServe,
				StorageMode:  "api",
				Network:      "preview",
				DatabasePath: dir,
				Plugins: config.PluginsConfig{
					Storage: config.StoragePluginsConfig{
						Blob:     plugin.Selection{Provider: "badger"},
						Metadata: plugin.Selection{Provider: provider},
					},
				},
			}
			m := &splitMigration{
				cfg:         cfg,
				logger:      logger,
				hostFactory: serverMigrationHost(t, provider),
			}
			// The archive is a SQLite store; the fixture serves it as the
			// server provider's.
			raw, err := sql.Open("sqlite", filepath.Join(dir, "metadata.sqlite"))
			require.NoError(t, err)
			_, err = raw.Exec(
				"UPDATE node_settings_gate SET value=? WHERE name='metadata_plugin'",
				provider,
			)
			require.NoError(t, errors.Join(err, raw.Close()))
			adoptedSlot := sqliteTipSlot(t, filepath.Join(dir, "metadata.sqlite"))
			require.NoError(t, m.run(ctx))

			host, err := m.newHost()
			require.NoError(t, err)
			deps, err := m.dependencies("core")
			require.NoError(t, err)
			stores := resolveReleasedStores(t, host, m.selections(), deps)
			consensus, err := database.New(
				ctx, m.databaseConfig("core", config.IndexerLayoutSplit), stores,
			)
			require.NoError(t, err)
			set, _, err := nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
				DataDir:           dir,
				Logger:            logger,
				CardanoNodeConfig: nodeCfg,
			})
			require.NoError(t, err)
			bus := event.NewEventBus(nil, logger)
			cm, err := chain.NewManager(ctx, consensus, bus)
			require.NoError(t, err)
			fatal := make(chan error, 1)
			ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
				Database:          consensus,
				UtxoSet:           set,
				ChainManager:      cm,
				EventBus:          bus,
				Logger:            logger,
				CardanoNodeConfig: nodeCfg,
				Network:           "preview",
				PromRegistry:      prometheus.NewRegistry(),
				FatalErrorFunc: func(err error) {
					select {
					case fatal <- err:
					default:
					}
				},
			})
			require.NoError(t, err)
			started := false
			t.Cleanup(func() {
				if started {
					require.NoError(t, ls.Close())
				}
				require.NoError(t, set.Close())
				require.NoError(t, consensus.Close())
				require.NoError(t, host.Stop(context.Background()))
			})
			adopted, err := consensus.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, adoptedSlot, adopted.Point.Slot)
			// A start that stops between the revert and the move: no set
			// is open, so the run stops where the UTxO rows would move. The
			// start after it does not revert a second window.
			move := nodestorage.ReleasedMigration{
				UtxoSet: nodestorage.UtxoSetConfig{
					DataDir:           dir,
					CardanoNodeConfig: nodeCfg,
				},
				Logger:       logger,
				RewindBlocks: k,
			}
			require.ErrorContains(t, move.Move(ctx, consensus, nil), "is not open")
			require.NoError(t, move.Move(ctx, consensus, set))
			require.NoError(t, (nodestorage.Reconcile{
				DB:     consensus,
				Set:    set,
				Logger: logger,
				Resume: ls.ResumeUtxoSetFrom,
			}).Run(ctx))
			reverted, err := consensus.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, adopted.BlockNumber-k, reverted.BlockNumber)
			require.Equal(
				t, adoptedSlot,
				sqliteTipSlot(t, filepath.Join(dir, "metadata.sqlite")),
				"the adopted IndexerDB is not reverted",
			)
			require.NoError(t, cm.SetLedger(ls))
			require.NoError(t, ls.Start(ctx))
			started = true
			awaitTip := func(slot uint64, what string) {
				t.Helper()
				var fatalErr error
				require.Eventually(t, func() bool {
					select {
					case fatalErr = <-fatal:
						return true
					default:
					}
					setTip, _, err := set.Tip()
					return err == nil && ls.Tip().Point.Slot == slot &&
						setTip.Slot == slot
				}, 2*time.Minute, 10*time.Millisecond, what)
				require.NoError(t, fatalErr, what)
			}
			awaitTip(
				adoptedSlot,
				"the ledger applies the blocks up to the adopted tip again",
			)

			target, err := database.BlockByNumber(
				ctx, consensus, adopted.BlockNumber-5,
			)
			require.NoError(t, err)
			bus.Publish(ledger.ChainsyncEventType, event.NewEvent(
				ledger.ChainsyncEventType,
				ledger.ChainsyncEvent{
					Rollback: true,
					Point:    ocommon.NewPoint(target.Slot, target.Hash),
				},
			))
			awaitTip(
				target.Slot,
				"the ledger and the LedgerDB roll back below the adopted tip",
			)
		})
	}
}
