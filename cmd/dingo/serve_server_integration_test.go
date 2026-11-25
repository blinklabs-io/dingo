//go:build dingo_extra_plugins && dingo_db_integration

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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/plugin"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// TestServePreflightOpensTheConsensusNamespace runs the serve preflights on
// an adopted PostgreSQL node. They open the consensus store, which is the
// consensus schema: opening the configured database instead would latch core
// mode into the IndexerDB, which then refuses its own API-mode open.
func TestServePreflightOpensTheConsensusNamespace(t *testing.T) {
	admin := os.Getenv("DINGO_POSTGRES_DSN")
	if admin == "" {
		t.Skip("DINGO_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	adminDB, err := sql.Open("pgx", admin)
	require.NoError(t, err)
	name := fmt.Sprintf("serve_preflight_%d", time.Now().UnixNano())
	_, err = adminDB.ExecContext(ctx, `CREATE DATABASE "`+name+`"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = adminDB.Exec(`DROP DATABASE "` + name + `" WITH (FORCE)`)
		require.NoError(t, adminDB.Close())
	})
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "api",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob: plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{
					Provider: "postgres",
					Config: map[string]any{
						"dsn": replaceDatabase(t, admin, name),
					},
				},
			},
		},
		ServerCoreNamespace: true,
	}
	require.NoError(
		t,
		checkSyncState(t.Context(), cfg, slog.New(slog.DiscardHandler)),
	)
	_, err = mithrilRewardRepairPending(ctx, cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	defer func() { require.NoError(t, host.Stop(ctx)) }()
	indexer, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		"postgres",
		cfg.Plugins.Storage.Metadata.Config,
		metadata.ProviderDependencies{
			StorageMode:         "api",
			GenesisStakingEmpty: true,
		},
	)
	require.NoError(t, err, "the IndexerDB still opens in API mode")
	gates, err := indexer.GetNodeSettingsGates(ctx)
	require.NoError(t, err)
	require.NotEqual(t, "core", gates["storage_mode"],
		"a preflight must not latch core mode into the IndexerDB")
	var schemas int
	core, err := sql.Open("pgx", replaceDatabase(t, admin, name))
	require.NoError(t, err)
	defer core.Close()
	require.NoError(t, core.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_namespace WHERE nspname = $1",
		metadata.CoreNamespaceName(name, "public"),
	).Scan(&schemas))
	require.Equal(t, 1, schemas, "the preflights opened the consensus schema")
}

// replaceDatabase returns the URL DSN admin selecting database name.
func replaceDatabase(t *testing.T, admin, name string) string {
	t.Helper()
	u, err := url.Parse(admin)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}
