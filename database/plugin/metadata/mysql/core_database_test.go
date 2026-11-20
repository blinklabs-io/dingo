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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/plugin"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestCoreDSNPreservesReleasedConnectionSettings(t *testing.T) {
	t.Parallel()
	original, err := mysqldriver.ParseDSN(
		"alice:secret@tcp(localhost:3307)/archive?parseTime=true&timeout=2s&readTimeout=3s&writeTimeout=4s&charset=utf8mb4&loc=UTC",
	)
	require.NoError(t, err)
	dsn, name, err := coreDSN(original.FormatDSN())
	require.NoError(t, err)
	selected, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	require.NotEqual(t, "archive", name)
	require.Len(t, name, 63)
	require.Equal(t, name, selected.DBName)
	selected.DBName = "archive"
	require.Equal(t, original.FormatDSN(), selected.FormatDSN())
	require.Equal(t, "archive", original.DBName)
	_, _, err = coreDSN("alice:secret@tcp(localhost:3307)/")
	require.ErrorContains(t, err, "must name its database")
}

func TestCoreProviderCreatesIsolatedDatabase(t *testing.T) {
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"MySQL is not configured; CI services exercise namespace provisioning",
		)
	}
	t.Parallel()
	ctx := context.Background()
	admin, err := sql.Open("mysql", mysqlConformanceRootDSN())
	require.NoError(t, err)
	sourceName := fmt.Sprintf("core_adoption_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE DATABASE `"+sourceName+"`")
	require.NoError(t, err)
	coreName := metadata.CoreNamespaceName(sourceName, "")
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + coreName + "`")
		_, _ = admin.Exec("DROP DATABASE `" + sourceName + "`")
		require.NoError(t, admin.Close())
	})
	sourceDSN := mysqlConformanceDSN(t, sourceName)
	for range 2 {
		host := plugin.NewHost()
		require.NoError(t, RegisterProvider(host))
		store, err := plugin.Resolve[metadata.MetadataStore](
			ctx,
			host,
			plugin.CapabilityStorageMetadata,
			"mysql",
			map[string]any{"dsn": sourceDSN},
			metadata.ProviderDependencies{
				StorageMode:         "core",
				CoreNamespace:       true,
				GenesisStakingEmpty: true,
			},
		)
		require.NoError(t, err)
		_, err = store.GetTip(nil)
		require.NoError(t, err)
		require.NoError(t, host.Stop(ctx))
	}
	selected, _, err := coreDSN(sourceDSN)
	require.NoError(t, err)
	core, err := sql.Open("mysql", selected)
	require.NoError(t, err)
	defer core.Close()
	var database string
	require.NoError(
		t,
		core.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database),
	)
	require.Equal(t, coreName, database)
	var sourceTables int
	require.NoError(
		t,
		admin.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = ?", sourceName).
			Scan(&sourceTables),
	)
	require.Zero(
		t,
		sourceTables,
		"consensus provisioning must not create tables in the API database",
	)
}

// TestCoreProviderNamesTheGrantItNeeds opens the consensus database as a
// user holding privileges on the API database alone, then with CREATE on the
// consensus database but no table privileges, which its schema migrations
// need.
func TestCoreProviderNamesTheGrantItNeeds(t *testing.T) {
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"MySQL is not configured; CI services exercise namespace provisioning",
		)
	}
	t.Parallel()
	ctx := context.Background()
	admin, err := sql.Open("mysql", mysqlConformanceRootDSN())
	require.NoError(t, err)
	sourceName := fmt.Sprintf("core_grant_%d", time.Now().UnixNano())
	coreName := metadata.CoreNamespaceName(sourceName, "")
	user := fmt.Sprintf("cg%d", time.Now().UnixNano()%1_000_000_000)
	for _, stmt := range []string{
		"CREATE DATABASE `" + sourceName + "`",
		"CREATE USER '" + user + "'@'%' IDENTIFIED BY 'pw'",
		"GRANT ALL PRIVILEGES ON `" + sourceName + "`.* TO '" + user + "'@'%'",
	} {
		_, err = admin.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + coreName + "`")
		_, _ = admin.Exec("DROP DATABASE `" + sourceName + "`")
		_, _ = admin.Exec("DROP USER '" + user + "'@'%'")
		require.NoError(t, admin.Close())
	})
	root, err := mysqldriver.ParseDSN(mysqlConformanceRootDSN())
	require.NoError(t, err)
	root.User, root.Passwd, root.DBName = user, "pw", sourceName
	dsn := root.FormatDSN()
	resolve := func() error {
		host := plugin.NewHost()
		require.NoError(t, RegisterProvider(host))
		defer func() { _ = host.Stop(ctx) }()
		_, err := plugin.Resolve[metadata.MetadataStore](
			ctx, host, plugin.CapabilityStorageMetadata, "mysql",
			map[string]any{"dsn": dsn},
			metadata.ProviderDependencies{
				StorageMode: "core", CoreNamespace: true, GenesisStakingEmpty: true,
			},
		)
		return err
	}
	grant := "GRANT ALL PRIVILEGES ON `" + coreName + "`.* TO '" + user + "'"
	err = resolve()
	require.ErrorContains(t, err, grant)
	require.ErrorContains(t, err, coreName)

	_, err = admin.ExecContext(ctx, "GRANT CREATE ON `"+coreName+"`.* TO '"+user+"'@'%'")
	require.NoError(t, err)
	err = resolve()
	require.ErrorContains(t, err, grant,
		"a refusal inside the schema migrations names the grant too")

	_, err = admin.ExecContext(ctx, "GRANT ALL PRIVILEGES ON `"+coreName+"`.* TO '"+user+"'@'%'")
	require.NoError(t, err)
	require.NoError(t, resolve())
}
