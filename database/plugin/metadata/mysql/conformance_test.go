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

package mysql

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// isMysqlConformanceConfigured mirrors internal/test/conformance's check of
// the same name so this suite skips/runs under the same conditions in CI and
// locally. Unlike this plugin's own TestOpenStoreAppliesPoolSettings (which
// only needs a DSN string, no live server), this suite needs privileges to
// create its own database (see mysqlConformanceRootDSN), so it specifically
// requires MYSQL_ROOT_PASSWORD or a full MYSQL_DSN override -- CI's
// go-test-linux job always sets MYSQL_ROOT_PASSWORD, so this runs
// automatically in CI.
func isMysqlConformanceConfigured() bool {
	return os.Getenv("MYSQL_ROOT_PASSWORD") != "" ||
		os.Getenv("MYSQL_DSN") != ""
}

// mysqlConformanceRootDSN builds a root DSN from the same MYSQL_HOST/PORT
// environment variables this plugin's own provider and
// internal/test/conformance read, authenticated as root (not the
// mysql/mysql user CI also provisions) since this suite needs CREATE
// DATABASE privileges to isolate itself. MYSQL_DSN, if set, overrides
// everything.
func mysqlConformanceRootDSN() string {
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

// mysqlConformanceDSN is the root DSN with DBName set to database, the same
// technique database/plugin/metadata/sqlstore/dialect_integration_test.go
// uses for its isolated per-run MySQL database.
func mysqlConformanceDSN(t *testing.T, database string) string {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(mysqlConformanceRootDSN())
	require.NoError(t, err)
	parsed.DBName = database
	return parsed.FormatDSN()
}

// mysqlDatabaseExists reports whether database already exists, checked
// before this suite's own CREATE DATABASE IF NOT EXISTS so cleanup can drop
// only what it created -- a fixed, predictable database name shared by
// every run against the same server must never destroy a database this
// test run did not create itself.
func mysqlDatabaseExists(t *testing.T, admin *sql.DB, database string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, admin.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.SCHEMATA "+
			"WHERE SCHEMA_NAME = ?",
		database,
	).Scan(&exists))
	return exists
}

func TestMetadataStoreConformance(t *testing.T) {
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"Skipping mysql conformance test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
	const database = "storage_conformance_test"

	admin, err := sql.Open("mysql", mysqlConformanceRootDSN())
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	preexisting := mysqlDatabaseExists(t, admin, database)
	_, err = admin.Exec("CREATE DATABASE IF NOT EXISTS `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		if !preexisting {
			_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		}
		_ = admin.Close()
	})

	storagetest.RunMetadataStoreConformance(
		t,
		func(t *testing.T) metadata.MetadataStore {
			t.Helper()
			store, err := openStore(
				t.Context(),
				Config{DSN: mysqlConformanceDSN(t, database)},
				metadata.ProviderDependencies{},
			)
			require.NoError(t, err)
			require.NoError(t, store.Start(t.Context()))
			t.Cleanup(func() {
				require.NoError(t, store.Close())
			})
			return store
		},
	)
}

func TestMetadataStoreResourceCleanup(t *testing.T) {
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"Skipping mysql resource cleanup test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
	const database = "storage_resource_cleanup_test"

	admin, err := sql.Open("mysql", mysqlConformanceRootDSN())
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	preexisting := mysqlDatabaseExists(t, admin, database)
	_, err = admin.Exec("CREATE DATABASE IF NOT EXISTS `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		if !preexisting {
			_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		}
		_ = admin.Close()
	})

	storagetest.AssertNoGoroutineLeak(t, func(t *testing.T) {
		store, err := openStore(
			t.Context(),
			Config{DSN: mysqlConformanceDSN(t, database)},
			metadata.ProviderDependencies{},
		)
		require.NoError(t, err)
		require.NoError(t, store.Start(t.Context()))
		txn := store.Transaction()
		require.NoError(t, store.SetCommitTimestamp(1, txn))
		require.NoError(t, txn.Commit())
		require.NoError(t, store.Close())
	})
}

// TestMetadataStoreUnreachableHostFailsWithoutHanging needs no live server:
// it points at a closed local port with a short driver-level Timeout (belt)
// and a context deadline (suspenders, in case a given driver version does
// not honor its own Timeout for the initial dial), so a genuinely
// unreachable host fails fast with an error instead of hanging until some
// much longer default dial timeout.
func TestMetadataStoreUnreachableHostFailsWithoutHanging(t *testing.T) {
	dsn := (&mysqldriver.Config{
		User:      "root",
		Net:       "tcp",
		Addr:      "127.0.0.1:1",
		Timeout:   3 * time.Second,
		ParseTime: true,
	}).FormatDSN()
	store, err := openStore(
		t.Context(),
		Config{DSN: dsn},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.Error(t, store.Start(ctx))
	require.Less(
		t,
		time.Since(start),
		10*time.Second,
		"an unreachable host should fail within the connect timeout, not hang",
	)
}

// TestMetadataStoreBadCredentialsFailsCleanly is gated on a real, reachable
// server being configured, because it needs one that actually rejects the
// password -- pointing at nothing would just repeat
// TestMetadataStoreUnreachableHostFailsWithoutHanging. It connects to the
// same host/port this package's other conformance tests use (see
// mysqlConformanceRootDSN) as the root user, but with a deliberately wrong
// password, so a real server is reachable and specifically rejects the
// credentials rather than erroring for any other reason.
func TestMetadataStoreBadCredentialsFailsCleanly(t *testing.T) {
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"Skipping mysql bad-credentials test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
	if os.Getenv("MYSQL_DSN") != "" {
		t.Skip(
			"Skipping mysql bad-credentials test: MYSQL_DSN is an opaque " +
				"override this test cannot safely mutate a password into",
		)
	}
	host := "localhost"
	if v := os.Getenv("MYSQL_HOST"); v != "" {
		host = v
	}
	port := "3306"
	if v := os.Getenv("MYSQL_PORT"); v != "" {
		port = v
	}
	dsn := (&mysqldriver.Config{
		User:      "root",
		Passwd:    "storagetest-wrong-password",
		Net:       "tcp",
		Addr:      host + ":" + port,
		ParseTime: true,
	}).FormatDSN()

	store, err := openStore(
		t.Context(),
		Config{DSN: dsn},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.Error(t, store.Start(t.Context()))
}
