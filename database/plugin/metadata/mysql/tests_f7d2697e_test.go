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
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
// technique database/plugin/metadata/sqlstore/dialect_3d29378a_test.go
// uses for its isolated per-run MySQL database.
func mysqlConformanceDSN(t *testing.T, database string) string {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(mysqlConformanceRootDSN())
	require.NoError(t, err)
	parsed.DBName = database
	return parsed.FormatDSN()
}

func TestMetadataStoreConformance(t *testing.T) {
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"Skipping mysql conformance test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
	// Unique per run (not a fixed, predictable name): two go test
	// invocations against the same server can overlap in time, and a fixed
	// name's "does it already exist" check cannot tell "another run is
	// still using this" apart from "an unrelated database happens to have
	// this name" -- either an unconditional drop can destroy an in-flight
	// sibling run, or a conditional one can skip dropping and leak. A name
	// that cannot have existed before this run needs neither: it is always
	// safe to drop unconditionally.
	database := fmt.Sprintf(
		"storage_conformance_%d",
		time.Now().UnixNano(),
	)

	admin, err := sql.Open("mysql", mysqlConformanceRootDSN())
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
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
	// Unique per run -- see TestMetadataStoreConformance's comment on the
	// same pattern.
	database := fmt.Sprintf(
		"storage_resource_cleanup_%d",
		time.Now().UnixNano(),
	)

	admin, err := sql.Open("mysql", mysqlConformanceRootDSN())
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
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
		txn := store.Transaction(t.Context())
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

func TestOpenStoreAppliesPoolSettings(t *testing.T) {
	store, err := openStore(
		t.Context(),
		Config{
			// A schema-less explicit DSN must not trigger CREATE DATABASE or
			// require a live server just to verify pool configuration.
			DSN:                 "user:pass@tcp(localhost:3306)/",
			PoolMaxOpenConns:    17,
			PoolMaxIdleConns:    3,
			PoolConnMaxLifetime: 30 * time.Minute,
		},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.Equal(t, 17, store.WritePoolStats().MaxOpenConnections)
}

func TestOpenStoreRejectsNegativePoolSettings(t *testing.T) {
	for _, cfg := range []Config{
		{PoolMaxOpenConns: -1},
		{PoolMaxIdleConns: -1},
		{PoolConnMaxLifetime: -time.Second},
	} {
		_, err := openStore(t.Context(), cfg, metadata.ProviderDependencies{})
		require.Error(t, err)
	}
}

func TestConfigYAMLDecodesPoolSettings(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
poolMaxOpenConns: 250
poolMaxIdleConns: 25
poolConnMaxLifetime: 1h30m
`), &cfg))
	require.Equal(t, 250, cfg.PoolMaxOpenConns)
	require.Equal(t, 25, cfg.PoolMaxIdleConns)
	require.Equal(t, 90*time.Minute, cfg.PoolConnMaxLifetime)
}

func TestAssembledDSNSetsStatementAndLockTimeoutParams(t *testing.T) {
	dsn, err := assembleDSN(Config{
		StatementTimeout: 5 * time.Second,
		LockTimeout:      1500 * time.Millisecond,
	})
	require.NoError(t, err)

	require.Contains(t, dsn, "max_execution_time=5000")
	// A sub-second LockTimeout rounds up to a whole second rather than
	// truncating to zero (unbounded) -- innodb_lock_wait_timeout has no
	// sub-second resolution.
	require.Contains(t, dsn, "innodb_lock_wait_timeout=2")
}

// TestAssembledDSNRoundsUpSubMillisecondStatementTimeout guards a real gap:
// time.Duration.Milliseconds() truncates, so a positive sub-millisecond
// StatementTimeout (500us) would format as "0" -- indistinguishable from
// unset, and so silently disabling the timeout instead of applying the
// shortest one MySQL can express (1ms).
func TestAssembledDSNRoundsUpSubMillisecondStatementTimeout(t *testing.T) {
	dsn, err := assembleDSN(Config{
		StatementTimeout: 500 * time.Microsecond,
	})
	require.NoError(t, err)

	require.Contains(t, dsn, "max_execution_time=1")
}

func TestAssembledDSNOmitsUnsetTimeouts(t *testing.T) {
	dsn, err := assembleDSN(Config{})
	require.NoError(t, err)

	require.NotContains(t, dsn, "max_execution_time")
	require.NotContains(t, dsn, "innodb_lock_wait_timeout")
}

func TestAssembledDSNSetsTransportTimeouts(t *testing.T) {
	dsn, err := assembleDSN(Config{
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	require.NoError(t, err)

	require.Contains(t, dsn, "readTimeout=2s")
	require.Contains(t, dsn, "writeTimeout=3s")
}

func TestOpenStoreIgnoresTimeoutsWhenDSNIsExplicit(t *testing.T) {
	// A schema-less explicit DSN must not trigger CREATE DATABASE or
	// require a live server; StatementTimeout/LockTimeout are only applied
	// while assembling a provider-generated DSN, so an explicit DSN's
	// timeouts (or lack of them) pass through untouched.
	store, err := openStore(
		t.Context(),
		Config{
			DSN:              "user:pass@tcp(localhost:3306)/",
			StatementTimeout: 5 * time.Second,
			LockTimeout:      time.Second,
		},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
}

func TestOpenStoreRejectsNegativeTimeouts(t *testing.T) {
	for _, cfg := range []Config{
		{DSN: "user:pass@tcp(localhost:3306)/", StatementTimeout: -time.Second},
		{DSN: "user:pass@tcp(localhost:3306)/", LockTimeout: -time.Second},
		{DSN: "user:pass@tcp(localhost:3306)/", ReadTimeout: -time.Second},
		{DSN: "user:pass@tcp(localhost:3306)/", WriteTimeout: -time.Second},
	} {
		_, err := openStore(t.Context(), cfg, metadata.ProviderDependencies{})
		require.Error(t, err)
	}
}

func TestConfigYAMLDecodesTimeouts(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
statementTimeout: 5s
lockTimeout: 1500ms
readTimeout: 2s
writeTimeout: 3s
`), &cfg))
	require.Equal(t, 5*time.Second, cfg.StatementTimeout)
	require.Equal(t, 1500*time.Millisecond, cfg.LockTimeout)
	require.Equal(t, 2*time.Second, cfg.ReadTimeout)
	require.Equal(t, 3*time.Second, cfg.WriteTimeout)
}
