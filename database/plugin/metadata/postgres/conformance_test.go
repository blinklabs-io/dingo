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

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/conformance"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/stretchr/testify/require"
)

// isPostgresConfigured mirrors internal/test/conformance's check of the same
// name so this suite skips/runs under the same conditions in CI and locally:
// CI's go-test-linux job always sets POSTGRES_PASSWORD, so this runs
// automatically in CI; a bare local `go test -tags dingo_extra_plugins ./...`
// with no Postgres running skips cleanly instead of failing.
func isPostgresConfigured() bool {
	return os.Getenv("POSTGRES_PASSWORD") != "" ||
		os.Getenv("POSTGRES_DSN") != ""
}

// postgresAdminDSN builds a libpq keyword/value DSN from the same
// POSTGRES_HOST/PORT/USER/PASSWORD/DATABASE/SSLMODE environment variables
// this plugin's own provider and internal/test/conformance read. POSTGRES_DSN,
// if set, overrides everything. Used to create/drop this suite's dedicated
// schema (see postgresConformanceDSN).
func postgresAdminDSN() string {
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
	database := "dingo_test"
	if v := os.Getenv("POSTGRES_DATABASE"); v != "" {
		database = v
	}
	sslMode := "disable"
	if v := os.Getenv("POSTGRES_SSLMODE"); v != "" {
		sslMode = v
	}
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		conformance.EscapeLibpqValue(host),
		conformance.EscapeLibpqValue(port),
		conformance.EscapeLibpqValue(user),
		conformance.EscapeLibpqValue(os.Getenv("POSTGRES_PASSWORD")),
		conformance.EscapeLibpqValue(database),
		conformance.EscapeLibpqValue(sslMode),
	)
}

// postgresConformanceDSN is postgresAdminDSN with the connection's
// search_path pinned to a dedicated schema. That isolates this suite's
// singleton rows (e.g. node_settings) from this plugin's own pool_test.go
// and internal/test/conformance's Postgres variant, both of which connect to
// the same dingo_test database concurrently as separate `go test ./...`
// processes.
func postgresConformanceDSN(schema string) string {
	return conformance.PostgresDSNWithSearchPath(postgresAdminDSN(), schema)
}

func TestMetadataStoreConformance(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip(
			"Skipping postgres conformance test: postgres not configured " +
				"(set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	// Unique per run (not a fixed, predictable name): two go test
	// invocations against the same server can overlap in time, and a fixed
	// name's "does it already exist" check cannot tell "another run is
	// still using this" apart from "an unrelated schema happens to have
	// this name" -- either an unconditional drop can destroy an in-flight
	// sibling run, or a conditional one can skip dropping and leak. A name
	// that cannot have existed before this run needs neither: it is always
	// safe to drop unconditionally.
	schema := fmt.Sprintf("storage_conformance_%d", time.Now().UnixNano())

	admin, err := sql.Open("pgx", postgresAdminDSN())
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})

	storagetest.RunMetadataStoreConformance(
		t,
		func(t *testing.T) metadata.MetadataStore {
			t.Helper()
			store, err := openStore(
				Config{DSN: postgresConformanceDSN(schema)},
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
	if !isPostgresConfigured() {
		t.Skip(
			"Skipping postgres resource cleanup test: postgres not " +
				"configured (set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	// Unique per run -- see TestMetadataStoreConformance's comment on the
	// same pattern.
	schema := fmt.Sprintf(
		"storage_resource_cleanup_%d",
		time.Now().UnixNano(),
	)

	admin, err := sql.Open("pgx", postgresAdminDSN())
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})

	storagetest.AssertNoGoroutineLeak(t, func(t *testing.T) {
		store, err := openStore(
			Config{DSN: postgresConformanceDSN(schema)},
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
// it points at a closed local port with a short connect_timeout (belt, since
// pgx honors it) and a context deadline (suspenders, in case a given driver
// version does not), so a genuinely unreachable host fails fast with an
// error instead of hanging until some much longer default dial timeout.
func TestMetadataStoreUnreachableHostFailsWithoutHanging(t *testing.T) {
	store, err := openStore(
		Config{
			DSN: "host=127.0.0.1 port=1 user=postgres password=x " +
				"dbname=x sslmode=disable connect_timeout=3",
		},
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
// same host/port/user/database this package's other conformance tests use
// (see postgresAdminDSN), but with a deliberately wrong password, so a real
// server is reachable and specifically rejects the credentials rather than
// erroring for any other reason.
func TestMetadataStoreBadCredentialsFailsCleanly(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip(
			"Skipping postgres bad-credentials test: postgres not " +
				"configured (set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	if os.Getenv("POSTGRES_DSN") != "" {
		t.Skip(
			"Skipping postgres bad-credentials test: POSTGRES_DSN is an " +
				"opaque override this test cannot safely mutate a " +
				"password into",
		)
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
	database := "dingo_test"
	if v := os.Getenv("POSTGRES_DATABASE"); v != "" {
		database = v
	}
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=storagetest-wrong-password "+
			"dbname=%s sslmode=disable",
		conformance.EscapeLibpqValue(host),
		conformance.EscapeLibpqValue(port),
		conformance.EscapeLibpqValue(user),
		conformance.EscapeLibpqValue(database),
	)

	store, err := openStore(
		Config{DSN: dsn},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.Error(t, store.Start(t.Context()))
}
