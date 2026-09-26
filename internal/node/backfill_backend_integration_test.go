//go:build dingo_db_integration

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

package node

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var backfillBackendSeq atomic.Uint64

// TestBackfillAnchorOnPostgresAndMySQL runs the backfill anchor tests against
// the two non-default metadata backends, each test in its own schema or
// database, reading the same DINGO_POSTGRES_DSN / DINGO_MYSQL_DSN variables
// as the sqlstore integration tests.
func TestBackfillAnchorOnPostgresAndMySQL(t *testing.T) {
	backends := []struct {
		name  string
		newDB func(*testing.T) *database.Database
	}{
		{"postgres", newPostgresBackfillDB},
		{"mysql", newMySQLBackfillDB},
	}
	tests := []struct {
		name string
		run  func(*testing.T, func(*testing.T) *database.Database)
	}{
		{
			"ReplaysRegistrationBeforeHistoricalWithdrawal",
			testBackfillReplaysRegistrationBeforeHistoricalWithdrawal,
		},
		{"ResumeStopsAtRecordedMithrilAnchor", testRun_ResumeStopsAtRecordedMithrilAnchor},
		{
			"RestoresSnapshotAccountDelegationAtAnchor",
			testRun_RestoresSnapshotAccountDelegationAtAnchor,
		},
		{"KeepsSnapshotDRepExpiryAtAnchor", testRun_KeepsSnapshotDRepExpiryAtAnchor},
		{
			"SettlesReplayedProposalsTheSnapshotDoesNotHold",
			testRun_SettlesReplayedProposalsTheSnapshotDoesNotHold,
		},
		{"KeepsImportedProtocolParameters", testRun_KeepsImportedProtocolParameters},
		{"EnactsPendingUpdateBeforeHardFork", testRunEnactsPendingUpdateBeforeHardFork},
		{"RederivesPParamsBelowTheAnchor", testRunRederivesPParamsBelowTheAnchor},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					tc.run(t, backend.newDB)
				})
			}
		})
	}
}

func backfillBackendName(prefix string) string {
	return fmt.Sprintf(
		"%s_%d_%d",
		prefix,
		time.Now().UnixNano(),
		backfillBackendSeq.Add(1),
	)
}

func newBackendBackfillDB(
	t *testing.T,
	name string,
	config map[string]any,
	register dbtest.StorageProvider,
) *database.Database {
	t.Helper()
	register.Name = name
	register.Config = config
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: t.TempDir(),
			Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		Metadata: register,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		dbtest.CloseDatabase(db) //nolint:errcheck
	})
	return db
}

func newPostgresBackfillDB(t *testing.T) *database.Database {
	t.Helper()
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := backfillBackendName("backfill")
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	return newBackendBackfillDB(
		t,
		"postgres",
		map[string]any{"dsn": storagetest.PostgresDSNWithSearchPath(dsn, schema)},
		dbtest.StorageProvider{Register: postgres.RegisterProvider},
	)
}

func newMySQLBackfillDB(t *testing.T) *database.Database {
	t.Helper()
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	name := backfillBackendName("backfill")
	_, err = admin.Exec("CREATE DATABASE `" + name + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + name + "`")
		_ = admin.Close()
	})
	cfg, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.DBName = name
	return newBackendBackfillDB(
		t,
		"mysql",
		map[string]any{"dsn": cfg.FormatDSN()},
		dbtest.StorageProvider{Register: mysql.RegisterProvider},
	)
}
