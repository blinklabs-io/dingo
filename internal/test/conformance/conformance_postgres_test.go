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

package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// init registers this build configuration's process teardown: the Postgres
// schema, the MySQL database, and their paired local blob directories. See
// postgresProcessSchema's doc comment in state_manager_postgres.go and
// mysqlProcessDatabase's in state_manager_mysql.go for why those are shared
// across every NewDingoPostgresStateManager/NewDingoMysqlStateManager call in
// the process, so cleanup belongs here once rather than in an individual
// manager's Close.
//
// Registering rather than defining a second TestMain is what keeps the two
// build configurations from drifting; see process_cleanup_test.go.
func init() {
	registerProcessCleanup(cleanupPostgresProcessResources)
	registerProcessCleanup(cleanupMysqlProcessResources)
}

// cleanupPostgresProcessResources drops this process's Postgres schema and
// removes its paired blob directory.
//
// A non-empty postgresProcessBlobDir is this process's own signal that a
// manager actually used the backend: it is not set until
// ensurePostgresProcessBlobDir runs, which only happens from inside
// NewDingoPostgresStateManager. A `go test` invocation that never configured or
// exercised Postgres skips cleanup rather than connecting to a DSN nothing in
// this run ever validated.
//
// Both steps always run: the schema drop failing must not skip removal of the
// directory paired with it.
func cleanupPostgresProcessResources() error {
	if postgresProcessBlobDir == "" {
		return nil
	}
	var errs []error
	if isPostgresConformanceConfigured() {
		if err := dropPostgresSchema(
			postgresConformanceDSN(),
			postgresProcessSchema,
		); err != nil {
			errs = append(errs, fmt.Errorf(
				"cleanup postgres process schema %q: %w",
				postgresProcessSchema,
				err,
			))
		}
	}
	if err := os.RemoveAll(postgresProcessBlobDir); err != nil {
		errs = append(errs, fmt.Errorf(
			"remove postgres process blob dir %q: %w",
			postgresProcessBlobDir,
			err,
		))
	}
	return errors.Join(errs...)
}

// cleanupMysqlProcessResources drops this process's MySQL database and removes
// its paired blob directory. See cleanupPostgresProcessResources for why an
// empty mysqlProcessBlobDir means this backend was never exercised, and why
// both steps always run.
func cleanupMysqlProcessResources() error {
	if mysqlProcessBlobDir == "" {
		return nil
	}
	var errs []error
	if isMysqlConformanceConfigured() {
		if err := dropMysqlDatabase(
			mysqlConformanceRootDSN(),
			mysqlProcessDatabase,
		); err != nil {
			errs = append(errs, fmt.Errorf(
				"cleanup mysql process database %q: %w",
				mysqlProcessDatabase,
				err,
			))
		}
	}
	if err := os.RemoveAll(mysqlProcessBlobDir); err != nil {
		errs = append(errs, fmt.Errorf(
			"remove mysql process blob dir %q: %w",
			mysqlProcessBlobDir,
			err,
		))
	}
	return errors.Join(errs...)
}

// mysqlConformanceDatabase is a fixed database name used only to build a
// plausible-looking DSN for the negative "unreachable host"/"bad
// credentials" acceptance tests below, where the connection is expected to
// fail before any database name would matter -- it is never used to
// actually create, migrate, or query a database. Real constructions
// (NewDingoMysqlStateManager) use a process-unique name instead; see
// mysqlProcessDatabase's doc comment in state_manager_mysql.go for why.
const mysqlConformanceDatabase = "dingo_conformance_test"

// isMysqlConformanceConfigured checks whether a MySQL root DSN has been
// supplied via environment variables. Unlike
// database/plugin/metadata/mysql's isMysqlConfigured (which only needs
// MYSQL_PASSWORD, since its test user is pre-granted access to dingo_test),
// this suite needs privileges to create its own database (see
// state_manager_mysql.go), so it specifically requires MYSQL_ROOT_PASSWORD
// or a full MYSQL_DSN override.
func isMysqlConformanceConfigured() bool {
	return os.Getenv("MYSQL_ROOT_PASSWORD") != "" ||
		os.Getenv("MYSQL_DSN") != ""
}

// skipIfMysqlConformanceNotConfigured skips the test unless a MySQL root
// DSN is available, so a plain `go test ./...` with no database running
// still passes.
func skipIfMysqlConformanceNotConfigured(t *testing.T) {
	t.Helper()
	if !isMysqlConformanceConfigured() {
		t.Skip(
			"Skipping mysql conformance test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
}

// mysqlConformanceRootDSN builds a root DSN from MYSQL_HOST/PORT/ROOT_PASSWORD
// environment variables -- the same host/port convention
// database/plugin/metadata/mysql/mysql_test.go reads, but authenticated as
// root since this suite needs CREATE DATABASE privileges that suite's
// regular test user doesn't have. MYSQL_DSN, if set, overrides everything.
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

// newTestMysqlConformanceManager creates a MySQL-backed DingoStateManager
// for testing, skipping the test if mysql isn't configured.
func newTestMysqlConformanceManager(t *testing.T) *DingoStateManager {
	t.Helper()
	skipIfMysqlConformanceNotConfigured(t)

	sm, err := NewDingoMysqlStateManager(mysqlConformanceRootDSN())
	require.NoError(t, err, "failed to create mysql state manager")
	return sm
}

var (
	mysqlCorpusOnce sync.Once
	mysqlCorpusRun  corpusRun
)

// mysqlCorpusResults returns the MySQL backend's memoized corpus replay.
// Requires the caller to have already skipped when MySQL is not configured.
func mysqlCorpusResults(t *testing.T) []conformance.VectorResult {
	t.Helper()
	mysqlCorpusOnce.Do(func() {
		// Construct here rather than through
		// newTestMysqlConformanceManager: that helper reports a
		// construction failure with require.NoError on its own
		// *testing.T, and sync.Once marks itself done even when its
		// function unwinds through t.FailNow's runtime.Goexit. A later
		// consumer in the same process would then read a zero-value
		// corpusRun -- nil results and nil error -- pass its
		// require.NoError, and fail in assertCorpus with a misleading
		// "produced no vectors" instead of the construction failure.
		// Storing the error keeps corpusRun's documented contract, and
		// matches sqliteCorpusResults.
		sm, err := NewDingoMysqlStateManager(mysqlConformanceRootDSN())
		if err != nil {
			mysqlCorpusRun = corpusRun{
				err: fmt.Errorf("new mysql state manager: %w", err),
			}
			return
		}
		defer sm.Close()
		mysqlCorpusRun = replayCorpus(sm)
	})
	require.NoError(t, mysqlCorpusRun.err, "mysql corpus replay")
	return mysqlCorpusRun.results
}

// TestRulesConformanceVectorsMysql replays the corpus against a real
// MySQL-backed state manager, then asserts, reports, and compares against the
// SQLite baseline in one pass. See TestRulesConformanceVectorsPostgres and
// conformance_postgres_test.go for why a per-dialect replay earns its cost while a repeated
// one does not.
func TestRulesConformanceVectorsMysql(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	results := mysqlCorpusResults(t)
	reportCorpus(t, "mysql", results)
	assertBackendMatchesSqlite(t, "mysql", results)
	assertCorpus(t, "mysql", results)
}

// TestNewDingoMysqlStateManagerRestartSurvivesReopen proves state committed
// through a real MySQL-backed DingoStateManager survives closing that
// manager and opening a new one against the same root DSN/database -- the
// MySQL analog of TestDingoStateManagerRestartSurvivesReopen
// (state_manager_test.go). Unlike the sqlite case there is no
// local file to reopen: the state lives on the MySQL server itself, so
// "restart" here means a fresh manager instance pointed at the same
// database.
func TestNewDingoMysqlStateManagerRestartSurvivesReopen(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	// Both manager instances share one local blob directory: the local
	// Badger blob store and the remote MySQL metadata store are paired at
	// construction (see newDingoMysqlStateManagerAtDatabase's doc comment),
	// so m2 must reuse m1's blob directory to reopen against the same
	// already-populated metadata store without tripping that pairing check.
	// NewDingoMysqlStateManager shares one database/blob-directory pair
	// across every call in its process and never drops the database on
	// Close (see mysqlProcessDatabase's doc comment in
	// state_manager_mysql.go) -- reusing it here would work today, but only
	// by accident, since nothing stops a sibling test elsewhere in this same
	// process from resetting it concurrently. Manage a database explicitly
	// instead, unique to this test run so it cannot collide with
	// mysqlProcessDatabase or any other test's database, and clean it up
	// once both managers are done.
	blobDataDir := t.TempDir()
	rootDSN := mysqlConformanceRootDSN()

	database := fmt.Sprintf("conformance_restart_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if err := dropMysqlDatabase(rootDSN, database); err != nil {
			t.Errorf("drop mysql restart-test database %q: %v", database, err)
		}
	})

	m1, err := newDingoMysqlStateManagerAtDatabase(
		rootDSN,
		blobDataDir,
		database,
	)
	require.NoError(t, err)

	pp := &conway.ConwayProtocolParameters{}
	require.NoError(t, m1.LoadInitialState(
		&conformance.ParsedInitialState{CurrentEpoch: 0},
		pp,
	))

	cred := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xb1),
	}
	tx, err := syntheticTransaction(
		"mysql-restart-stake-registration",
		[]common.Certificate{
			&common.StakeRegistrationCertificate{
				CertType:        uint(common.CertificateTypeStakeRegistration),
				StakeCredential: cred,
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, m1.ApplyTransaction(tx, 100))
	require.NoError(t, m1.Close())

	m2, err := newDingoMysqlStateManagerAtDatabase(
		rootDSN,
		blobDataDir,
		database,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Close()) }()

	provider := m2.GetStateProvider()
	require.True(
		t,
		provider.IsStakeCredentialRegistered(cred),
		"stake registration committed by m1 must be visible from a fresh manager pointed at the same database",
	)
}

// TestNewDingoMysqlStateManagerRollbackDiscardsWrites is the MySQL analog
// of TestDingoStateManagerRollbackDiscardsWrites: a write inside a real,
// rolled-back MySQL transaction is not visible via a fresh read.
func TestNewDingoMysqlStateManagerRollbackDiscardsWrites(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	m := newTestMysqlConformanceManager(t)
	defer func() { require.NoError(t, m.Close()) }()

	cred := testHash28(0xb2)

	txn := m.db.Transaction(true)
	defer txn.Release()
	account := &models.Account{
		StakingKey:    cred[:],
		CredentialTag: 0,
		Active:        true,
	}
	require.NoError(t, m.db.CreateAccount(txn, account))
	require.NoError(t, txn.Rollback())

	got, err := m.db.GetAccountByCredential(0, cred[:], false, nil)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
	require.Nil(t, got)
}

// TestNewDingoMysqlStateManagerUnreachableHostFails proves an unreachable
// MySQL host fails DingoStateManager construction with a real,
// bounded-time error -- not a hang and not a silently-successful no-op
// backend -- mirroring database/plugin/metadata/mysql's own
// TestMetadataStoreUnreachableHostFailsWithoutHanging. No live MySQL
// server is required for this test: it points at a closed local port with
// a short driver-level Timeout.
func TestNewDingoMysqlStateManagerUnreachableHostFails(t *testing.T) {
	dsn := (&mysqldriver.Config{
		User:      "root",
		Net:       "tcp",
		Addr:      "127.0.0.1:1",
		Timeout:   3 * time.Second,
		ParseTime: true,
		DBName:    mysqlConformanceDatabase,
	}).FormatDSN()

	start := time.Now()
	m, err := NewDingoMysqlStateManager(dsn)
	require.Error(t, err)
	require.Nil(t, m)
	require.Less(
		t,
		time.Since(start),
		15*time.Second,
		"an unreachable host should fail within the connect timeout, not hang",
	)
}

// TestNewDingoMysqlStateManagerBadCredentialsFails proves a reachable
// MySQL server that rejects the supplied credentials fails
// DingoStateManager construction cleanly, mirroring
// database/plugin/metadata/mysql's own
// TestMetadataStoreBadCredentialsFailsCleanly. It requires a real,
// reachable server (unlike the unreachable-host case above) so the
// failure is specifically credential rejection.
func TestNewDingoMysqlStateManagerBadCredentialsFails(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)
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
		DBName:    mysqlConformanceDatabase,
	}).FormatDSN()

	m, err := NewDingoMysqlStateManager(dsn)
	require.Error(t, err)
	require.Nil(t, m)
}

// dropMysqlDatabase drops database over an admin connection built from
// rootDSN (with DBName cleared, matching truncateMysqlConformanceDatabase's
// reasoning). Used to tear down the restart test's own explicitly managed
// database (see TestNewDingoMysqlStateManagerRestartSurvivesReopen) and,
// by TestMain (conformance_postgres_test.go), this whole process's
// mysqlProcessDatabase once every test has finished. Either way cleanup is
// a plain drop rather than truncateMysqlConformanceDatabase's in-place
// empty: nothing else needs the database to keep existing afterward.
func dropMysqlDatabase(rootDSN, database string) error {
	cfg, err := mysqldriver.ParseDSN(rootDSN)
	if err != nil {
		return fmt.Errorf("parse mysql root DSN: %w", err)
	}
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open mysql admin connection: %w", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		"DROP DATABASE IF EXISTS " + mysqlQuoteIdentifier(database),
	); err != nil {
		return fmt.Errorf("drop mysql database %q: %w", database, err)
	}
	return nil
}

// TestDeleteMysqlTablesClearsDespiteForeignKeys proves the reset empties a
// child and its parent in one transaction without ordering the deletes by
// dependency. The managed table list comes from information_schema in
// whatever order the server returns it, so a reset that respected foreign
// keys would fail on the first table that still has referencing rows.
func TestDeleteMysqlTablesClearsDespiteForeignKeys(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	cfg, err := mysqldriver.ParseDSN(mysqlConformanceRootDSN())
	require.NoError(t, err)
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)

	database := "fkreset_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	_, err = db.Exec("CREATE DATABASE " + mysqlQuoteIdentifier(database))
	require.NoError(t, err)
	dropAndCloseOnCleanup(
		t, db, "DROP DATABASE "+mysqlQuoteIdentifier(database),
	)

	ctx := context.Background()
	qualify := func(name string) string {
		return mysqlQuoteIdentifier(database) + "." +
			mysqlQuoteIdentifier(name)
	}
	_, err = db.ExecContext(ctx, "CREATE TABLE "+qualify("parent")+
		" (id INT AUTO_INCREMENT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE "+qualify("child")+
		" (id INT AUTO_INCREMENT PRIMARY KEY, parent_id INT, "+
		"CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES "+
		qualify("parent")+" (id))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO "+qualify("parent")+
		" (id) VALUES (1)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO "+qualify("child")+
		" (parent_id) VALUES (1)")
	require.NoError(t, err)

	// Parent first: the order a dependency-aware reset could not use.
	require.NoError(t, deleteMysqlTables(
		ctx, db, []string{qualify("parent"), qualify("child")},
	))

	for _, name := range []string{"parent", "child"} {
		var count int
		require.NoError(t, db.QueryRowContext(
			ctx, "SELECT COUNT(*) FROM "+qualify(name),
		).Scan(&count))
		require.Zero(t, count, "%s must be empty after reset", name)
	}
}

// isPostgresConformanceConfigured checks whether postgres connection info
// has been supplied via environment variables. Mirrors
// database/plugin/metadata/postgres's isPostgresConfigured so both suites
// skip/run under the same conditions in CI and locally.
func isPostgresConformanceConfigured() bool {
	return os.Getenv("POSTGRES_PASSWORD") != "" ||
		os.Getenv("POSTGRES_DSN") != ""
}

// skipIfPostgresConformanceNotConfigured skips the test unless postgres
// connection info is available, matching
// database/plugin/metadata/postgres/postgres_test.go's convention so a
// plain `go test ./...` with no database running still passes.
func skipIfPostgresConformanceNotConfigured(t *testing.T) {
	t.Helper()
	if !isPostgresConformanceConfigured() {
		t.Skip(
			"Skipping postgres conformance test: postgres not configured " +
				"(set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
}

// postgresConformanceDSN builds a libpq-style DSN from the same
// POSTGRES_HOST/PORT/USER/PASSWORD/DATABASE/SSLMODE environment variables
// database/plugin/metadata/postgres/postgres_test.go reads, so both suites
// point at the same server/database when run together (they stay isolated
// from each other via a dedicated Postgres schema -- see
// state_manager_postgres.go). POSTGRES_DSN, if set, overrides everything.
func postgresConformanceDSN() string {
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
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=UTC",
		storagetest.EscapeLibpqValue(host),
		storagetest.EscapeLibpqValue(port),
		storagetest.EscapeLibpqValue(user),
		storagetest.EscapeLibpqValue(os.Getenv("POSTGRES_PASSWORD")),
		storagetest.EscapeLibpqValue(database),
		storagetest.EscapeLibpqValue(sslMode),
	)
}

// TestPostgresConformanceDSNEscapesSpecialCharacterPassword proves
// postgresConformanceDSN survives a legal libpq password containing a
// space, a quote, and a backslash -- exactly the class of credential
// storagetest.EscapeLibpqValue exists to handle, and exactly what a
// probe once found broken here before every DSN component was passed
// through it: an unquoted, unescaped keyword/value pair ends at the first
// whitespace, so pgx.ParseConfig(postgresConformanceDSN()) silently
// truncated this password to just "review" -- a real POSTGRES_PASSWORD
// value like this would authenticate with the wrong (truncated) password
// against a live server rather than fail DSN parsing outright.
func TestPostgresConformanceDSNEscapesSpecialCharacterPassword(t *testing.T) {
	const specialPassword = "review pass'word\\tail"
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("POSTGRES_PASSWORD", specialPassword)

	cfg, err := pgx.ParseConfig(postgresConformanceDSN())
	require.NoError(
		t,
		err,
		"postgresConformanceDSN produced an unparseable DSN",
	)
	require.Equal(t, specialPassword, cfg.Password)
}

// newTestPostgresConformanceManager creates a Postgres-backed
// DingoStateManager for testing, skipping the test if postgres isn't
// configured.
func newTestPostgresConformanceManager(t *testing.T) *DingoStateManager {
	t.Helper()
	skipIfPostgresConformanceNotConfigured(t)

	sm, err := NewDingoPostgresStateManager(postgresConformanceDSN())
	require.NoError(t, err, "failed to create postgres state manager")
	return sm
}

var (
	postgresCorpusOnce sync.Once
	postgresCorpusRun  corpusRun
)

// postgresCorpusResults returns the Postgres backend's memoized corpus replay.
// Callers must have already skipped when Postgres is not configured.
func postgresCorpusResults(t *testing.T) []conformance.VectorResult {
	t.Helper()
	postgresCorpusOnce.Do(func() {
		// Construct here rather than through
		// newTestPostgresConformanceManager: that helper reports a
		// construction failure with require.NoError on its own
		// *testing.T, and sync.Once marks itself done even when its
		// function unwinds through t.FailNow's runtime.Goexit. A later
		// consumer in the same process would then read a zero-value
		// corpusRun -- nil results and nil error -- pass its
		// require.NoError, and fail in assertCorpus with a misleading
		// "produced no vectors" instead of the construction failure.
		// Storing the error keeps corpusRun's documented contract, and
		// matches sqliteCorpusResults.
		sm, err := NewDingoPostgresStateManager(postgresConformanceDSN())
		if err != nil {
			postgresCorpusRun = corpusRun{
				err: fmt.Errorf("new postgres state manager: %w", err),
			}
			return
		}
		defer sm.Close()
		postgresCorpusRun = replayCorpus(sm)
	})
	require.NoError(t, postgresCorpusRun.err, "postgres corpus replay")
	return postgresCorpusRun.results
}

// TestRulesConformanceVectorsPostgres is the pass/fail gate for the
// Postgres-backed DingoStateManager, and also reports the progress statistics
// and the SQLite comparison that previously each cost their own corpus replay.
//
// The corpus exercises gouroboros ledger rules, which do not vary by storage
// backend, so this run is not here for rule coverage -- it is here to drive
// Dingo's storage layer through Postgres' dialect. See state_provider_test.go for the
// two real bugs that found and for why one pass per dialect is the right
// amount.
func TestRulesConformanceVectorsPostgres(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	results := postgresCorpusResults(t)
	reportCorpus(t, "postgres", results)
	assertBackendMatchesSqlite(t, "postgres", results)
	assertCorpus(t, "postgres", results)
}

// TestNewDingoPostgresStateManagerRestartSurvivesReopen proves state
// committed through a real Postgres-backed DingoStateManager survives
// closing that manager and opening a new one against the same DSN/schema
// -- the Postgres analog of
// TestDingoStateManagerRestartSurvivesReopen (state_manager_test.go).
// Unlike the sqlite case there is no local file to reopen: the state lives
// on the Postgres server itself, so "restart" here means a fresh manager
// instance pointed at the same database/schema.
func TestNewDingoPostgresStateManagerRestartSurvivesReopen(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	// Both manager instances share one local blob directory: the local
	// Badger blob store and the remote Postgres metadata store are paired
	// at construction (see newDingoPostgresStateManagerAtSchema's doc
	// comment), so m2 must reuse m1's blob directory to reopen against the
	// same already-populated metadata store without tripping that pairing
	// check. NewDingoPostgresStateManager shares one schema/blob-directory
	// pair across every call in its process and never drops the schema on
	// Close (see postgresProcessSchema's doc comment in
	// state_manager_postgres.go) -- reusing it here would work today, but
	// only by accident, since nothing stops a sibling test elsewhere in this
	// same process from resetting it concurrently. Manage a schema
	// explicitly instead, unique to this test run so it cannot collide with
	// postgresProcessSchema or any other test's schema, and clean it up once
	// both managers are done.
	blobDataDir := t.TempDir()
	dsn := postgresConformanceDSN()

	schema := fmt.Sprintf("conformance_restart_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if err := dropPostgresSchema(dsn, schema); err != nil {
			t.Errorf("drop postgres restart-test schema %q: %v", schema, err)
		}
	})

	m1, err := newDingoPostgresStateManagerAtSchema(dsn, blobDataDir, schema)
	require.NoError(t, err)

	pp := &conway.ConwayProtocolParameters{}
	require.NoError(t, m1.LoadInitialState(
		&conformance.ParsedInitialState{CurrentEpoch: 0},
		pp,
	))

	cred := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa1),
	}
	tx, err := syntheticTransaction(
		"pg-restart-stake-registration",
		[]common.Certificate{
			&common.StakeRegistrationCertificate{
				CertType:        uint(common.CertificateTypeStakeRegistration),
				StakeCredential: cred,
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, m1.ApplyTransaction(tx, 100))
	require.NoError(t, m1.Close())

	m2, err := newDingoPostgresStateManagerAtSchema(dsn, blobDataDir, schema)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Close()) }()

	provider := m2.GetStateProvider()
	require.True(
		t,
		provider.IsStakeCredentialRegistered(cred),
		"stake registration committed by m1 must be visible from a fresh manager pointed at the same schema",
	)
}

// TestNewDingoPostgresStateManagerRollbackDiscardsWrites is the Postgres
// analog of TestDingoStateManagerRollbackDiscardsWrites: a write inside a
// real, rolled-back Postgres transaction is not visible via a fresh read.
func TestNewDingoPostgresStateManagerRollbackDiscardsWrites(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	m := newTestPostgresConformanceManager(t)
	defer func() { require.NoError(t, m.Close()) }()

	cred := testHash28(0xa2)

	txn := m.db.Transaction(true)
	defer txn.Release()
	account := &models.Account{
		StakingKey:    cred[:],
		CredentialTag: 0,
		Active:        true,
	}
	require.NoError(t, m.db.CreateAccount(txn, account))
	require.NoError(t, txn.Rollback())

	got, err := m.db.GetAccountByCredential(0, cred[:], false, nil)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
	require.Nil(t, got)
}

// TestNewDingoPostgresStateManagerUnreachableHostFails proves an
// unreachable Postgres host fails DingoStateManager construction with a
// real, bounded-time error -- not a hang and not a silently-successful
// no-op backend -- mirroring
// database/plugin/metadata/postgres's own
// TestMetadataStoreUnreachableHostFailsWithoutHanging. No live Postgres
// server is required for this test: it points at a closed local port with
// a short connect_timeout.
func TestNewDingoPostgresStateManagerUnreachableHostFails(t *testing.T) {
	dsn := "host=127.0.0.1 port=1 user=postgres password=x " +
		"dbname=x sslmode=disable connect_timeout=3"

	start := time.Now()
	m, err := NewDingoPostgresStateManager(dsn)
	require.Error(t, err)
	require.Nil(t, m)
	require.Less(
		t,
		time.Since(start),
		15*time.Second,
		"an unreachable host should fail within the connect timeout, not hang",
	)
}

// TestNewDingoPostgresStateManagerBadCredentialsFails proves a reachable
// Postgres server that rejects the supplied credentials fails
// DingoStateManager construction cleanly, mirroring
// database/plugin/metadata/postgres's own
// TestMetadataStoreBadCredentialsFailsCleanly. It requires a real,
// reachable server (unlike the unreachable-host case above) so the
// failure is specifically credential rejection.
func TestNewDingoPostgresStateManagerBadCredentialsFails(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)
	if os.Getenv("POSTGRES_DSN") != "" {
		t.Skip(
			"Skipping postgres bad-credentials test: POSTGRES_DSN is an " +
				"opaque override this test cannot safely mutate a password into",
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
	database := "dingo_test"
	if v := os.Getenv("POSTGRES_DATABASE"); v != "" {
		database = v
	}
	dsn := fmt.Sprintf(
		"host=%s port=%s user=postgres password=storagetest-wrong-password "+
			"dbname=%s sslmode=disable",
		storagetest.EscapeLibpqValue(host),
		storagetest.EscapeLibpqValue(port),
		storagetest.EscapeLibpqValue(database),
	)

	m, err := NewDingoPostgresStateManager(dsn)
	require.Error(t, err)
	require.Nil(t, m)
}

// dropPostgresSchema drops schema (and everything in it) over an ordinary,
// unscoped connection to dsn. Used to tear down the restart test's own
// explicitly managed schema (see
// TestNewDingoPostgresStateManagerRestartSurvivesReopen) and, by TestMain
// (conformance_postgres_test.go), this whole process's postgresProcessSchema
// once every test has finished. Either way cleanup is a plain drop rather
// than truncatePostgresConformanceSchema's in-place empty: nothing else
// needs the schema to keep existing afterward.
func dropPostgresSchema(dsn, schema string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open postgres admin connection: %w", err)
	}
	defer db.Close()
	// schema is always Go-generated (a process-and-time-derived unique
	// name), never operator/DSN input, so string concatenation here
	// carries no injection risk -- same reasoning as
	// ensurePostgresConformanceSchema.
	if _, err := db.Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
	); err != nil {
		return fmt.Errorf("drop postgres schema %q: %w", schema, err)
	}
	return nil
}

// TestDeletePostgresTablesClearsDespiteForeignKeys proves the reset empties a
// child and its parent without ordering the deletes by dependency. The
// managed table list comes from information_schema in whatever order the
// server returns it, and the conformance schema's foreign keys are NOT
// DEFERRABLE, so sequential deletes in that order would fail on the first
// table that still has referencing rows.
func TestDeletePostgresTablesClearsDespiteForeignKeys(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	db, err := sql.Open("pgx", postgresConformanceDSN())
	require.NoError(t, err)

	schema := "fkreset_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	_, err = db.Exec(`CREATE SCHEMA ` + pgQuoteIdent(schema))
	require.NoError(t, err)
	dropAndCloseOnCleanup(
		t, db, `DROP SCHEMA `+pgQuoteIdent(schema)+` CASCADE`,
	)

	ctx := context.Background()
	parent := pgQuoteQualified(schema, "parent")
	child := pgQuoteQualified(schema, "child")
	_, err = db.ExecContext(ctx,
		`CREATE TABLE `+parent+` (id bigserial PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`CREATE TABLE `+child+` (id bigserial PRIMARY KEY, `+
			`parent_id bigint REFERENCES `+parent+`(id))`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO `+parent+` DEFAULT VALUES`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO `+child+` (parent_id) SELECT id FROM `+parent)
	require.NoError(t, err)

	// Parent first: the order a dependency-aware reset could not use.
	require.NoError(t, deletePostgresTables(
		ctx, db, schema, []string{parent, child},
	))

	for _, qualified := range []string{parent, child} {
		var count int
		require.NoError(t, db.QueryRowContext(
			ctx, `SELECT count(*) FROM `+qualified,
		).Scan(&count))
		require.Zero(t, count, "%s must be empty after reset", qualified)
	}
}
