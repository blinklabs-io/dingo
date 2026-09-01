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

package conformance

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

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
// reviewer's probe found broken here before every DSN component was passed
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
	require.NoError(t, err, "postgresConformanceDSN produced an unparseable DSN")
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
		sm := newTestPostgresConformanceManager(t)
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
// Dingo's storage layer through Postgres' dialect. See corpus_test.go for the
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
// TestDingoStateManagerRestartSurvivesReopen (state_manager_backend_test.go).
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
// (conformance_main_test.go), this whole process's postgresProcessSchema
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
