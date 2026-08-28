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

// TestRulesConformanceVectorsPostgres is the strict pass/fail gate for the
// Postgres-backed DingoStateManager: it fails immediately on the first
// vector mismatch (via harness.RunAllVectors), mirroring
// TestRulesConformanceVectors in conformance_test.go.
func TestRulesConformanceVectorsPostgres(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	testdataRoot, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err, "failed to extract embedded testdata")

	sm := newTestPostgresConformanceManager(t)
	defer sm.Close()

	harness := conformance.NewHarness(sm, conformance.HarnessConfig{
		TestdataRoot: testdataRoot,
		Debug:        testing.Verbose(),
	})

	harness.RunAllVectors(t)
}

// TestRulesConformanceVectorsWithResultsPostgres runs the harness against
// both the SQLite-backed and Postgres-backed state managers in the same
// test and compares them, rather than asserting a hardcoded vector count:
// the two runs should exercise the identical number of vectors with
// identical pass counts, and the comparison stays correct even as the
// embedded ouroboros-mock vector corpus grows or shrinks.
func TestRulesConformanceVectorsWithResultsPostgres(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	sqliteRoot, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err, "failed to extract embedded testdata")

	sqliteSm, err := NewDingoStateManager()
	require.NoError(t, err)
	defer sqliteSm.Close()

	sqliteHarness := conformance.NewHarness(sqliteSm, conformance.HarnessConfig{
		TestdataRoot: sqliteRoot,
	})
	sqliteResults, err := sqliteHarness.RunAllVectorsWithResults()
	require.NoError(t, err, "failed to run sqlite vectors")

	pgRoot, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err, "failed to extract embedded testdata")

	pgSm := newTestPostgresConformanceManager(t)
	defer pgSm.Close()

	pgHarness := conformance.NewHarness(pgSm, conformance.HarnessConfig{
		TestdataRoot: pgRoot,
	})
	pgResults, err := pgHarness.RunAllVectorsWithResults()
	require.NoError(t, err, "failed to run postgres vectors")

	var pgPassed, pgFailed int
	for _, result := range pgResults {
		if result.Success {
			pgPassed++
		} else {
			pgFailed++
		}
	}

	t.Logf("Conformance Test Results (PostgreSQL):")
	t.Logf("  Total vectors: %d", len(pgResults))
	t.Logf("  Passed: %d", pgPassed)
	t.Logf("  Failed: %d", pgFailed)
	if len(pgResults) > 0 {
		t.Logf(
			"  Pass rate: %.1f%%",
			float64(pgPassed)/float64(len(pgResults))*100,
		)
	}
	if pgFailed > 0 && testing.Verbose() {
		t.Log("First failures:")
		failCount := 0
		for _, result := range pgResults {
			if !result.Success && failCount < 5 {
				t.Logf("  %s: %v", result.Title, result.Error)
				failCount++
			}
		}
		if pgFailed > 5 {
			t.Logf("  ... and %d more failures", pgFailed-5)
		}
	}

	require.Equal(
		t,
		len(sqliteResults),
		len(pgResults),
		"postgres backend exercised a different number of vectors than "+
			"sqlite; vector discovery/extraction should be backend-invariant",
	)
	require.Zero(t, pgFailed, "postgres backend failed vectors sqlite passed")
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
	// at construction (see newDingoPostgresStateManagerAt's doc comment),
	// so m2 must reuse m1's blob directory to reopen against the same
	// already-populated metadata store without tripping that pairing
	// check.
	blobDataDir := t.TempDir()
	dsn := postgresConformanceDSN()

	// blobDataDir is always a fresh, empty t.TempDir(), so this test needs
	// an equally fresh, empty metadata side to pair with it -- reusing
	// postgresConformanceSchema (the schema every other test in this suite
	// shares) would trip database.New's commit-timestamp consistency check
	// against whatever those other tests have already committed there, and
	// truncating that shared schema here would just move the same mismatch
	// onto the *other* tests instead (they pair the stable, suite-shared
	// postgresConformanceBlobDir with that schema, and this test's own
	// commits -- made through blobDataDir, not that stable directory --
	// would advance the shared schema's commit timestamp out from under
	// them). A schema unique to this test run sidesteps the problem
	// entirely: nothing else ever touches it.
	schema := fmt.Sprintf("conformance_restart_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = dropPostgresSchema(dsn, schema)
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
// unscoped connection to dsn. Used to tear down a test-owned, uniquely
// named schema created via newDingoPostgresStateManagerAtSchema (see
// TestNewDingoPostgresStateManagerRestartSurvivesReopen) -- unlike the
// suite-shared postgresConformanceSchema, a per-test schema has no other
// caller relying on it surviving, so cleanup is a plain drop rather than
// truncatePostgresConformanceSchema's in-place empty.
func dropPostgresSchema(dsn, schema string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open postgres admin connection: %w", err)
	}
	defer db.Close()
	// schema is always Go-generated (a test-owned unique name), never
	// operator/DSN input, so string concatenation here carries no
	// injection risk -- same reasoning as ensurePostgresConformanceSchema.
	if _, err := db.Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
	); err != nil {
		return fmt.Errorf("drop postgres schema %q: %w", schema, err)
	}
	return nil
}
