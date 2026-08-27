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
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

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

// TestRulesConformanceVectorsMysql is the strict pass/fail gate for the
// MySQL-backed DingoStateManager: it fails immediately on the first vector
// mismatch (via harness.RunAllVectors), mirroring TestRulesConformanceVectors
// in conformance_test.go.
func TestRulesConformanceVectorsMysql(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	testdataRoot, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err, "failed to extract embedded testdata")

	sm := newTestMysqlConformanceManager(t)
	defer sm.Close()

	harness := conformance.NewHarness(sm, conformance.HarnessConfig{
		TestdataRoot: testdataRoot,
		Debug:        testing.Verbose(),
	})

	harness.RunAllVectors(t)
}

// TestRulesConformanceVectorsWithResultsMysql runs the harness against both
// the SQLite-backed and MySQL-backed state managers in the same test and
// compares them, rather than asserting a hardcoded vector count: the two
// runs should exercise the identical number of vectors with identical pass
// counts, and the comparison stays correct even as the embedded
// ouroboros-mock vector corpus grows or shrinks.
func TestRulesConformanceVectorsWithResultsMysql(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

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

	mysqlRoot, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err, "failed to extract embedded testdata")

	mysqlSm := newTestMysqlConformanceManager(t)
	defer mysqlSm.Close()

	mysqlHarness := conformance.NewHarness(mysqlSm, conformance.HarnessConfig{
		TestdataRoot: mysqlRoot,
	})
	mysqlResults, err := mysqlHarness.RunAllVectorsWithResults()
	require.NoError(t, err, "failed to run mysql vectors")

	var mysqlPassed, mysqlFailed int
	for _, result := range mysqlResults {
		if result.Success {
			mysqlPassed++
		} else {
			mysqlFailed++
		}
	}

	t.Logf("Conformance Test Results (MySQL):")
	t.Logf("  Total vectors: %d", len(mysqlResults))
	t.Logf("  Passed: %d", mysqlPassed)
	t.Logf("  Failed: %d", mysqlFailed)
	if len(mysqlResults) > 0 {
		t.Logf(
			"  Pass rate: %.1f%%",
			float64(mysqlPassed)/float64(len(mysqlResults))*100,
		)
	}
	if mysqlFailed > 0 && testing.Verbose() {
		t.Log("First failures:")
		failCount := 0
		for _, result := range mysqlResults {
			if !result.Success && failCount < 5 {
				t.Logf("  %s: %v", result.Title, result.Error)
				failCount++
			}
		}
		if mysqlFailed > 5 {
			t.Logf("  ... and %d more failures", mysqlFailed-5)
		}
	}

	require.Equal(
		t,
		len(sqliteResults),
		len(mysqlResults),
		"mysql backend exercised a different number of vectors than "+
			"sqlite; vector discovery/extraction should be backend-invariant",
	)
	require.Zero(t, mysqlFailed, "mysql backend failed vectors sqlite passed")
}

// TestNewDingoMysqlStateManagerRestartSurvivesReopen proves state committed
// through a real MySQL-backed DingoStateManager survives closing that
// manager and opening a new one against the same root DSN/database -- the
// MySQL analog of TestDingoStateManagerRestartSurvivesReopen
// (state_manager_backend_test.go). Unlike the sqlite case there is no
// local file to reopen: the state lives on the MySQL server itself, so
// "restart" here means a fresh manager instance pointed at the same
// database.
func TestNewDingoMysqlStateManagerRestartSurvivesReopen(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	// Both manager instances share one local blob directory: the local
	// Badger blob store and the remote MySQL metadata store are paired at
	// construction (see newDingoMysqlStateManagerAt's doc comment), so m2
	// must reuse m1's blob directory to reopen against the same
	// already-populated metadata store without tripping that pairing
	// check.
	blobDataDir := t.TempDir()
	rootDSN := mysqlConformanceRootDSN()

	// blobDataDir is always a fresh, empty t.TempDir(), so this test needs
	// an equally fresh, empty metadata side to pair with it -- reusing
	// mysqlConformanceDatabase (the database every other test in this suite
	// shares) would trip database.New's commit-timestamp consistency check
	// against whatever those other tests have already committed there, and
	// truncating that shared database here would just move the same
	// mismatch onto the *other* tests instead (they pair the stable,
	// suite-shared mysqlConformanceBlobDir with that database, and this
	// test's own commits -- made through blobDataDir, not that stable
	// directory -- would advance the shared database's commit timestamp
	// out from under them). A database unique to this test run sidesteps
	// the problem entirely: nothing else ever touches it.
	database := fmt.Sprintf("conformance_restart_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = dropMysqlDatabase(rootDSN, database)
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
// reasoning). Used to tear down a test-owned, uniquely named database
// created via newDingoMysqlStateManagerAtDatabase (see
// TestNewDingoMysqlStateManagerRestartSurvivesReopen) -- unlike the
// suite-shared mysqlConformanceDatabase, a per-test database has no other
// caller relying on it surviving, so cleanup is a plain drop rather than
// truncateMysqlConformanceDatabase's in-place empty.
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
