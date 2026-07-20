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
	"os"
	"testing"

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
	return os.Getenv("MYSQL_ROOT_PASSWORD") != "" || os.Getenv("MYSQL_DSN") != ""
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
