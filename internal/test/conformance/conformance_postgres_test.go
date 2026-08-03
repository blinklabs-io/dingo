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
	"fmt"
	"os"
	"testing"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// isPostgresConformanceConfigured checks whether postgres connection info
// has been supplied via environment variables. Mirrors
// database/plugin/metadata/postgres's isPostgresConfigured so both suites
// skip/run under the same conditions in CI and locally.
func isPostgresConformanceConfigured() bool {
	return os.Getenv("POSTGRES_PASSWORD") != "" || os.Getenv("POSTGRES_DSN") != ""
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
		host, port, user, os.Getenv("POSTGRES_PASSWORD"), database, sslMode,
	)
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
