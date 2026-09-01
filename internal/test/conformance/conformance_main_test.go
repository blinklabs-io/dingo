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
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMain drops this process's Postgres schema and MySQL database, and
// removes their paired local blob directories, once after every test in
// this process has finished -- see postgresProcessSchema's doc comment in
// state_manager_postgres.go and mysqlProcessDatabase's doc comment in
// state_manager_mysql.go for why those are shared across every
// NewDingoPostgresStateManager/NewDingoMysqlStateManager call in this
// process (so cleanup belongs here, once, rather than in an individual
// manager's Close).
//
// A non-empty postgresProcessBlobDir/mysqlProcessBlobDir is this process's
// own signal that a manager actually used that backend: neither is set
// until ensurePostgresProcessBlobDir/ensureMysqlProcessBlobDir runs, which
// only happens from inside NewDingoPostgresStateManager/
// NewDingoMysqlStateManager. A `go test` invocation that never configured
// or exercised one of the two backends leaves that backend's directory
// empty and skips cleanup for it, rather than connecting to a DSN nothing
// in this run ever validated.
func TestMain(m *testing.M) {
	code := m.Run()

	// cleanupFailed tracks whether any step below failed, without letting a
	// failure skip the steps after it: every cleanup path below always
	// runs, and only the final exit code reflects a failure, so one failed
	// drop/removal never leaves a sibling resource (e.g. the blob directory
	// paired with a schema that failed to drop) uncleaned as a side effect
	// of returning early.
	cleanupFailed := false

	if err := cleanupCorpusTestdata(); err != nil {
		log.Printf("conformance: remove shared vector extraction: %v", err)
		cleanupFailed = true
	}

	if postgresProcessBlobDir != "" {
		if isPostgresConformanceConfigured() {
			if err := dropPostgresSchema(
				postgresConformanceDSN(),
				postgresProcessSchema,
			); err != nil {
				log.Printf(
					"conformance: cleanup postgres process schema %q: %v",
					postgresProcessSchema,
					err,
				)
				cleanupFailed = true
			}
		}
		if err := os.RemoveAll(postgresProcessBlobDir); err != nil {
			log.Printf(
				"conformance: remove postgres process blob dir %q: %v",
				postgresProcessBlobDir,
				err,
			)
			cleanupFailed = true
		}
	}

	if mysqlProcessBlobDir != "" {
		if isMysqlConformanceConfigured() {
			if err := dropMysqlDatabase(
				mysqlConformanceRootDSN(),
				mysqlProcessDatabase,
			); err != nil {
				log.Printf(
					"conformance: cleanup mysql process database %q: %v",
					mysqlProcessDatabase,
					err,
				)
				cleanupFailed = true
			}
		}
		if err := os.RemoveAll(mysqlProcessBlobDir); err != nil {
			log.Printf(
				"conformance: remove mysql process blob dir %q: %v",
				mysqlProcessBlobDir,
				err,
			)
			cleanupFailed = true
		}
	}

	os.Exit(processCleanupExitCode(code, cleanupFailed))
}

// processCleanupExitCode folds a process-cleanup failure into the test run's
// own exit code. A cleanup failure must fail the run even when every test
// passed -- otherwise the leaked schema/database/directory this exists to
// catch is invisible to anything that only checks the exit code (CI, a
// local `go test && echo ok`). A real test failure's exit code is never
// downgraded, only upgraded from 0.
func processCleanupExitCode(testExitCode int, cleanupFailed bool) int {
	if cleanupFailed && testExitCode == 0 {
		return 1
	}
	return testExitCode
}

// TestProcessCleanupExitCodeFailsOnCleanupFailure proves a process-cleanup
// failure (a schema/database drop or blob directory removal error) makes
// TestMain report a nonzero exit code even when every test in the process
// passed -- a reviewer's forced RemoveAll permission failure otherwise
// logged "permission denied" but left `go test` exiting 0, silently
// leaking the per-run schema/database/directory this cleanup exists to
// remove.
func TestProcessCleanupExitCodeFailsOnCleanupFailure(t *testing.T) {
	require.Equal(t, 0, processCleanupExitCode(0, false))
	require.Equal(t, 1, processCleanupExitCode(0, true))
	require.Equal(
		t,
		2,
		processCleanupExitCode(2, true),
		"a genuine test failure's exit code must never be downgraded",
	)
	require.Equal(t, 2, processCleanupExitCode(2, false))
}
