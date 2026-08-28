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
			}
		}
		if err := os.RemoveAll(postgresProcessBlobDir); err != nil {
			log.Printf(
				"conformance: remove postgres process blob dir %q: %v",
				postgresProcessBlobDir,
				err,
			)
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
			}
		}
		if err := os.RemoveAll(mysqlProcessBlobDir); err != nil {
			log.Printf(
				"conformance: remove mysql process blob dir %q: %v",
				mysqlProcessBlobDir,
				err,
			)
		}
	}

	os.Exit(code)
}
