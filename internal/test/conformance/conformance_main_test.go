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
	"errors"
	"fmt"
	"os"
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
