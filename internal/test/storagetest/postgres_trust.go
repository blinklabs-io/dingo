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

package storagetest

import (
	"context"
	"database/sql"
	"testing"
	"time"

	// Registers the "pgx" database/sql driver.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// SkipIfPostgresIgnoresPassword skips t when the server at dsn accepts the
// connection without authenticating it (pg_hba.conf "trust"), because a
// bad-credentials test cannot observe a rejection there. The official
// postgres image trusts loopback and unix-socket connections, so such a test
// would otherwise fail on a developer machine.
//
// It asks the server itself: system_user is NULL when no authentication took
// place. Any other outcome stays strict, so the test still runs and fails if
// a server wrongly accepts a bad password: a rejected connection (the
// expected case), and a server older than 16 where system_user does not
// exist.
func SkipIfPostgresIgnoresPassword(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return
	}
	defer db.Close()
	var unauthenticated bool
	if err := db.QueryRowContext(
		ctx, `SELECT system_user IS NULL`,
	).Scan(&unauthenticated); err != nil {
		return
	}
	if unauthenticated {
		t.Skip(
			"Skipping postgres bad-credentials test: the server does " +
				"not authenticate this connection (pg_hba.conf trust)",
		)
	}
}
