//go:build dingo_db_integration

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

package sqlstore

import "testing"

func TestPostgresStoredHashLengths(t *testing.T) {
	newStore := func(t *testing.T) *Store {
		t.Helper()
		dsn, schema := newPostgresIntegrationSchema(t)
		return newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema)
	}
	t.Run("pool registrations", func(t *testing.T) {
		testGetPoolRegistrationsRejectsMalformedStoredHashes(t, newStore)
	})
	t.Run("stake registrations", func(t *testing.T) {
		testGetStakeRegistrationsByCredentialRejectsMalformedStoredKey(
			t,
			newStore(t),
		)
	})
}

func TestMySQLStoredHashLengths(t *testing.T) {
	newStore := func(t *testing.T) *Store {
		t.Helper()
		dsn, database := newMySQLIntegrationDatabase(t)
		return newIntegrationSQLStore(t, "mysql", dsn, "mysql", database)
	}
	t.Run("pool registrations", func(t *testing.T) {
		testGetPoolRegistrationsRejectsMalformedStoredHashes(t, newStore)
	})
	t.Run("stake registrations", func(t *testing.T) {
		testGetStakeRegistrationsByCredentialRejectsMalformedStoredKey(
			t,
			newStore(t),
		)
	})
}
