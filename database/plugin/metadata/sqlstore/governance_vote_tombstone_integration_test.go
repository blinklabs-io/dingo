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

// The vote deletion journal compares and writes a boolean column; these run
// the SQLite scenarios against the server dialects.

func TestPostgresGovernanceVoteTombstones(t *testing.T) {
	for name, scenario := range governanceVoteTombstoneScenarios {
		t.Run(name, func(t *testing.T) {
			dsn, schema := newPostgresIntegrationSchema(t)
			scenario(
				t,
				newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
			)
		})
	}
}

func TestMySQLGovernanceVoteTombstones(t *testing.T) {
	for name, scenario := range governanceVoteTombstoneScenarios {
		t.Run(name, func(t *testing.T) {
			dsn, database := newMySQLIntegrationDatabase(t)
			scenario(
				t,
				newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
			)
		})
	}
}

var governanceVoteTombstoneScenarios = map[string]func(*testing.T, *Store){
	"AtSlot":   testVotesAtSlotOmitsVoteDeletedBeforeRecast,
	"Rollback": testVoteRollbackRestoresDeletionAcrossRecast,
	"Page":     testVotesPageDeletionBelongsToDeletedCast,
}
