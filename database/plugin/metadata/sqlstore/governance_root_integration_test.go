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

// The root lookup's correlated NOT EXISTS subquery references the outer
// governance_proposal row by table name, and the submission order depends on
// NULL ordering and the companion-table upsert; run both on each real
// backend.
func TestPostgresLastEnactedGovernanceRootIntegration(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	exerciseLastEnactedGovernanceRoot(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLLastEnactedGovernanceRootIntegration(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	exerciseLastEnactedGovernanceRoot(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

func TestPostgresGovernanceProposalSubmissionOrderIntegration(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	exerciseGovernanceProposalSubmissionOrder(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLGovernanceProposalSubmissionOrderIntegration(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	exerciseGovernanceProposalSubmissionOrder(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}
