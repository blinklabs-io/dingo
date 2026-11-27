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

var expiredContentScenarios = []struct {
	name string
	run  func(*testing.T, *expiringChain)
}{
	{"datum re-pointed by a later carrier", testExpiredDatumRowIsRepointedByALaterCarrier},
	{"script re-pointed by a later reference script", testExpiredScriptRowIsRepointedByALaterReferenceScript},
	{"datum moves to a retained carrier", testDatumRowMovesToARetainedCarrierWhenItsBlockExpires},
	{"script moves to a retained carrier", testScriptRowMovesToARetainedCarrierWhenItsBlockExpires},
	{"readable row keeps its first carrier", testReadableContentRowKeepsItsFirstCarrier},
	{"rollback keeps a surviving carrier", testRollbackPastARepointedRowKeepsItsSurvivingCarrier},
}

func TestPostgresExpiredContentRows(t *testing.T) {
	for _, scenario := range expiredContentScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			dsn, schema := newPostgresIntegrationSchema(t)
			scenario.run(t, newExpiringChainOver(
				t, newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
			))
		})
	}
}

func TestMySQLExpiredContentRows(t *testing.T) {
	for _, scenario := range expiredContentScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			dsn, database := newMySQLIntegrationDatabase(t)
			scenario.run(t, newExpiringChainOver(
				t, newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
			))
		})
	}
}
