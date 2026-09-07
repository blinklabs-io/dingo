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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseAddColumnStatement covers the parser the per-dialect
// already-applied classifiers use to confirm a replayed expand statement.
// Quoting differs per dialect, and a statement that is not an ADD COLUMN must
// not be claimed, or an unrelated "already exists" failure would be swallowed.
func TestParseAddColumnStatement(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		statement  string
		table      string
		column     string
		definition string
		ok         bool
	}{
		{
			name:       "backtick quoted",
			statement:  "ALTER TABLE `committee_member` ADD COLUMN `term_start_slot_set` boolean NOT NULL DEFAULT false",
			table:      "committee_member",
			column:     "term_start_slot_set",
			definition: "boolean NOT NULL DEFAULT false",
			ok:         true,
		},
		{
			name:       "double quoted",
			statement:  `ALTER TABLE "committee_member" ADD COLUMN "cold_credential_tag" BIGINT NOT NULL DEFAULT 0`,
			table:      "committee_member",
			column:     "cold_credential_tag",
			definition: "BIGINT NOT NULL DEFAULT 0",
			ok:         true,
		},
		{
			name:       "unquoted with trailing semicolon",
			statement:  "ALTER TABLE account_import_baseline ADD COLUMN deposit_amount text;",
			table:      "account_import_baseline",
			column:     "deposit_amount",
			definition: "text",
			ok:         true,
		},
		{
			name:       "lowercase keywords",
			statement:  "alter table `t` add column `c` integer",
			table:      "t",
			column:     "c",
			definition: "integer",
			ok:         true,
		},
		{
			// A same-named index is a different object; claiming it would let
			// a genuine duplicate-index failure be treated as applied.
			name:      "create index is not an add column",
			statement: "CREATE INDEX `idx_a` ON `t`(`c`)",
			ok:        false,
		},
		{
			name:      "drop column is not an add column",
			statement: "ALTER TABLE `t` DROP COLUMN `c`",
			ok:        false,
		},
		{
			name:      "truncated add column",
			statement: "ALTER TABLE `t` ADD COLUMN",
			ok:        false,
		},
		{
			name:      "empty statement",
			statement: "",
			ok:        false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table, column, definition, ok := parseAddColumnStatement(test.statement)
			require.Equal(t, test.ok, ok)
			if !test.ok {
				return
			}
			require.Equal(t, test.table, table)
			require.Equal(t, test.column, column)
			require.Equal(t, test.definition, definition)
		})
	}
}
