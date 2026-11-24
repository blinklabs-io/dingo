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

package dbtest

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
)

// NewReleasedDatabase composes a database opened as the source of the
// v0.94.2 migration: Badger and SQLite, under database.RoleReleased, whose
// metadata store holds the ledger state a test seeds before moving it into a
// live UTxO set.
func NewReleasedDatabase(
	tb testing.TB,
	config *database.Config,
) (*database.Database, error) {
	tb.Helper()
	// alonzopparams:word-not-required: the harness creates the database it
	// opens, so it never meets a legacy Alonzo row.
	cfg := database.Config{}
	if config != nil {
		cfg = *config
	}
	cfg.Role = database.RoleReleased
	return NewDatabaseWithOptions(tb, Options{Config: &cfg})
}
