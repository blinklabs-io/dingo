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

package database_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// newIndexerDB opens the ledger's database and returns an IndexerDB over its
// block store, the only database whose SQL store holds the rows a test of
// the SQL facade reads and writes.
func newIndexerDB(t *testing.T) *database.Database {
	t.Helper()
	ledger, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	db, err := dbtest.NewIndexerDatabase(t, ledger, dbtest.IndexerOptions{})
	require.NoError(t, err)
	return db
}
