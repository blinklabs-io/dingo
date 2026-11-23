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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The ledger's database marks a block store no release has opened as holding
// its ledger state in the LedgerDB, and refuses one v0.94.2 opened until the
// migration has marked it.
func TestLedgerDatabaseRefusesABlockStoreV0942Opened(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	state, err := ReadLedgerStoreState(db.Blob())
	require.NoError(t, err)
	require.Equal(t, LedgerStoreCurrent, state)
	_, err = db.blobStoreID()
	require.NoError(t, err)

	// What v0.94.2 leaves: an identity and no marker.
	require.NoError(t, db.DeleteChainState(LedgerMigratedKey, nil))
	require.NoError(t, closeTestDatabase(db))
	_, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.ErrorIs(t, err, ErrLedgerNotMigrated)

	released, err := newTestDatabaseAt(
		t, dir, dir, &Config{DataDir: dir, Role: RoleReleased},
	)
	require.NoError(t, err)
	state, err = ReadLedgerStoreState(released.Blob())
	require.NoError(t, err)
	require.Equal(t, LedgerStoreReleased, state)
	require.NoError(t, MarkLedgerMigrated(released.Blob()))
	require.NoError(t, closeTestDatabase(released))
	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))
}
