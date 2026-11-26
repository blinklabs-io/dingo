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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// A set loaded from the previous account format migrates the SQL activation
// relation into its account rows, then retains it across a restart.
func TestLoadLedgerStateMigratesAccountInactivityActivation(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	for _, e := range []struct{ start, id uint64 }{{0, 0}, {10, 1}} {
		require.NoError(t, db.SetEpoch(
			e.start, e.id, nil, nil, nil, nil, 6, 1000, 10, nil,
		))
	}
	key := bytes.Repeat([]byte{0x71}, 28)
	require.NoError(t, db.CreateAccount(t.Context(), nil, &models.Account{
		StakingKey: key, Active: true, AddedSlot: 5, CreatedSlot: 5,
	}))
	stamped, err := db.StampAllActiveAccountExpirations(t.Context(), 3, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), stamped)
	require.NoError(t, db.SetSyncState(
		database.DelegatorInactivityActivatedSyncKey, "1", nil,
	))

	dir := t.TempDir()
	set, err := utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	a, err := set.Account(0, key)
	require.NoError(t, err)
	require.True(t, a.InactivityActivation)
	require.Equal(t, uint64(1), a.InactivityActivationEpoch)
	require.NoError(t, set.Close())

	set, err = utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	a, err = set.Account(0, key)
	require.NoError(t, err)
	require.True(t, a.InactivityActivation)
	require.Equal(t, uint64(1), a.InactivityActivationEpoch)
}
