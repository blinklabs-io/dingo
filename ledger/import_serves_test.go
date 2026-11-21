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

package ledger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledgerstate"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestNodeStartedFromAnImportServesTheImportedState imports a devnet
// snapshot, empties the metadata store's ledger-state tables, and opens the
// LedgerDB set as a node's start does. The node must serve the snapshot's
// accounts, pools, treasury and tip nonce from the LedgerDB alone.
func TestNodeStartedFromAnImportServesTheImportedState(t *testing.T) {
	t.Parallel()
	state, err := ledgerstate.ParseSnapshot(
		"../ledgerstate/testdata/devnet-ledger-snapshot-epoch4.cbor",
	)
	require.NoError(t, err)
	certState, err := ledgerstate.ParseCertState(state.CertStateData)
	require.NoError(t, err)
	require.NotEmpty(t, certState.Accounts)
	require.NotEmpty(t, certState.Pools)
	state.UTxOHD = false
	state.UTxOData = []byte{0xa0}

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	set := OpenImportSetForTest(t)
	loader, ok := set.(ledgerstate.UtxoSetLoader)
	require.True(t, ok)
	require.NoError(t, ledgerstate.ImportLedgerState(
		t.Context(),
		ledgerstate.ImportConfig{
			Database: db,
			State:    state,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			EpochLength: func(uint) (uint, uint, error) {
				return 1000, 500, nil
			},
			UtxoSet:  loader,
			LedgerDB: NewImportLedgerDB(db, set, nil),
		},
	))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	for _, f := range nodesettings.Families() {
		for _, table := range statedrain.TableNames(f) {
			_, err := raw.Exec("DELETE FROM " + table)
			require.NoError(t, err)
		}
	}
	AdoptLiveSetForTest(t, db, set)

	ls := &LedgerState{db: db}
	fxAttachLedgerDB(t, ls)
	ldb := ls.ledgerDB()
	for _, a := range certState.Accounts {
		tag, err := models.CredentialTagFromUint(uint(a.StakingKey.Type))
		require.NoError(t, err)
		account, err := ldb.GetAccountByCredential(
			t.Context(), tag, a.StakingKey.Hash, true, nil,
		)
		require.NoError(t, err, "account %x", a.StakingKey.Hash)
		require.Equal(t, a.Reward, uint64(account.Reward))
		require.Equal(t, a.PoolKeyHash, []byte(account.Pool))
	}
	for _, p := range certState.Pools {
		pool, err := ldb.GetPool(
			t.Context(), lcommon.PoolKeyHash(p.PoolKeyHash), false, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, pool, "pool %x", p.PoolKeyHash)
		require.Equal(t, p.VrfKeyHash, []byte(pool.VrfKeyHash))
	}
	requireTreasuryValue(t, ls, nil, state.Treasury)
	nonce, ok, err := ldb.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, state.Tip.Slot, nonce.Slot)
	require.Equal(t, state.EvolvingNonce, nonce.Nonce)
}
