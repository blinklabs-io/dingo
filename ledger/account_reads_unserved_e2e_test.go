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

package ledger_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The account lookups and lists answer from the LedgerDB set with the
// metadata store's reads hidden, and a predefined DRep, which has no set key,
// is refused as unserved rather than read from the emptied table.
func TestAccountReadsUseLoadedSetAfterDrain(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, "set")
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pools := []lcommon.PoolKeyHash{lcommon.PoolKeyHash(h.pool)}
	ref := h.deleg
	reached, hide := h.scenario.HideStoreReads(
		"GetAccountByCredential", "GetAccountsByCredential", "GetActiveAccountCredentials",
		"GetActiveDelegatorCountsByPool", "GetActiveDelegatorCredentialsByPool", "GetDRepDelegators",
	)
	account, err := ldb.GetAccountByCredential(ctx, 0, ref.Key, true, nil)
	require.NoError(t, err)
	require.Equal(t, ref.Key, account.StakingKey)
	require.Equal(t, h.pool, account.Pool)
	require.True(t, account.Active)
	accounts, err := ldb.GetAccountsByCredential(ctx, []models.StakeCredentialRef{ref}, true, nil)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	for _, got := range accounts {
		require.Equal(t, account, got)
	}
	active, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []models.StakeCredentialRef{h.owner, h.deleg}, active)
	counts, err := ldb.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{string(h.pool): 2}, counts)
	credentials, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]models.StakeCredentialRef{
		string(h.pool): {h.owner, h.deleg},
	}, credentials)
	delegators, err := ldb.GetDRepDelegators(ctx, 0, ref.Key, nil)
	require.NoError(t, err)
	require.Empty(t, delegators)
	_, err = ldb.GetDRepDelegators(ctx, 2, nil, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved, "predefined DRep has no set key")
	hide(false)
	require.Zero(t, reached(), "account reads reached the metadata store")
}
