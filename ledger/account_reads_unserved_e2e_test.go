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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// With accounts drained and no set serving them, the account lookups and
// lists fail as unserved rather than fall through to the emptied table, the
// empty-credential DRep delegator read included.
func TestAccountReadsUseLoadedSetAfterDrain(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, "set")
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pools := []lcommon.PoolKeyHash{lcommon.PoolKeyHash(h.pool)}
	ref := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x72}, 28))
	wantAccount, err := h.store.GetAccountByCredential(ctx, 0, ref.Key, true, nil)
	require.NoError(t, err)
	wantAccount.ID = 0
	wantAccounts, err := h.store.GetAccountsByCredential(ctx, []models.StakeCredentialRef{ref}, true, nil)
	require.NoError(t, err)
	for _, account := range wantAccounts {
		account.ID = 0
	}
	wantActive, err := h.store.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	wantCounts, err := h.store.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	wantCredentials, err := h.store.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	wantDelegators, err := h.store.GetDRepDelegators(ctx, 0, ref.Key, nil)
	require.NoError(t, err)
	h.scenario.DrainFamilies(nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts))
	reached, hide := h.scenario.HideStoreReads(
		"GetAccountByCredential", "GetAccountsByCredential", "GetActiveAccountCredentials",
		"GetActiveDelegatorCountsByPool", "GetActiveDelegatorCredentialsByPool", "GetDRepDelegators",
	)
	gotAccount, err := ldb.GetAccountByCredential(ctx, 0, ref.Key, true, nil)
	require.NoError(t, err)
	gotAccount.ID = 0
	require.Equal(t, wantAccount, gotAccount)
	gotAccounts, err := ldb.GetAccountsByCredential(ctx, []models.StakeCredentialRef{ref}, true, nil)
	require.NoError(t, err)
	for _, account := range gotAccounts {
		account.ID = 0
	}
	require.Equal(t, wantAccounts, gotAccounts)
	gotActive, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	require.ElementsMatch(t, wantActive, gotActive)
	gotCounts, err := ldb.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, wantCounts, gotCounts)
	gotCredentials, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, wantCredentials, gotCredentials)
	gotDelegators, err := ldb.GetDRepDelegators(ctx, 0, ref.Key, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, wantDelegators, gotDelegators)
	_, err = ldb.GetDRepDelegators(ctx, 2, nil, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved, "predefined DRep has no set key")
	hide(false)
	require.Zero(t, reached(), "account reads reached the metadata store")
}
