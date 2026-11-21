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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// accountReads makes every account table read through the ledger's LedgerDB
// and requires each answer to be the metadata store's. It returns the pool's
// delegator credentials.
func (h *liveStakeHarness) accountReads(
	t *testing.T,
	drepKeys ...[]byte,
) []models.StakeCredentialRef {
	t.Helper()
	ldb := h.scenario.LedgerDB()
	pools := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(lcommon.NewBlake2b224(h.pool)),
	}
	wa, err := h.store.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	ga, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	require.ElementsMatch(t, wa, ga)
	wn, err := h.store.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	gn, err := ldb.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, wn, gn)
	wc, err := h.store.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	gc, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	require.Len(t, gc, len(wc))
	for pool, want := range wc {
		got, ok := gc[pool]
		require.True(t, ok, "pool %x is missing", pool)
		require.ElementsMatch(t, want, got, "pool %x", pool)
	}
	for _, key := range drepKeys {
		wd, err := h.store.GetDRepDelegators(t.Context(), 0, key, nil)
		require.NoError(t, err)
		gd, err := ldb.GetDRepDelegators(t.Context(), 0, key, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, wd, gd)
	}
	return gc[string(h.pool)]
}

// TestAccountTableReadsAgreeWithTheLiveUtxoSet requires the set's accounts
// to give the metadata store's active credentials, delegator counts and
// credentials per pool, and DRep delegators, through a vote delegation, a
// pool reap and a rollback.
func TestAccountTableReadsAgreeWithTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, false)
	// The rollback undoes only what is written after the boundary.
	h.scenario.ApplyBoundary()
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	check := func(stage string, drepKeys ...[]byte) []models.StakeCredentialRef {
		t.Helper()
		return h.accountReads(t, drepKeys...)
	}
	creds := check("initial", drepKey)
	require.Equal(t, []models.StakeCredentialRef{h.owner, h.deleg}, creds)
	delegators, err := h.store.GetDRepDelegators(t.Context(), 0, drepKey, nil)
	require.NoError(t, err)
	require.Empty(t, delegators)

	h.scenario.Reap(h.pool)
	require.Empty(t, check("after a pool reap", drepKey))

	// Writes come in slot order, after the reap's.
	h.scenario.VoteDelegate(h.owner, drepKey, 0, 8)
	check("after a vote delegation", drepKey)
	delegators, err = h.store.GetDRepDelegators(t.Context(), 0, drepKey, nil)
	require.NoError(t, err)
	require.Equal(t, []models.StakeCredentialRef{h.owner}, delegators)

	h.scenario.RollbackBeforeBoundary()
	require.Len(t, check("after the rollback", drepKey), 2,
		"the rollback restores the delegations")
	delegators, err = h.store.GetDRepDelegators(t.Context(), 0, drepKey, nil)
	require.NoError(t, err)
	require.Empty(t, delegators, "the rollback undoes the vote delegation")
}

// TestAccountTableReadsSeeTheirTransactionsStagedAccounts requires the
// delegator reads in a write transaction to list a script credential that
// transaction registered and delegated, first, as the store does.
func TestAccountTableReadsSeeTheirTransactionsStagedAccounts(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, false)
	script := bytes.Repeat([]byte{0x74}, 28)
	creds, count := h.scenario.RegisterCredentialInsideTxn(1, script, h.pool)
	require.Equal(t, uint64(3), count)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(1, script), h.owner, h.deleg,
	}, creds, "a script credential sorts before a key credential")
}
