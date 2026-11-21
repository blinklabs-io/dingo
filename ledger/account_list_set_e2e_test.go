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
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

var accountListReadNames = []string{
	"GetActiveAccountCredentials", "GetActiveDelegatorCountsByPool",
	"GetActiveDelegatorCredentialsByPool", "GetDRepDelegators",
}

// requireAccountListReads reads every account list read through the ledger's
// LedgerDB and requires each answer to be the metadata store's.
func (h *liveStakeHarness) requireAccountListReads(
	t *testing.T,
	stage string,
	drepKeys ...[]byte,
) []models.StakeCredentialRef {
	t.Helper()
	ldb := h.scenario.LedgerDB()
	pools := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(lcommon.NewBlake2b224(h.pool)),
		lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{0xee}, 28)),
		),
	}
	sorted := func(refs []models.StakeCredentialRef) []models.StakeCredentialRef {
		out := slices.Clone(refs)
		slices.SortFunc(out, func(a, b models.StakeCredentialRef) int {
			return bytes.Compare([]byte(a.MapKey()), []byte(b.MapKey()))
		})
		return out
	}
	wa, err := h.store.GetActiveAccountCredentials(nil)
	require.NoError(t, err, stage)
	ga, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err, stage)
	require.Equal(t, sorted(wa), sorted(ga), stage)
	wn, err := h.store.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err, stage)
	gn, err := ldb.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err, stage)
	require.Equal(t, wn, gn, stage)
	wc, err := h.store.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err, stage)
	gc, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err, stage)
	require.Equal(t, wc, gc, stage)
	for _, key := range drepKeys {
		wd, err := h.store.GetDRepDelegators(t.Context(), 0, key, nil)
		require.NoError(t, err, stage)
		gd, err := ldb.GetDRepDelegators(t.Context(), 0, key, nil)
		require.NoError(t, err, stage)
		require.ElementsMatch(t, wd, gd, stage)
		require.Equal(t, len(wd), len(gd), stage)
	}
	return gc[string(h.pool)]
}

// TestAccountListReadsFromTheLiveUtxoSet reads the account list reads in set
// mode with the metadata store's failing, through a vote delegation, a pool
// reap and a rollback, and requires every answer to be the store's.
func TestAccountListReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, "set")
	reached, _ := h.scenario.HideStoreReads(accountListReadNames...)
	h.scenario.ApplyBoundary()
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	require.Equal(t, []models.StakeCredentialRef{h.owner, h.deleg},
		h.requireAccountListReads(t, "initial", drepKey))
	h.scenario.Reap(h.pool)
	require.Empty(t, h.requireAccountListReads(t, "after a pool reap", drepKey))
	h.scenario.VoteDelegate(h.owner, drepKey, 0, 8)
	h.requireAccountListReads(t, "after a vote delegation", drepKey)
	delegators, err := h.store.GetDRepDelegators(t.Context(), 0, drepKey, nil)
	require.NoError(t, err)
	require.Equal(t, []models.StakeCredentialRef{h.owner}, delegators)
	h.scenario.RollbackBeforeBoundary()
	require.Len(
		t,
		h.requireAccountListReads(t, "after the rollback", drepKey),
		2,
	)
	require.Zero(
		t,
		reached(),
		"an account list read reached the metadata store",
	)
}

// TestAccountListReadsSeeTheirTransactionsStagedAccountsInSetMode requires the
// delegator reads in a write transaction to list a script credential that
// transaction registered and delegated, first, with the store's reads failing.
func TestAccountListReadsSeeTheirTransactionsStagedAccountsInSetMode(
	t *testing.T,
) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, "set")
	reached, _ := h.scenario.HideStoreReads(accountListReadNames...)
	script := bytes.Repeat([]byte{0x74}, 28)
	creds, count := h.scenario.RegisterCredentialInsideTxn(1, script, h.pool)
	require.Equal(t, uint64(3), count)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(1, script), h.owner, h.deleg,
	}, creds, "a script credential sorts before a key credential")
	require.Zero(
		t,
		reached(),
		"an account list read reached the metadata store",
	)
}
