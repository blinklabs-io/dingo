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

// accountListReads is what the account list reads answer through the ledger's
// LedgerDB.
type accountListReads struct {
	active     []models.StakeCredentialRef
	counts     map[string]uint64
	delegators []models.StakeCredentialRef
	drep       []models.StakeCredentialRef
}

// readAccountLists makes every account list read through the ledger's
// LedgerDB, for the harness pool and the delegators of drepKey.
func (h *liveStakeHarness) readAccountLists(
	t *testing.T,
	stage string,
	drepKey []byte,
) accountListReads {
	t.Helper()
	ldb := h.scenario.LedgerDB()
	pools := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(lcommon.NewBlake2b224(h.pool)),
	}
	sorted := func(refs []models.StakeCredentialRef) []models.StakeCredentialRef {
		out := slices.Clone(refs)
		slices.SortFunc(out, func(a, b models.StakeCredentialRef) int {
			return bytes.Compare([]byte(a.MapKey()), []byte(b.MapKey()))
		})
		return out
	}
	var (
		got accountListReads
		err error
	)
	got.active, err = ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err, stage)
	got.active = sorted(got.active)
	got.counts, err = ldb.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err, stage)
	byPool, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err, stage)
	got.delegators = byPool[string(h.pool)]
	got.drep, err = ldb.GetDRepDelegators(t.Context(), 0, drepKey, nil)
	require.NoError(t, err, stage)
	return got
}

// requireAccountListReads makes every account list read through the ledger's
// LedgerDB, for each of drepKeys, and returns the harness pool's delegators.
func (h *liveStakeHarness) requireAccountListReads(
	t *testing.T,
	stage string,
	drepKeys ...[]byte,
) []models.StakeCredentialRef {
	t.Helper()
	var delegators []models.StakeCredentialRef
	for _, key := range drepKeys {
		delegators = h.readAccountLists(t, stage, key).delegators
	}
	return delegators
}

// TestAccountListReadsFromTheLiveUtxoSet reads the account lists from the
// LedgerDB set through a pool reap, a vote delegation and a rollback, with
// the metadata store's reads hidden.
func TestAccountListReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, "set")
	reached, _ := h.scenario.HideStoreReads(accountListReadNames...)
	h.scenario.ApplyBoundary()
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	both := []models.StakeCredentialRef{h.owner, h.deleg}

	got := h.readAccountLists(t, "initial", drepKey)
	require.Equal(t, both, got.active)
	require.Equal(t, map[string]uint64{string(h.pool): 2}, got.counts)
	require.Equal(t, both, got.delegators)
	require.Empty(t, got.drep)

	h.scenario.Reap(h.pool)
	got = h.readAccountLists(t, "after a pool reap", drepKey)
	require.Equal(t, both, got.active, "a reap leaves the accounts registered")
	require.Zero(t, got.counts[string(h.pool)])
	require.Empty(t, got.delegators)
	require.Empty(t, got.drep)

	h.scenario.VoteDelegate(h.owner, drepKey, 0, 8)
	got = h.readAccountLists(t, "after a vote delegation", drepKey)
	require.Equal(t, []models.StakeCredentialRef{h.owner}, got.drep)
	require.Empty(t, got.delegators)

	h.scenario.RollbackBeforeBoundary()
	got = h.readAccountLists(t, "after the rollback", drepKey)
	require.Equal(t, both, got.active)
	require.Equal(t, map[string]uint64{string(h.pool): 2}, got.counts)
	require.Equal(t, both, got.delegators, "the rollback restores the delegations")
	require.Empty(t, got.drep, "the rollback undoes the vote delegation")
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
