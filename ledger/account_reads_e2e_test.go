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
// delegator credentials and how many reads it made that the set compares.
func (h *liveStakeHarness) accountReads(
	t *testing.T,
	drepKeys ...[]byte,
) ([]models.StakeCredentialRef, float64) {
	t.Helper()
	ldb := h.scenario.LedgerDB()
	pools := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(lcommon.NewBlake2b224(h.pool)),
	}
	wa, err := h.store.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	ga, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	require.Equal(t, wa, ga)
	wn, err := h.store.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	gn, err := ldb.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, wn, gn)
	wc, err := h.store.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	gc, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, wc, gc)
	for _, key := range drepKeys {
		wd, err := h.store.GetDRepDelegators(t.Context(), 0, key, nil)
		require.NoError(t, err)
		gd, err := ldb.GetDRepDelegators(t.Context(), 0, key, nil)
		require.NoError(t, err)
		require.Equal(t, wd, gd)
	}
	return gc[string(h.pool)], float64(3 + len(drepKeys))
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
	verified0, mismatches0 := h.scenario.AccountReads()
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	check := func(stage string, drepKeys ...[]byte) []models.StakeCredentialRef {
		t.Helper()
		creds, reads := h.accountReads(t, drepKeys...)
		v, m := h.scenario.AccountReads()
		require.Equal(
			t,
			mismatches0,
			m,
			"%s: the set differs from the store",
			stage,
		)
		require.Equal(
			t,
			reads,
			v-verified0,
			"%s: a read was not compared",
			stage,
		)
		verified0 = v
		return creds
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
	verified0, mismatches0 := h.scenario.AccountReads()
	script := bytes.Repeat([]byte{0x74}, 28)
	creds, count := h.scenario.RegisterCredentialInsideTxn(1, script, h.pool)
	require.Equal(t, uint64(3), count)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(1, script), h.owner, h.deleg,
	}, creds, "a script credential sorts before a key credential")
	verified, mismatches := h.scenario.AccountReads()
	require.Equal(t, float64(3), verified-verified0)
	require.Equal(
		t,
		mismatches0,
		mismatches,
		"the set lists the staged account",
	)
}

// TestAccountTableReadVerificationCountsADivergence requires a verify-mode
// read to count a mismatch when the metadata store holds an active account
// the set does not.
func TestAccountTableReadVerificationCountsADivergence(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, false)
	verified0, mismatches0 := h.scenario.AccountReads()
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	// Written around the ledger's LedgerDB, so the set never sees it.
	require.NoError(
		t,
		h.scenario.DB.CreateAccount(t.Context(), nil, &models.Account{
			StakingKey: bytes.Repeat([]byte{0x75}, 28),
			Pool:       h.pool,
			Drep:       drepKey,
			DrepType:   0,
			Active:     true,
		}),
	)
	creds, reads := h.accountReads(t, drepKey)
	require.Len(t, creds, 3, "the read answers from the metadata store")
	verified, mismatches := h.scenario.AccountReads()
	require.Equal(t, reads, verified-verified0)
	require.Equal(
		t,
		reads,
		mismatches-mismatches0,
		"the set has no such account",
	)
}
