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
	"fmt"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A credit to an account that is not registered reports
// models.ErrAccountNotFound whether or not the accounts family is drained:
// POOLREAP, governance refunds and MIR route the amount to the treasury on
// that answer.
func TestDrainedCreditToUnregisteredAccountReportsNotFound(t *testing.T) {
	t.Parallel()
	missing := bytes.Repeat([]byte{0x5e}, 28)
	families := map[string][]nodesettings.Family{
		"undrained": nil,
		"drained":   {nodesettings.FamilyAccounts},
	}
	for name, drain := range families {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newP5Harness(t, drain, nil)
			require.NoError(t, h.drainErr)
			ldb := h.scenario.LedgerDB()
			for _, c := range []struct {
				kind   string
				credit func() error
			}{
				{"single", func() error {
					txn := ldb.Begin(t.Context(), true)
					defer txn.Release()
					return ldb.AddAccountRewardByCredential(
						t.Context(), 0, missing, 500, 100, []byte{0x01}, txn,
					)
				}},
				{"post snapshot", func() error {
					txn := ldb.Begin(t.Context(), true)
					defer txn.Release()
					return ldb.AddPostSnapshotAccountRewardByCredential(
						t.Context(), 0, missing, 500, 100, []byte{0x02}, txn,
					)
				}},
				{"batch", func() error {
					txn := ldb.Begin(t.Context(), true)
					defer txn.Release()
					return ldb.AddAccountRewardsByCredential(
						t.Context(), []models.AccountRewardCredit{{
							StakingKey: missing, Amount: 500, Slot: 100,
							SourceHash: []byte{0x03},
						}}, txn,
					)
				}},
			} {
				require.ErrorIs(
					t, c.credit(), models.ErrAccountNotFound, "%s", c.kind,
				)
			}
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.AddAccountRewardByCredential(
				t.Context(), 0, missing, 0, 100, []byte{0x04}, txn,
			), "a zero credit is not applied and finds no account")
			require.False(
				t, h.scenario.SetFailed(),
				"a credit the caller handles must not stop the set",
			)
		})
	}
}

// A pool retires whose reward account was deregistered: the deposit goes to
// the treasury, the account is not credited and the set keeps following the
// ledger, with accounts (which drain with dreps) drained and with every
// graduated family drained too, as in an undrained run.
func TestPoolReapOfDeregisteredRewardAccountMatchesAnUndrainedRun(
	t *testing.T,
) {
	t.Parallel()
	reap := func(h *p5Harness) ledger.ReapOutcome {
		require.NoError(t, h.drainErr)
		return h.scenario.ReapRetiredPool()
	}
	want := reap(newP5Harness(t, nil, nil))
	require.True(t, want.Registered, "the reward account is registered first")
	require.NotZero(t, want.Refund.DepositHeld)
	require.False(t, want.SetFailed)
	require.Zero(t, want.Reward)
	require.Contains(t, want.Network[0], fmt.Sprintf(
		" %d 5000", 1_000+uint64(want.Refund.DepositHeld),
	), "the deposit is added to the treasury")

	t.Run("accounts and dreps drained", func(t *testing.T) {
		t.Parallel()
		got := reap(newP5Harness(
			t, []nodesettings.Family{
				nodesettings.FamilyAccounts, nodesettings.FamilyDReps,
			}, nil,
		))
		require.Equal(t, want, got)
	})
	t.Run("accounts, dreps and every graduated family drained", func(t *testing.T) {
		t.Parallel()
		h := newP5Harness(t, nil, nil)
		h.scenario.ConfigureLeiosValidator()
		fixture, err := h.store.GetBlockNoncesInSlotRange(0, math.MaxInt64, nil)
		require.NoError(t, err)
		h.scenario.StageNoncesInSet(fixture)
		h.scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
		h.scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
		h.scenario.InstallDrainedSources()
		src := drainTestSource{set: h.set, db: h.scenario.DB}
		entries := statedrain.Graduated(src)
		for _, e := range statedrain.Staged(src) {
			if e.Family == nodesettings.FamilyAccounts ||
				e.Family == nodesettings.FamilyDReps {
				entries = append(entries, e)
			}
		}
		h.drainErr = statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{h.scenario.DB, h.scenario.DrainFamilies},
			Set:   src, Guard: h.guard, VerifyBeforeDrain: true,
		}, entries)
		require.Equal(t, want, reap(h))
	})
}
