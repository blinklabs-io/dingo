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
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// setHarness is a boundary scenario in set read mode.
type setHarness struct {
	scenario ledger.BoundaryEpochStateScenario
	set      *utxoset.Set
	// store is the v0.94.2 metadata store's LedgerDB the set loads from.
	store ledgerdb.LedgerDB
	// refs and pool are the harness's accounts and the pool the first one
	// delegates to.
	refs []models.StakeCredentialRef
	pool []byte
}

// newSetHarness opens the scenario and loads the set from its v0.94.2
// metadata store.
func newSetHarness(t *testing.T) *setHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &setHarness{
		set: set,
		scenario: ledger.NewGuardedBoundaryEpochStateScenarioForTest(
			t, set, nil,
		),
	}
	h.store = dbadapter.New(h.scenario.DB)
	require.NoError(t, set.Accounts(
		func(a *models.Account) error {
			h.refs = append(h.refs,
				models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey))
			if h.pool == nil {
				h.pool = bytes.Clone(a.Pool)
			}
			return nil
		}))
	require.NoError(t, set.LoadLedgerState(t.Context(), h.store))
	return h
}

// reads is the LedgerDB the scenario's reads are rendered from.
func (h *setHarness) reads() ledgerdb.LedgerDB {
	return h.scenario.LedgerDB()
}

// With the set failed, the LedgerDB fails the read rather than answer
// nothing.
func TestMIRGenesisReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// A failed set serves no read.
	scenario.FailSet()
	ldb := scenario.LedgerDB()
	_, err = ldb.GetGenesisDelegationForSlot(
		bytes.Repeat([]byte{1}, 28), math.MaxInt64, genesisMIRWindow, nil,
	)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetGenesisDelegationsInSlotRange(0, math.MaxInt64, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
}

var setDRepKeys = [][]byte{
	bytes.Repeat([]byte{0x5d}, 28), bytes.Repeat([]byte{0x5e}, 28),
}

// setTouched is what a drive wrote beyond the accounts the harness holds.
type setTouched struct {
	refs  []models.StakeCredentialRef
	dreps [][]byte
}

func accountText(a *models.Account) string {
	if a == nil {
		return "nil"
	}
	return fmt.Sprintf("%t/%x/%x/%d/%d/%d", a.Active, a.Pool, a.Drep,
		a.DrepType, a.Reward, a.ExpirationEpoch)
}

func drepText(d *models.Drep) string {
	if d == nil {
		return "nil"
	}
	return fmt.Sprintf("%d/%x/%t/%d/%d/%d/%s", d.CredentialTag, d.Credential,
		d.Active, d.AddedSlot, d.LastActivityEpoch, d.ExpiryEpoch, d.AnchorURL)
}

// accountDRepReads renders the account, DRep and dormancy reads of ldb.
func accountDRepReads(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	h *setHarness,
	touched setTouched,
) []string {
	t.Helper()
	ctx := t.Context()
	refs := append(slices.Clone(h.refs), touched.refs...)
	var out []string
	for _, ref := range refs {
		for _, inactive := range []bool{true, false} {
			a, err := ldb.GetAccountByCredential(
				ctx, ref.Tag, ref.Key, inactive, nil,
			)
			if errors.Is(err, models.ErrAccountNotFound) {
				a, err = nil, nil
			}
			require.NoError(t, err)
			out = append(out, fmt.Sprintf("account %x %t %s", ref.Key, inactive,
				accountText(a)))
		}
		rows, err := ldb.GetAccountRegistrationHistoryByCredential(
			ctx, ref.Tag, ref.Key, 0, 0, "asc", nil,
		)
		require.NoError(t, err)
		for i := range rows {
			out = append(out, fmt.Sprintf("history %x %s", ref.Key,
				historyRowText(&rows[i])))
		}
	}
	active, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	var keys []string
	for _, r := range active {
		keys = append(keys, fmt.Sprintf("%d/%x", r.Tag, r.Key))
	}
	slices.Sort(keys)
	out = append(out, fmt.Sprintf("active accounts %v", keys))
	if len(h.pool) > 0 {
		counts, err := ldb.GetActiveDelegatorCountsByPool(
			[]lcommon.PoolKeyHash{lcommon.PoolKeyHash(h.pool)}, nil,
		)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("delegators %v", counts))
	}
	dreps, err := ldb.GetActiveDreps(ctx, nil)
	require.NoError(t, err)
	var texts []string
	for _, d := range dreps {
		texts = append(texts, drepText(d))
	}
	slices.Sort(texts)
	out = append(out, fmt.Sprintf("active dreps %v", texts))
	for _, key := range touched.dreps {
		d, err := ldb.GetDrepByCredential(ctx, 0, key, true, nil)
		if errors.Is(err, models.ErrDrepNotFound) {
			d, err = nil, nil
		}
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("drep %x %s", key, drepText(d)))
		delegators, err := ldb.GetDRepDelegators(ctx, 0, key, nil)
		require.NoError(t, err)
		var ds []string
		for _, r := range delegators {
			ds = append(ds, fmt.Sprintf("%d/%x", r.Tag, r.Key))
		}
		slices.Sort(ds)
		out = append(out, fmt.Sprintf("delegators of %x %v", key, ds))
		dep, err := ldb.GetDrepLastRegistrationDeposit(ctx, 0, key, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("deposit %x %s", key, depositText(dep)))
	}
	dormant, err := ldb.GetDormantDRepEpochs(ctx, nil)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("dormant %d", dormant))
	return out
}

type setDrive struct {
	name  string
	drive func(t *testing.T, h *setHarness) setTouched
}

var setDrives = []setDrive{
	{"governance before v10", func(t *testing.T, h *setHarness) setTouched {
		h.scenario.Governance(h.refs[0], 9)
		return setTouched{dreps: setDRepKeys[:2]}
	}},
	{"governance from v10", func(t *testing.T, h *setHarness) setTouched {
		h.scenario.Governance(h.refs[0], 10)
		return setTouched{dreps: setDRepKeys[:2]}
	}},
	{
		"reward credit and withdrawal",
		func(t *testing.T, h *setHarness) setTouched {
			h.scenario.Credit(h.refs[0], 100)
			require.NoError(t, h.scenario.Withdraw(h.refs[0], 40))
			h.scenario.FoldForWithdrawal(h.refs[0])
			return setTouched{dreps: setDRepKeys[:2]}
		},
	},
	{"delegation reap", func(t *testing.T, h *setHarness) setTouched {
		h.scenario.Reap(h.pool)
		return setTouched{dreps: setDRepKeys[:2]}
	}},
	{"CIP-0163 expiry", func(t *testing.T, h *setHarness) setTouched {
		h.scenario.ExpireAtBoundary(h.refs[0])
		return setTouched{dreps: setDRepKeys[:2]}
	}},
	{"registrations", func(t *testing.T, h *setHarness) setTouched {
		s := h.scenario.Registrations()
		out := []models.StakeCredentialRef{s.Imported}
		for _, k := range [][]byte{s.K, s.L, s.M} {
			out = append(out, models.NewStakeCredentialRef(0, k))
		}
		return setTouched{refs: out, dreps: [][]byte{s.D, s.E}}
	}},
}

// accountsRun drives one scenario through a rollback, rendering the account
// and DRep reads after each step.
func accountsRun(
	t *testing.T,
	d setDrive,
) (map[string][]string, *setHarness) {
	t.Helper()
	h := newSetHarness(t)
	out := map[string][]string{}
	touched := d.drive(t, h)
	out["applied"] = accountDRepReads(t, h.reads(), h, touched)
	h.scenario.RollbackBeforeBoundary()
	out["rolled back"] = accountDRepReads(t, h.reads(), h, touched)
	return out, h
}

// With the set failed, the LedgerDB fails the read rather than answer
// nothing.
func TestAccountAndDRepReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// A failed set serves no read.
	scenario.FailSet()
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	key := bytes.Repeat([]byte{1}, 28)
	_, err = ldb.GetAccountByCredential(ctx, 0, key, true, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetActiveAccountCredentials(nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetAccountRegistrationHistoryByCredential(
		ctx,
		0,
		key,
		0,
		0,
		"asc",
		nil,
	)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.AccountLastWitnessSlots(ctx, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, key),
	}, 100, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetActiveDreps(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetDrepByCredential(ctx, 0, key, true, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetDormantDRepEpochs(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
}
