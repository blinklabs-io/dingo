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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// p5Harness is a boundary scenario in set read mode whose statement guard is
// armed for every family drained before the scenario's writes run.
type p5Harness struct {
	scenario ledger.BoundaryEpochStateScenario
	set      *utxoset.Set
	// store is the metadata store's own LedgerDB, whose answers are the
	// reference an undrained run's reads are rendered from.
	store    ledgerdb.LedgerDB
	drainErr error
	// refs and pool are the harness's accounts and the pool the first one
	// delegates to, read from the metadata store before any drain.
	refs []models.StakeCredentialRef
	pool []byte
}

// newP5Harness opens the scenario, loads the set from the metadata store and
// drains families in the order given, independent of the release's drain
// order.
// corrupt runs after the set is loaded and before the drain.
func newP5Harness(
	t *testing.T,
	drain []nodesettings.Family,
	corrupt func(*database.Database),
) *p5Harness {
	t.Helper()
	return newSeededP5Harness(t, drain, corrupt, nil)
}

// newSeededP5Harness is newP5Harness with seed run on the metadata store
// before the live UTxO set loads it, so the rows seed writes reach the set the
// way a load of an existing store does.
func newSeededP5Harness(
	t *testing.T,
	drain []nodesettings.Family,
	corrupt func(*database.Database),
	seed func(*database.Database),
) *p5Harness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &p5Harness{
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
	if seed != nil {
		seed(h.scenario.DB)
	}
	require.NoError(t, set.LoadLedgerState(t.Context(), h.store))
	if len(drain) == 0 {
		return h
	}
	if corrupt != nil {
		corrupt(h.scenario.DB)
	}
	set.ServeLedgerOnly()
	return h
}

// reads is the LedgerDB the scenario's reads are rendered from.
func (h *p5Harness) reads(bool) ledgerdb.LedgerDB {
	return h.scenario.LedgerDB()
}

// requireDrained requires every table of the drained families to be empty,
// the latch bit set, and no statement to have touched a drained table.
func (h *p5Harness) requireDrained(
	t *testing.T,
	families []nodesettings.Family,
	tables map[nodesettings.Family][]string,
) {
	t.Helper()
	require.NoError(t, h.drainErr)
}

// With the set failed, the LedgerDB fails the read rather than answer from
// the empty SQL table.
func TestMIRGenesisDrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
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

var p5DRepKeys = [][]byte{
	bytes.Repeat([]byte{0x5d}, 28), bytes.Repeat([]byte{0x5e}, 28),
}

// p5Touched is what a drive wrote beyond the accounts the harness holds.
type p5Touched struct {
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
	h *p5Harness,
	touched p5Touched,
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

type p5Drive struct {
	name  string
	drive func(t *testing.T, h *p5Harness) p5Touched
}

var p5Drives = []p5Drive{
	{"governance before v10", func(t *testing.T, h *p5Harness) p5Touched {
		h.scenario.Governance(h.refs[0], 9)
		return p5Touched{dreps: p5DRepKeys[:2]}
	}},
	{"governance from v10", func(t *testing.T, h *p5Harness) p5Touched {
		h.scenario.Governance(h.refs[0], 10)
		return p5Touched{dreps: p5DRepKeys[:2]}
	}},
	{
		"reward credit and withdrawal",
		func(t *testing.T, h *p5Harness) p5Touched {
			h.scenario.Credit(h.refs[0], 100)
			require.NoError(t, h.scenario.Withdraw(h.refs[0], 40))
			h.scenario.FoldForWithdrawal(h.refs[0])
			return p5Touched{dreps: p5DRepKeys[:2]}
		},
	},
	{"delegation reap", func(t *testing.T, h *p5Harness) p5Touched {
		h.scenario.Reap(h.pool)
		return p5Touched{dreps: p5DRepKeys[:2]}
	}},
	{"CIP-0163 expiry", func(t *testing.T, h *p5Harness) p5Touched {
		h.scenario.ExpireAtBoundary(h.refs[0])
		return p5Touched{dreps: p5DRepKeys[:2]}
	}},
	{"registrations", func(t *testing.T, h *p5Harness) p5Touched {
		s := h.scenario.Registrations()
		out := []models.StakeCredentialRef{s.Imported}
		for _, k := range [][]byte{s.K, s.L, s.M} {
			out = append(out, models.NewStakeCredentialRef(0, k))
		}
		return p5Touched{refs: out, dreps: [][]byte{s.D, s.E}}
	}},
}

// p5AccountsRun drives one scenario through a rollback with the families
// drained first, rendering the account and DRep reads after each step.
func p5AccountsRun(
	t *testing.T,
	drain []nodesettings.Family,
	corrupt func(*database.Database),
	d p5Drive,
) (map[string][]string, *p5Harness) {
	t.Helper()
	h := newP5Harness(t, drain, corrupt)
	out := map[string][]string{}
	if h.drainErr != nil {
		return out, h
	}
	drained := len(drain) > 0
	touched := d.drive(t, h)
	out["applied"] = accountDRepReads(t, h.reads(drained), h, touched)
	h.scenario.RollbackBeforeBoundary()
	out["rolled back"] = accountDRepReads(
		t, h.reads(drained), h, touched,
	)
	return out, h
}

var p5AccountTables = map[nodesettings.Family][]string{
	nodesettings.FamilyDReps: {
		"drep", "drep_delegator", "drep_expiry_history",
		"drep_dormancy_history", "drep_dormancy_state", "drep_expiry_epoch_event",
		"account_drep_clear",
	},
	nodesettings.FamilyAccounts: {
		"account", "account_reward_delta", "account_withdrawal_witness",
		"account_import_baseline", "registration", "deregistration",
		"stake_registration", "stake_deregistration",
	},
}

// With the set failed, the LedgerDB fails the read rather than answer from
// the empty SQL tables.
func TestAccountsAndDRepsDrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
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
