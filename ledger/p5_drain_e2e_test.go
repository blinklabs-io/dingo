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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
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
	guard    *statedrain.TripGuard
	// store is the metadata store's own LedgerDB, whose answers are the
	// reference an undrained run's reads are rendered from.
	store ledgerdb.LedgerDB
	// drainErr is what statedrain.Run returned.
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
	guard := statedrain.NewTripGuard()
	h := &p5Harness{
		set:   set,
		guard: guard,
		scenario: ledger.NewGuardedBoundaryEpochStateScenarioForTest(
			t, set, ledger.UtxoSetReadsSet, guard,
		),
	}
	h.store = dbadapter.New(h.scenario.DB)
	require.NoError(t, h.scenario.DB.Metadata().IterateAccounts(nil,
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
	src := drainTestSource{set: set, db: h.scenario.DB}
	candidates := append(statedrain.Graduated(src), statedrain.Staged(src)...)
	var entries []statedrain.Entry
	for _, f := range drain {
		before := len(entries)
		for _, e := range candidates {
			if e.Family == f {
				entries = append(entries, e)
			}
		}
		require.Greater(
			t, len(entries), before,
			"family %s has no drain entry", f,
		)
	}
	h.drainErr = statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{h.scenario.DB, h.scenario.DrainFamilies},
		Set:   src, Guard: guard, VerifyBeforeDrain: true,
	}, entries)
	return h
}

// reads is the LedgerDB the scenario's reads are rendered from: the metadata
// store's when nothing is drained, the ledger's otherwise.
func (h *p5Harness) reads(drained bool) ledgerdb.LedgerDB {
	if drained {
		return h.scenario.LedgerDB()
	}
	return h.store
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
	require.Empty(t, h.guard.Trips(), "a drained table was read or written")
	mask, err := h.scenario.DB.DrainedFamilies(t.Context())
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, h.scenario.DB)
	require.NoError(t, err)
	for _, f := range families {
		require.True(t, mask.Has(f), "latch bit of %s", f)
		for _, table := range tables[f] {
			require.Zero(t, tableRows(t, raw, table), "drained %s", table)
		}
	}
}

func genesisMIRReadsText(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	s ledger.GenesisMIRSlots,
) []string {
	t.Helper()
	var out []string
	for _, slot := range []uint64{
		s.DelegA + genesisMIRWindow - 1, s.DelegA + genesisMIRWindow,
		s.DelegB + genesisMIRWindow - 1, s.DelegB + genesisMIRWindow,
		math.MaxInt64,
	} {
		d, err := ldb.GetGenesisDelegationForSlot(
			s.Genesis, slot, genesisMIRWindow, nil,
		)
		require.NoError(t, err)
		if d == nil {
			out = append(out, fmt.Sprintf("for %d nil", slot))
			continue
		}
		out = append(out, fmt.Sprintf(
			"for %d %x %x %d", slot, d.GenesisDelegateHash, d.VrfKeyHash,
			d.AddedSlot,
		))
	}
	for _, r := range [][2]uint64{
		{0, math.MaxInt64}, {s.DelegA, s.DelegB}, {s.DelegB, s.DelegB},
		{s.DelegB, s.DelegA},
	} {
		rows, err := ldb.GetGenesisDelegationsInSlotRange(r[0], r[1], nil)
		require.NoError(t, err)
		var texts []string
		for _, d := range rows {
			texts = append(texts, fmt.Sprintf(
				"%x/%x/%d/%d/%d", d.GenesisDelegateHash, d.VrfKeyHash,
				d.AddedSlot, d.BlockIndex, d.CertIndex,
			))
		}
		out = append(
			out,
			fmt.Sprintf("range %v %s", r, strings.Join(texts, ",")),
		)
	}
	effects, err := ldb.GetMIRCertsInSlotRange(
		t.Context(), s.EpochStart, s.Boundary, nil,
	)
	require.NoError(t, err)
	for _, e := range effects {
		var rewards []string
		for _, r := range e.Rewards {
			rewards = append(
				rewards,
				fmt.Sprintf("%x=%s", r.Credential, r.Amount),
			)
		}
		slices.Sort(rewards)
		out = append(out, fmt.Sprintf(
			"mir %d %d %d %v", e.AddedSlot, e.Pot, e.OtherPot, rewards,
		))
	}
	return out
}

// p5GenesisMIRRun drives the genesis key delegations and MIR certificates
// and their boundary through a rollback, with the families drained first.
func p5GenesisMIRRun(
	t *testing.T,
	drain []nodesettings.Family,
	corrupt func(*database.Database),
) (map[string][]string, *p5Harness) {
	t.Helper()
	h := newP5Harness(t, drain, corrupt)
	out := map[string][]string{}
	if h.drainErr != nil {
		return out, h
	}
	drained := len(drain) > 0
	slots := h.scenario.GenesisMIR(func() {})
	out["applied"] = genesisMIRReadsText(t, h.reads(drained), slots)
	state, err := h.scenario.LedgerDB().GetNetworkState(nil)
	require.NoError(t, err)
	out["pots"] = []string{fmt.Sprintf("%d %d", state.Treasury, state.Reserves)}
	h.scenario.RollbackBeforeBoundary()
	out["rolled back"] = genesisMIRReadsText(t, h.reads(drained), slots)
	return out, h
}

// With genesis_delegation drained, the delegations header verification reads,
// a MIR certificate and its boundary, and a rollback answer as an undrained
// run does, and no statement touches the table.
func TestMIRGenesisDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	families := []nodesettings.Family{nodesettings.FamilyMIRGenesis}
	undrained, plain := p5GenesisMIRRun(t, nil, nil)
	require.NoError(t, plain.drainErr)
	drained, h := p5GenesisMIRRun(t, families, nil)
	h.requireDrained(t, families, map[nodesettings.Family][]string{
		nodesettings.FamilyMIRGenesis: {"genesis_delegation"},
	})
	rawPlain, err := dbtest.RawSQLiteMetadata(t, plain.scenario.DB)
	require.NoError(t, err)
	require.Positive(t, tableRows(t, rawPlain, "genesis_delegation"))
	for name, want := range undrained {
		require.Equal(t, want, drained[name], name)
	}
	require.Len(t, drained, 3)
	require.Contains(
		t,
		strings.Join(drained["applied"], "\n"),
		"range [0 9223372036854775807]",
	)
}

// A set that differs from the metadata store refuses the drain and leaves
// the rows.
func TestMIRGenesisVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	families := []nodesettings.Family{nodesettings.FamilyMIRGenesis}
	h := newP5Harness(t, families, func(db *database.Database) {
		raw, err := dbtest.RawSQLiteMetadata(t, db)
		require.NoError(t, err)
		_, err = raw.Exec(`
INSERT INTO genesis_delegation (
    genesis_hash, genesis_delegate_hash, vrf_key_hash,
    added_slot, block_index, cert_index, certificate_id
) VALUES (x'01', x'02', x'03', 7, 0, 0, 999999)`)
		require.NoError(t, err)
	})
	require.ErrorIs(t, h.drainErr, statedrain.ErrGateRefused)
	require.ErrorContains(t, h.drainErr, "differ")
	mask, err := h.scenario.DB.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.False(t, mask.Has(nodesettings.FamilyMIRGenesis))
	raw, err := dbtest.RawSQLiteMetadata(t, h.scenario.DB)
	require.NoError(t, err)
	require.Positive(t, tableRows(t, raw, "genesis_delegation"))
}

// With the family drained and the set not holding it, the LedgerDB fails the
// read rather than answer from the empty SQL table.
func TestMIRGenesisDrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	// The set never loads the metadata store's rows.
	f := nodesettings.FamilyMIRGenesis
	require.NoError(t, scenario.DB.RecordDrainedFamily(t.Context(), f))
	scenario.DrainFamilies(nodesettings.FamilyMask(0).With(f))
	ldb := scenario.LedgerDB()
	_, err = ldb.GetGenesisDelegationForSlot(
		bytes.Repeat([]byte{1}, 28), math.MaxInt64, genesisMIRWindow, nil,
	)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.GetGenesisDelegationsInSlotRange(0, math.MaxInt64, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
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

// With the accounts and dreps families drained, each scenario reads as an
// undrained run does and no statement touches a drained table.
func TestAccountsAndDRepsDrainMatchAnUndrainedRun(t *testing.T) {
	t.Parallel()
	families := []nodesettings.Family{
		nodesettings.FamilyDReps, nodesettings.FamilyAccounts,
	}
	for _, d := range p5Drives {
		t.Run(d.name, func(t *testing.T) {
			t.Parallel()
			undrained, plain := p5AccountsRun(t, nil, nil, d)
			require.NoError(t, plain.drainErr)
			drained, h := p5AccountsRun(t, families, nil, d)
			h.requireDrained(t, families, p5AccountTables)
			for name, want := range undrained {
				require.Equal(t, want, drained[name], name)
			}
			require.Len(t, drained, 2)
		})
	}
}

// A set that differs from the metadata store refuses the drain and leaves
// the rows.
func TestAccountsAndDRepsVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		family  nodesettings.Family
		table   string
		corrupt string
	}{
		{
			nodesettings.FamilyDReps, "drep",
			"INSERT INTO drep (credential, anchor_hash, added_slot, " +
				"credential_tag, active) VALUES (x'aa', x'bb', 1, 0, 1)",
		},
		{
			nodesettings.FamilyDReps, "drep_expiry_history",
			"INSERT INTO drep_expiry_history (credential_tag, credential, " +
				"added_slot, last_activity_epoch, expiry_epoch) " +
				"VALUES (0, x'aa', 1, 1, 2)",
		},
		{
			nodesettings.FamilyDReps, "drep_dormancy_history",
			"INSERT INTO drep_dormancy_history (added_slot, " +
				"previous_dormant_epochs) VALUES (3, 9)",
		},
		{
			nodesettings.FamilyDReps, "drep_expiry_epoch_event",
			"INSERT INTO drep_expiry_epoch_event (added_slot) VALUES (5)",
		},
		{
			nodesettings.FamilyDReps, "account_drep_clear",
			"INSERT INTO account_drep_clear (credential_tag, staking_key, " +
				"added_slot) VALUES (0, x'cc', 4)",
		},
		{
			nodesettings.FamilyAccounts, "account",
			"UPDATE account SET reward = '999999'",
		},
		{
			nodesettings.FamilyAccounts, "account_import_baseline",
			"UPDATE account_import_baseline SET added_slot = added_slot + 7",
		},
	} {
		t.Run(tc.family.String()+"/"+tc.table, func(t *testing.T) {
			t.Parallel()
			h := newP5Harness(t,
				[]nodesettings.Family{tc.family},
				func(db *database.Database) {
					raw, err := dbtest.RawSQLiteMetadata(t, db)
					require.NoError(t, err)
					_, err = raw.Exec(tc.corrupt)
					require.NoError(t, err)
				})
			require.ErrorIs(t, h.drainErr, statedrain.ErrGateRefused)
			require.ErrorContains(t, h.drainErr, "differ")
			mask, err := h.scenario.DB.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.False(t, mask.Has(tc.family))
			raw, err := dbtest.RawSQLiteMetadata(t, h.scenario.DB)
			require.NoError(t, err)
			require.Positive(t, tableRows(t, raw, tc.table))
		})
	}
}

// With the families drained and the set not holding them, the LedgerDB fails
// the read rather than answer from the empty SQL tables.
func TestAccountsAndDRepsDrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	// The set never loads the metadata store's rows.
	var mask nodesettings.FamilyMask
	for _, f := range []nodesettings.Family{
		nodesettings.FamilyAccounts, nodesettings.FamilyDReps,
	} {
		require.NoError(t, scenario.DB.RecordDrainedFamily(t.Context(), f))
		mask = mask.With(f)
	}
	scenario.DrainFamilies(mask)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	key := bytes.Repeat([]byte{1}, 28)
	_, err = ldb.GetAccountByCredential(ctx, 0, key, true, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.GetActiveAccountCredentials(nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.GetAccountRegistrationHistoryByCredential(
		ctx,
		0,
		key,
		0,
		0,
		"asc",
		nil,
	)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.AccountLastWitnessSlots(ctx, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, key),
	}, 100, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.GetActiveDreps(ctx, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.GetDrepByCredential(ctx, 0, key, true, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = ldb.GetDormantDRepEpochs(ctx, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
}
