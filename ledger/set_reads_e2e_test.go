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

// setModeScenario builds the boundary scenario in set mode over a loaded
// set. The store is the metadata store read around the ledger.
func setModeScenario(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, *utxoset.Set, ledgerdb.LedgerDB) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	return scenario, set, store
}

func accountRow(a *models.Account) string {
	c := *a
	c.ID, c.CertificateID, c.ImportDeposit = 0, 0, nil
	if len(c.Pool) == 0 {
		c.Pool = nil
	}
	if len(c.Drep) == 0 {
		c.Drep = nil
	}
	return fmt.Sprintf("%+v", c)
}

// requireAccountReads reads every account the metadata store holds, and one
// it does not, through the ledger's LedgerDB, and requires the answers to be
// the store's.
func requireAccountReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	stage string,
) int {
	t.Helper()
	var accounts []*models.Account
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			c := *a
			accounts = append(accounts, &c)
			return nil
		}),
	)
	missing := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x99}, 28))
	refs := []models.StakeCredentialRef{missing}
	for _, a := range accounts {
		refs = append(
			refs,
			models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey),
		)
	}
	_, err := ldb.GetAccountByCredential(
		t.Context(), missing.Tag, missing.Key, true, nil,
	)
	require.ErrorIs(t, err, models.ErrAccountNotFound, stage)
	for _, inactive := range []bool{false, true} {
		want, err := store.GetAccountsByCredential(
			t.Context(),
			refs,
			inactive,
			nil,
		)
		require.NoError(t, err, stage)
		got, err := ldb.GetAccountsByCredential(
			t.Context(),
			refs,
			inactive,
			nil,
		)
		require.NoError(t, err, stage)
		require.Len(t, got, len(want), "%s inactive=%t", stage, inactive)
		for _, a := range accounts {
			ref := models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
			one, err := ldb.GetAccountByCredential(
				t.Context(), a.CredentialTag, a.StakingKey, inactive, nil,
			)
			w := want[ref.MapKey()]
			if w == nil {
				require.ErrorIs(t, err, models.ErrAccountNotFound, stage)
				require.NotContains(t, got, ref.MapKey(), stage)
				continue
			}
			require.NoError(t, err, stage)
			require.Equal(t, accountRow(w), accountRow(one), stage)
			require.Equal(
				t,
				accountRow(w),
				accountRow(got[ref.MapKey()]),
				stage,
			)
		}
	}
	return len(accounts)
}

// TestAccountReadsFromTheLiveUtxoSet reads accounts through the ledger's
// LedgerDB in set mode with every metadata store account read failing, across
// the writes that change accounts and their rollback, and requires the
// answers to be the store's. A read that reached the store would fail.
func TestAccountReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	storeReads, _ := scenario.HideStoreReads(
		"GetAccountByCredential", "GetAccountsByCredential",
	)
	ldb := scenario.LedgerDB()
	require.Positive(t, requireAccountReads(t, ldb, store, "before"))
	scenario.ApplyBoundary()
	requireAccountReads(t, ldb, store, "after the boundary")
	var ref models.StakeCredentialRef
	var pool []byte
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			if a.Active && ref.Key == nil {
				ref = models.NewStakeCredentialRef(
					a.CredentialTag,
					a.StakingKey,
				)
			}
			if a.Active && len(a.Pool) > 0 && pool == nil {
				pool = a.Pool
			}
			return nil
		}),
	)
	require.NotNil(t, ref.Key)
	require.NotNil(t, pool)
	scenario.Credit(ref, 7)
	requireAccountReads(t, ldb, store, "after credits")
	require.NoError(t, scenario.Withdraw(ref, 5))
	requireAccountReads(t, ldb, store, "after a withdrawal")
	scenario.Reap(pool)
	requireAccountReads(t, ldb, store, "after a pool reap")
	scenario.Expire(ref)
	requireAccountReads(t, ldb, store, "after expirations")
	scenario.Governance(ref, 9)
	requireAccountReads(t, ldb, store, "after governance")
	scenario.RollbackBeforeBoundary()
	requireAccountReads(t, ldb, store, "after the rollback")
	require.Zero(t, storeReads(), "an account read reached the metadata store")
	verified, mismatches := scenario.AccountReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

func drepRow(d *models.Drep) string {
	c := *d
	c.ID, c.Delegators = 0, nil
	return fmt.Sprintf("%+v", c)
}

// requireDrepReads reads every DRep the metadata store holds, and some it
// does not, the active set and the dormancy counter through the ledger's
// LedgerDB, and requires the answers to be the store's. It returns how many
// DReps are active.
func requireDrepReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	stage string,
) int {
	t.Helper()
	ctx := t.Context()
	keys := [][]byte{
		bytes.Repeat([]byte{0x5d}, 28), bytes.Repeat([]byte{0x5e}, 28),
		bytes.Repeat([]byte{0x98}, 28),
	}
	require.NoError(t, store.IterateDreps(nil, func(d *models.Drep) error {
		keys = append(keys, d.Credential)
		return nil
	}))
	for _, key := range keys {
		for _, tag := range []uint8{0, 1} {
			for _, inactive := range []bool{false, true} {
				want, wantErr := store.GetDrepByCredential(
					ctx,
					tag,
					key,
					inactive,
					nil,
				)
				got, err := ldb.GetDrepByCredential(
					ctx,
					tag,
					key,
					inactive,
					nil,
				)
				if errors.Is(wantErr, models.ErrDrepNotFound) {
					require.ErrorIs(t, err, models.ErrDrepNotFound, stage)
					continue
				}
				require.NoError(t, wantErr, stage)
				require.NoError(t, err, "%s %x", stage, key)
				require.Equal(t, drepRow(want), drepRow(got), stage)
			}
		}
	}
	want, err := store.GetActiveDreps(ctx, nil)
	require.NoError(t, err)
	got, err := ldb.GetActiveDreps(ctx, nil)
	require.NoError(t, err, stage)
	rows := func(ds []*models.Drep) []string {
		var out []string
		for _, d := range ds {
			out = append(out, drepRow(d))
		}
		return out
	}
	require.ElementsMatch(t, rows(want), rows(got), stage)
	wantDormant, err := store.GetDormantDRepEpochs(ctx, nil)
	require.NoError(t, err)
	gotDormant, err := ldb.GetDormantDRepEpochs(ctx, nil)
	require.NoError(t, err, stage)
	require.Equal(t, wantDormant, gotDormant, stage)
	return len(want)
}

// TestDrepReadsFromTheLiveUtxoSet reads DReps through the ledger's LedgerDB
// in set mode with every metadata store DRep read failing, across
// registration, activity, dormancy, deregistration and the rollback, and
// requires the answers to be the store's.
func TestDrepReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	storeReads, hide := scenario.HideStoreReads(
		"GetDrepByCredential", "GetActiveDreps", "GetDormantDRepEpochs",
	)
	ldb := scenario.LedgerDB()
	// BumpDormantDRepExpiries reads the dormancy counter from the store
	// before and after its write to learn whether the store applied it.
	readsOnly := func(fn func()) {
		hide(true)
		defer hide(false)
		fn()
	}
	hide(false)
	readsOnly(
		func() { require.Zero(t, requireDrepReads(t, ldb, store, "before")) },
	)
	scenario.ApplyBoundary()
	var ref models.StakeCredentialRef
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			if a.Active && ref.Key == nil {
				ref = models.NewStakeCredentialRef(
					a.CredentialTag,
					a.StakingKey,
				)
			}
			return nil
		}),
	)
	require.NotNil(t, ref.Key)
	scenario.VoteDelegate(ref, bytes.Repeat([]byte{0x5e}, 28), 0, 3)
	readsOnly(func() {
		require.Equal(
			t,
			1,
			requireDrepReads(t, ldb, store, "after a registration"),
		)
	})
	scenario.Governance(ref, 9)
	readsOnly(func() {
		require.Equal(t, 1, requireDrepReads(t, ldb, store,
			"after activity, dormancy and a deregistration"))
	})
	scenario.RollbackBeforeBoundary()
	readsOnly(func() { requireDrepReads(t, ldb, store, "after the rollback") })
	require.Zero(t, storeReads(), "a DRep read reached the metadata store")
	verified, mismatches := scenario.DrepReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

func latestEpoch(t *testing.T, store ledgerdb.LedgerDB) models.Epoch {
	t.Helper()
	epochs, err := store.GetEpochs(nil)
	require.NoError(t, err)
	var cur models.Epoch
	for _, e := range epochs {
		if e.EpochId > cur.EpochId {
			cur = e
		}
	}
	return cur
}

func poolRegistrationRow(r *models.PoolRegistration) string {
	c := ledger.CleanPoolRegistration(r)
	margin := "nil"
	if c.Margin != nil && c.Margin.Rat != nil {
		margin = c.Margin.Rat.String()
	}
	var owners []string
	for _, o := range c.Owners {
		owners = append(owners, fmt.Sprintf("%x", o.KeyHash))
	}
	slices.Sort(owners)
	relays := fmt.Sprintf("%d", len(c.Relays))
	return fmt.Sprintf(
		"%x %x %d %d %d %x/%d %q %x %s %v %s slot=%d",
		c.PoolKeyHash,
		c.VrfKeyHash,
		c.Pledge,
		c.Cost,
		c.RewardAccountCredentialTag,
		c.RewardAccount,
		len(c.RewardAccount),
		c.MetadataUrl,
		c.MetadataHash,
		margin,
		owners,
		relays,
		c.AddedSlot,
	)
}

// requirePoolReads reads the registration in effect now of pools in the
// latest epoch through the ledger's LedgerDB and requires the answer to be
// the metadata store's. It returns how many pools have one.
func requirePoolReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	pools [][]byte,
	stage string,
) int {
	t.Helper()
	cur := latestEpoch(t, store)
	pkhs := []lcommon.PoolKeyHash{
		lcommon.PoolKeyHash(bytes.Repeat([]byte{0x9a}, 28)),
	}
	for _, p := range pools {
		pkhs = append(pkhs, lcommon.PoolKeyHash(p))
	}
	want, err := store.GetPoolRegistrationsEffectiveForEpoch(
		pkhs, cur.StartSlot, cur.EpochId, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	got, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
		pkhs, cur.StartSlot, cur.EpochId, math.MaxInt64, nil,
	)
	require.NoError(t, err, stage)
	rows := func(rs []models.PoolRegistration) []string {
		var out []string
		for i := range rs {
			out = append(out, poolRegistrationRow(&rs[i]))
		}
		return out
	}
	require.ElementsMatch(t, rows(want), rows(got), stage)
	return len(want)
}

// TestPoolReadsFromTheLiveUtxoSet reads the registration in effect now of
// pools through the ledger's LedgerDB in set mode with the metadata store's
// read failing, and requires the answers to be the store's: a pool
// registered before the epoch, a registration that took effect at a
// boundary and a pool retired by one. A pool that registered after the
// epoch began is a case the set cannot answer, and reaches the store.
func TestPoolReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	reached, hide := scenario.HideStoreReads(
		"GetPoolRegistrationsEffectiveForEpoch",
	)
	ldb := scenario.LedgerDB()
	var harnessPool []byte
	poolHashes, err := store.ListPoolKeyHashes(nil)
	require.NoError(t, err)
	require.NotEmpty(t, poolHashes)
	harnessPool = poolHashes[0]
	newPool := bytes.Repeat([]byte{0x6a}, 28)
	pools := [][]byte{harnessPool, newPool}
	require.Equal(t, 1, requirePoolReads(t, ldb, store, pools, "before"))
	scenario.ApplyBoundary()
	// The boundary's snapshot reads registrations at a bounded slot, which
	// the store answers.
	atBoundary := reached()
	require.Equal(
		t,
		1,
		requirePoolReads(t, ldb, store, pools, "after the boundary"),
	)
	require.Equal(
		t,
		atBoundary,
		reached(),
		"a pool read reached the metadata store",
	)
	scenario.Pools(func() {
		// The new pool registered after the epoch began: the set keeps no
		// earlier registration, so the store answers.
		cur := latestEpoch(t, store)
		read := func(pool []byte) ([]models.PoolRegistration, error) {
			return ldb.GetPoolRegistrationsEffectiveForEpoch(
				[]lcommon.PoolKeyHash{lcommon.PoolKeyHash(pool)},
				cur.StartSlot, cur.EpochId, math.MaxInt64, nil,
			)
		}
		before := reached()
		served, err := read(harnessPool)
		require.NoError(t, err)
		require.Len(
			t,
			served,
			1,
			"a registration from before the epoch is the set's",
		)
		require.Equal(t, before, reached())
		_, err = read(newPool)
		require.Error(t, err)
		require.Greater(t, reached(), before,
			"a registration newer than the epoch is the store's to answer")
		hide(false)
		require.Equal(
			t,
			2,
			requirePoolReads(t, ldb, store, pools, "a mid-epoch registration"),
		)
		hide(true)
	})
	require.Equal(
		t,
		1,
		requirePoolReads(
			t,
			ldb,
			store,
			pools,
			"after a promotion and a retirement",
		),
	)
	scenario.RollbackBeforeBoundary()
	require.Equal(
		t,
		1,
		requirePoolReads(t, ldb, store, pools, "after the rollback"),
	)
	verified, mismatches := scenario.PoolReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// TestPoolReadsStayOnTheStoreWhenLeiosCertifies requires the registration in
// effect now to be read from the metadata store on a node that certifies
// Leios endorser blocks, whose registrations take effect at a slot the set
// does not hold.
func TestPoolReadsStayOnTheStoreWhenLeiosCertifies(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	reached, _ := scenario.HideStoreReads(
		"GetPoolRegistrationsEffectiveForEpoch",
	)
	scenario.CertifyLeios()
	cur := latestEpoch(t, dbadapter.New(scenario.DB))
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0xd1}, 28))
	_, err := scenario.LedgerDB().GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash{
			pool,
		}, cur.StartSlot, cur.EpochId, math.MaxInt64, nil,
	)
	require.Error(t, err)
	require.Equal(t, int64(1), reached(), "the read reached the metadata store")
}

var opCertReadNames = []string{
	"LatestPoolOpCertSequence", "LatestPoolOpCertSequenceAfter",
	"LatestPoolOpCertSequenceAtOrBefore", "LatestPoolOpCertSequenceInRange",
	"LatestPoolOpCertSequences",
}

type opCertAnswer struct {
	sequence uint64
	found    bool
}

// opCertReadsMatchStore runs each read of pool through the ledger's LedgerDB
// with the metadata store hidden, falling back to the store unhidden for a
// read the set cannot answer, requires every answer to be the store's, and
// returns the reads that reached the store.
func opCertReadsMatchStore(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	hide func(bool),
	slots ledger.OpCertSlots,
	pool []byte,
) []string {
	t.Helper()
	ctx := t.Context()
	pkh := lcommon.PoolKeyHash(pool)
	reads := map[string]func(ledgerdb.LedgerDB) (uint64, bool, error){
		"latest": func(l ledgerdb.LedgerDB) (uint64, bool, error) {
			return l.LatestPoolOpCertSequence(ctx, pkh, nil)
		},
	}
	for _, slot := range []uint64{slots.Base, slots.Raised, slots.Lowered, slots.Batched} {
		reads[fmt.Sprintf("after %d", slot)] = func(l ledgerdb.LedgerDB) (uint64, bool, error) {
			return l.LatestPoolOpCertSequenceAfter(ctx, pkh, slot, nil)
		}
	}
	for _, slot := range []uint64{
		slots.Base - 1, slots.Base, slots.Raised, slots.Repeated,
		slots.Lowered, slots.Batched, slots.Batched + 1,
	} {
		reads[fmt.Sprintf("at or before %d", slot)] = func(l ledgerdb.LedgerDB) (uint64, bool, error) {
			return l.LatestPoolOpCertSequenceAtOrBefore(ctx, pkh, slot, nil)
		}
	}
	for _, r := range [][2]uint64{
		{slots.Base, slots.Batched}, {slots.Raised, slots.Lowered},
		{slots.Repeated, slots.Lowered}, {slots.Lowered + 1, slots.Batched},
		{slots.Batched, slots.Base},
	} {
		reads[fmt.Sprintf("in %d..%d", r[0], r[1])] = func(l ledgerdb.LedgerDB) (uint64, bool, error) {
			return l.LatestPoolOpCertSequenceInRange(ctx, pkh, r[0], r[1], nil)
		}
	}
	var viaStore []string
	for what, read := range reads {
		ws, wf, err := read(store)
		require.NoError(t, err, what)
		hide(true)
		gs, gf, err := read(ldb)
		hide(false)
		if err != nil {
			viaStore = append(viaStore, what)
			gs, gf, err = read(ldb)
			require.NoError(t, err, what)
		}
		require.Equal(
			t,
			opCertAnswer{ws, wf},
			opCertAnswer{gs, gf},
			"%x %s",
			pool,
			what,
		)
	}
	return viaStore
}

// TestOpCertReadsFromTheLiveUtxoSet reads op-cert counters through the
// ledger's LedgerDB in set mode with the metadata store's counter reads
// failing, over counters that rise, repeat, drop and are batched, and
// requires the answers to be the store's. The one read the set cannot tell,
// a range that starts after the highest counter was reached and whose latest
// observation is a lower one, reaches the store.
func TestOpCertReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	_, hide := scenario.HideStoreReads(opCertReadNames...)
	hide(false)
	ldb := scenario.LedgerDB()
	slots := scenario.OpCerts()
	viaStore := opCertReadsMatchStore(t, ldb, store, hide, slots, slots.Pool)
	require.Equal(
		t,
		[]string{
			fmt.Sprintf("in %d..%d", slots.Repeated, slots.Lowered),
		},
		viaStore,
	)
	require.Empty(
		t,
		opCertReadsMatchStore(t, ldb, store, hide, slots, slots.Other),
	)
	hide(true)
	got, err := ldb.LatestPoolOpCertSequences(t.Context(), nil)
	require.NoError(t, err)
	want, err := store.LatestPoolOpCertSequences(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	// A counter reached and repeated: after its first observation the only
	// later row holds the same sequence, which the set knows because the
	// pool's latest observation repeats its highest counter.
	repeated := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6f}, 28))
	for _, slot := range []uint64{slots.Batched + 2, slots.Batched + 3} {
		txn := ldb.Begin(t.Context(), true)
		require.NoError(t, ldb.UpdatePoolOpCertSequence(
			t.Context(), repeated, 7, slot, txn,
		))
		require.NoError(t, txn.Commit())
		txn.Release()
	}
	hide(true)
	seq, found, err := ldb.LatestPoolOpCertSequenceAfter(
		t.Context(), repeated, slots.Batched+2, nil,
	)
	require.NoError(t, err, "the latest observation repeats the highest")
	require.True(t, found)
	require.Equal(t, uint64(7), seq)
	hide(true)
	_, found, err = ldb.LatestPoolOpCertSequenceAfter(
		t.Context(), repeated, slots.Batched+1, nil,
	)
	require.NoError(t, err, "the counter first reached after the slot")
	require.True(t, found)
	scenario.OpCertsInsideTxn(slots.Pool, 9, slots.Batched+1)
	hide(false)
	scenario.RollbackBeforeBoundary()
	require.Empty(
		t,
		opCertReadsMatchStore(t, ldb, store, hide, slots, slots.Pool),
		"after the rollback the counter from before it is one observation",
	)
	verified, mismatches := scenario.OpCertReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// TestOpCertReadsStayOnTheStoreForSlotsTheSetHasNoRecordOf requires reads of
// a counter as of a slot before the set's record, which a set loaded from the
// store has only for each pool's latest observation, to reach the metadata
// store, and the reads the set can tell to be answered without it.
func TestOpCertReadsStayOnTheStoreForSlotsTheSetHasNoRecordOf(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	store := dbadapter.New(scenario.DB)
	poolA := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x41}, 28))
	poolB := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x42}, 28))
	for _, w := range []struct {
		pool           lcommon.PoolKeyHash
		sequence, slot uint64
	}{{poolA, 4, 100}, {poolB, 3, 150}, {poolA, 6, 200}} {
		require.NoError(t, store.UpdatePoolOpCertSequence(
			t.Context(), w.pool, w.sequence, w.slot, nil,
		))
	}
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	reached, _ := scenario.HideStoreReads(opCertReadNames...)
	ldb := scenario.LedgerDB()
	ctx := t.Context()

	latest, found, err := ldb.LatestPoolOpCertSequence(ctx, poolA, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(6), latest)
	at250, _, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		ctx,
		poolA,
		250,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), at250)
	_, found, err = ldb.LatestPoolOpCertSequenceAfter(ctx, poolA, 200, nil)
	require.NoError(t, err)
	require.False(t, found, "no observation after the latest")
	require.Zero(t, reached())

	_, _, err = ldb.LatestPoolOpCertSequenceAtOrBefore(ctx, poolA, 150, nil)
	require.Error(t, err, "slot 150 is before the set's record")
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(ctx, poolA, 100, nil)
	require.Error(t, err, "the set cannot say what was observed after slot 100")
	require.Equal(t, int64(2), reached())
}

func committeeMemberRows(ms []*models.CommitteeMember) []string {
	var out []string
	for _, m := range ms {
		deleted := "-"
		if m.DeletedSlot != nil {
			deleted = fmt.Sprint(*m.DeletedSlot)
		}
		out = append(out, fmt.Sprintf("%d/%x/%d/%d/%t/%d/%s",
			m.ColdCredentialTag, m.ColdCredHash, m.ExpiresEpoch,
			m.TermStartSlot, m.TermStartSlotSet, m.AddedSlot, deleted))
	}
	slices.Sort(out)
	return out
}

func committeeAuthRows(as []*models.AuthCommitteeHot) []string {
	var out []string
	for _, a := range as {
		out = append(out, fmt.Sprintf("%d/%x/%d/%x/%d",
			a.ColdCredentialTag, a.ColdCredential,
			a.HotCredentialTag, a.HotCredential, a.AddedSlot))
	}
	slices.Sort(out)
	return out
}

// requireCommitteeStoreReads reads every committee and constitution query
// through the ledger's LedgerDB and requires the answers to be the metadata
// store's, rows' IDs and order aside.
func requireCommitteeStoreReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	slots ledger.CommitteeSlots,
	stage string,
) {
	t.Helper()
	ctx := t.Context()
	wc, err := store.GetConstitution(nil)
	require.NoError(t, err)
	gc, err := ldb.GetConstitution(nil)
	require.NoError(t, err, stage)
	require.Equal(t, wc == nil, gc == nil, stage)
	if wc != nil {
		wc.ID, gc.ID = 0, 0
		require.Equal(t, wc, gc, stage)
	}
	wq, err := store.GetCommitteeQuorum(ctx, nil)
	require.NoError(t, err)
	gq, err := ldb.GetCommitteeQuorum(ctx, nil)
	require.NoError(t, err, stage)
	require.Equal(t, wq == nil, gq == nil, stage)
	if wq != nil {
		require.Zero(t, wq.Cmp(gq), stage)
	}
	wm, err := store.GetCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	gm, err := ldb.GetCommitteeMembers(ctx, nil)
	require.NoError(t, err, stage)
	require.Equal(t, committeeMemberRows(wm), committeeMemberRows(gm), stage)
	wa, err := store.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.NoError(t, err)
	ga, err := ldb.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.NoError(t, err, stage)
	require.Equal(t, committeeMemberRows(wa), committeeMemberRows(ga), stage)
	wh, err := store.GetActiveCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	gh, err := ldb.GetActiveCommitteeMembers(ctx, nil)
	require.NoError(t, err, stage)
	require.Equal(t, committeeAuthRows(wh), committeeAuthRows(gh), stage)
	wn, err := store.GetCommitteeAuthorizedCount(ctx, nil)
	require.NoError(t, err)
	gn, err := ldb.GetCommitteeAuthorizedCount(ctx, nil)
	require.NoError(t, err, stage)
	require.Equal(t, wn, gn, stage)
	for _, from := range []uint64{0, slots.Reauth, slots.Reauth + 1} {
		ws, err := store.GetCommitteeHotAuthorizationsSince(ctx, from, nil)
		require.NoError(t, err)
		gs, err := ldb.GetCommitteeHotAuthorizationsSince(ctx, from, nil)
		require.NoError(t, err, stage)
		require.Equal(
			t,
			committeeAuthRows(ws),
			committeeAuthRows(gs),
			"%s since %d",
			stage,
			from,
		)
	}
	for _, cold := range [][]byte{slots.A, slots.B, slots.C} {
		for _, term := range []uint64{0, slots.Update} {
			w, werr := store.GetCommitteeMember(ctx, 0, cold, term, nil)
			g, gerr := ldb.GetCommitteeMember(ctx, 0, cold, term, nil)
			if errors.Is(werr, models.ErrCommitteeMemberNotFound) {
				require.ErrorIs(
					t,
					gerr,
					models.ErrCommitteeMemberNotFound,
					stage,
				)
			} else {
				require.NoError(t, werr)
				require.NoError(t, gerr, stage)
				require.Equal(t, committeeAuthRows([]*models.AuthCommitteeHot{w}),
					committeeAuthRows([]*models.AuthCommitteeHot{g}), stage)
			}
			wr, err := store.IsCommitteeMemberResigned(ctx, 0, cold, term, nil)
			require.NoError(t, err)
			gr, err := ldb.IsCommitteeMemberResigned(ctx, 0, cold, term, nil)
			require.NoError(t, err, stage)
			require.Equal(t, wr, gr, stage)
		}
	}
	creds := []models.CommitteeCredential{
		{
			Credential: slots.A,
		}, {Credential: slots.B, TermStartSlot: slots.Update},
		{Credential: slots.B}, {Credential: slots.C},
	}
	wr, err := store.GetResignedCommitteeMembers(ctx, creds, nil)
	require.NoError(t, err)
	gr, err := ldb.GetResignedCommitteeMembers(ctx, creds, nil)
	require.NoError(t, err, stage)
	require.Equal(t, wr, gr, stage)
}

// TestCommitteeReadsFromTheLiveUtxoSet reads the committee and constitution
// through the ledger's LedgerDB in set mode with every metadata store
// committee read failing, after the committee writes, inside a transaction
// that writes more and after the rollback, and requires the answers to be
// the store's.
func TestCommitteeReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	reached, _ := scenario.HideStoreReads(
		"GetConstitution", "GetCommitteeQuorum", "GetCommitteeMembers",
		"GetCommitteeMembersIncludeDeleted", "GetCommitteeMember",
		"IsCommitteeMemberResigned", "GetResignedCommitteeMembers",
		"GetActiveCommitteeMembers", "GetCommitteeAuthorizedCount",
		"GetCommitteeHotAuthorizationsSince",
	)
	ldb := scenario.LedgerDB()
	slots := scenario.Committee()
	requireCommitteeStoreReads(t, ldb, store, slots, "after the writes")
	scenario.CommitteeInsideTxn(slots.NoConfidence + 1)
	requireCommitteeStoreReads(t, ldb, store, slots, "after an abandoned write")
	scenario.RollbackBeforeBoundary()
	requireCommitteeStoreReads(t, ldb, store, slots, "after the rollback")
	require.Zero(t, reached(), "a committee read reached the metadata store")
	verified, mismatches := scenario.CommitteeReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// requireLastEnacted requires the ledger's LedgerDB to name the proposal the
// metadata store names as the last enacted of actionTypes, row ID included.
func requireLastEnacted(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	actionTypes []uint8,
	stage string,
) *models.GovernanceProposal {
	t.Helper()
	want, err := store.GetLastEnactedGovernanceProposal(
		t.Context(),
		actionTypes,
		nil,
	)
	require.NoError(t, err)
	got, err := ldb.GetLastEnactedGovernanceProposal(
		t.Context(),
		actionTypes,
		nil,
	)
	require.NoError(t, err, stage)
	require.Equal(t, want == nil, got == nil, stage)
	if want != nil {
		require.Equal(t, want.ID, got.ID, stage)
		require.Equal(t, want.TxHash, got.TxHash, stage)
		require.Equal(t, want.ActionIndex, got.ActionIndex, stage)
		require.Equal(t, want.ActionType, got.ActionType, stage)
		require.Equal(t, want.EnactedEpoch, got.EnactedEpoch, stage)
		require.Equal(t, want.EnactedSlot, got.EnactedSlot, stage)
	}
	return got
}

// TestLastEnactedProposalReadsFromTheLiveUtxoSet reads the last enacted
// proposal through the ledger's LedgerDB in set mode with the metadata
// store's read failing, while the proposal is written, enacted and rolled
// back, and requires the answers to be the store's.
func TestLastEnactedProposalReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	reached, _ := scenario.HideStoreReads("GetLastEnactedGovernanceProposal")
	ldb := scenario.LedgerDB()
	require.Nil(t, requireLastEnacted(t, ldb, store, []uint8{1}, "before"))
	slots := scenario.Proposals()
	enacted := requireLastEnacted(
		t,
		ldb,
		store,
		[]uint8{1},
		"after the enactment",
	)
	require.NotNil(t, enacted)
	require.Equal(t, slots.P1, enacted.TxHash)
	require.Nil(
		t,
		requireLastEnacted(t, ldb, store, []uint8{0, 2}, "another purpose"),
	)
	scenario.ProposalsInsideTxn(slots.Enact + 1)
	requireLastEnacted(t, ldb, store, []uint8{1}, "after an abandoned write")
	scenario.RollbackBeforeBoundary()
	require.Nil(
		t,
		requireLastEnacted(t, ldb, store, []uint8{1}, "after the rollback"),
	)
	require.Zero(t, reached(), "the read reached the metadata store")
	_, mismatches := scenario.GovernanceReads()
	require.Zero(t, mismatches)
}

var liveStakeReadNames = []string{
	"GetLiveStakeInputsForPools", "GetDelegatedPoolKeyHashes",
	"GetStakeByPools", "GetDRepVotingPower", "GetDRepVotingPowerBatch",
	"GetDRepVotingPowerByType",
}

// TestLiveStakeReadsFromTheLiveUtxoSet reads live stake through the ledger's
// LedgerDB in set mode with the metadata store's live stake reads failing,
// through credits, a withdrawal, vote delegations, an expiration gate, a pool
// reap and a rollback, and requires every answer to be the store's.
func TestLiveStakeReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, true, ledger.UtxoSetReadsSet)
	reached, _ := h.scenario.HideStoreReads(liveStakeReadNames...)
	h.scenario.ApplyBoundary()
	inputs, _ := h.readAll(t, nil)
	got, ok := stakeOf(inputs, h.deleg)
	require.True(t, ok)
	require.Equal(t, uint64(20_000_000), got)
	require.Len(t, inputs, 2)
	h.scenario.Credit(h.deleg, 7)
	inputs, _ = h.readAll(t, nil)
	got, _ = stakeOf(inputs, h.deleg)
	require.Equal(t, uint64(20_000_000+14), got, "reward balance counts")
	require.NoError(t, h.scenario.Withdraw(h.deleg, 5))
	h.readAll(t, nil)
	h.scenario.Reap(h.pool)
	inputs, _ = h.readAll(t, nil)
	require.Empty(t, inputs)
	h.scenario.Expire(h.deleg)
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	h.scenario.VoteDelegate(h.owner, drepKey, 0, 8)
	h.scenario.VoteDelegate(h.deleg, nil, 2, 9)
	owner, err := h.store.GetAccountByCredential(
		t.Context(), h.owner.Tag, h.owner.Key, false, nil,
	)
	require.NoError(t, err)
	h.readAll(t, [][]byte{drepKey}, 1, owner.ExpirationEpoch+1)
	h.scenario.RollbackBeforeBoundary()
	inputs, _ = h.readAll(t, [][]byte{drepKey}, owner.ExpirationEpoch+1)
	require.Len(t, inputs, 2, "the rollback restores the delegations")
	require.Zero(t, reached(), "a live stake read reached the metadata store")
	verified, mismatches := h.scenario.LiveStakeReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// TestLiveStakeReadsStayOnTheStoreWithPendingCredits requires the reads that
// include pending reward credits to reach the metadata store while it holds
// any and the set has not loaded the reward rows that carry them, and the
// reads that do not include them to stay on the set.
func TestLiveStakeReadsStayOnTheStoreWithPendingCredits(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t,
		func(s *utxoset.Set) ledger.UtxoSetWriter {
			return unloadedRewardSet{s}
		},
		ledger.UtxoSetReadsSet,
	)
	round := h.round(11)
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.AddAppliedRewardCreditRound(round, txn))
		require.NoError(t, ldb.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{
				h.output(h.deleg, 11, 9, h.base, round.BoundarySlot),
			}, txn))
	})
	reached, hide := h.scenario.HideStoreReads(liveStakeReadNames...)
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pools := [][]byte{h.pool}
	_, err := ldb.GetLiveStakeInputsForPools(pools, 0, nil)
	require.Error(t, err, "the pending credit is the store's to add")
	_, err = ldb.GetDRepVotingPower(ctx, 0, h.owner.Key, 0, nil)
	require.Error(t, err)
	_, err = ldb.GetDRepVotingPowerBatch(ctx, nil, 0, nil)
	require.Error(t, err)
	_, err = ldb.GetDRepVotingPowerByType(ctx, []uint64{2, 3}, 0, nil)
	require.Error(t, err)
	require.Equal(t, int64(4), reached())
	_, _, err = ldb.GetStakeByPools(ctx, pools, nil)
	require.NoError(t, err, "stake by pools counts no credits")
	_, err = ldb.GetDelegatedPoolKeyHashes(nil)
	require.NoError(t, err)
	require.Equal(t, int64(4), reached())
	hide(false)
	in, err := ldb.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	got, _ := stakeOf(in, h.deleg)
	require.Equal(
		t,
		uint64(20_000_009),
		got,
		"the store adds the pending credit",
	)
}

// TestLiveStakeReadsCountPendingCreditsFromTheLiveUtxoSet requires the reads
// that include pending reward credits to be answered from the set once it
// holds the reward rows, with the same figures as the metadata store, which
// adds the credits itself.
func TestLiveStakeReadsCountPendingCreditsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, ledger.UtxoSetReadsSet)
	round := h.round(11)
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.AddAppliedRewardCreditRound(round, txn))
		require.NoError(t, ldb.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{
				h.output(h.deleg, 11, 9, h.base, round.BoundarySlot),
			}, txn))
	})
	reached, _ := h.scenario.HideStoreReads(slices.Concat(
		liveStakeReadNames, rewardOutputReadNames,
	)...)
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pools := [][]byte{h.pool}
	in, err := ldb.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	got, _ := stakeOf(in, h.deleg)
	require.Equal(t, uint64(20_000_009), got, "the set adds the pending credit")
	_, err = ldb.GetDRepVotingPower(ctx, 0, h.owner.Key, 0, nil)
	require.NoError(t, err)
	_, err = ldb.GetDRepVotingPowerBatch(ctx, nil, 0, nil)
	require.NoError(t, err)
	_, err = ldb.GetDRepVotingPowerByType(ctx, []uint64{2, 3}, 0, nil)
	require.NoError(t, err)
	require.Zero(t, reached(), "a live stake read reached the metadata store")
	hide := h.rewardRows(t, h.store)
	require.Equal(t, hide, h.rewardRows(t, ldb))
}

// boundaryOutcome runs the boundary scenarios against a set in reads mode and
// returns what the metadata store holds after each phase, row IDs aside. In
// set mode every read the set serves fails against the store.
func boundaryOutcome(t *testing.T, reads string) map[string][]string {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set, reads)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	// The harness's stake is rows the set never saw; a node's set holds the
	// outputs the rows came from.
	scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
	var reached func() int64
	if reads == ledger.UtxoSetReadsSet {
		// BumpDormantDRepExpiries reads the dormancy counter from the store
		// around its own write, so that read is not hidden.
		reached, _ = scenario.HideStoreReads(slices.Concat(
			[]string{
				"GetAccountByCredential", "GetAccountsByCredential",
				"GetDrepByCredential", "GetActiveDreps",
				"GetConstitution", "GetCommitteeQuorum", "GetCommitteeMembers",
				"GetCommitteeMembersIncludeDeleted", "GetCommitteeMember",
				"IsCommitteeMemberResigned", "GetResignedCommitteeMembers",
				"GetActiveCommitteeMembers", "GetCommitteeAuthorizedCount",
				"GetCommitteeHotAuthorizationsSince",
			},
			opCertReadNames, liveStakeReadNames, governanceReadNames,
			accountListReadNames, poolListReadNames, rewardOutputReadNames,
			opCertSlotReadNames, registrationReadNames,
			pparamUpdateReadNames,
		)...)
	}
	out := map[string][]string{}
	sorted := func(rows []string) []string {
		slices.Sort(rows)
		return rows
	}
	snapshot := func(phase string) {
		t.Helper()
		var rows []string
		require.NoError(
			t,
			store.IterateAccounts(nil, func(a *models.Account) error {
				rows = append(rows, accountRow(a))
				return nil
			}),
		)
		out[phase+"/accounts"] = sorted(rows)
		rows = nil
		require.NoError(t, store.IterateDreps(nil, func(d *models.Drep) error {
			rows = append(rows, drepRow(d))
			return nil
		}))
		out[phase+"/dreps"] = sorted(rows)
		rows = nil
		epochs, err := store.GetEpochs(nil)
		require.NoError(t, err)
		for _, e := range epochs {
			rows = append(
				rows,
				fmt.Sprintf("epoch %d %d %d", e.EpochId, e.StartSlot, e.EraId),
			)
			for _, typ := range []string{"mark", "set", "go"} {
				snaps, err := store.GetPoolStakeSnapshotsByEpoch(
					e.EpochId,
					typ,
					nil,
				)
				require.NoError(t, err)
				for _, sn := range snaps {
					rows = append(rows, fmt.Sprintf(
						"snapshot %d %s %x %d %d",
						e.EpochId,
						typ,
						sn.PoolKeyHash,
						sn.TotalStake,
						sn.DelegatorCount,
					))
				}
			}
			donations, err := store.SumNetworkDonationsForEpoch(e.EpochId, nil)
			require.NoError(t, err)
			rows = append(
				rows,
				fmt.Sprintf("donations %d %d", e.EpochId, donations),
			)
		}
		state, err := store.GetNetworkState(nil)
		require.NoError(t, err)
		rows = append(rows, fmt.Sprintf("network %d %d %d",
			state.Slot, state.Treasury, state.Reserves))
		out[phase+"/epochs"] = sorted(rows)
		rows = nil
		pools, err := store.ListPoolKeyHashes(nil)
		require.NoError(t, err)
		for _, p := range pools {
			rows = append(rows, fmt.Sprintf("pool %x", p))
		}
		counters, err := store.LatestPoolOpCertSequences(t.Context(), nil)
		require.NoError(t, err)
		for p, n := range counters {
			rows = append(rows, fmt.Sprintf("counter %x %d", p, n))
		}
		out[phase+"/pools"] = sorted(rows)
		rows = nil
		members, err := store.GetCommitteeMembersIncludeDeleted(
			t.Context(),
			nil,
		)
		require.NoError(t, err)
		rows = append(rows, committeeMemberRows(members)...)
		auths, err := store.GetCommitteeHotAuthorizationsSince(
			t.Context(),
			0,
			nil,
		)
		require.NoError(t, err)
		rows = append(rows, committeeAuthRows(auths)...)
		quorum, err := store.GetCommitteeQuorum(t.Context(), nil)
		require.NoError(t, err)
		if quorum != nil {
			rows = append(rows, "quorum "+quorum.String())
		}
		out[phase+"/committee"] = sorted(rows)
		rows = nil
		proposals, err := store.GetGovernanceProposalSet(t.Context(), nil)
		require.NoError(t, err)
		for _, p := range proposals {
			rows = append(
				rows,
				fmt.Sprintf("proposal %x/%d", p.TxHash, p.ActionIndex),
			)
		}
		out[phase+"/proposals"] = sorted(rows)
		rows = nil
		allProposals, err := store.GetAllGovernanceProposals(t.Context(), nil)
		require.NoError(t, err)
		rows = append(rows, proposalRows(allProposals, true)...)
		allVotes, err := store.GetAllGovernanceVotes(t.Context(), nil)
		require.NoError(t, err)
		for _, v := range allVotes {
			rows = append(rows, fmt.Sprintf(
				"%x#%d %s", v.TxHash, v.ActionIndex, voteRow(&v.Vote),
			))
		}
		out[phase+"/governance rows"] = sorted(rows)
	}
	snapshot("start")
	scenario.ApplyBoundary()
	snapshot("boundary")
	var ref models.StakeCredentialRef
	var pool []byte
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			if a.Active && ref.Key == nil {
				ref = models.NewStakeCredentialRef(
					a.CredentialTag,
					a.StakingKey,
				)
			}
			if a.Active && len(a.Pool) > 0 && pool == nil {
				pool = a.Pool
			}
			return nil
		}),
	)
	scenario.Donate(40)
	scenario.Credit(ref, 7)
	require.NoError(t, scenario.Withdraw(ref, 5))
	snapshot("rewards")
	scenario.Reap(pool)
	scenario.Expire(ref)
	scenario.Governance(ref, 9)
	snapshot("governance")
	scenario.Pools(func() {})
	scenario.OpCerts()
	snapshot("pools and op-certs")
	scenario.Committee()
	scenario.Proposals()
	snapshot("committee and proposals")
	scenario.RollbackBeforeBoundary()
	snapshot("rollback")
	if reached != nil {
		require.Zero(t, reached(), "a read the set serves reached the store")
	}
	return out
}

// TestBoundaryScenariosHaveTheSameOutcomeInSetMode runs the boundary, reward,
// DRep, pool, op-cert, committee and governance scenarios and their rollback
// once in verify mode and once in set mode, with the reads the set serves
// failing against the metadata store, and requires the same ledger outcome
// after every phase: the same accounts, DReps, epochs, snapshots, pots, pools,
// counters, committee rows, proposals and votes, row IDs included.
func TestBoundaryScenariosHaveTheSameOutcomeInSetMode(t *testing.T) {
	t.Parallel()
	verify := boundaryOutcome(t, ledger.UtxoSetReadsVerify)
	setMode := boundaryOutcome(t, ledger.UtxoSetReadsSet)
	require.NotEmpty(t, verify["boundary/accounts"])
	require.Equal(t, len(verify), len(setMode))
	for phase, want := range verify {
		require.Equal(t, want, setMode[phase], phase)
	}
}
