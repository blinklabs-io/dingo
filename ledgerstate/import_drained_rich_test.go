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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/drainfix"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

var richAnchorDormant = uint64(3)

var (
	richPool1 = bytes.Repeat([]byte{0x51}, 28)
	richPool2 = bytes.Repeat([]byte{0x52}, 28)
	richDrep1 = bytes.Repeat([]byte{0x61}, 28)
	richDrep2 = bytes.Repeat([]byte{0x62}, 28)
)

func richCertState(t *testing.T, anchor bool) cbor.RawMessage {
	t.Helper()
	return richCertStateOpts(t, anchor, false, false)
}

func richCertStateFuture(t *testing.T, anchor, future bool) cbor.RawMessage {
	t.Helper()
	return richCertStateOpts(t, anchor, future, false)
}

// richCertStateOpts builds the rich cert state. With future set the
// non-anchor snapshot stages a re-registration of pool 0x52 in
// futurePoolParams; with dangling set the anchor's account stays and still delegates to the
// DRep the anchor held and the snapshot no longer registers.
func richCertStateOpts(t *testing.T, anchor, future, dangling bool) cbor.RawMessage {
	t.Helper()
	acct := drainFixtureStakingKey
	pool, drep := richPool2, richDrep2
	if dangling && !anchor {
		acct, drep = drainFixtureAnchorKey, richDrep1
	}
	if anchor {
		acct, pool, drep = drainFixtureAnchorKey, richPool1, richDrep1
	}
	credMap := encodeCredentialMapEntry(t,
		testCredentialKey{Type: 0, Hash: toFixed28(acct)},
		[]any{uint64(4_000_000), uint64(2_000_000), pool, []any{uint64(0), drep}},
	)
	emptyMap := encodeCborMap(t)
	dAccts, err := cbor.Encode([]any{cbor.RawMessage(credMap), cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	dState, err := cbor.Encode([]any{cbor.RawMessage(dAccts)})
	require.NoError(t, err)
	var poolParams, deposits []byte
	if anchor {
		poolParams = encodeCborMap(t, richPool1, testPoolParams(0x51), richPool2, testPoolParams(0x52))
		deposits = encodeCborMap(t, richPool1, testPoolDeposit, richPool2, testPoolDeposit)
	} else {
		poolParams = encodeCborMap(t, richPool2, testPoolParams(0x52))
		deposits = encodeCborMap(t, richPool2, testPoolDeposit)
	}
	futureParams := emptyMap
	if future && !anchor {
		futureParams = encodeCborMap(t, richPool2, testPoolParams(0x70))
	}
	pState := encodeTestPState(t, poolParams, futureParams, emptyMap, deposits)
	drepState := func(delegator []byte) []any {
		return []any{uint64(500), nil, uint64(500_000_000), []any{[]any{uint64(0), delegator}}}
	}
	var drepMap []byte
	if anchor {
		drepMap = encodeCborMap(t,
			[]any{uint64(0), richDrep1}, drepState(acct),
			[]any{uint64(0), richDrep2}, []any{uint64(500), nil, uint64(500_000_000)},
		)
	} else {
		delegator := drepState(acct)
		if dangling {
			delegator = []any{uint64(500), nil, uint64(500_000_000)}
		}
		drepMap = encodeCborMap(t, []any{uint64(0), richDrep2}, delegator)
	}
	dormant := uint64(0)
	if anchor {
		dormant = richAnchorDormant
	}
	vState, err := cbor.Encode([]any{cbor.RawMessage(drepMap), map[any]any{}, dormant})
	require.NoError(t, err)
	out, err := cbor.Encode([]any{cbor.RawMessage(vState), cbor.RawMessage(pState), cbor.RawMessage(dState)})
	require.NoError(t, err)
	return out
}

func richConfig(t *testing.T, db *database.Database, slot uint64, anchor, reconcile bool, set UtxoSetLoader) ImportConfig {
	cfg := drainFixtureConfig(t, db, slot, reconcile, set)
	cfg.State.CertStateData = richCertState(t, anchor)
	return cfg
}

func richConfigFuture(t *testing.T, db *database.Database, slot uint64, anchor, reconcile bool, set UtxoSetLoader) ImportConfig {
	cfg := drainFixtureConfig(t, db, slot, reconcile, set)
	cfg.State.CertStateData = richCertStateFuture(t, anchor, true)
	return cfg
}

func richConfigDangling(t *testing.T, db *database.Database, slot uint64, anchor, reconcile bool, set UtxoSetLoader) ImportConfig {
	cfg := drainFixtureConfig(t, db, slot, reconcile, set)
	cfg.State.CertStateData = richCertStateOpts(t, anchor, false, true)
	return cfg
}

func richUndrained(t *testing.T, anchorSlot, slot uint64) *database.Database {
	db := newDrainFixtureDB(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, anchorSlot, true, false, nil)))
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, slot, false, true, nil)))
	return db
}

func richDrained(t *testing.T, anchorSlot, slot uint64) *drainedFixture {
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, anchorSlot, true, false, set)))
	drainfix.Drain(t, db, set)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, slot, false, true, set)))
	return &drainedFixture{db: db, set: set}
}

// TestDrainedCatchUpRichParity catches a drained database up with a snapshot
// that moves every account, pool and DRep, and compares each family with the
// rows an undrained database holds after the same catch-up.
func TestDrainedCatchUpRichParity(t *testing.T) {
	t.Parallel()
	undrained := richUndrained(t, drainFixtureAnchorSlot, drainFixtureCatchUpSlot)
	f := richDrained(t, drainFixtureAnchorSlot, drainFixtureCatchUpSlot)
	ldb := dbadapter.New(undrained)
	for _, fam := range nodesettings.Families() {
		if len(statedrain.Tables(fam)) == 0 && fam != nodesettings.FamilyEraSwitch {
			continue
		}
		if fam == nodesettings.FamilyGovernance {
			continue
		}
		t.Run(fam.String(), func(t *testing.T) {
			require.NoError(t, ledger.VerifyFamily(t.Context(), fam, ldb, f.set))
		})
	}
	requireSameProposals(t, ldb, f.set)
}

func TestDrainedCatchUpDormancyParity(t *testing.T) {
	t.Parallel()
	undrained := richUndrained(t, drainFixtureAnchorSlot, drainFixtureCatchUpSlot)
	f := richDrained(t, drainFixtureAnchorSlot, drainFixtureCatchUpSlot)
	ldb := dbadapter.New(undrained)
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	want, err := ldb.GetDormantDRepEpochs(t.Context(), txn)
	require.NoError(t, err)
	got, err := f.set.DormantEpochs()
	require.NoError(t, err)
	require.Equal(t, want, got, "dormant DRep epoch counter after catch-up")
}

// TestDrainedCatchUpRefusesASetItCannotRollBackToTheSnapshot catches a drained
// database whose set was bootstrapped past the snapshot's slot up with that
// snapshot and requires the import to refuse it, leaving the set as it was:
// the set holds no history below its bootstrap point to roll back through.
func TestDrainedCatchUpRefusesASetItCannotRollBackToTheSnapshot(t *testing.T) {
	t.Parallel()
	const pastSlot = 300_000
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, pastSlot, true, false, set)))
	drainfix.Drain(t, db, set)

	err := ImportLedgerState(t.Context(), richConfig(t, db, drainFixtureCatchUpSlot, false, true, set))

	require.ErrorContains(t, err, "cannot be rolled back to it")
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(pastSlot), tip.Slot)
	a, err := set.Account(0, drainFixtureAnchorKey)
	require.NoError(t, err)
	require.NotNil(t, a, "the set still holds the state it had")
}

// TestUndrainedSetCatchUpMatchesSQL catches a database that keeps its ledger
// state in SQL and in the set up with a newer snapshot, loading the set from
// the rows the import wrote, and requires every family the set holds to equal
// the rows SQL holds.
func TestUndrainedSetCatchUpMatchesSQL(t *testing.T) {
	t.Parallel()
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, drainFixtureAnchorSlot, true, false, set)))
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, drainFixtureCatchUpSlot, false, true, set)))
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	ldb := dbadapter.New(db)
	for _, fam := range nodesettings.Families() {
		if len(statedrain.Tables(fam)) == 0 && fam != nodesettings.FamilyEraSwitch {
			continue
		}
		if fam == nodesettings.FamilyGovernance {
			continue
		}
		t.Run(fam.String(), func(t *testing.T) {
			require.NoError(t, ledger.VerifyFamily(t.Context(), fam, ldb, set))
		})
	}
	requireSameProposals(t, ldb, set)
}

func verifyEveryFamily(t *testing.T, ldb dbadapter.Adapter, set *utxoset.Set) {
	t.Helper()
	for _, fam := range nodesettings.Families() {
		if len(statedrain.Tables(fam)) == 0 && fam != nodesettings.FamilyEraSwitch {
			continue
		}
		if fam == nodesettings.FamilyGovernance {
			continue
		}
		t.Run(fam.String(), func(t *testing.T) {
			require.NoError(t, ledger.VerifyFamily(t.Context(), fam, ldb, set))
		})
	}
}

// TestDrainedCatchUpKeepsAPendingReRegistration catches a drained database up
// with a snapshot that stages a re-registration of a pool and requires the
// set to hold the registration in force and the staged one apart, as the
// metadata store of an undrained database does.
func TestDrainedCatchUpKeepsAPendingReRegistration(t *testing.T) {
	t.Parallel()
	undrained := newDrainFixtureDB(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, undrained, drainFixtureAnchorSlot, true, false, nil)))
	require.NoError(t, ImportLedgerState(t.Context(), richConfigFuture(t, undrained, drainFixtureCatchUpSlot, false, true, nil)))
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, drainFixtureAnchorSlot, true, false, set)))
	drainfix.Drain(t, db, set)
	require.NoError(t, ImportLedgerState(t.Context(), richConfigFuture(t, db, drainFixtureCatchUpSlot, false, true, set)))

	st, err := set.PoolState(richPool2)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.NotNil(t, st.Future, "the staged re-registration is held")
	require.NotNil(t, st.Params)
	require.Equal(t, bytes.Repeat([]byte{0x71}, 32), st.Future.VrfKeyHash)
	require.Equal(t, bytes.Repeat([]byte{0x53}, 32), st.Params.VrfKeyHash)
	verifyEveryFamily(t, dbadapter.New(undrained), set)
}

// TestFreshSetLoadHoldsAPendingReRegistration imports a snapshot that stages a
// re-registration of a pool, loads a set from the rows the import wrote, and
// requires the set to hold the staged registration beside the one in force.
func TestFreshSetLoadHoldsAPendingReRegistration(t *testing.T) {
	t.Parallel()
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfigFuture(t, db, drainFixtureAnchorSlot, false, false, set)))
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))

	st, err := set.PoolState(richPool2)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.NotNil(t, st.Future, "the staged re-registration is held")
	require.Equal(t, bytes.Repeat([]byte{0x71}, 32), st.Future.VrfKeyHash)
	require.Equal(t, bytes.Repeat([]byte{0x53}, 32), st.Params.VrfKeyHash)
	verifyEveryFamily(t, dbadapter.New(db), set)
}

// TestDrainedCatchUpDanglingDRepDelegationMatchesSQL catches a drained
// database up with a snapshot whose live account still delegates to a DRep
// the snapshot no longer registers, and requires the set to hold the same
// DRep rows and delegator links as an undrained database.
func TestDrainedCatchUpDanglingDRepDelegationMatchesSQL(t *testing.T) {
	t.Parallel()
	undrained := newDrainFixtureDB(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, undrained, drainFixtureAnchorSlot, true, false, nil)))
	require.NoError(t, ImportLedgerState(t.Context(), richConfigDangling(t, undrained, drainFixtureCatchUpSlot, false, true, nil)))
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(t.Context(), richConfig(t, db, drainFixtureAnchorSlot, true, false, set)))
	drainfix.Drain(t, db, set)
	require.NoError(t, ImportLedgerState(t.Context(), richConfigDangling(t, db, drainFixtureCatchUpSlot, false, true, set)))

	verifyEveryFamily(t, dbadapter.New(undrained), set)
}

// A catch-up that merges into a set whose DReps family is drained keeps the
// DRep history rows the set held and adds the snapshot's, so the set holds the
// history an undrained catch-up leaves in the metadata store.
func TestDrainedCatchUpMergeKeepsTheDRepHistory(t *testing.T) {
	t.Parallel()
	undrained := richUndrained(t, drainFixtureAnchorSlot, drainFixtureCatchUpSlot)
	f := richDrained(t, drainFixtureAnchorSlot, drainFixtureCatchUpSlot)

	txn := dbadapter.New(undrained).Begin(t.Context(), false)
	defer txn.Release()
	want, err := dbadapter.New(undrained).DRepHistoryRows(txn)
	require.NoError(t, err)
	require.NotEmpty(t, want.Expiry, "the catch-up leaves expiry history")
	got, err := f.set.DRepHistory()
	require.NoError(t, err)
	require.ElementsMatch(t, want.Expiry, got.Expiry)
	require.ElementsMatch(t, want.Dormancy, got.Dormancy)
	require.ElementsMatch(t, want.Events, got.Events)
	require.ElementsMatch(t, want.Clears, got.Clears)
	lost, err := f.set.DRepHistoryLost()
	require.NoError(t, err)
	require.False(t, lost)
}
