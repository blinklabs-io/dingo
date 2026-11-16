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

package statedrain_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// testFamily is a family only these tests drain: a bit inside the known mask
// that no release graduates.
const testFamily = nodesettings.FamilyNonces

type fakeTable struct {
	rows int64
}

// fakeStore is an in-memory statedrain.Store. failAfter, when positive,
// makes the DeleteLedgerStateRows call after that many succeed return an
// error, as an interruption between chunks does.
type fakeStore struct {
	sqlTip    ocommon.Point
	latch     nodesettings.FamilyMask
	syncState map[string]string
	tables    map[string]*fakeTable
	deletes   int
	failAfter int
	// failKeyDelete makes DeleteSyncState fail for that key.
	failKeyDelete string
	recorded      []nodesettings.Family
}

var errInterrupted = errors.New("interrupted")

func newFakeStore(rows int64) *fakeStore {
	return &fakeStore{
		sqlTip:    ocommon.Point{Slot: 100, Hash: []byte{1}},
		syncState: map[string]string{},
		tables:    map[string]*fakeTable{"t1": {rows}, "t2": {rows}},
	}
}

func (s *fakeStore) SQLTip(context.Context) (ocommon.Point, error) {
	return s.sqlTip, nil
}

func (s *fakeStore) DrainedFamilies(
	context.Context,
) (nodesettings.FamilyMask, error) {
	return s.latch, nil
}

func (s *fakeStore) RecordDrainedFamily(
	_ context.Context,
	f nodesettings.Family,
) error {
	s.latch = s.latch.With(f)
	s.recorded = append(s.recorded, f)
	return nil
}

func (s *fakeStore) DeleteLedgerStateRows(
	_ context.Context,
	table, _ string,
	limit int,
) (int64, error) {
	if s.failAfter > 0 && s.deletes >= s.failAfter {
		return 0, errInterrupted
	}
	s.deletes++
	t := s.tables[table]
	n := min(t.rows, int64(limit))
	t.rows -= n
	return n, nil
}

func (s *fakeStore) GetSyncState(
	_ context.Context,
	key string,
) (string, error) {
	return s.syncState[key], nil
}

func (s *fakeStore) SetSyncState(_ context.Context, key, value string) error {
	s.syncState[key] = value
	return nil
}

func (s *fakeStore) DeleteSyncState(_ context.Context, key string) error {
	if key == s.failKeyDelete {
		return errInterrupted
	}
	delete(s.syncState, key)
	return nil
}

// ListSyncStateKeys makes fakeStore a statedrain.SyncKeyLister.
func (s *fakeStore) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	var keys []string
	for k := range s.syncState {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

type fakeSet struct {
	tip ocommon.Point
	ok  bool
}

func (s fakeSet) Tip() (ocommon.Point, bool, error) { return s.tip, s.ok, nil }

type recordingGuard struct {
	armed map[nodesettings.Family][]string
}

func (g *recordingGuard) Arm(f nodesettings.Family, tables []string) {
	if g.armed == nil {
		g.armed = map[nodesettings.Family][]string{}
	}
	g.armed[f] = tables
}

func currentSet() fakeSet {
	return fakeSet{tip: ocommon.Point{Slot: 100, Hash: []byte{1}}, ok: true}
}

func testEntry() statedrain.Entry {
	return statedrain.Entry{
		Family: testFamily,
		Tables: []statedrain.Table{
			{Name: "t1", Key: "id"},
			{Name: "t2", Key: "id"},
		},
		Loaded: func() (bool, error) { return true, nil },
		Verify: func(context.Context) error { return nil },
	}
}

type fakeSource struct {
	fakeSet
	loaded   map[nodesettings.Family]bool
	verified []nodesettings.Family
}

func (s *fakeSource) Loaded(f nodesettings.Family) (bool, error) {
	return s.loaded[f], nil
}

func (s *fakeSource) Verify(_ context.Context, f nodesettings.Family) error {
	s.verified = append(s.verified, f)
	return nil
}

func TestGraduatedListNamesTheFamiliesAndTheirTables(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		fakeSet: currentSet(),
		loaded:  map[nodesettings.Family]bool{nodesettings.FamilyNonces: true},
	}
	entries := statedrain.Graduated(src)
	var families []nodesettings.Family
	tables := map[nodesettings.Family][]string{}
	for _, e := range entries {
		families = append(families, e.Family)
		for _, tb := range e.Tables {
			tables[e.Family] = append(tables[e.Family], tb.Name)
		}
	}
	require.Equal(t, []nodesettings.Family{
		nodesettings.FamilyNonces,
		nodesettings.FamilyEraSwitch,
		nodesettings.FamilyNetworkDonations,
		nodesettings.FamilyLiveStake,
		nodesettings.FamilyCommitteeConstitution,
		nodesettings.FamilyGovernance,
		nodesettings.FamilyPParamsUpdates,
		nodesettings.FamilyMIRGenesis,
		nodesettings.FamilyRewardBlocksFees,
		nodesettings.FamilyRewardInputs,
		nodesettings.FamilyOpCerts,
		nodesettings.FamilyDReps,
		nodesettings.FamilyAccounts,
		nodesettings.FamilyPoolsHistory,
		nodesettings.FamilyRewardOutputs,
		nodesettings.FamilySummaries,
		nodesettings.FamilyEpochs,
	}, families)
	require.Equal(t, []string{"epoch_summary"},
		tables[nodesettings.FamilySummaries])
	require.Equal(t, []string{"epoch"}, tables[nodesettings.FamilyEpochs])
	require.Equal(t, []string{"block_nonce"}, tables[nodesettings.FamilyNonces])
	require.Equal(t, []string{"reward_live_stake"},
		tables[nodesettings.FamilyLiveStake])
	require.Equal(t, []string{
		"auth_committee_hot", "resign_committee_cold",
		"committee_member", "committee_quorum", "constitution",
	}, tables[nodesettings.FamilyCommitteeConstitution])
	require.Empty(t, tables[nodesettings.FamilyEraSwitch],
		"the era switch is a sync key, not a table")
	require.Equal(t, []string{statedrain.EraSwitchSyncKey}, entries[1].SyncKeys)
	require.Equal(t, []string{"network_donation", "network_state"},
		tables[nodesettings.FamilyNetworkDonations])
	require.Equal(t, []string{"pparam_update", "pparams"},
		tables[nodesettings.FamilyPParamsUpdates])
	require.Equal(t,
		[]string{
			statedrain.SyntheticV2SyncKey,
			statedrain.SyntheticV2ClearedEpochSyncKey,
		},
		entries[6].SyncKeys)
	require.Equal(t, []string{"genesis_delegation"},
		tables[nodesettings.FamilyMIRGenesis])
	require.Equal(t, []string{
		"reward_stake_input", "reward_pool_input", "reward_seed_failure",
		"reward_snapshot", "reward_ada_pots",
	}, tables[nodesettings.FamilyRewardInputs])
	require.Equal(t,
		[]string{statedrain.RewardStakeInputsPendingSyncKey},
		entries[9].SyncKeys)
	require.Equal(t, []string{
		"imported_pool_block_count", "imported_epoch_block_total",
	}, tables[nodesettings.FamilyRewardBlocksFees])
	require.Equal(t, []string{"pool_opcert_sequence"},
		tables[nodesettings.FamilyOpCerts])
	require.Equal(t, []string{
		"drep_delegator", "drep_expiry_history", "drep_dormancy_history",
		"drep_dormancy_state", "drep_expiry_epoch_event",
		"account_drep_clear", "drep",
	}, tables[nodesettings.FamilyDReps])
	require.Equal(t, []string{
		"account_reward_delta", "account_withdrawal_witness",
		"account_import_baseline", "registration", "deregistration",
		"stake_registration", "stake_deregistration", "account",
	}, tables[nodesettings.FamilyAccounts])
	require.Equal(t, []string{
		"pool_registration_owner", "pool_registration_relay",
		"pool_retirement", "pool_registration", "pool",
	}, tables[nodesettings.FamilyPoolsHistory])
	require.Equal(t, []string{
		"reward_account_output", "reward_pool_output", "reward_credit_round",
	}, tables[nodesettings.FamilyRewardOutputs])
	outputs := entries[len(entries)-3]
	require.Equal(t, nodesettings.FamilyRewardOutputs, outputs.Family)
	require.Equal(t,
		[]string{statedrain.RewardEligibilityRecheckSyncKey},
		outputs.SyncKeys)
	require.Equal(t,
		[]string{statedrain.RewardPrecomputeCursorSyncPrefix},
		outputs.SyncKeyPrefixes)
	loaded, err := entries[0].Loaded()
	require.NoError(t, err)
	require.True(t, loaded)
	loaded, err = entries[2].Loaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, entries[2].Verify(t.Context()))
	require.Equal(t, []nodesettings.Family{
		nodesettings.FamilyNetworkDonations,
	}, src.verified)
}

func TestRunDrainsAndRecordsTheBit(t *testing.T) {
	t.Parallel()
	store := newFakeStore(12)
	guard := &recordingGuard{}
	env := statedrain.Env{
		Store: store, Set: currentSet(), Guard: guard, ChunkRows: 5,
	}
	require.NoError(
		t,
		statedrain.Run(t.Context(), env, []statedrain.Entry{testEntry()}),
	)

	require.Zero(t, store.tables["t1"].rows)
	require.Zero(t, store.tables["t2"].rows)
	require.True(t, store.latch.Has(testFamily))
	require.Equal(t, []nodesettings.Family{testFamily}, store.recorded)
	require.Empty(t, store.syncState, "the migration marker is cleared")
	require.Equal(t, []string{"t1", "t2"}, guard.armed[testFamily])
}

func TestRunRefusesEachUnmetPrecondition(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mutate func(*statedrain.Env, *statedrain.Entry, *fakeStore)
		verify bool
	}{
		"markers not loaded": {
			mutate: func(_ *statedrain.Env, e *statedrain.Entry, _ *fakeStore) {
				e.Loaded = func() (bool, error) { return false, nil }
			},
		},
		"no marker check declared": {
			mutate: func(_ *statedrain.Env, e *statedrain.Entry, _ *fakeStore) {
				e.Loaded = nil
			},
		},
		"set tip behind the SQL tip": {
			mutate: func(env *statedrain.Env, _ *statedrain.Entry, _ *fakeStore) {
				env.Set = fakeSet{
					tip: ocommon.Point{Slot: 99, Hash: []byte{1}}, ok: true,
				}
			},
		},
		"same slot, different hash": {
			mutate: func(env *statedrain.Env, _ *statedrain.Entry, _ *fakeStore) {
				env.Set = fakeSet{
					tip: ocommon.Point{Slot: 100, Hash: []byte{2}}, ok: true,
				}
			},
		},
		"set has recorded no tip": {
			mutate: func(env *statedrain.Env, _ *statedrain.Entry, _ *fakeStore) {
				env.Set = fakeSet{}
			},
		},
		"verification fails": {
			verify: true,
			mutate: func(_ *statedrain.Env, e *statedrain.Entry, _ *fakeStore) {
				e.Verify = func(context.Context) error {
					return errors.New("row differs")
				}
			},
		},
		"verification requested but not declared": {
			verify: true,
			mutate: func(_ *statedrain.Env, e *statedrain.Entry, _ *fakeStore) {
				e.Verify = nil
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newFakeStore(3)
			entry := testEntry()
			env := statedrain.Env{
				Store: store, Set: currentSet(), VerifyBeforeDrain: tc.verify,
			}
			tc.mutate(&env, &entry, store)
			err := statedrain.Run(t.Context(), env, []statedrain.Entry{entry})
			require.ErrorIs(t, err, statedrain.ErrGateRefused)
			require.Zero(t, store.deletes, "no row is deleted")
			require.Zero(t, store.latch)
			require.Empty(t, store.syncState, "no marker is written")
		})
	}
}

func TestRunSkipsVerifyUnlessAsked(t *testing.T) {
	t.Parallel()
	store := newFakeStore(1)
	entry := testEntry()
	entry.Verify = func(context.Context) error {
		return errors.New("must not run")
	}
	env := statedrain.Env{Store: store, Set: currentSet()}
	require.NoError(
		t,
		statedrain.Run(t.Context(), env, []statedrain.Entry{entry}),
	)
	require.True(t, store.latch.Has(testFamily))
}

func TestRunResumesAnInterruptedDrain(t *testing.T) {
	t.Parallel()
	store := newFakeStore(12)
	store.failAfter = 4 // t1 in three chunks, then one chunk of t2
	entry := testEntry()
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	err := statedrain.Run(t.Context(), env, []statedrain.Entry{entry})
	require.ErrorIs(t, err, errInterrupted)
	require.Zero(t, store.latch, "the bit waits for the last delete")
	require.NotEmpty(t, store.syncState, "the marker survives the interruption")
	require.Positive(t, store.tables["t2"].rows)

	// Resuming must not re-run the gate: the set tip no longer matters once
	// rows are gone, and verification would see the deleted rows as a
	// difference.
	store.failAfter = 0
	env.Set = fakeSet{}
	entry.Loaded = func() (bool, error) { return false, nil }
	entry.Verify = func(context.Context) error { return errors.New("partial") }
	env.VerifyBeforeDrain = true
	require.NoError(
		t,
		statedrain.Run(t.Context(), env, []statedrain.Entry{entry}),
	)
	require.Zero(t, store.tables["t1"].rows)
	require.Zero(t, store.tables["t2"].rows)
	require.True(t, store.latch.Has(testFamily))
	require.Empty(t, store.syncState)
}

func TestRunIsIdempotent(t *testing.T) {
	t.Parallel()
	store := newFakeStore(7)
	guard := &recordingGuard{}
	env := statedrain.Env{
		Store: store, Set: currentSet(), Guard: guard, ChunkRows: 5,
	}
	entries := []statedrain.Entry{testEntry()}
	require.NoError(t, statedrain.Run(t.Context(), env, entries))
	deletes := store.deletes

	// A second start finds the bit and touches nothing, even when the set
	// has moved on.
	env.Set = fakeSet{}
	guard.armed = nil
	require.NoError(t, statedrain.Run(t.Context(), env, entries))
	require.Equal(t, deletes, store.deletes)
	require.Equal(t, []nodesettings.Family{testFamily}, store.recorded)
	require.Contains(
		t,
		guard.armed,
		testFamily,
		"the guard is armed on restart",
	)
}

func TestRunRefusesALatchBitThisBinaryDoesNotKnow(t *testing.T) {
	t.Parallel()
	store := newFakeStore(1)
	store.latch = nodesettings.FamilyMask(1) << 40
	env := statedrain.Env{Store: store, Set: currentSet()}
	err := statedrain.Run(t.Context(), env, nil)
	require.ErrorContains(t, err, "newer release")
}

func TestRunStopsOnCancelledContextBetweenChunks(t *testing.T) {
	t.Parallel()
	store := newFakeStore(100)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	err := statedrain.Run(ctx, env, []statedrain.Entry{testEntry()})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, store.latch)
}

func syncKeyEntry() statedrain.Entry {
	e := testEntry()
	e.SyncKeys = []string{"owned"}
	return e
}

func TestRunDeletesTheFamilysSyncKeys(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["owned"] = "v"
	store.syncState["other"] = "kept"
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	require.NoError(
		t,
		statedrain.Run(t.Context(), env, []statedrain.Entry{syncKeyEntry()}),
	)
	require.Equal(t, map[string]string{"other": "kept"}, store.syncState)
	require.True(t, store.latch.Has(testFamily))
}

func TestRunRefusedGateKeepsTheSyncKeys(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["owned"] = "v"
	entry := syncKeyEntry()
	entry.Verify = func(context.Context) error { return errors.New("differ") }
	env := statedrain.Env{
		Store: store, Set: currentSet(), VerifyBeforeDrain: true,
	}
	err := statedrain.Run(t.Context(), env, []statedrain.Entry{entry})
	require.ErrorIs(t, err, statedrain.ErrGateRefused)
	require.Equal(t, "v", store.syncState["owned"])
	require.Zero(t, store.latch)
}

// An interruption between the tables and the keys leaves the marker, and the
// next start deletes the key without running the gate again.
func TestRunResumesAnInterruptedSyncKeyDeletion(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["owned"] = "v"
	store.failKeyDelete = "owned"
	entry := syncKeyEntry()
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	err := statedrain.Run(t.Context(), env, []statedrain.Entry{entry})
	require.ErrorIs(t, err, errInterrupted)
	require.Zero(t, store.latch, "the bit waits for the last delete")
	require.Equal(t, "v", store.syncState["owned"])
	require.NotEmpty(
		t,
		store.syncState[statedrain.MarkerPrefix+testFamily.String()],
	)

	store.failKeyDelete = ""
	env.Set = fakeSet{}
	env.VerifyBeforeDrain = true
	entry.Verify = func(context.Context) error { return errors.New("partial") }
	require.NoError(
		t,
		statedrain.Run(t.Context(), env, []statedrain.Entry{entry}),
	)
	require.Empty(t, store.syncState)
	require.True(t, store.latch.Has(testFamily))
}

func prefixEntry(prefixes ...string) statedrain.Entry {
	e := testEntry()
	e.SyncKeyPrefixes = prefixes
	return e
}

// Every key under a named prefix goes, a key outside it stays, and the
// drain's own marker survives a prefix that would match it.
func TestRunDeletesSyncKeysByPrefix(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["fam.1"] = "a"
	store.syncState["fam.2"] = "b"
	store.syncState["famous"] = "c"
	store.syncState["other"] = "kept"
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	require.NoError(
		t,
		statedrain.Run(t.Context(), env, []statedrain.Entry{
			prefixEntry("fam."),
		}),
	)
	require.Equal(
		t, map[string]string{"famous": "c", "other": "kept"}, store.syncState,
	)
	require.True(t, store.latch.Has(testFamily))

}

// failRecordStore fails the latch write that follows the key deletes.
type failRecordStore struct{ *fakeStore }

func (s failRecordStore) RecordDrainedFamily(
	context.Context,
	nodesettings.Family,
) error {
	return errInterrupted
}

// A prefix that matches the drain's own marker leaves it, so an interruption
// before the latch write still resumes without the gate.
func TestRunKeepsItsMarkerWhenAPrefixMatchesIt(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["x"] = "gone"
	env := statedrain.Env{
		Store: failRecordStore{store}, Set: currentSet(), ChunkRows: 5,
	}
	err := statedrain.Run(t.Context(), env, []statedrain.Entry{
		prefixEntry("ledger.", "x"),
	})
	require.ErrorIs(t, err, errInterrupted)
	require.Equal(t, map[string]string{
		statedrain.MarkerPrefix + testFamily.String(): "1",
	}, store.syncState)
}

func TestRunRefusesAnEmptySyncKeyPrefixAndKeepsTheKeys(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["keep"] = "v"
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	err := statedrain.Run(
		t.Context(), env, []statedrain.Entry{prefixEntry("")},
	)
	require.ErrorIs(t, err, statedrain.ErrGateRefused)
	require.Equal(t, "v", store.syncState["keep"])
	require.Zero(t, store.latch)
}

// bareStore hides fakeStore's key listing.
type bareStore struct{ statedrain.Store }

func TestRunRefusesSyncKeyPrefixesWithoutAKeyLister(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	env := statedrain.Env{
		Store: bareStore{store}, Set: currentSet(), ChunkRows: 5,
	}
	err := statedrain.Run(
		t.Context(), env, []statedrain.Entry{prefixEntry("fam.")},
	)
	require.ErrorIs(t, err, statedrain.ErrGateRefused)
	require.Zero(t, store.latch)
}

// An interruption while deleting by prefix leaves the marker, so the next
// start finishes without the gate.
func TestRunResumesAnInterruptedPrefixDeletion(t *testing.T) {
	t.Parallel()
	store := newFakeStore(3)
	store.syncState["fam.1"] = "a"
	store.failKeyDelete = "fam.1"
	env := statedrain.Env{Store: store, Set: currentSet(), ChunkRows: 5}
	entry := prefixEntry("fam.")
	err := statedrain.Run(t.Context(), env, []statedrain.Entry{entry})
	require.ErrorIs(t, err, errInterrupted)
	require.Zero(t, store.latch)

	store.failKeyDelete = ""
	env.Set = fakeSet{}
	env.VerifyBeforeDrain = true
	entry.Verify = func(context.Context) error { return errors.New("partial") }
	require.NoError(
		t, statedrain.Run(t.Context(), env, []statedrain.Entry{entry}),
	)
	require.Empty(t, store.syncState)
	require.True(t, store.latch.Has(testFamily))
}
