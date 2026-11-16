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
	"context"
	"database/sql"
	"fmt"
	"slices"
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
	"github.com/stretchr/testify/require"
)

// renderLiveStake renders every live stake read of the LedgerDB for the pool
// and DReps, so two runs can be compared by what the ledger would see.
func renderLiveStake(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	pool []byte,
	drepKeys [][]byte,
	expiries ...uint64,
) []string {
	t.Helper()
	ctx := t.Context()
	pools := [][]byte{pool}
	var out []string
	for _, expiry := range append([]uint64{0}, expiries...) {
		inputs, err := ldb.GetLiveStakeInputsForPools(pools, expiry, nil)
		require.NoError(t, err)
		for _, in := range sortedInputs(inputs) {
			out = append(out, fmt.Sprintf(
				"input %d %x/%d %x %d", expiry, in.PoolKeyHash,
				in.CredentialTag, in.StakingKey, in.Stake,
			))
		}
		refs := []models.StakeCredentialRef{}
		for _, key := range drepKeys {
			refs = append(refs, models.NewStakeCredentialRef(0, key))
			power, err := ldb.GetDRepVotingPower(ctx, 0, key, expiry, nil)
			require.NoError(t, err)
			out = append(out, fmt.Sprintf("power %d %x %d", expiry, key, power))
		}
		batch, err := ldb.GetDRepVotingPowerBatch(ctx, refs, expiry, nil)
		require.NoError(t, err)
		for _, ref := range refs {
			out = append(out, fmt.Sprintf(
				"batch %d %x %d", expiry, ref.Key, batch[ref.MapKey()],
			))
		}
		byType, err := ldb.GetDRepVotingPowerByType(
			ctx, []uint64{2, 3}, expiry, nil,
		)
		require.NoError(t, err)
		for _, typ := range []uint64{2, 3} {
			out = append(out, fmt.Sprintf(
				"type %d %d %d", expiry, typ, byType[typ],
			))
		}
	}
	delegated, err := ldb.GetDelegatedPoolKeyHashes(nil)
	require.NoError(t, err)
	slices.SortFunc(delegated, bytes.Compare)
	for _, p := range delegated {
		out = append(out, fmt.Sprintf("delegated %x", p))
	}
	stakes, delegators, err := ldb.GetStakeByPools(ctx, pools, nil)
	require.NoError(t, err)
	out = append(
		out,
		fmt.Sprintf("stake %d", stakes[string(pool)]),
		fmt.Sprintf("delegators %d", delegators[string(pool)]),
	)
	return out
}

func renderBoundaryStake(a ledger.BoundaryStakeAnswers) []string {
	var out []string
	for _, p := range a.Active {
		out = append(out, fmt.Sprintf("active %x", p))
	}
	for _, p := range a.Delegated {
		out = append(out, fmt.Sprintf("boundary delegated %x", p))
	}
	for p, v := range a.Stakes {
		out = append(out, fmt.Sprintf(
			"boundary stake %x %d %d", p, v, a.Delegators[p],
		))
	}
	for _, in := range sortedInputs(a.Inputs) {
		out = append(out, fmt.Sprintf(
			"boundary input %x/%d %x %d",
			in.PoolKeyHash, in.CredentialTag, in.StakingKey, in.Stake,
		))
	}
	for _, in := range sortedInputs(a.Pointers) {
		out = append(out, fmt.Sprintf(
			"boundary pointer %x/%d %x %d",
			in.PoolKeyHash, in.CredentialTag, in.StakingKey, in.Stake,
		))
	}
	slices.Sort(out)
	return out
}

type liveStakeDrainResult struct {
	answers map[string][]string
	guard   *statedrain.TripGuard
	db      *database.Database
	set     *utxoset.Set
	err     error
}

// liveStakeDrainOpts changes a run just before its drain: corrupt edits the
// metadata store through raw SQL or gives the set stake the store lacks, and
// wrap hands the drain a set that differs from the real one.
type liveStakeDrainOpts struct {
	corrupt func(raw *sql.DB, addSetStake func(key []byte, amount uint64))
	wrap    func(*utxoset.Set) ledger.UtxoSetWriter
}

// wrappedDrainSource is drainTestSource over a wrapped set.
type wrappedDrainSource struct {
	drainTestSource
	set ledger.UtxoSetWriter
}

func (s wrappedDrainSource) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.set)
}

func (s wrappedDrainSource) Verify(
	ctx context.Context,
	f nodesettings.Family,
) error {
	return ledger.VerifyFamily(ctx, f, dbadapter.New(s.db), s.set)
}

// runLiveStakeDrain drives stake-changing UTxO writes, delegations, reward
// credits and folds, the epoch boundary's snapshot, DRep voting power and a
// rollback, draining the live-stake family after the first reads when drain
// is set.
func runLiveStakeDrain(
	t *testing.T,
	drain bool,
	opts liveStakeDrainOpts,
) liveStakeDrainResult {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet, guard,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	owner := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x71}, 28))
	deleg := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x72}, 28))
	scenario.AddSetStake(owner.Key, 20_000_000)
	scenario.AddSetStake(deleg.Key, 20_000_000)
	pool := bytes.Repeat([]byte{0xd1}, 28)
	drepKey := bytes.Repeat([]byte{0x5d}, 28)
	res := liveStakeDrainResult{
		answers: map[string][]string{}, guard: guard, db: scenario.DB, set: set,
	}
	phase := func(name string, drepKeys [][]byte, expiries ...uint64) {
		res.answers[name] = renderLiveStake(
			t, scenario.LedgerDB(), pool, drepKeys, expiries...,
		)
	}
	phase("initial", nil)

	if drain {
		if opts.corrupt != nil {
			raw, err := dbtest.RawSQLiteMetadata(t, scenario.DB)
			require.NoError(t, err)
			opts.corrupt(raw, scenario.AddSetStake)
		}
		var src statedrain.Source = drainTestSource{set: set, db: scenario.DB}
		if opts.wrap != nil {
			src = wrappedDrainSource{
				drainTestSource{set: set, db: scenario.DB}, opts.wrap(set),
			}
		}
		var entries []statedrain.Entry
		for _, e := range statedrain.Graduated(src) {
			if e.Family == nodesettings.FamilyLiveStake {
				entries = append(entries, e)
			}
		}
		require.Len(t, entries, 1)
		res.err = statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies},
			Set:   src, Guard: guard, VerifyBeforeDrain: true,
		}, entries)
		if res.err != nil {
			return res
		}
	}
	phase("drained", nil)

	f := scenario.BoundaryStake(0)
	phase("fixture", nil)
	res.answers["boundary fixture"] = renderBoundaryStake(
		scenario.ReadBoundaryStake(f, false),
	)
	scenario.ApplyBoundary()
	phase("boundary", nil)
	scenario.Credit(deleg, 7)
	phase("credit", nil)
	require.NoError(t, scenario.Withdraw(deleg, 5))
	phase("withdrawal", nil)
	scenario.FoldForWithdrawal(deleg)
	phase("fold", nil)
	scenario.Expire(deleg)
	scenario.VoteDelegate(owner, drepKey, 0, 8)
	scenario.VoteDelegate(deleg, nil, 2, 9)
	phase("votes", [][]byte{drepKey}, 1, 5)
	scenario.Reap(pool)
	phase("reap", [][]byte{drepKey}, 1, 5)
	scenario.RollbackBeforeBoundary()
	phase("rollback", [][]byte{drepKey}, 1, 5)
	return res
}

// TestLiveStakeDrainMatchesAnUndrainedRun requires a drained node to answer
// every live stake read as an undrained one does, with no read or write of
// reward_live_stake, and to leave the table empty.
func TestLiveStakeDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	undrained := runLiveStakeDrain(t, false, liveStakeDrainOpts{})
	drained := runLiveStakeDrain(t, true, liveStakeDrainOpts{})
	require.NoError(t, drained.err)
	require.Empty(t, drained.guard.Trips(), "a drained table was touched")
	for name, want := range undrained.answers {
		require.Equal(t, want, drained.answers[name], name)
	}
	require.NotEmpty(t, drained.answers["boundary fixture"])
	require.NotEmpty(t, drained.answers["rollback"])

	rawPlain, err := dbtest.RawSQLiteMetadata(t, undrained.db)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, drained.db)
	require.NoError(t, err)
	require.Positive(t, tableRows(t, rawPlain, "reward_live_stake"))
	require.Zero(t, tableRows(t, raw, "reward_live_stake"))
	mask, err := drained.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyLiveStake))
}

// A set that differs from the metadata store's reward_live_stake, or has not
// loaded the accounts and reward rows it derives live stake from, refuses the
// drain and leaves the rows.
func TestLiveStakeDrainRefusesADisagreement(t *testing.T) {
	t.Parallel()
	owner := bytes.Repeat([]byte{0x71}, 28)
	for _, tc := range []struct {
		name string
		opts liveStakeDrainOpts
		want string
	}{
		{
			"a stored stake figure", liveStakeDrainOpts{
				corrupt: func(raw *sql.DB, _ func([]byte, uint64)) {
					_, err := raw.Exec(
						`UPDATE reward_live_stake SET utxo_stake = '1',
						total_stake = '1' WHERE staking_key = ?`, owner,
					)
					require.NoError(t, err)
				},
			}, "differ",
		},
		{
			"stake only the set holds", liveStakeDrainOpts{
				corrupt: func(_ *sql.DB, add func([]byte, uint64)) {
					add(bytes.Repeat([]byte{0x99}, 28), 5)
				},
			}, "only in the set",
		},
		{
			"a row only the store holds", liveStakeDrainOpts{
				corrupt: func(raw *sql.DB, _ func([]byte, uint64)) {
					_, err := raw.Exec(
						`INSERT INTO reward_live_stake (staking_key,
						credential_tag, utxo_stake, reward_stake, total_stake,
						registered, updated_slot) VALUES (?, 0, '8', '0', '8',
						FALSE, 1)`, bytes.Repeat([]byte{0x98}, 28),
					)
					require.NoError(t, err)
				},
			}, "only in the store",
		},
		{
			"a stored delegation", liveStakeDrainOpts{
				corrupt: func(raw *sql.DB, _ func([]byte, uint64)) {
					_, err := raw.Exec(
						`UPDATE reward_live_stake SET pool_key_hash = ?
						WHERE staking_key = ?`,
						bytes.Repeat([]byte{0xee}, 28), owner,
					)
					require.NoError(t, err)
				},
			}, "differ",
		},
		{
			"reward rows not loaded", liveStakeDrainOpts{
				wrap: func(s *utxoset.Set) ledger.UtxoSetWriter {
					return unloadedRewardSet{s}
				},
			}, "markers not loaded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := runLiveStakeDrain(t, true, tc.opts)
			require.ErrorIs(t, res.err, statedrain.ErrGateRefused)
			require.ErrorContains(t, res.err, tc.want)
			mask, err := res.db.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.False(t, mask.Has(nodesettings.FamilyLiveStake))
			raw, err := dbtest.RawSQLiteMetadata(t, res.db)
			require.NoError(t, err)
			require.Positive(t, tableRows(t, raw, "reward_live_stake"))
			require.Empty(t, res.guard.Trips())
		})
	}
}

// With the family drained and a set that cannot serve live stake, every live
// stake read fails rather than answer from the empty table.
func TestDrainedLiveStakeReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	// The set never loads the metadata store's accounts.
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyLiveStake,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyLiveStake),
	)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	pools := [][]byte{bytes.Repeat([]byte{0xd1}, 28)}
	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x5d}, 28)),
	}
	_, err = ldb.GetLiveStakeInputsForPools(pools, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetDelegatedPoolKeyHashes(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.GetStakeByPools(ctx, pools, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetDRepVotingPower(ctx, 0, refs[0].Key, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetDRepVotingPowerBatch(ctx, refs, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetDRepVotingPowerByType(ctx, []uint64{2}, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}

// A drained database writes no reward_live_stake row through blocks, a
// rollback, or a resume from a set behind the ledger, while the set holds the
// stake an undrained database's rows hold.
func TestLiveStakeDrainedBlocksRollbackAndResumeWriteNoRows(t *testing.T) {
	t.Parallel()
	setStake := func(set *utxoset.Set) map[string]string {
		got := map[string]string{}
		require.NoError(t, set.EachCredentialStake(
			func(tag uint8, hash []byte, n uint64) error {
				got[fmt.Sprintf("%d/%X", tag, hash)] = fmt.Sprint(n)
				return nil
			},
		))
		return got
	}
	run := func(t *testing.T, drained bool) (
		*database.Database, *utxoset.Set, ledger.ShadowScenario,
	) {
		t.Helper()
		set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = set.Close() })
		var db *database.Database
		attach := func(d *database.Database) { db = d }
		var scenario ledger.ShadowScenario
		if drained {
			scenario = ledger.RunUtxoSetOnlyDrainedScenarioForTest(
				t, set,
				nodesettings.FamilyMask(0).With(nodesettings.FamilyLiveStake),
				attach,
			)
		} else {
			scenario = ledger.RunUtxoSetOnlyScenarioForTest(t, set, attach)
		}
		return db, set, scenario
	}

	plainDB, plainSet, plain := run(t, false)
	rawPlain, err := dbtest.RawSQLiteMetadata(t, plainDB)
	require.NoError(t, err)
	require.Positive(t, tableRows(t, rawPlain, "reward_live_stake"))
	require.NotEmpty(t, setStake(plainSet))

	db, set, scenario := run(t, true)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	require.Zero(t, tableRows(t, raw, "reward_live_stake"), "after the blocks")
	require.Equal(t, setStake(plainSet), setStake(set))

	// The same rollback an undrained database refreshes the rows of.
	behindAt := scenario.Points[len(scenario.Points)-2]
	require.NoError(t, scenario.Rollback(behindAt))
	require.Zero(t, tableRows(t, raw, "reward_live_stake"), "after a rollback")
	require.NoError(t, plain.Rollback(behindAt))
	require.Equal(t, setStake(plainSet), setStake(set))

	// The set falls behind the ledger and the ledger resumes it.
	resumeAt := scenario.Points[1]
	_, err = set.Rollback(resumeAt)
	require.NoError(t, err)
	resumed, err := scenario.Resume(t.Context(), resumeAt)
	require.NoError(t, err)
	require.True(t, resumed)
	require.NotEmpty(t, setStake(set))
	require.Zero(t, tableRows(t, raw, "reward_live_stake"), "after a resume")

	_, err = plainSet.Rollback(resumeAt)
	require.NoError(t, err)
	resumed, err = plain.Resume(t.Context(), resumeAt)
	require.NoError(t, err)
	require.True(t, resumed)
	require.Equal(t, setStake(plainSet), setStake(set))
	require.Positive(t, tableRows(t, rawPlain, "reward_live_stake"))
}

// The pool relay provider's stake lookup, which a Blockfrost-style relay list
// reads, is answered from the set once the family is drained.
func TestRelayProviderStakeIsServedFromTheSetOnceDrained(t *testing.T) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0xd1}, 28)
	stake := func(drain bool) (map[string]uint64, []statedrain.Trip) {
		set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
		require.NoError(t, err)
		t.Cleanup(func() { _ = set.Close() })
		guard := statedrain.NewTripGuard()
		scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
			t, set, ledger.UtxoSetReadsSet, guard,
		)
		require.NoError(t, set.LoadLedgerState(
			t.Context(), dbadapter.New(scenario.DB),
		))
		scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
		scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
		if drain {
			src := drainTestSource{set: set, db: scenario.DB}
			var entries []statedrain.Entry
			for _, e := range statedrain.Graduated(src) {
				if e.Family == nodesettings.FamilyLiveStake {
					entries = append(entries, e)
				}
			}
			require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{scenario.DB, scenario.DrainFamilies},
				Set:   src, Guard: guard, VerifyBeforeDrain: true,
			}, entries))
		}
		got, err := scenario.RelayProviderStake([][]byte{pool})
		require.NoError(t, err)
		return got, guard.Trips()
	}
	want, _ := stake(false)
	require.Equal(t, uint64(40_000_000), want[string(pool)])
	got, trips := stake(true)
	require.Empty(t, trips)
	require.Equal(t, want, got)
}
