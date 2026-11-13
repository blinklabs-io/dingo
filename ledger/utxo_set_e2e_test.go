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
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// recordingSet applies each commit's batch to a set and keeps a copy.
type recordingSet struct {
	*utxoset.Set
	mu      sync.Mutex
	batches []recordedBatch
}

type recordedBatch struct {
	tip     *ocommon.Point
	changes []ledger.UtxoEvent
}

func (r *recordingSet) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	r.mu.Lock()
	r.batches = append(r.batches, recordedBatch{tip: tip, changes: changes})
	r.mu.Unlock()
	return r.Set.ApplyUtxoChanges(tip, changes)
}

// TestLiveUtxoSetFollowsTheLedgerAndResumes applies real blocks, including an
// invalid transaction's collateral return, with the live UTxO set attached
// and no event bus. It requires the set to equal the metadata store's live
// set at the ledger tip after applying and after a rollback. It then replays
// the recorded commits into a second set up to an earlier block, which is how
// a set left behind by a crash looks, resumes the ledger from that set's tip,
// and requires the two to agree there.
func TestLiveUtxoSetFollowsTheLedgerAndResumes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{types.StorageModeCore, types.StorageModeAPI} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			rec := &recordingSet{Set: set}
			var db *database.Database
			scenario := ledger.RunShadowScenarioForTest(
				t,
				mode,
				nil,
				rec,
				func(d *database.Database) { db = d },
			)
			require.Len(t, scenario.Points, 5)
			requireSetMatches(t, set, db, scenario.Points[4])
			require.NoError(t, scenario.Rollback(scenario.Points[3]))
			requireSetMatches(t, set, db, scenario.Points[3])

			// A second set that took the commits up to Points[1] only.
			behind, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = behind.Close() })
			rec.mu.Lock()
			batches := append([]recordedBatch(nil), rec.batches...)
			rec.mu.Unlock()
			reached := false
			for _, b := range batches {
				require.NoError(t, behind.ApplyUtxoChanges(b.tip, b.changes))
				if b.tip != nil && b.tip.Slot == scenario.Points[1].Slot {
					reached = true
					break
				}
			}
			require.True(t, reached, "no commit recorded Points[1] as its tip")

			require.NoError(t, behind.Reconcile(
				t.Context(),
				dbadapter.New(db),
				scenario.Resume,
			))
			requireSetMatches(t, behind, db, scenario.Points[1])
			requireSetMatches(t, set, db, scenario.Points[1])
		})
	}
}

func requireSetMatches(
	t *testing.T,
	set *utxoset.Set,
	db *database.Database,
	at ocommon.Point,
) {
	t.Helper()
	ldb := dbadapter.New(db)
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	ledgerTip, err := ldb.GetTip(txn)
	require.NoError(t, err)
	require.Equal(t, at, ledgerTip.Point, "ledger tip")
	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, at.Slot, setTip.Slot, "set tip")
	require.Equal(t, at.Hash, setTip.Hash, "set tip")
	var live uint64
	require.NoError(t, ldb.IterateLiveUtxos(
		t.Context(),
		txn,
		func(u *models.Utxo) error {
			live++
			got, ok, err := set.Get(u.TxId, u.OutputIdx)
			require.NoError(t, err)
			require.True(t, ok, "live output %x#%d missing from the set",
				u.TxId, u.OutputIdx)
			require.Equal(t, u.Cbor, got)
			return nil
		},
	))
	require.Positive(t, live)
	n, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, live, n, "the set holds outputs the store does not")
}

// TestValidationReadsFromTheLiveUtxoSet resolves every output the scenario
// left live, and every one it spent, the way validation does. In verify mode
// every answer must agree with the metadata store, and in set mode the set's
// answers must be the store's.
func TestValidationReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{ledger.UtxoSetReadsVerify, ledger.UtxoSetReadsSet} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			rec := &recordingSet{Set: set}
			var db *database.Database
			scenario := ledger.RunUtxoSetScenarioForTest(
				t,
				types.StorageModeCore,
				nil,
				rec,
				reads,
				func(d *database.Database) { db = d },
			)
			ldb := dbadapter.New(db)
			var refs []models.UtxoId
			want := map[string][]byte{}
			require.NoError(t, ldb.IterateLiveUtxos(
				t.Context(), nil,
				func(u *models.Utxo) error {
					refs = append(refs, models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx})
					want[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
					return nil
				},
			))
			spent := 0
			for _, b := range rec.batches {
				for _, c := range b.changes {
					for _, r := range c.Removed {
						if _, live := want[fmt.Sprintf("%x#%d", r.Hash, r.Idx)]; !live {
							refs = append(refs, r)
							spent++
						}
					}
				}
			}
			require.Positive(t, spent, "the scenario spent nothing")

			got, err := scenario.ReadsForValidation(refs)
			require.NoError(t, err)
			gotByRef := map[string][]byte{}
			for _, u := range got {
				gotByRef[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
			}
			require.Equal(t, want, gotByRef)
			verified, mismatches := scenario.UtxoSetReads()
			if reads == ledger.UtxoSetReadsVerify {
				require.InDelta(t, float64(len(refs)), verified, 0)
			}
			require.Zero(t, mismatches)
		})
	}
}

// TestLiveUtxoSetStakeMatchesTheStoresRows requires the live UTxO set's
// per-credential stake to equal the sum of the metadata store's live rows by
// (credential_tag, staking_key), which is what reward_live_stake.utxo_stake
// holds, after applying real blocks and after a rollback.
func TestLiveUtxoSetStakeMatchesTheStoresRows(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	var db *database.Database
	scenario := ledger.RunShadowScenarioForTest(
		t,
		types.StorageModeCore,
		nil,
		set,
		func(d *database.Database) { db = d },
	)
	requireStakeMatches(t, set, db)
	require.NoError(t, scenario.Rollback(scenario.Points[2]))
	requireStakeMatches(t, set, db)
}

func requireStakeMatches(t *testing.T, set *utxoset.Set, db *database.Database) {
	t.Helper()
	ldb := dbadapter.New(db)
	want := map[string]uint64{}
	require.NoError(t, ldb.IterateLiveUtxos(
		t.Context(), nil,
		func(u *models.Utxo) error {
			if len(u.StakingKey) > 0 {
				want[fmt.Sprintf("%d/%x", u.CredentialTag, u.StakingKey)] += uint64(u.Amount)
			}
			return nil
		},
	))
	require.NotEmpty(t, want, "the scenario holds no staked output")
	got := map[string]uint64{}
	require.NoError(t, set.EachCredentialStake(
		func(tag uint8, hash []byte, n uint64) error {
			got[fmt.Sprintf("%d/%x", tag, hash)] = n
			return nil
		},
	))
	require.Equal(t, want, got)
}

// TestUtxoSetOnlyMatchesTheMetadataStore applies the same real blocks, and
// the same rollback, once with the metadata store holding the UTxO set and
// once with the live UTxO set as its only copy. Everything outside the UTxO
// set must be identical, and the only copy must equal the first run's live
// utxo rows and their stake.
func TestUtxoSetOnlyMatchesTheMetadataStore(t *testing.T) {
	t.Parallel()
	open := func() *utxoset.Set {
		s, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	withRows, only := open(), open()
	var rowsDB, onlyDB *database.Database
	rowsRun := ledger.RunShadowScenarioForTest(
		t, types.StorageModeCore, nil, withRows,
		func(d *database.Database) { rowsDB = d },
	)
	onlyRun := ledger.RunUtxoSetOnlyScenarioForTest(
		t, only, func(d *database.Database) { onlyDB = d },
	)
	compare := func(at ocommon.Point) {
		t.Helper()
		require.Equal(t,
			ledger.MetadataDigestOutsideUtxoSetForTest(t, rowsDB),
			ledger.MetadataDigestOutsideUtxoSetForTest(t, onlyDB),
		)
		requireSetMatches(t, only, rowsDB, at)
		requireStakeMatches(t, only, rowsDB)
		n := 0
		require.NoError(t, dbadapter.New(onlyDB).IterateLiveUtxos(
			t.Context(), nil,
			func(*models.Utxo) error { n++; return nil },
		))
		require.Zero(t, n, "the metadata store holds utxo rows")
		stale, err := onlyDB.Metadata().RewardLiveStakeNeedsBackfill(nil)
		require.NoError(t, err)
		require.False(t, stale, "reward_live_stake needs a rebuild")
	}
	compare(rowsRun.Points[4])

	// The ledger's lookups agree: by address over every live output's
	// address, and including spent outputs over every output the first
	// run's rows recorded.
	var addrs []gledger.Address
	seen := map[string]bool{}
	var allRefs []models.UtxoId
	raw, err := dbtest.RawSQLiteMetadata(t, rowsDB)
	require.NoError(t, err)
	rows, err := raw.Query(`SELECT t.hash, u.output_idx FROM utxo AS u
		JOIN "transaction" AS t ON t.id = u.tx_id`)
	require.NoError(t, err)
	for rows.Next() {
		var ref models.UtxoId
		require.NoError(t, rows.Scan(&ref.Hash, &ref.Idx))
		allRefs = append(allRefs, ref)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, dbadapter.New(rowsDB).IterateLiveUtxos(
		t.Context(), nil,
		func(u *models.Utxo) error {
			out, err := u.Decode()
			if err != nil {
				return err
			}
			a := out.Address()
			if k := a.String(); !seen[k] {
				seen[k] = true
				addrs = append(addrs, a)
			}
			return nil
		},
	))
	require.Greater(t, len(allRefs), int(rowsCount(t, rowsDB)), "no spent rows")
	byAddr := func(fn func([]gledger.Address) ([]models.Utxo, error)) []string {
		got, err := fn(addrs)
		require.NoError(t, err)
		var out []string
		for _, u := range got {
			out = append(out, fmt.Sprintf("%x#%d=%x", u.TxId, u.OutputIdx, u.Cbor))
		}
		return out
	}
	require.ElementsMatch(t, byAddr(rowsRun.UtxosByAddress), byAddr(onlyRun.UtxosByAddress))
	for _, ref := range allRefs {
		want, err := rowsRun.UtxoIncludingSpent(ref.Hash, ref.Idx)
		require.NoError(t, err)
		got, err := onlyRun.UtxoIncludingSpent(ref.Hash, ref.Idx)
		require.NoError(t, err)
		require.NotNil(t, got, "%x#%d", ref.Hash, ref.Idx)
		require.Equal(t, want.Cbor, got.Cbor, "%x#%d", ref.Hash, ref.Idx)
	}

	wantWhole, err := rowsRun.UtxoWhole(0, nil)
	require.NoError(t, err)
	gotWhole, err := onlyRun.UtxoWhole(0, nil)
	require.NoError(t, err)
	require.Equal(t, wantWhole, gotWhole, "GetUTxOWhole")
	// Queries pinned to each earlier point agree too, from the set's
	// journal in place of spent rows.
	for i, p := range rowsRun.Points {
		want, err := rowsRun.UtxoWhole(p.Slot, p.Hash)
		require.NoError(t, err)
		got, err := onlyRun.UtxoWhole(p.Slot, p.Hash)
		require.NoError(t, err)
		require.Equal(t, want, got, "GetUTxOWhole as of slot %d", p.Slot)
		if i == 0 {
			require.NotEqual(t, wantWhole, got, "no history behind the tip")
		}
		require.ElementsMatch(t,
			byAddr(func(a []gledger.Address) ([]models.Utxo, error) {
				return rowsRun.UtxosByAddressAt(p, a)
			}),
			byAddr(func(a []gledger.Address) ([]models.Utxo, error) {
				return onlyRun.UtxosByAddressAt(p, a)
			}),
			"by address as of slot %d", p.Slot,
		)
		wantIn, err := rowsRun.UtxosByTxInAt(p, allRefs)
		require.NoError(t, err)
		gotIn, err := onlyRun.UtxosByTxInAt(p, allRefs)
		require.NoError(t, err)
		require.Equal(t, wantIn, gotIn, "by tx in as of slot %d", p.Slot)
	}

	require.NoError(t, rowsRun.Rollback(rowsRun.Points[2]))
	require.NoError(t, onlyRun.Rollback(onlyRun.Points[2]))
	compare(rowsRun.Points[2])

	// A rebuild of reward_live_stake from the live set writes the rows the
	// rebuild from utxo rows writes, including credentials with stake and no
	// account.
	rawRows, err := dbtest.RawSQLiteMetadata(t, rowsDB)
	require.NoError(t, err)
	var noAccount int
	require.NoError(t, rawRows.QueryRow(`
SELECT COUNT(*) FROM reward_live_stake r
WHERE r.utxo_stake <> '0' AND NOT EXISTS (
  SELECT 1 FROM account a
  WHERE a.credential_tag = r.credential_tag AND a.staking_key = r.staking_key
)`).Scan(&noAccount))
	require.Positive(t, noAccount, "no staked credential without an account")
	rawOnly, err := dbtest.RawSQLiteMetadata(t, onlyDB)
	require.NoError(t, err)
	_, err = rawOnly.Exec(`DELETE FROM reward_live_stake`)
	require.NoError(t, err)
	slot := rowsRun.Points[2].Slot
	require.NoError(t, rowsDB.RebuildRewardLiveStake(t.Context(), slot, nil))
	require.NoError(t, onlyDB.RebuildRewardLiveStake(t.Context(), slot, nil))
	compare(rowsRun.Points[2])
}

func rowsCount(t *testing.T, db *database.Database) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, dbadapter.New(db).IterateLiveUtxos(
		t.Context(), nil,
		func(*models.Utxo) error { n++; return nil },
	))
	return n
}
