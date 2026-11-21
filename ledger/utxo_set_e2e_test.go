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
	"math/big"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// recordingSet applies each commit's batch to a set and keeps a copy.
type recordingSet struct {
	*utxoset.Set
	mu      sync.Mutex
	batches []recordedBatch
}

func (s *recordingSet) Inner() *utxoset.Set { return s.Set }

type recordedBatch struct {
	tip     *ocommon.Point
	changes ledger.LedgerChanges
}

func (r *recordingSet) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	return r.ApplyLedgerChanges(tip, ledger.LedgerChanges{Utxo: changes})
}

func (r *recordingSet) ApplyLedgerChanges(
	tip *ocommon.Point,
	changes ledger.LedgerChanges,
) error {
	r.mu.Lock()
	r.batches = append(r.batches, recordedBatch{tip: tip, changes: changes})
	r.mu.Unlock()
	return r.Set.ApplyLedgerChanges(tip, changes)
}

func optString(p *uint64) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprint(*p)
}

// setPParamUpdateRows returns how many protocol parameter update proposals
// the set holds.
func setPParamUpdateRows(t *testing.T, set *utxoset.Set) int {
	t.Helper()
	n := 0
	require.NoError(t, set.PParamUpdates(func(models.PParamUpdate) error {
		n++
		return nil
	}))
	return n
}

// setPParamsRows returns how many protocol parameter rows the set holds
// across every era.
func setPParamsRows(t *testing.T, set *utxoset.Set) int {
	t.Helper()
	n := 0
	for _, era := range eras.ErasWithDijkstra {
		rows, err := set.PParamsForEra(era.Id)
		require.NoError(t, err)
		n += len(rows)
	}
	return n
}

// TestValidationReadsFromTheLiveUtxoSet resolves every output the scenario
// left live, and every one it spent, the way validation does. With metadata reads available
// every answer must agree with the metadata store, and from the live set the set's
// answers must be the store's.
func TestValidationReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{"set"} {
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
				func(d *database.Database) { db = d },
			)
			ldb := dbadapter.New(db)
			var refs []models.UtxoId
			want := map[string][]byte{}
			require.NoError(t, ldb.IterateLiveUtxos(
				t.Context(), nil,
				func(u *models.Utxo) error {
					refs = append(
						refs,
						models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx},
					)
					want[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
					return nil
				},
			))
			spent := 0
			for _, b := range rec.batches {
				for _, c := range b.changes.Utxo {
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
		})
	}
}

// TestNonceReadsFromTheLiveUtxoSet reads the block nonces the blocks of a
// run recorded, by range, by point and the latest, and requires a rollback
// to remove the ones above its target.
func TestNonceReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.RunUtxoSetScenarioForTest(
		t, types.StorageModeCore, nil, set, func(*database.Database) {},
	)
	check := func(at ocommon.Point) int {
		t.Helper()
		got, err := scenario.BlockNoncesInSlotRange(0, at.Slot+1)
		require.NoError(t, err)
		require.NotEmpty(t, got)
		for i, row := range got {
			if i > 0 {
				require.Greater(t, row.Slot, got[i-1].Slot, "slot order")
			}
			n, err := scenario.BlockNonce(
				ocommon.Point{Slot: row.Slot, Hash: row.Hash},
			)
			require.NoError(t, err)
			require.Equal(t, row.Nonce, n)
		}
		last := got[len(got)-1]
		require.Equal(t, at.Slot, last.Slot, "the tip block has a nonce")
		require.Equal(t, at.Hash, last.Hash)
		latest, ok, err := scenario.LatestBlockNonce()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, last.Slot, latest.Slot)
		above, err := scenario.BlockNoncesInSlotRange(at.Slot+1, math.MaxInt64)
		require.NoError(t, err)
		require.Empty(t, above, "no nonce above the tip")
		return len(got)
	}
	atTip := check(scenario.Points[4])
	require.NoError(t, scenario.Rollback(scenario.Points[3]))
	require.Equal(t, atTip-1, check(scenario.Points[3]),
		"the rollback removes the rolled-back block's nonce")
}

// TestLiveUtxoSetEpochStateFollowsABoundary crosses an epoch boundary with
// the snapshot manager writing through the ledger, and rolls it back. The
// set holds what each step wrote, and the rollback leaves what was there
// before the boundary.
func TestLiveUtxoSetEpochStateFollowsABoundary(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// The harness's outputs have no CBOR to rebuild a set from; the test
	// follows the epoch state only.
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	// snapshots counts the pool stake snapshot rows the set holds.
	snapshots := func(stage string) int {
		t.Helper()
		epochs, err := set.Epochs()
		require.NoError(t, err)
		require.NotEmpty(t, epochs, stage)
		held := make(map[uint64]struct{})
		for _, e := range epochs {
			// Snapshots are captured for the epochs after the boundary too.
			for epoch := e.EpochId; epoch <= e.EpochId+3; epoch++ {
				held[epoch] = struct{}{}
			}
		}
		n := 0
		for epoch := range held {
			for _, typ := range []string{"mark", "set", "go", "actv"} {
				rows, err := set.PoolStakeSnapshots(epoch, typ, 0)
				require.NoError(t, err)
				n += len(rows)
			}
		}
		return n
	}
	firstAccount := func(match func(*models.Account) bool) *models.Account {
		t.Helper()
		var found *models.Account
		require.NoError(t, set.Accounts(func(a *models.Account) error {
			if found == nil && match(a) {
				c := *a
				found = &c
			}
			return nil
		}))
		return found
	}
	beforeBoundary := snapshots("before the boundary")
	scenario.ApplyBoundary()
	require.Greater(t, snapshots("after the boundary"), beforeBoundary,
		"the boundary captured no snapshots")
	scenario.Donate(40)
	active := firstAccount(func(a *models.Account) bool { return a.Active })
	require.NotNil(t, active, "the scenario has no active account")
	ref := models.NewStakeCredentialRef(active.CredentialTag, active.StakingKey)
	before := uint64(active.Reward)
	scenario.Credit(ref, 7)
	credited, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.Equal(t, before+14, uint64(credited.Reward),
		"each credit source applies once")
	require.NoError(t, scenario.Withdraw(ref, 5))
	withdrawn, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.Equal(t, before+9, uint64(withdrawn.Reward))
	delegated := firstAccount(func(a *models.Account) bool {
		return a.Active && len(a.Pool) > 0
	})
	require.NotNil(t, delegated, "the scenario has no delegated account")
	pool := delegated.Pool
	delegator := models.NewStakeCredentialRef(
		delegated.CredentialTag, delegated.StakingKey,
	)
	scenario.Reap(pool)
	reaped, err := set.Account(delegator.Tag, delegator.Key)
	require.NoError(t, err)
	require.Empty(t, reaped.Pool)
	scenario.Expire(ref)
	scenario.Governance(ref, 9)
	poolSlots := scenario.Pools(func() {})
	newPool := poolSlots.Pool
	slots := scenario.OpCerts()
	requireCounter := func(pool []byte, sequence, slot uint64) {
		t.Helper()
		st, err := set.OpCertState(pool)
		require.NoError(t, err)
		require.NotNil(t, st, "pool %x has no counter", pool)
		require.Equal(t, sequence, st.Sequence, "pool %x sequence", pool)
		require.Equal(t, slot, st.Slot, "pool %x latest slot", pool)
	}
	requireCounter(slots.Pool, 6, slots.Batched)
	requireCounter(slots.Other, 2, slots.Repeated)
	cslots := scenario.Committee()
	func() {
		st, err := set.CommitteeStateAt(nil)
		require.NoError(t, err)
		require.Len(t, st.Members, 4, "A and B at base, C and A at the update")
		require.Nil(t, st.LatestQuorum(), "the no-confidence action clears the quorum")
		require.Empty(t, st.SeatedMembers())
		require.Equal(t, "https://example.test/c2", st.CurrentConstitution().AnchorURL)
		require.True(t, st.ResignedSince(0, cslots.B, cslots.Base))
		require.False(t, st.ResignedSince(0, cslots.B, cslots.Update))
		require.Len(t, st.Auths, 2)
		require.Equal(t, cslots.Reauth, st.AuthSince(0, cslots.A, 0).AddedSlot)
	}()
	gslots := scenario.Proposals()
	func() {
		proposals, err := set.ProposalsAt(nil)
		require.NoError(t, err)
		require.Len(t, proposals, 2)
		votes := 0
		for _, p := range proposals {
			cast, err := set.VotesAt(p.TxHash, p.ActionIndex, nil)
			require.NoError(t, err)
			votes += len(cast)
		}
		require.Equal(t, 2, votes)
		p1, err := set.ProposalAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		require.Equal(t, gslots.Enact, *p1.EnactedSlot)
		require.Equal(t, gslots.RatifyAgain, *p1.RatifiedSlot)
		require.Equal(t, []byte{0x02, 0x03}, p1.GovActionCbor)
		p2, err := set.ProposalAt(gslots.P2, 0, nil)
		require.NoError(t, err)
		require.Equal(t, gslots.Drop, *p2.DroppedSlot)
		votesOnP1, err := set.VotesAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		for _, v := range votesOnP1 {
			require.Nil(t, v.DeletedSlot, "the DRep's later vote undeletes it")
		}
	}()
	st, err := set.PoolState(newPool)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Nil(t, st.Future, "the boundary promotes the re-registration")
	require.Equal(t, uint64(2_000_000), uint64(st.Params.Pledge))
	expiring, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.NotZero(t, expiring.ExpirationEpoch)
	states, err := set.NetworkStates()
	require.NoError(t, err)
	require.Len(t, states, 2, "the boundary wrote no network state")
	donations, err := set.NetworkDonations()
	require.NoError(t, err)
	require.Len(t, donations, 2)
	scenario.RollbackBeforeBoundary()
	require.Equal(t, beforeBoundary, snapshots("after the rollback"),
		"the rollback removes the boundary's snapshots")
	restored, err := set.Account(delegator.Tag, delegator.Key)
	require.NoError(t, err)
	require.Equal(t, pool, restored.Pool, "the rollback restores the delegation")
	func() {
		st, err := set.CommitteeStateAt(nil)
		require.NoError(t, err)
		require.Len(t, st.Members, 2, "the rollback removes the later member rows")
		require.Len(t, st.SeatedMembers(), 2, "the rollback restores the soft-deleted rows")
		require.Equal(t, big.NewRat(1, 2), st.LatestQuorum())
		require.Equal(t, "https://example.test/c1", st.CurrentConstitution().AnchorURL)
		require.Empty(t, st.Auths)
		require.Empty(t, st.Resigns)
	}()
	func() {
		p1, err := set.ProposalAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, p1, "a proposal submitted before the rollback stays")
		require.Nil(t, p1.RatifiedSlot)
		require.Nil(t, p1.EnactedSlot)
		require.Equal(t, []byte{0x01}, p1.GovActionCbor,
			"the rollback restores the action CBOR from before its translation")
		p2, err := set.ProposalAt(gslots.P2, 0, nil)
		require.NoError(t, err)
		require.Nil(t, p2, "the rollback removes a proposal submitted after it")
		votes, err := set.VotesAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		require.Empty(t, votes)
	}()
	requireCounter(slots.Pool, 3, slots.Base)
	gone, err := set.OpCertState(slots.Other)
	require.NoError(t, err)
	require.Nil(t, gone, "the rollback removes a counter first observed after it")
	donations, err = set.NetworkDonations()
	require.NoError(t, err)
	require.Len(t, donations, 1, "the rollback left the later donation")
}

// TestLiveUtxoSetEpochStateUndoesAMidEpochEraSwitch requires the set's epoch
// rows to follow a mid-epoch era switch and a rollback to before it, which
// gives the epoch row its source era back without removing it. Reads inside
// the rollback must give the source era before the set applies it.
func TestLiveUtxoSetEpochStateUndoesAMidEpochEraSwitch(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{
		"set",
	} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewMidEpochEraSwitchScenarioForTest(t, set)
			pparamsRows := func() int {
				t.Helper()
				return setPParamsRows(t, set)
			}
			eraOf := func(epoch uint64) uint {
				t.Helper()
				require.Positive(t, pparamsRows())
				e, err := scenario.LedgerDB().GetEpoch(epoch, nil)
				require.NoError(t, err)
				require.NotNil(t, e)
				return e.EraId
			}
			before := pparamsRows()
			scenario.ApplySwitch()
			require.Equal(t, eras.ConwayEraDesc.Id, eraOf(3))
			require.Greater(t, pparamsRows(), before,
				"the switch wrote no translated parameters")
			if reads == "set" {
				require.Equal(t, eras.BabbageEraDesc.Id,
					scenario.EraInsideRollback(),
					"a read inside the rollback sees the source era")
			}
			scenario.RollbackBeforeSwitch()
			require.Equal(t, eras.BabbageEraDesc.Id, eraOf(3))
			require.Equal(t, before, pparamsRows())
		})
	}
}

// TestLiveUtxoSetPParamsFollowAClassicUpdate requires the set's protocol
// parameter rows to follow a boundary that enacts a classic update and
// translates into Conway, and its rollback, with reads agreeing with the
// metadata store.
func TestLiveUtxoSetPParamsFollowAClassicUpdate(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{
		"set",
	} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(t, set)
			rows := func() int {
				t.Helper()
				n := 0
				for _, era := range []eras.EraDesc{eras.BabbageEraDesc, eras.ConwayEraDesc} {
					got, err := scenario.LedgerDB().ListPParamsForEra(era.Id, nil)
					require.NoError(t, err)
					held, err := set.PParamsForEra(era.Id)
					require.NoError(t, err)
					require.Len(t, got, len(held), "era %d", era.Id)
					n += len(got)
				}
				return n
			}
			before := rows()
			scenario.ApplyBoundary()
			// The enacted Babbage row and the translated Conway row.
			require.Equal(t, before+2, rows())
			if reads == "set" {
				afterWrite, afterRollback, afterBoth := scenario.ConwayRowsInsideTxn()
				require.Equal(t, 2, afterWrite,
					"a read in the transaction sees its own write")
				require.Zero(t, afterRollback,
					"a read in the transaction sees its staged rollback")
				require.Equal(t, 1, afterBoth,
					"a write after the rollback survives it")
			}
			scenario.RollbackBeforeBoundary()
			require.Equal(t, before, rows())
		})
	}
}

// TestLiveUtxoSetPParamUpdatesFollowTheLedger requires the set's protocol
// parameter update proposals to equal the metadata store's: the ones it
// loads, the ones valid transactions store through the single and the
// batched write, not those of an invalid transaction, across the boundary
// that enacts an update and after a rollback, which removes only the
// proposals stored after its target.
func TestLiveUtxoSetPParamUpdatesFollowTheLedger(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
	)
	seeded := setPParamUpdateRows(t, set)
	require.Positive(t, seeded, "the scenario seeded no update proposal")

	at := scenario.RollbackTarget.Slot
	// Proposals for the next epoch, so none changes what the boundary enacts.
	for _, p := range []ledger.UpdateProposal{
		{Slot: at, Epoch: 1, Index: 0, Keys: []byte{1, 2, 3}, MaxTxSize: 20_000},
		{Slot: at + 1, Epoch: 1, Index: 0, Keys: []byte{1, 2}, MaxTxSize: 21_000},
		{Slot: at + 1, Epoch: 1, Index: 1, Keys: []byte{1}, MaxTxSize: 22_000, Batched: true},
		{Slot: at + 2, Epoch: 1, Index: 0, Keys: []byte{4}, MaxTxSize: 23_000, Invalid: true},
		{Slot: at + 3, Epoch: 1, Index: 0, Keys: []byte{5}, MaxTxSize: 24_000, Batched: true},
	} {
		scenario.Propose(p)
	}
	const proposed, kept = 7, 3
	require.Equal(t, seeded+proposed,
		setPParamUpdateRows(t, set),
		"an invalid transaction's proposals are not stored")
	scenario.ApplyBoundary()
	require.Equal(t, seeded+proposed,
		setPParamUpdateRows(t, set),
		"the boundary keeps every proposal")
	scenario.RollbackBeforeBoundary()
	require.Equal(t, seeded+kept,
		setPParamUpdateRows(t, set),
		"the rollback removes the proposals stored after its target")
}

// TestLiveUtxoSetAccountsFollowGenesisStaking requires the accounts genesis
// staking writes, after the set has loaded the empty account table at
// origin, to reach the set.
func TestLiveUtxoSetAccountsFollowGenesisStaking(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewGenesisScenarioForTest(t, set)
	ldb := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), ldb))
	scenario.CreateGenesis()
	staked := 0
	require.NoError(t, set.Accounts(func(a *models.Account) error {
		staked++
		require.True(t, a.Active, "genesis staker %x", a.StakingKey)
		require.NotEmpty(t, a.Pool, "genesis staker %x", a.StakingKey)
		return nil
	}))
	require.Positive(t, staked, "genesis staking made no accounts")
	// Genesis accounts are ledger state, which only the set holds.
	require.NoError(t, ldb.IterateAccounts(nil, func(a *models.Account) error {
		t.Errorf("the metadata store holds genesis account %x", a.StakingKey)
		return nil
	}))
}

// TestLiveUtxoSetHoldsEachPoolsLatestOpCert opens the set over op-cert
// counters an earlier release left in its tables, and requires the set to
// hold each pool's highest counter and the slot of its latest observation,
// and a read as of a slot before that observation to be answered from it.
func TestLiveUtxoSetHoldsEachPoolsLatestOpCert(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	poolA := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x41}, 28))
	poolB := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x42}, 28))
	scenario := ledger.NewSeededBoundaryEpochStateScenarioForTest(
		t, set, nil, func(db *database.Database) {
			for _, w := range []struct {
				pool           lcommon.PoolKeyHash
				sequence, slot uint64
			}{{poolA, 4, 100}, {poolB, 3, 150}, {poolA, 6, 200}} {
				require.NoError(t, dbadapter.New(db).UpdatePoolOpCertSequence(
					t.Context(), w.pool, w.sequence, w.slot, nil,
				))
			}
		},
	)
	ldb := scenario.LedgerDB()
	held := map[string][2]uint64{}
	require.NoError(t, set.OpCertStates(func(st *ledger.OpCertState) error {
		held[string(st.Pool)] = [2]uint64{st.Sequence, st.Slot}
		return nil
	}))
	require.Equal(t, map[string][2]uint64{
		string(poolA[:]): {6, 200}, string(poolB[:]): {3, 150},
	}, held)

	// The set has no record of poolA at slot 150, so the counter it was
	// loaded with answers.
	at150, _, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		t.Context(), poolA, 150, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), at150)
	at250, _, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		t.Context(), poolA, 250, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), at250)
}

// liveStakeHarness is the boundary harness with the live UTxO set holding the
// stake of the harness's two delegated accounts, which the metadata store
// holds as rows.
type liveStakeHarness struct {
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	pool     []byte
	owner    models.StakeCredentialRef
	deleg    models.StakeCredentialRef
}

func newLiveStakeHarness(t *testing.T, stake bool) *liveStakeHarness {
	t.Helper()
	return newLiveStakeHarnessWithReads(t, stake, "metadata-visible")
}

func newLiveStakeHarnessWithReads(
	t *testing.T,
	stake bool,
	reads string,
) *liveStakeHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	h := &liveStakeHarness{
		scenario: scenario,
		store:    store,
		pool:     bytes.Repeat([]byte{0xd1}, 28),
		owner:    models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x71}, 28)),
		deleg:    models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x72}, 28)),
	}
	if stake {
		scenario.AddSetStake(h.owner.Key, 20_000_000)
		scenario.AddSetStake(h.deleg.Key, 20_000_000)
	}
	return h
}

func stakeOf(inputs []*models.RewardStakeInput, ref models.StakeCredentialRef) (uint64, bool) {
	for _, in := range inputs {
		if in.CredentialTag == ref.Tag && bytes.Equal(in.StakingKey, ref.Key) {
			return uint64(in.Stake), true
		}
	}
	return 0, false
}

// TestLiveStakeReadsSeeTheirTransactionsStagedAccounts requires a live stake
// read in a write transaction to see the account change that transaction has
// staged, as the metadata store does.
func TestLiveStakeReadsSeeTheirTransactionsStagedAccounts(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, true)
	stake := h.scenario.CreditInsideTxn(h.deleg, h.pool, 11)
	require.Equal(t, uint64(20_000_011), stake)
}

// TestLiveStakeReadsSeeAnAccountRegisteredInTheirTransaction requires the
// pool's live stake read in a write transaction to list a credential that
// transaction registered and delegated, which the set's committed index does
// not hold.
func TestLiveStakeReadsSeeAnAccountRegisteredInTheirTransaction(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, true)
	key := bytes.Repeat([]byte{0x73}, 28)
	keys := h.scenario.RegisterInsideTxn(key, h.pool)
	require.Len(t, keys, 3, "the new credential joins the two delegators")
	require.Contains(t, keys, key)
}

// TestLiveUtxoSetRewardGuardKeepsActivationFloor requires the ledger to
// preserve the activation membership used by the production reward guard. A
// witness in the preceding epoch expires before the reward snapshot, while
// the activation floor keeps the account active through that snapshot.
func TestLiveUtxoSetRewardGuardKeepsActivationFloor(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)

	var ref models.StakeCredentialRef
	require.NoError(t, set.Accounts(func(a *models.Account) error {
		if a.Active && a.CredentialTag == 0 && ref.Key == nil {
			ref = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
		}
		return nil
	}))
	require.NotNil(t, ref.Key)
	scenario.WithdrawAtSlot(ref, scenario.RollbackSlot(), 0)
	activationEpoch, boundary := scenario.ActivateInactivityAtBoundary(1)
	witness, found, err := set.LastWitnessSlot(ref, boundary)
	require.NoError(t, err)
	require.True(t, found)
	require.Less(t, witness, boundary)
	require.False(t, scenario.RewardAccountGuarded(
		ref,
		activationEpoch+1,
		boundary,
	), "the activation floor keeps the stamped account reward-eligible")
	activated, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.True(t, activated.InactivityActivation)
	require.Equal(t, activationEpoch, activated.InactivityActivationEpoch)
	scenario.RollbackBeforeBoundary()
	rolledBack, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.False(t, rolledBack.InactivityActivation)
	require.Zero(t, rolledBack.InactivityActivationEpoch)
}

// TestSetReadsOfAPinnedSnapshotAreTheSetAsOfThePin requires live-set reads of
// a transaction pinned before later commits to answer the pin's DReps and
// votes, as the ratification job's do: the scenario asserts the earlier
// expiry and the single vote the pin saw.
func TestSetReadsOfAPinnedSnapshotAreTheSetAsOfThePin(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.SnapshotReadsAfterLaterCommit()
}

// TestSetReadsOfAPinnedSnapshotSeeABatchCommittedBeforeItsApply requires a
// pin taken after a commit, before its batch reaches the set, to read that
// commit and nothing after it: the snapshot of the set lacks the batch, so the
// pin carries it, and a later commit is not in either.
func TestSetReadsOfAPinnedSnapshotSeeABatchCommittedBeforeItsApply(
	t *testing.T,
) {
	t.Parallel()
	modes := []string{"set"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewBoundaryEpochStateScenarioForTest(
				t, set,
			)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			got := scenario.PinInsideCommit()
			require.Equal(t, got.Epoch+21, got.Pinned,
				"the pinned read sees the commit it was pinned after")
			require.Equal(t, got.Epoch+22, got.Latest,
				"an unpinned read sees the latest")
		})
	}
}
