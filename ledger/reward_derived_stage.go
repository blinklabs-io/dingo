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

package ledger

import (
	"bytes"
	"cmp"
	"encoding/json"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// RewardPrecomputeCursorKey is the sync_state key of the precompute cursor of
// a snapshot epoch.
func RewardPrecomputeCursorKey(snapshotEpoch uint64) string {
	return rewardPrecomputeCursorKey(snapshotEpoch)
}

func isRewardCursorKey(key string) bool {
	return strings.HasPrefix(key, rewardPrecomputeCursorPrefix)
}

func (s stagingLedgerDB) rewardOutputsDrained() bool {
	return s.ls.familyDrained(nodesettings.FamilyRewardOutputs)
}

// rewardOutputsServed returns the reader the pending credit reads are
// answered from, nil when they stay on the metadata store. A drained family
// with no serving set has no answer to give.
func (s stagingLedgerDB) rewardOutputsServed(
	txn ledgerdb.Txn,
) (RewardOutputReader, error) {
	if r := s.ls.rewardOutputServing(txn); r != nil {
		return r, nil
	}
	if s.rewardOutputsDrained() {
		return nil, drainedUnserved(nodesettings.FamilyRewardOutputs)
	}
	return nil, nil
}

func (s stagingLedgerDB) stageDerived(
	txn ledgerdb.Txn,
	changes ...RewardDerivedChange,
) {
	if !s.rewardOutputsDrained() && s.ls.rewardDerivedReader(txn) == nil {
		return
	}
	s.ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			RewardDerived: changes,
		},
	)
}

func (s stagingLedgerDB) stageDerivedAccounts(
	txn ledgerdb.Txn,
	outputs []*models.RewardAccountOutput,
) {
	byEpoch := map[uint64][]*models.RewardAccountOutput{}
	var order []uint64
	for _, o := range outputs {
		if o == nil {
			continue
		}
		cp := cloneRewardOutput(o)
		if _, ok := byEpoch[o.Epoch]; !ok {
			order = append(order, o.Epoch)
		}
		byEpoch[o.Epoch] = append(byEpoch[o.Epoch], &cp)
	}
	for _, e := range order {
		s.stageDerived(txn, RewardDerivedChange{
			Kind: RewardDerivedAccounts, Epoch: e, Accounts: byEpoch[e],
		})
	}
}

func (s stagingLedgerDB) SaveRewardPoolOutputs(
	outputs []*models.RewardPoolOutput,
	txn ledgerdb.Txn,
) error {
	if !s.rewardOutputsDrained() {
		if err := s.LedgerDB.SaveRewardPoolOutputs(outputs, txn); err != nil {
			return err
		}
	}
	byEpoch := map[uint64][]*models.RewardPoolOutput{}
	var order []uint64
	for _, o := range outputs {
		if o == nil {
			continue
		}
		cp := *o
		cp.PoolKeyHash = bytes.Clone(o.PoolKeyHash)
		if _, ok := byEpoch[o.Epoch]; !ok {
			order = append(order, o.Epoch)
		}
		byEpoch[o.Epoch] = append(byEpoch[o.Epoch], &cp)
	}
	for _, e := range order {
		s.stageDerived(txn, RewardDerivedChange{
			Kind: RewardDerivedPools, Epoch: e, Pools: byEpoch[e],
		})
	}
	return nil
}

// derivedServed returns the view the derived reads are answered from, nil
// when they stay on the metadata store.
func (s stagingLedgerDB) derivedServed(
	txn ledgerdb.Txn,
) (*rewardDerivedView, error) {
	if r := s.ls.rewardDerivedServing(txn); r != nil {
		v := s.ls.newRewardDerivedView(r, txn)
		return &v, nil
	}
	if s.rewardOutputsDrained() {
		return nil, drainedUnserved(nodesettings.FamilyRewardOutputs)
	}
	return nil, nil
}

// derivedCompared returns the view to compare a store read with, in verify
// mode when the set holds the rows.
func (s stagingLedgerDB) derivedCompared(
	txn ledgerdb.Txn,
) *rewardDerivedView {
	if !s.ls.verifying() {
		return nil
	}
	r := s.ls.rewardDerivedReader(txn)
	if r == nil {
		return nil
	}
	if loaded, err := r.RewardDerivedLoaded(); err != nil || !loaded {
		return nil
	}
	v := s.ls.newRewardDerivedView(r, txn)
	return &v
}

func samePoolOutput(a, b *models.RewardPoolOutput) bool {
	return a.Epoch == b.Epoch && bytes.Equal(a.PoolKeyHash, b.PoolKeyHash) &&
		a.OptimalReward == b.OptimalReward && a.TotalReward == b.TotalReward &&
		a.LeaderReward == b.LeaderReward &&
		a.LeaderRewardDeficit == b.LeaderRewardDeficit &&
		a.MemberRewardTotal == b.MemberRewardTotal &&
		a.OwnerStake == b.OwnerStake && a.Undistributed == b.Undistributed &&
		a.Unspendable == b.Unspendable && a.CapturedSlot == b.CapturedSlot &&
		a.BoundarySlot == b.BoundarySlot &&
		(a.ApparentPerformance == nil) == (b.ApparentPerformance == nil) &&
		(a.ApparentPerformance == nil ||
			a.ApparentPerformance.Cmp(b.ApparentPerformance.Rat) == 0)
}

func (s stagingLedgerDB) GetRewardPoolOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolOutput, error) {
	v, err := s.derivedServed(txn)
	if err != nil {
		return nil, err
	}
	if v != nil {
		return v.pools(epoch)
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetRewardPoolOutputs(epoch, txn)
	if err != nil {
		return nil, err
	}
	if cv := s.derivedCompared(txn); cv != nil {
		got, gerr := cv.pools(epoch)
		s.ls.compareComputed(
			g, computedRewardPoolOutputs,
			gerr == nil && slices.EqualFunc(want, got, samePoolOutput), gerr,
		)
	}
	return want, nil
}

func (s stagingLedgerDB) GetRewardAccountOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	v, err := s.derivedServed(txn)
	if err != nil {
		return nil, err
	}
	if v != nil {
		return v.accounts(epoch, nil)
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetRewardAccountOutputs(epoch, txn)
	if err != nil {
		return nil, err
	}
	if cv := s.derivedCompared(txn); cv != nil {
		got, gerr := cv.accounts(epoch, nil)
		s.ls.compareComputed(
			g, computedRewardAccountOutputs,
			gerr == nil && slices.EqualFunc(want, got, sameRewardOutput), gerr,
		)
	}
	return want, nil
}

// GetRewardAccountOutputsForEligibility returns every output of the
// credential in epochs, folded or not: the set keeps an epoch's outputs until
// it is pruned and the one caller reads a round that is not yet credited.
func (s stagingLedgerDB) GetRewardAccountOutputsForEligibility(
	epochs []uint64,
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	ref := models.NewStakeCredentialRef(credentialTag, stakingKey)
	read := func(v *rewardDerivedView) ([]*models.RewardAccountOutput, error) {
		if len(epochs) == 0 || len(stakingKey) == 0 {
			return nil, nil
		}
		var out []*models.RewardAccountOutput
		for _, e := range slices.Sorted(slices.Values(epochs)) {
			rows, err := v.accounts(e, &ref)
			if err != nil {
				return nil, err
			}
			out = append(out, rows...)
		}
		slices.SortStableFunc(out, func(a, b *models.RewardAccountOutput) int {
			return cmp.Or(
				cmp.Compare(a.Epoch, b.Epoch),
				bytes.Compare(a.PoolKeyHash, b.PoolKeyHash),
				cmp.Compare(a.RewardType, b.RewardType),
			)
		})
		return out, nil
	}
	v, err := s.derivedServed(txn)
	if err != nil {
		return nil, err
	}
	if v != nil {
		return read(v)
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetRewardAccountOutputsForEligibility(
		epochs, credentialTag, stakingKey, txn,
	)
	if err != nil {
		return nil, err
	}
	if cv := s.derivedCompared(txn); cv != nil {
		got, gerr := read(cv)
		s.ls.compareComputed(
			g, computedRewardAccountOutputs,
			gerr == nil && slices.EqualFunc(want, got, sameRewardOutput), gerr,
		)
	}
	return want, nil
}

// TakeRewardEligibilityRecheck is empty once the family is drained: the
// durable rollback counter the precompute cursor records replaces the list.
func (s stagingLedgerDB) TakeRewardEligibilityRecheck(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if s.rewardOutputsDrained() {
		return nil, nil
	}
	return s.LedgerDB.TakeRewardEligibilityRecheck(txn)
}

// DeleteRewardOutputsBeforeEpoch stages the prune of the derived rows below
// epoch.
func (s stagingLedgerDB) pruneDerived(txn ledgerdb.Txn, epoch uint64) {
	s.stageDerived(txn, RewardDerivedChange{
		Kind: RewardDerivedPrune, PruneBelow: epoch,
	})
}

type rewardCursorHeader struct {
	SnapshotEpoch uint64 `json:"snapshot_epoch"`
	CapturedSlot  uint64 `json:"captured_slot"`
}

func (s stagingLedgerDB) cursorRead(
	key string,
	txn ledgerdb.Txn,
) (string, bool, error) {
	epoch, ok := cursorEpochOfKey(key)
	if !ok {
		return "", false, nil
	}
	v, err := s.derivedServed(txn)
	if err != nil || v == nil {
		return "", false, err
	}
	raw, err := v.cursor(epoch)
	return string(raw), true, err
}

func (s stagingLedgerDB) cursorWrite(
	key, value string,
	txn ledgerdb.Txn,
) {
	epoch, ok := cursorEpochOfKey(key)
	if !ok {
		return
	}
	var h rewardCursorHeader
	_ = json.Unmarshal([]byte(value), &h)
	s.stageDerived(txn, RewardDerivedChange{
		Kind: RewardDerivedCursor, Epoch: epoch, Slot: h.CapturedSlot,
		Cursor: []byte(value),
	})
}

// claimPendingFromSet is ClaimPendingRewardCreditsForCredential for a drained
// family: the credential's unfolded credits in credited rounds come from the
// set and are staged as folded.
func (s stagingLedgerDB) claimPendingFromSet(
	txn ledgerdb.Txn,
	tag uint8,
	key []byte,
) ([]*models.RewardAccountOutput, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, drainedUnserved(nodesettings.FamilyRewardOutputs)
	}
	if len(key) == 0 {
		return nil, nil
	}
	rows, err := s.ls.pendingRewardOutputsView(r, txn, tag, key)
	if err != nil {
		return nil, err
	}
	out := make([]*models.RewardAccountOutput, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}
	s.ls.stageRewardFolds(txn, out)
	return out, nil
}

// claimUnfoldedFromSet is ClaimUnfoldedRewardCredits for a drained family:
// up to limit unfolded credits of one credited round, in credential order.
func (s stagingLedgerDB) claimUnfoldedFromSet(
	txn ledgerdb.Txn,
	epoch uint64,
	limit int,
) ([]*models.RewardAccountOutput, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, drainedUnserved(nodesettings.FamilyRewardOutputs)
	}
	credited, err := s.ls.rewardRoundSeenBy(r, txn, epoch)
	if err != nil || !credited || limit <= 0 {
		return nil, err
	}
	refs, err := s.ls.epochRewardOutputRefs(r, txn, epoch, limit)
	if err != nil {
		return nil, err
	}
	var out []*models.RewardAccountOutput
	for _, ref := range refs {
		rows, err := s.ls.pendingRewardOutputsView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		for i := range rows {
			if rows[i].Epoch == epoch &&
				bytes.Equal(rows[i].PoolKeyHash, ref.Pool) &&
				rows[i].RewardType == ref.Type {
				out = append(out, &rows[i])
			}
		}
	}
	s.ls.stageRewardFolds(txn, out)
	return out, nil
}

// epochRewardOutputRefs lists up to limit unfolded outputs of epoch as txn
// sees the set, ordered by credential, pool and type.
func (ls *LedgerState) epochRewardOutputRefs(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	epoch uint64,
	limit int,
) ([]RewardOutputRef, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	st := ls.rewardStagedOps(txn)
	type id struct {
		tag  uint8
		key  string
		pool string
		typ  string
	}
	staged := map[id]*RewardOutputChange{}
	wiped := false
	for i := range st.outputs {
		c := &st.outputs[i]
		if c.Output.Epoch != epoch {
			continue
		}
		if c.Kind == RewardOutputWipe {
			wiped = true
			clear(staged)
			continue
		}
		staged[id{
			c.Output.CredentialTag, string(c.Output.StakingKey),
			string(c.Output.PoolKeyHash), c.Output.RewardType,
		}] = c
	}
	refs := map[id]RewardOutputRef{}
	if !wiped {
		// Each staged change can hide one committed row.
		committed, err := r.EpochRewardOutputs(
			epoch,
			limit+len(staged),
			st.ceiling,
		)
		if err != nil {
			return nil, err
		}
		for _, o := range committed {
			k := id{o.Tag, string(o.Key), string(o.Pool), o.Type}
			if c, ok := staged[k]; ok && c.Kind == RewardOutputDelete {
				continue
			}
			refs[k] = o
		}
	}
	for k, c := range staged {
		if c.Kind == RewardOutputPut {
			refs[k] = RewardOutputRef{
				Tag: c.Output.CredentialTag, Key: c.Output.StakingKey,
				Epoch: epoch, Pool: c.Output.PoolKeyHash,
				Type: c.Output.RewardType,
			}
		}
	}
	out := make([]RewardOutputRef, 0, len(refs))
	for _, o := range refs {
		out = append(out, o)
	}
	slices.SortFunc(out, func(a, b RewardOutputRef) int {
		return cmp.Or(
			cmp.Compare(a.Tag, b.Tag), bytes.Compare(a.Key, b.Key),
			bytes.Compare(a.Pool, b.Pool), cmp.Compare(a.Type, b.Type),
		)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// getCursor reads a precompute cursor from the set when the set serves the
// family, and compares the store's with the set's in verify mode.
func (s stagingLedgerDB) getCursor(
	key string,
	txn ledgerdb.Txn,
) (string, error) {
	if raw, served, err := s.cursorRead(key, txn); served || err != nil {
		return raw, err
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetSyncState(key, txn)
	if err != nil {
		return want, err
	}
	if cv := s.derivedCompared(txn); cv != nil {
		epoch, _ := cursorEpochOfKey(key)
		got, gerr := cv.cursor(epoch)
		s.ls.compareComputed(
			g, computedRewardCursor, gerr == nil && string(got) == want, gerr,
		)
	}
	return want, nil
}
