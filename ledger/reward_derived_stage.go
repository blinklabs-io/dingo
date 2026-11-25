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
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
)

// RewardPrecomputeCursorKey is the sync_state key of the precompute cursor of
// a snapshot epoch.
func RewardPrecomputeCursorKey(snapshotEpoch uint64) string {
	return epochengine.RewardPrecomputeCursorKey(snapshotEpoch)
}

func isRewardCursorKey(key string) bool {
	return strings.HasPrefix(key, models.RewardPrecomputeCursorPrefix)
}

// rewardOutputsServed returns the configured set reader or an error when the
// set cannot serve reward outputs.
func (s stagingLedgerDB) rewardOutputsServed(
	txn ledgerdb.Txn,
) (RewardOutputReader, error) {
	if r := s.ls.rewardOutputServing(txn); r != nil {
		return r, nil
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyRewardOutputs)
}

func (s stagingLedgerDB) stageDerived(
	txn ledgerdb.Txn,
	changes ...RewardDerivedChange,
) {
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

// derivedServed returns the set view for derived reward output reads.
func (s stagingLedgerDB) derivedServed(
	txn ledgerdb.Txn,
) (*rewardDerivedView, error) {
	if r := s.ls.rewardDerivedServing(txn); r != nil {
		v := s.ls.newRewardDerivedView(r, txn)
		return &v, nil
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyRewardOutputs)
}

func (s stagingLedgerDB) GetRewardPoolOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolOutput, error) {
	v, err := s.derivedServed(txn)
	if err != nil {
		return nil, err
	}
	return v.pools(epoch)
}

func (s stagingLedgerDB) GetRewardAccountOutputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	v, err := s.derivedServed(txn)
	if err != nil {
		return nil, err
	}
	return v.accounts(epoch, nil)
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
	return read(v)
}

// TakeRewardEligibilityRecheck is empty: the
// durable rollback counter the precompute cursor records replaces the list.
func (s stagingLedgerDB) TakeRewardEligibilityRecheck(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	return nil, nil
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
	epoch, ok := epochengine.CursorEpochOfKey(key)
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
	epoch, ok := epochengine.CursorEpochOfKey(key)
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
		return nil, ledgerStateUnserved(nodesettings.FamilyRewardOutputs)
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

// claimUnfoldedFromSet is ClaimUnfoldedRewardCredits read from the set: up
// to limit unfolded credits of one credited round, in credential order.
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
		return nil, ledgerStateUnserved(nodesettings.FamilyRewardOutputs)
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

// getCursor reads a precompute cursor from the set, the only place one is
// written.
func (s stagingLedgerDB) getCursor(
	key string,
	txn ledgerdb.Txn,
) (string, error) {
	raw, served, err := s.cursorRead(key, txn)
	if err != nil {
		return "", err
	}
	if !served {
		return "", fmt.Errorf("malformed reward precompute cursor key %q", key)
	}
	return raw, nil
}
