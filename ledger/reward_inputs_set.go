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
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// RewardInputEpochWindow is how many epochs before the newest the set keeps
// the stake rows of a reward snapshot for, as the metadata store does; its
// pool, snapshot and pots rows are kept for good.
const RewardInputEpochWindow = 3

// rewardSnapshotMark is the only snapshot type reward calculation reads.
const rewardSnapshotMark = "mark"

// RewardInputKind is what a RewardInputChange does.
type RewardInputKind uint8

const (
	// RewardInputPots sets the epoch's ADA pots.
	RewardInputPots RewardInputKind = iota
	// RewardInputSnapshot sets a snapshot header.
	RewardInputSnapshot
	// RewardInputPools sets pool inputs.
	RewardInputPools
	// RewardInputStakes sets stake inputs. The rows of one epoch the set
	// holds none of are written under a range tombstone.
	RewardInputStakes
	// RewardInputReplace deletes the pool and stake inputs of an epoch.
	RewardInputReplace
	// RewardInputPrune drops the stake rows of every epoch below PruneBelow.
	RewardInputPrune
	// RewardInputPending replaces the snapshots whose stake rows are still
	// being written with Pending.
	RewardInputPending
	// RewardInputLikelihood sets the non-myopic likelihood state of an epoch
	// and replaces its pool likelihoods.
	RewardInputLikelihood
)

// RewardInputChange is one change to the reward snapshot inputs the live
// UTxO set keeps, applied in order. A row is journaled at the slot the
// metadata store rolls it back by: the later of its captured and boundary
// slots.
type RewardInputChange struct {
	Kind       RewardInputKind
	Epoch      uint64
	Pots       *models.RewardAdaPots
	Snapshot   *models.RewardSnapshot
	Pools      []*models.RewardPoolInput
	Stakes     []*models.RewardStakeInput
	PruneBelow uint64
	Pending    []RewardStakeInputsPendingEntry
	// Likelihood and LikelihoodPools are a RewardInputLikelihood change's
	// state and the pools it ranks, journaled at the state's boundary slot.
	Likelihood      *models.RewardLikelihoodState
	LikelihoodPools []*models.RewardPoolLikelihood
}

// RewardInputSlot is the slot a rollback to that slot or later keeps the
// row at, and an earlier one drops it.
func RewardInputSlot(captured, boundary uint64) uint64 {
	return max(captured, boundary)
}

// RewardInputsReader serves reward snapshot inputs from a live UTxO set that
// keeps them. An asOf reads the rows as a rollback to that slot would leave
// them.
type RewardInputsReader interface {
	RewardInputsLoaded() (bool, error)
	// RewardPots returns the epoch's ADA pots, nil when there are none.
	RewardPots(epoch uint64, asOf *uint64) (*models.RewardAdaPots, error)
	// RewardSnapshotRow returns the snapshot header, nil when there is none.
	RewardSnapshotRow(
		epoch uint64, typ string, asOf *uint64,
	) (*models.RewardSnapshot, error)
	// RewardFailureRow returns the failure reason and the slot it was
	// captured at; the reason is empty when there is none.
	RewardFailureRow(
		epoch uint64, typ string, asOf *uint64,
	) (string, uint64, error)
	// RewardPoolRows returns the epoch's pool inputs ordered by pool.
	RewardPoolRows(
		epoch uint64,
		asOf *uint64,
	) ([]*models.RewardPoolInput, error)
	// RewardStakeRows returns the epoch's stake inputs ordered by pool,
	// credential tag and credential; a non-nil lo and hi bound the pool,
	// inclusive.
	RewardStakeRows(
		epoch uint64, lo, hi []byte, asOf *uint64,
	) ([]*models.RewardStakeInput, error)
	// RewardInputEpochs lists the epochs the set holds a snapshot, pool or
	// stake row of, oldest first, and those it holds pots of.
	RewardInputEpochs() (snapshots, pots []uint64, err error)
	// RewardStakePending returns the snapshots whose stake rows are still
	// being written, oldest first.
	RewardStakePending(
		asOf *uint64,
	) ([]RewardStakeInputsPendingEntry, error)
	// RewardLikelihoodStates returns every likelihood state, oldest epoch
	// first.
	RewardLikelihoodStates(
		asOf *uint64,
	) ([]*models.RewardLikelihoodState, error)
	// RewardPoolLikelihoodRows returns the pool likelihoods of epoch's
	// state, ordered by pool.
	RewardPoolLikelihoodRows(
		epoch uint64,
		asOf *uint64,
	) ([]*models.RewardPoolLikelihood, error)
}

// rewardInputsReader returns the set's reader when it keeps reward inputs
// and has not failed.
func (ls *LedgerState) rewardInputsReader(txn ledgerdb.Txn) RewardInputsReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(RewardInputsReader)
	if !ok {
		return nil
	}
	return r
}

// rewardInputsServing returns the reader when reward input reads are
// answered from the set, nil when the set does not serve them.
func (ls *LedgerState) rewardInputsServing(
	txn ledgerdb.Txn,
) RewardInputsReader {
	r := ls.rewardInputsReader(txn)
	if r == nil ||
		!ls.setServes(&ls.rewardInputsSetLoaded, r.RewardInputsLoaded) {
		return nil
	}
	return r
}

func rewardStakeLess(a, b *models.RewardStakeInput) int {
	return cmp.Or(
		bytes.Compare(a.PoolKeyHash, b.PoolKeyHash),
		cmp.Compare(a.CredentialTag, b.CredentialTag),
		bytes.Compare(a.StakingKey, b.StakingKey),
	)
}

// rewardInputsStaged is what the batches a transaction sees did to the
// reward inputs, in order, with the changes a staged rollback undoes dropped.
type rewardInputsStaged struct {
	ceiling *uint64
	changes []RewardInputChange
}

// rewardInputsStagedOps returns txn's staged reward input changes. The
// caller holds utxoSetApplyMu for reading.
func (ls *LedgerState) rewardInputsStagedOps(
	txn ledgerdb.Txn,
) rewardInputsStaged {
	var st rewardInputsStaged
	for _, o := range ls.stagedEpochStateOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if st.ceiling == nil || to < *st.ceiling {
				st.ceiling = &to
			}
			st.changes = dropRewardInputsAfter(st.changes, to)
		}
		st.changes = append(st.changes, o.rewardInputs...)
	}
	return st
}

// dropRewardInputsAfter removes from changes what a rollback to slot undoes:
// a row at a later slot, and an op whose every row is.
func dropRewardInputsAfter(
	changes []RewardInputChange,
	slot uint64,
) []RewardInputChange {
	out := make([]RewardInputChange, 0, len(changes))
	for _, c := range changes {
		switch c.Kind {
		case RewardInputPots:
			if RewardInputSlot(c.Pots.CapturedSlot, 0) > slot {
				continue
			}
		case RewardInputSnapshot:
			if RewardInputSlot(
				c.Snapshot.CapturedSlot, c.Snapshot.BoundarySlot,
			) > slot {
				continue
			}
		case RewardInputReplace, RewardInputPrune:
		case RewardInputLikelihood:
			if c.Likelihood.BoundarySlot > slot {
				continue
			}
		case RewardInputPending:
			c.Pending = slices.DeleteFunc(
				slices.Clone(c.Pending),
				func(e RewardStakeInputsPendingEntry) bool {
					return e.BoundarySlot > slot
				},
			)
		case RewardInputPools:
			c.Pools = slices.DeleteFunc(
				slices.Clone(c.Pools),
				func(p *models.RewardPoolInput) bool {
					return RewardInputSlot(
						p.CapturedSlot,
						p.BoundarySlot,
					) > slot
				},
			)
		case RewardInputStakes:
			var kept []*models.RewardStakeInput
			for _, s := range c.Stakes {
				if RewardInputSlot(s.CapturedSlot, s.BoundarySlot) <= slot {
					kept = append(kept, s)
				}
			}
			c.Stakes = kept
		}
		out = append(out, c)
	}
	return out
}

// rewardInputsView answers reads of the reward inputs as txn sees the set:
// its committed rows as the lowest staged rollback leaves them with the
// staged changes the rollbacks did not drop applied in order.
type rewardInputsView struct {
	r  RewardInputsReader
	at *uint64
	st rewardInputsStaged
}

func (ls *LedgerState) newRewardInputsView(
	r RewardInputsReader,
	txn ledgerdb.Txn,
) rewardInputsView {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	st := ls.rewardInputsStagedOps(txn)
	return rewardInputsView{r: r, at: st.ceiling, st: st}
}

func (v rewardInputsView) pots(epoch uint64) (*models.RewardAdaPots, error) {
	row, err := v.r.RewardPots(epoch, v.at)
	if err != nil {
		return nil, err
	}
	for _, c := range v.st.changes {
		if c.Kind == RewardInputPots && c.Pots.Epoch == epoch {
			cp := *c.Pots
			row = &cp
		}
	}
	return row, nil
}

func (v rewardInputsView) snapshot(
	epoch uint64,
	typ string,
) (*models.RewardSnapshot, error) {
	row, err := v.r.RewardSnapshotRow(epoch, typ, v.at)
	if err != nil {
		return nil, err
	}
	for _, c := range v.st.changes {
		switch {
		case c.Kind == RewardInputSnapshot &&
			c.Snapshot.Epoch == epoch && c.Snapshot.SnapshotType == typ:
			cp := *c.Snapshot
			row = &cp
		}
	}
	return row, nil
}

// pendingRaw is the pending sync_state value as the view has it.
func (v rewardInputsView) pendingRaw() (string, error) {
	entries, err := v.r.RewardStakePending(v.at)
	if err != nil {
		return "", err
	}
	for _, c := range v.st.changes {
		if c.Kind == RewardInputPending {
			entries = c.Pending
		}
	}
	return FormatRewardStakeInputsPending(entries)
}

func (v rewardInputsView) failure(epoch uint64, typ string) (string, error) {
	reason, _, err := v.r.RewardFailureRow(epoch, typ, v.at)
	if err != nil {
		return "", err
	}
	return reason, nil
}

func (v rewardInputsView) pools(
	epoch uint64,
) ([]*models.RewardPoolInput, error) {
	rows, err := v.r.RewardPoolRows(epoch, v.at)
	if err != nil {
		return nil, err
	}
	touched := false
	for _, c := range v.st.changes {
		touched = touched || (c.Epoch == epoch &&
			(c.Kind == RewardInputPools || c.Kind == RewardInputReplace))
	}
	if !touched {
		return rows, nil
	}
	byPool := make(map[string]*models.RewardPoolInput, len(rows))
	for _, p := range rows {
		byPool[string(p.PoolKeyHash)] = p
	}
	for _, c := range v.st.changes {
		switch {
		case c.Kind == RewardInputReplace && c.Epoch == epoch:
			clear(byPool)
		case c.Kind == RewardInputPools:
			for _, p := range c.Pools {
				if p.Epoch == epoch {
					cp := *p
					byPool[string(p.PoolKeyHash)] = &cp
				}
			}
		}
	}
	out := make([]*models.RewardPoolInput, 0, len(byPool))
	for _, p := range byPool {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b *models.RewardPoolInput) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	return out, nil
}

func inPoolRange(pool, lo, hi []byte) bool {
	return (lo == nil || bytes.Compare(pool, lo) >= 0) &&
		(hi == nil || bytes.Compare(pool, hi) <= 0)
}

// stakeSlotBounder is a reader that reports the slots an epoch's first and
// last stake rows were captured at without reading the epoch.
type stakeSlotBounder interface {
	RewardStakeSlotBounds(epoch uint64) (first, last uint64, found bool, err error)
}

// stakesCapturedOutside reports whether the epoch's stake rows were all
// captured outside from through through, without reading them. Every stake row
// of an epoch comes from one distribution and carries one captured slot, so an
// epoch whose first and last rows share a slot outside the range holds no row
// in it; an epoch whose first and last rows differ is not decided here. It is
// false when the reader cannot tell, or when staged changes or a rollback
// ceiling make the committed rows differ from what the view serves.
func (v rewardInputsView) stakesCapturedOutside(
	epoch, from, through uint64,
) (bool, error) {
	b, ok := v.r.(stakeSlotBounder)
	if !ok || v.at != nil {
		return false, nil
	}
	for _, c := range v.st.changes {
		if (c.Epoch == epoch &&
			(c.Kind == RewardInputStakes || c.Kind == RewardInputReplace)) ||
			(c.Kind == RewardInputPrune && epoch < c.PruneBelow) {
			return false, nil
		}
	}
	first, last, found, err := b.RewardStakeSlotBounds(epoch)
	if err != nil {
		return false, err
	}
	if !found {
		return true, nil
	}
	return first == last && (first < from || first > through), nil
}

func (v rewardInputsView) stakes(
	epoch uint64,
	lo, hi []byte,
) ([]*models.RewardStakeInput, error) {
	rows, err := v.r.RewardStakeRows(epoch, lo, hi, v.at)
	if err != nil {
		return nil, err
	}
	touched := false
	for _, c := range v.st.changes {
		touched = touched || (c.Epoch == epoch &&
			(c.Kind == RewardInputStakes || c.Kind == RewardInputReplace)) ||
			(c.Kind == RewardInputPrune && epoch < c.PruneBelow)
	}
	if !touched {
		return rows, nil
	}
	key := func(s *models.RewardStakeInput) string {
		return string(s.PoolKeyHash) + string([]byte{s.CredentialTag}) +
			string(s.StakingKey)
	}
	byKey := make(map[string]*models.RewardStakeInput, len(rows))
	for _, s := range rows {
		byKey[key(s)] = s
	}
	for _, c := range v.st.changes {
		switch {
		case c.Kind == RewardInputReplace && c.Epoch == epoch,
			c.Kind == RewardInputPrune && epoch < c.PruneBelow:
			clear(byKey)
		case c.Kind == RewardInputStakes && c.Epoch == epoch:
			for _, s := range c.Stakes {
				if s.Epoch == epoch && inPoolRange(s.PoolKeyHash, lo, hi) {
					byKey[key(s)] = s
				}
			}
		}
	}
	out := make([]*models.RewardStakeInput, 0, len(byKey))
	for _, s := range byKey {
		cp := *s
		out = append(out, &cp)
	}
	slices.SortFunc(out, rewardStakeLess)
	return out, nil
}
