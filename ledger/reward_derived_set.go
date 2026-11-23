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

// RewardDerivedKind is what a RewardDerivedChange does.
type RewardDerivedKind uint8

const (
	// RewardDerivedPools sets pool outputs.
	RewardDerivedPools RewardDerivedKind = iota
	// RewardDerivedAccounts sets account outputs, whatever their flags.
	RewardDerivedAccounts
	// RewardDerivedCursor sets an epoch's precompute cursor.
	RewardDerivedCursor
	// RewardDerivedReplace deletes the pool outputs, account outputs and
	// cursor of an epoch. A rollback does not undo it.
	RewardDerivedReplace
	// RewardDerivedPrune deletes the derived rows of every epoch below
	// PruneBelow.
	RewardDerivedPrune
)

// RewardDerivedChange is one change to the derived reward rows the live UTxO
// set keeps, applied in order. The rows of an epoch are dropped by a rollback
// to a slot below their captured slot, as the metadata store deletes the
// outputs captured after the target; Slot is the captured slot of a cursor.
type RewardDerivedChange struct {
	Kind       RewardDerivedKind
	Epoch      uint64
	Slot       uint64
	Pools      []*models.RewardPoolOutput
	Accounts   []*models.RewardAccountOutput
	Cursor     []byte
	PruneBelow uint64
}

// RewardDerivedReader serves the derived reward rows from a live UTxO set
// that keeps them. An asOf reads the rows as a rollback to that slot would
// leave them.
type RewardDerivedReader interface {
	RewardDerivedLoaded() (bool, error)
	// RewardPoolOutputRows returns the epoch's pool outputs ordered by pool.
	RewardPoolOutputRows(
		epoch uint64, asOf *uint64,
	) ([]*models.RewardPoolOutput, error)
	// RewardAccountOutputRows returns the epoch's account outputs ordered by
	// credential tag, credential, pool and type.
	RewardAccountOutputRows(
		epoch uint64, asOf *uint64,
	) ([]*models.RewardAccountOutput, error)
	// RewardCredentialOutputRows returns one credential's outputs of epoch,
	// ordered by pool and type.
	RewardCredentialOutputRows(
		epoch uint64, tag uint8, key []byte, asOf *uint64,
	) ([]*models.RewardAccountOutput, error)
	// RewardCursorRow returns the epoch's precompute cursor.
	RewardCursorRow(epoch uint64, asOf *uint64) ([]byte, bool, error)
	// RewardRollbackSeq returns the durable rollback counter.
	RewardRollbackSeq() (uint64, error)
	// RewardDerivedEpochs lists the epochs holding a derived row.
	RewardDerivedEpochs() ([]uint64, error)
}

// rewardDerivedReader returns the set's reader when it keeps derived reward
// rows and has not failed.
func (ls *LedgerState) rewardDerivedReader(
	txn ledgerdb.Txn,
) RewardDerivedReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(RewardDerivedReader)
	if !ok {
		return nil
	}
	return r
}

// rewardDerivedServing returns the reader when the derived reward reads are
// answered from the set, nil when the set does not serve them.
func (ls *LedgerState) rewardDerivedServing(
	txn ledgerdb.Txn,
) RewardDerivedReader {
	r := ls.rewardDerivedReader(txn)
	if r == nil ||
		!ls.setServes(&ls.rewardDerivedSetLoaded, r.RewardDerivedLoaded) {
		return nil
	}
	return r
}

type rewardDerivedStaged struct {
	ceiling *uint64
	changes []RewardDerivedChange
}

// rewardDerivedStagedOps returns txn's staged derived changes, with those a
// staged rollback undoes dropped. The caller holds utxoSetApplyMu for
// reading.
func (ls *LedgerState) rewardDerivedStagedOps(
	txn ledgerdb.Txn,
) rewardDerivedStaged {
	var st rewardDerivedStaged
	for _, o := range ls.stagedEpochStateOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if st.ceiling == nil || to < *st.ceiling {
				st.ceiling = &to
			}
			st.changes = dropRewardDerivedAfter(st.changes, to)
		}
		st.changes = append(st.changes, o.rewardDerived...)
	}
	return st
}

func dropRewardDerivedAfter(
	changes []RewardDerivedChange,
	slot uint64,
) []RewardDerivedChange {
	out := make([]RewardDerivedChange, 0, len(changes))
	for _, c := range changes {
		switch c.Kind {
		case RewardDerivedCursor:
			if c.Slot > slot {
				continue
			}
		case RewardDerivedPools:
			c.Pools = slices.DeleteFunc(
				slices.Clone(c.Pools),
				func(p *models.RewardPoolOutput) bool {
					return p.CapturedSlot > slot
				},
			)
		case RewardDerivedAccounts:
			c.Accounts = slices.DeleteFunc(
				slices.Clone(c.Accounts),
				func(a *models.RewardAccountOutput) bool {
					return a.CapturedSlot > slot
				},
			)
		case RewardDerivedReplace, RewardDerivedPrune:
		}
		out = append(out, c)
	}
	return out
}

// rewardDerivedView answers reads of the derived rows as txn sees the set.
type rewardDerivedView struct {
	r  RewardDerivedReader
	at *uint64
	st rewardDerivedStaged
}

func (ls *LedgerState) newRewardDerivedView(
	r RewardDerivedReader,
	txn ledgerdb.Txn,
) rewardDerivedView {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	st := ls.rewardDerivedStagedOps(txn)
	return rewardDerivedView{r: r, at: st.ceiling, st: st}
}

// cleared reports whether a staged change removed the committed rows of
// epoch, and the changes that follow the last such removal.
func (v rewardDerivedView) cleared(epoch uint64) (bool, []RewardDerivedChange) {
	cleared := false
	last := -1
	for i, c := range v.st.changes {
		if (c.Kind == RewardDerivedReplace && c.Epoch == epoch) ||
			(c.Kind == RewardDerivedPrune && epoch < c.PruneBelow) {
			cleared, last = true, i
		}
	}
	return cleared, v.st.changes[last+1:]
}

func (v rewardDerivedView) pools(
	epoch uint64,
) ([]*models.RewardPoolOutput, error) {
	cleared, changes := v.cleared(epoch)
	byPool := map[string]*models.RewardPoolOutput{}
	if !cleared {
		rows, err := v.r.RewardPoolOutputRows(epoch, v.at)
		if err != nil {
			return nil, err
		}
		for _, p := range rows {
			byPool[string(p.PoolKeyHash)] = p
		}
	}
	for _, c := range changes {
		if c.Kind != RewardDerivedPools {
			continue
		}
		for _, p := range c.Pools {
			if p.Epoch == epoch {
				cp := *p
				cp.PoolKeyHash = bytes.Clone(p.PoolKeyHash)
				byPool[string(p.PoolKeyHash)] = &cp
			}
		}
	}
	out := make([]*models.RewardPoolOutput, 0, len(byPool))
	for _, p := range byPool {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b *models.RewardPoolOutput) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	return out, nil
}

func accountOutputLess(a, b *models.RewardAccountOutput) int {
	return cmp.Or(
		cmp.Compare(a.CredentialTag, b.CredentialTag),
		bytes.Compare(a.StakingKey, b.StakingKey),
		bytes.Compare(a.PoolKeyHash, b.PoolKeyHash),
		cmp.Compare(a.RewardType, b.RewardType),
	)
}

func accountOutputID(a *models.RewardAccountOutput) string {
	return string([]byte{a.CredentialTag}) + string(a.StakingKey) + "/" +
		string(a.PoolKeyHash) + "/" + a.RewardType
}

// accounts returns the epoch's account outputs; with credential set, only
// that credential's.
func (v rewardDerivedView) accounts(
	epoch uint64,
	credential *models.StakeCredentialRef,
) ([]*models.RewardAccountOutput, error) {
	cleared, changes := v.cleared(epoch)
	rows := map[string]*models.RewardAccountOutput{}
	if !cleared {
		var got []*models.RewardAccountOutput
		var err error
		if credential != nil {
			got, err = v.r.RewardCredentialOutputRows(
				epoch, credential.Tag, credential.Key, v.at,
			)
		} else {
			got, err = v.r.RewardAccountOutputRows(epoch, v.at)
		}
		if err != nil {
			return nil, err
		}
		for _, a := range got {
			rows[accountOutputID(a)] = a
		}
	}
	for _, c := range changes {
		if c.Kind != RewardDerivedAccounts {
			continue
		}
		for _, a := range c.Accounts {
			if a.Epoch != epoch || (credential != nil &&
				(a.CredentialTag != credential.Tag ||
					!bytes.Equal(a.StakingKey, credential.Key))) {
				continue
			}
			cp := cloneRewardOutput(a)
			rows[accountOutputID(&cp)] = &cp
		}
	}
	out := make([]*models.RewardAccountOutput, 0, len(rows))
	for _, a := range rows {
		out = append(out, a)
	}
	slices.SortFunc(out, accountOutputLess)
	return out, nil
}

func (v rewardDerivedView) cursor(epoch uint64) ([]byte, error) {
	cleared, changes := v.cleared(epoch)
	var raw []byte
	if !cleared {
		var err error
		raw, _, err = v.r.RewardCursorRow(epoch, v.at)
		if err != nil {
			return nil, err
		}
	}
	for _, c := range changes {
		if c.Kind == RewardDerivedCursor && c.Epoch == epoch {
			raw = bytes.Clone(c.Cursor)
		}
	}
	return raw, nil
}
