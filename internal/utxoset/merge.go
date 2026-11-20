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

package utxoset

import (
	"bytes"
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// LoadUtxoSetCatchUp replaces the set's outputs with the ones feed puts, as of
// point, for a catch-up whose snapshot is newer than the set, and keeps the
// ledger-state families the set holds: a catch-up of a database that keeps its
// ledger state in SQL keeps the rows older than the snapshot, and a set that
// is the only copy has to as well. The families' snapshot rows are merged in by
// the next LoadLedgerState. A set past point is rolled back to it through its
// journal first, and the load fails when the journal does not reach it. A set
// with no tip holds nothing the snapshot continues, so it is replaced whole.
func (s *Set) LoadUtxoSetCatchUp(
	point ocommon.Point,
	feed func(put func(txID []byte, index uint32, cbor []byte) error) error,
) error {
	incomplete, err := s.store.BootstrapIncomplete()
	if err != nil {
		return fmt.Errorf("utxoset: catch-up load: %w", err)
	}
	if !incomplete {
		tip, ok, err := s.store.Tip()
		if err != nil {
			return fmt.Errorf("utxoset: catch-up load: %w", err)
		}
		if !ok {
			return s.LoadUtxoSet(point, feed)
		}
		if tip.Slot > point.Slot {
			// The set holds the history of blocks past the snapshot, and its
			// families hold rows those blocks wrote. Rolling back to the
			// snapshot's point undoes both; replacing the set whole would
			// lose the history below it as well.
			if _, err := s.store.Rollback(point); err != nil {
				return fmt.Errorf(
					"utxoset: catch-up load: the set is at slot %d, past the snapshot's slot %d, and cannot be rolled back to it: %w",
					tip.Slot, point.Slot, err,
				)
			}
		}
	}
	err = s.store.BootstrapCatchUp(point,
		func(put func(badgerutxo.Output) error) error {
			return feed(func(txID []byte, index uint32, cbor []byte) error {
				return put(badgerutxo.Output{
					Ref:  badgerutxo.NewRef(txID, index),
					Cbor: cbor,
				})
			})
		},
	)
	if err != nil {
		return fmt.Errorf("utxoset: catch-up load: %w", err)
	}
	n, _ := s.store.Count()
	s.logger.Info(
		"loaded the live UTxO set for a catch-up, keeping its ledger-state families",
		"live_outputs", n,
		"slot", point.Slot,
	)
	return nil
}

// mergeState is what a merge notes of the snapshot's rows, so the rows the
// snapshot no longer lists can be marked inactive afterwards.
type mergeState struct {
	accounts  map[string]struct{}
	dreps     map[string]struct{}
	pools     map[string]struct{}
	proposals map[string]struct{}
}

func newMergeState() *mergeState {
	return &mergeState{
		accounts:  map[string]struct{}{},
		dreps:     map[string]struct{}{},
		pools:     map[string]struct{}{},
		proposals: map[string]struct{}{},
	}
}

func (m *mergeState) noteAccount(tag uint8, key []byte) {
	if m != nil {
		m.accounts[string(append([]byte{tag}, key...))] = struct{}{}
	}
}

func (m *mergeState) noteDrep(k []byte) {
	if m != nil {
		m.dreps[string(k)] = struct{}{}
	}
}

func (m *mergeState) notePool(k []byte) {
	if m != nil {
		m.pools[string(k)] = struct{}{}
	}
}

func (m *mergeState) noteProposal(k []byte) {
	if m != nil {
		m.proposals[string(k)] = struct{}{}
	}
}

// epochAtSlot returns the epoch row that holds slot: the one with the highest
// start slot at or below it. The merge's tip is the snapshot's point, so this
// is the snapshot's epoch even where the metadata store holds later epoch rows;
// the store's own reconcile retires stale pools in that epoch. With no row at
// or below slot it returns the newest row.
func epochAtSlot(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	slot uint64,
) (models.Epoch, error) {
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return models.Epoch{}, err
	}
	var best *models.Epoch
	for i := range epochs {
		e := &epochs[i]
		if e.StartSlot > slot {
			continue
		}
		if best == nil || e.EpochId > best.EpochId {
			best = e
		}
	}
	if best == nil {
		return latestEpoch(ldb, txn)
	}
	return *best, nil
}

// mergeLedgerState loads the snapshot's rows of every family over the ones the
// set holds, then marks the accounts, DReps, pools and proposals the snapshot
// no longer lists inactive, as the metadata store's catch-up reconcile does
// for the rows it holds. The pending mark clears only when all of it has been
// written; a merge that stops part way writes the same rows again.
func (s *Set) mergeLedgerState(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) (err error) {
	if err := s.store.BeginMerge(); err != nil {
		return err
	}
	s.merge = newMergeState()
	defer func() {
		s.merge = nil
		if endErr := s.store.EndMerge(err == nil); endErr != nil && err == nil {
			err = endErr
		}
	}()
	if err := s.loadFamilies(ctx, ldb); err != nil {
		return err
	}
	return s.reconcileStale(ctx, ldb)
}

const staleChunk = 500

// reconcileStale marks every account, DRep, pool and proposal the set holds
// live and the merged snapshot does not list. Like the metadata store's
// reconcile it never deletes history: an account turns inactive with no
// delegation, a DRep inactive, a pool scheduled to retire at the current epoch, and a proposal soft-deleted.
func (s *Set) reconcileStale(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	m := s.merge
	tip, ok, err := s.store.Tip()
	if err != nil {
		return fmt.Errorf("utxoset: read tip: %w", err)
	}
	if !ok {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	epoch, err := epochAtSlot(ldb, txn, tip.Slot)
	txn.Release()
	if err != nil {
		return fmt.Errorf("utxoset: read the snapshot's epoch: %w", err)
	}
	accounts := make([]badgerutxo.AccountPut, 0)
	rows := make([]badgerutxo.RowPut, 0)
	err = s.store.Accounts(func(r badgerutxo.AccountRow) error {
		if !r.Active {
			return nil
		}
		if _, ok := m.accounts[string(append([]byte{r.Tag}, r.Key...))]; ok {
			return nil
		}
		accounts = append(accounts, badgerutxo.AccountPut{
			Slot: tip.Slot,
			Row: badgerutxo.AccountRow{
				Tag: r.Tag, Key: r.Key, Active: false, Reward: r.Reward,
				AddedSlot: tip.Slot, CreatedSlot: r.CreatedSlot,
				ExpirationEpoch: r.ExpirationEpoch,
			},
		})
		ref := models.NewStakeCredentialRef(r.Tag, r.Key)
		value, err := encodeImportBaseline(ledger.ImportBaseline{
			Ref: ref, AddedSlot: tip.Slot,
		})
		if err != nil {
			return err
		}
		rows = append(rows, badgerutxo.RowPut{
			Slot:  tip.Slot,
			Key:   importBaselineKey(ref),
			Value: value,
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: scan accounts: %w", err)
	}
	deactivatedDreps := make(map[string]struct{})
	err = s.store.Rows([]byte{badgerutxo.DRepTag}, func(k, v []byte) error {
		if _, ok := m.dreps[string(k)]; ok {
			return nil
		}
		d, err := decodeDrep(k, v)
		if err != nil {
			return err
		}
		if !d.Active {
			return nil
		}
		d.Active = false
		deactivatedDreps[string(k)] = struct{}{}
		rows = append(rows, badgerutxo.RowPut{
			Slot: tip.Slot, Key: bytes.Clone(k), Value: encodeDrep(*d),
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: scan DReps: %w", err)
	}
	err = s.store.Rows([]byte{badgerutxo.PoolTag}, func(k, v []byte) error {
		if _, ok := m.pools[string(k)]; ok {
			return nil
		}
		pool := bytes.Clone(k[1:])
		st, err := decodePoolState(pool, v)
		if err != nil {
			return err
		}
		retiring := epoch.EpochId
		st.RetiringEpoch = &retiring
		sv, err := encodePoolState(*st)
		if err != nil {
			return err
		}
		ev, err := encodePoolEvent(ledger.PoolEvent{
			Slot:          tip.Slot,
			Position:      ledger.PoolEventSyntheticPosition,
			RetiringEpoch: epoch.EpochId,
		})
		if err != nil {
			return err
		}
		rows = append(rows,
			badgerutxo.RowPut{Slot: tip.Slot, Key: bytes.Clone(k), Value: sv},
			badgerutxo.RowPut{
				Slot:  tip.Slot,
				Key:   poolEventKey(pool, tip.Slot, ledger.PoolEventSyntheticPosition),
				Value: ev,
			},
		)
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: scan pools: %w", err)
	}
	proposals, err := s.ProposalsAt(nil)
	if err != nil {
		return fmt.Errorf("utxoset: scan proposals: %w", err)
	}
	for i := range proposals {
		p := proposals[i]
		key := proposalPrefix(p.TxHash, p.ActionIndex)
		if _, ok := m.proposals[string(key)]; ok ||
			p.EnactedEpoch != nil || p.DroppedEpoch != nil || p.DeletedSlot != nil {
			continue
		}
		deleted := tip.Slot
		p.DeletedSlot = &deleted
		rows = append(rows, badgerutxo.RowPut{
			Slot: tip.Slot, Key: key, Value: encodeProposal(p),
		})
	}
	links, err := s.staleDrepLinks(tip.Slot, accounts, deactivatedDreps)
	if err != nil {
		return err
	}
	rows = append(rows, links...)
	for len(accounts) > 0 || len(rows) > 0 {
		na, nr := min(staleChunk, len(accounts)), min(staleChunk, len(rows))
		if _, err := s.store.ApplyLedgerBatch(nil, badgerutxo.LedgerBatch{
			Accounts: accounts[:na], Rows: rows[:nr],
		}); err != nil {
			return fmt.Errorf("utxoset: mark stale rows inactive: %w", err)
		}
		accounts, rows = accounts[na:], rows[nr:]
	}
	return nil
}

// staleDrepLinks returns the deletes of the live delegator links whose account
// no longer delegates to the link's DRep: an account the merge deactivates, or
// one the snapshot delegates elsewhere. A DRep that is not active holds no
// delegator links, even where a live account's delegation still names it, as
// the metadata store's DRep deactivation leaves it.
func (s *Set) staleDrepLinks(
	slot uint64,
	deactivated []badgerutxo.AccountPut,
	deactivatedDreps map[string]struct{},
) ([]badgerutxo.RowPut, error) {
	gone := make(map[string]struct{}, len(deactivated))
	for _, a := range deactivated {
		gone[string(append([]byte{a.Row.Tag}, a.Row.Key...))] = struct{}{}
	}
	out := make([]badgerutxo.RowPut, 0)
	err := s.store.Keys([]byte{badgerutxo.DRepLinkTag}, func(k []byte) error {
		drep, stake, err := decodeDrepLinkKey(k)
		if err != nil {
			return err
		}
		_, drepGone := deactivatedDreps[string(drepKey(drep.Tag, drep.Key))]
		if _, ok := gone[string(append([]byte{stake.Tag}, stake.Key...))]; !ok && !drepGone {
			r, found, err := s.store.Account(stake.Tag, stake.Key)
			if err != nil {
				return err
			}
			if found && r.Active && bytes.Equal(r.Drep, drep.Key) {
				return nil
			}
		}
		out = append(out, badgerutxo.RowPut{
			Slot: slot, Key: bytes.Clone(k), Delete: true,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("utxoset: scan DRep links: %w", err)
	}
	return out, nil
}
