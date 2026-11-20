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
	"context"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// OpCertState is a pool's operational certificate counter as the metadata
// store's per-slot rows give it: the highest sequence observed, the slot of
// the pool's latest observation, and the sequence observed at that slot.
// LastSequence below Sequence means a lower counter followed the highest.
type OpCertState struct {
	Pool         []byte
	Sequence     uint64
	Slot         uint64
	LastSequence uint64
}

// OpCertChange is a pool's counter as the observation at Slot leaves it.
type OpCertChange struct {
	Slot  uint64
	State OpCertState
}

// OpCertReader serves op-cert counters from a live UTxO set that computes
// them.
type OpCertReader interface {
	OpCertsLoaded() (bool, error)
	OpCertState(pool []byte) (*OpCertState, error)
	OpCertStateAt(pool []byte, slot uint64) (*OpCertState, error)
	OpCertStates(fn func(*OpCertState) error) error
}

func (ls *LedgerState) opCertReader(txn ledgerdb.Txn) OpCertReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(OpCertReader)
	if !ok {
		return nil
	}
	return r
}

// opCertView returns pool's counter as txn sees the set, as of slot when at
// is set: the last staged change at or before slot that no staged rollback
// dropped, else the set's counter after the lowest staged rollback.
//
// The oldest counter the set holds for a pool is its baseline, the counter
// map of the tip the set was loaded at or of the slot its journal starts
// from, and it answers every earlier slot. Its Slot may therefore be after
// the slot asked for.
func (ls *LedgerState) opCertView(
	r OpCertReader,
	txn ledgerdb.Txn,
	pool []byte,
	at *uint64,
) (*OpCertState, error) {
	ceiling := uint64(math.MaxUint64)
	changes := make([]OpCertChange, 0)
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			ceiling = min(ceiling, to)
			changes = slices.DeleteFunc(changes, func(c OpCertChange) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.opCerts {
			if bytes.Equal(c.State.Pool, pool) {
				changes = append(changes, c)
			}
		}
	}
	for _, c := range slices.Backward(changes) {
		if at == nil || c.Slot <= *at {
			st := c.State
			return &st, nil
		}
	}
	slot := ceiling
	if at != nil {
		slot = min(*at, ceiling)
	}
	if slot == math.MaxUint64 {
		return r.OpCertState(pool)
	}
	return r.OpCertStateAt(pool, slot)
}

// opCertServing returns the op-cert reader when counter reads are answered
// from the set, or nil when they stay on the metadata store.
func (ls *LedgerState) opCertServing(txn ledgerdb.Txn) OpCertReader {
	r := ls.opCertReader(txn)
	if r == nil || !ls.setServes(&ls.opCertsSetLoaded, r.OpCertsLoaded) {
		return nil
	}
	return r
}

// opCertAfterFromSet answers LatestPoolOpCertSequenceAfter from the set when
// the set can tell. The set keeps each pool's highest sequence, not each
// row's, so the highest sequence after afterSlot is known when the pool has
// no observation after it, when its latest observation repeats the highest,
// or when the highest rose after afterSlot: it was then observed after it.
// A lower counter observed last, with the highest already reached by
// afterSlot, could be the only one after it, and the set cannot tell.
func (ls *LedgerState) opCertAfterFromSet(
	r OpCertReader,
	txn ledgerdb.Txn,
	pool []byte,
	afterSlot uint64,
) (sequence uint64, found, ok bool, err error) {
	latest, err := ls.opCertView(r, txn, pool, nil)
	if err != nil {
		return 0, false, false, err
	}
	if latest == nil || latest.Slot <= afterSlot {
		return 0, false, true, nil
	}
	if latest.LastSequence == latest.Sequence {
		return latest.Sequence, true, true, nil
	}
	prior, err := ls.opCertView(r, txn, pool, &afterSlot)
	if err != nil {
		return 0, false, false, err
	}
	if prior == nil || latest.Sequence > prior.Sequence {
		return latest.Sequence, true, true, nil
	}
	return 0, false, false, nil
}

// opCertInRangeFromSet answers LatestPoolOpCertSequenceInRange from the set
// when the set can tell, on the same terms as opCertAfterFromSet: no
// observation in the range, the highest sequence rose within it, or the
// latest observation in it repeats the highest.
func (ls *LedgerState) opCertInRangeFromSet(
	r OpCertReader,
	txn ledgerdb.Txn,
	pool []byte,
	startSlot, endSlot uint64,
) (sequence uint64, found, ok bool, err error) {
	if startSlot > endSlot {
		return 0, false, true, nil
	}
	atEnd, err := ls.opCertView(r, txn, pool, &endSlot)
	if err != nil {
		return 0, false, false, err
	}
	if atEnd == nil || atEnd.Slot < startSlot {
		return 0, false, true, nil
	}
	if startSlot == 0 || atEnd.LastSequence == atEnd.Sequence {
		return atEnd.Sequence, true, true, nil
	}
	before := startSlot - 1
	atStart, err := ls.opCertView(r, txn, pool, &before)
	if err != nil {
		return 0, false, false, err
	}
	if atStart == nil || atEnd.Sequence > atStart.Sequence {
		return atEnd.Sequence, true, true, nil
	}
	return 0, false, false, nil
}

// stageOpCert stages an observed counter as the metadata store records it:
// the pool's highest sequence rises only when the observation is higher,
// and the slot of its latest observation only moves forward. An
// observation that changes neither stages nothing.
func (ls *LedgerState) stageOpCert(
	txn ledgerdb.Txn,
	pool lcommon.PoolKeyHash,
	sequence, slot uint64,
) {
	r := ls.opCertReader(txn)
	if r == nil {
		// The SQL write of a drained family was skipped, so a counter the
		// set cannot stage would be lost.
		if ls.familyDrained(nodesettings.FamilyOpCerts) {
			ls.failAccountStaging(drainedUnserved(nodesettings.FamilyOpCerts))
		}
		return
	}
	prev, err := ls.opCertView(r, txn, pool.Bytes(), nil)
	if err != nil {
		ls.failAccountStaging(err)
		return
	}
	next := OpCertState{
		Pool: bytes.Clone(pool.Bytes()), Sequence: sequence, Slot: slot,
		LastSequence: sequence,
	}
	if prev != nil {
		next.Sequence = max(prev.Sequence, sequence)
		next.Slot = max(prev.Slot, slot)
		next.LastSequence = prev.LastSequence
		if slot >= prev.Slot {
			next.LastSequence = sequence
		}
		if bytes.Equal(next.Pool, prev.Pool) &&
			next.Sequence == prev.Sequence && next.Slot == prev.Slot &&
			next.LastSequence == prev.LastSequence {
			return
		}
	}
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		OpCerts: []OpCertChange{{Slot: slot, State: next}},
	})
}

// opCertsDrained reports whether the op-cert rows are gone from the metadata
// store, so their SQL writes are skipped and the staged counter is the only
// record.
func (s stagingLedgerDB) opCertsDrained() bool {
	return s.ls.familyDrained(nodesettings.FamilyOpCerts)
}

// opCertUnserved is the error of a read the set did not answer: it holds no
// op-cert counters, or a lower counter followed the highest after a slot the
// highest was already reached by.
func (s stagingLedgerDB) opCertUnserved() error {
	return s.ls.setFamilyUnserved(nodesettings.FamilyOpCerts)
}

func (s stagingLedgerDB) UpdatePoolOpCertSequence(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	if !s.opCertsDrained() {
		err := s.LedgerDB.UpdatePoolOpCertSequence(
			ctx, pkh, sequence, slot, txn,
		)
		if err != nil {
			return err
		}
	}
	s.ls.stageOpCert(txn, pkh, sequence, slot) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	s.ls.stageBlockMinted(txn, pkh, slot)      //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}

func (s stagingLedgerDB) UpdatePoolOpCertSequenceBatched(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
) error {
	if !s.opCertsDrained() {
		err := s.LedgerDB.UpdatePoolOpCertSequenceBatched(
			ctx, pkh, sequence, slot, acc, txn,
		)
		if err != nil {
			return err
		}
	}
	s.ls.stageOpCert(txn, pkh, sequence, slot) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	s.ls.stageBlockMinted(txn, pkh, slot)      //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}

func (s stagingLedgerDB) LatestPoolOpCertSequence(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return 0, false, s.opCertUnserved()
	}
	st, err := s.ls.opCertView(r, txn, pkh.Bytes(), nil)
	if err != nil || st == nil {
		return 0, false, err
	}
	return st.Sequence, true, nil
}

// LatestPoolOpCertSequenceAfter returns the highest recorded sequence after
// afterSlot.
func (s stagingLedgerDB) LatestPoolOpCertSequenceAfter(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	afterSlot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return 0, false, s.opCertUnserved()
	}
	seq, found, ok, err := s.ls.opCertAfterFromSet(r, txn, pkh.Bytes(), afterSlot)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, s.opCertUnserved()
	}
	return seq, found, nil
}

func (s stagingLedgerDB) LatestPoolOpCertSequenceAtOrBefore(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return 0, false, s.opCertUnserved()
	}
	st, err := s.ls.opCertView(r, txn, pkh.Bytes(), &slot)
	if err != nil || st == nil {
		return 0, false, err
	}
	return st.Sequence, true, nil
}

// LatestPoolOpCertSequenceInRange returns the highest recorded sequence in
// the requested range.
func (s stagingLedgerDB) LatestPoolOpCertSequenceInRange(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return 0, false, s.opCertUnserved()
	}
	seq, found, ok, err := s.ls.opCertInRangeFromSet(r, txn, pkh.Bytes(), startSlot, endSlot)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, s.opCertUnserved()
	}
	return seq, found, nil
}

// LatestPoolOpCertSequences returns every pool's highest recorded sequence.
func (s stagingLedgerDB) LatestPoolOpCertSequences(
	ctx context.Context,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return nil, s.opCertUnserved()
	}
	got := make(map[string]uint64)
	if err := s.ls.opCertLatest(r, txn, got); err != nil {
		return nil, err
	}
	return got, nil
}

// opCertPools returns every pool the set holds a counter for or the batches
// txn sees have staged one for, each once.
func (ls *LedgerState) opCertPools(
	r OpCertReader,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	var pools [][]byte
	seen := make(map[string]struct{})
	add := func(p []byte) {
		if _, ok := seen[string(p)]; !ok {
			seen[string(p)] = struct{}{}
			pools = append(pools, p)
		}
	}
	if err := r.OpCertStates(func(st *OpCertState) error {
		add(st.Pool)
		return nil
	}); err != nil {
		return nil, err
	}
	for _, o := range ls.stagedOps(txn) {
		for _, c := range o.opCerts {
			add(c.State.Pool)
		}
	}
	return pools, nil
}

// opCertLatest fills latest with every pool's highest sequence as txn sees
// the set.
func (ls *LedgerState) opCertLatest(
	r OpCertReader,
	txn ledgerdb.Txn,
	latest map[string]uint64,
) error {
	pools, err := ls.opCertPools(r, txn)
	if err != nil {
		return err
	}
	for _, p := range pools {
		st, err := ls.opCertView(r, txn, p, nil)
		if err != nil {
			return err
		}
		if st != nil {
			latest[string(p)] = st.Sequence
		}
	}
	return nil
}

// opCertObservedAt reports whether any pool's counter was observed at
// exactly slot: a pool whose counter as of slot has slot as its latest
// observation.
func (ls *LedgerState) opCertObservedAt(
	r OpCertReader,
	txn ledgerdb.Txn,
	slot uint64,
) (bool, error) {
	pools, err := ls.opCertPools(r, txn)
	if err != nil {
		return false, err
	}
	for _, p := range pools {
		st, err := ls.opCertView(r, txn, p, &slot)
		if err != nil {
			return false, err
		}
		if st != nil && st.Slot == slot {
			return true, nil
		}
	}
	return false, nil
}

// opCertLatestAt fills latest with every pool's highest sequence as of slot
// as txn sees the set.
func (ls *LedgerState) opCertLatestAt(
	r OpCertReader,
	txn ledgerdb.Txn,
	slot uint64,
	latest map[string]uint64,
) error {
	pools, err := ls.opCertPools(r, txn)
	if err != nil {
		return err
	}
	for _, p := range pools {
		st, err := ls.opCertView(r, txn, p, &slot)
		if err != nil {
			return err
		}
		if st != nil {
			latest[string(p)] = st.Sequence
		}
	}
	return nil
}

// LatestPoolOpCertSequencesAtOrBefore returns every pool's highest recorded
// sequence as of slot.
func (s stagingLedgerDB) LatestPoolOpCertSequencesAtOrBefore(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return nil, s.opCertUnserved()
	}
	got := make(map[string]uint64)
	if err := s.ls.opCertLatestAt(r, txn, slot, got); err != nil {
		return nil, err
	}
	return got, nil
}

// PoolOpCertSequencesExistAtSlot reports whether the set recorded any pool
// op-cert sequence at slot.
func (s stagingLedgerDB) PoolOpCertSequencesExistAtSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	r := s.ls.opCertServing(txn)
	if r == nil {
		return false, s.opCertUnserved()
	}
	return s.ls.opCertObservedAt(r, txn, slot)
}
