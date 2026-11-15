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

package badgerutxo

import (
	"errors"
	"sync"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/dgraph-io/badger/v4"
)

// ErrSnapshotReleased is returned by a read of a snapshot after Release.
var ErrSnapshotReleased = errors.New("badgerutxo: snapshot released")

// ErrSnapshotReadOnly is returned by a write through a snapshot.
var ErrSnapshotReadOnly = errors.New("badgerutxo: snapshot is read-only")

// snapshot is one Badger read transaction shared by every read of a
// snapshot Store. A read-only transaction is safe for concurrent reads; what
// is not is Discard racing one, so release waits for the reads in flight.
type snapshot struct {
	txn *badger.Txn

	mu     sync.Mutex
	idle   *sync.Cond
	active int
	closed bool
}

func (sn *snapshot) acquire() (*badger.Txn, error) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed {
		return nil, ErrSnapshotReleased
	}
	sn.active++
	return sn.txn, nil
}

func (sn *snapshot) done() {
	sn.mu.Lock()
	sn.active--
	if sn.active == 0 {
		sn.idle.Broadcast()
	}
	sn.mu.Unlock()
}

func (sn *snapshot) release() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed {
		return
	}
	sn.closed = true
	for sn.active > 0 {
		sn.idle.Wait()
	}
	sn.txn.Discard()
}

// Snapshot returns a read-only Store whose reads all see the database as it
// is now: the state of every change applied before the call, and none after.
// Release it with Close; until then Badger keeps every version the snapshot
// can still read, so a snapshot is held for the duration of one job, not
// longer. A snapshot must not span a Load of any family, which drops the
// rows the snapshot reads.
func (s *Store) Snapshot() (*Store, error) {
	if s.snap != nil {
		return nil, ErrSnapshotReadOnly
	}
	sn := &snapshot{txn: s.beginRead()}
	sn.idle = sync.NewCond(&sn.mu)
	return &Store{db: s.db, agg: s.agg, snap: sn}, nil
}

// view runs fn on the snapshot's transaction, or on a fresh read
// transaction of the database.
func (s *Store) view(fn func(txn *badger.Txn) error) error {
	if s.snap == nil {
		txn := s.beginRead()
		defer txn.Discard()
		return fn(txn)
	}
	txn, err := s.snap.acquire()
	if err != nil {
		return err
	}
	defer s.snap.done()
	return fn(txn)
}

// readTxn opens a read transaction the caller discards. A snapshot has no
// transaction to hand out.
func (s *Store) readTxn() (*badger.Txn, error) {
	if s.snap != nil {
		return nil, ErrSnapshotReadOnly
	}
	return s.beginRead(), nil
}

// beginRead starts a read transaction once no chunked batch is part
// written. The transaction then reads its own version, so the wait covers
// only its start.
func (s *Store) beginRead() *badger.Txn {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.db.NewTransaction(false)
}

func (s *Store) update(fn func(txn *badger.Txn) error) error {
	if s.snap != nil {
		return ErrSnapshotReadOnly
	}
	return s.db.Update(fn)
}

func (s *Store) dropPrefix(prefixes ...[]byte) error {
	if drained := nodesettings.FamilyMask(s.loadsRefused.Load()); drained != 0 {
		var held bool
		err := s.view(func(txn *badger.Txn) error {
			for _, p := range prefixes {
				if !tagDrained(drained, p) {
					continue
				}
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				opts.Prefix = p
				it := txn.NewIterator(opts)
				it.Rewind()
				held = held || it.Valid()
				it.Close()
			}
			return nil
		})
		if err != nil {
			return err
		}
		if held {
			return ErrLoadsRefused
		}
	}
	return s.dropPrefixAlways(prefixes...)
}

// dropPrefixAlways is dropPrefix for a rebuild, which replaces the whole
// store rather than reload one family from a source that may be gone.
func (s *Store) dropPrefixAlways(prefixes ...[]byte) error {
	if s.snap != nil {
		return ErrSnapshotReadOnly
	}
	// A load leaves the dropped families without history before the tip.
	var floor uint64
	if err := s.view(func(txn *badger.Txn) error {
		tip, ok, err := readTip(txn)
		if ok {
			floor = tip.Slot + 1
		}
		return err
	}); err != nil {
		return err
	}
	if err := s.raiseJournalFloor(floor); err != nil {
		return err
	}
	for _, p := range prefixes {
		if len(p) > 0 && !rollsBackWithoutJournal(p[0]) {
			if err := s.raiseFloor(loadFloorKey, floor); err != nil {
				return err
			}
			break
		}
	}
	return s.db.DropPrefix(prefixes...)
}

// newWriteBatch panics on a snapshot: every path that writes a batch is a
// writer, which the ledger never reaches through a snapshot.
func (s *Store) newWriteBatch() *badger.WriteBatch {
	if s.snap != nil {
		panic(ErrSnapshotReadOnly)
	}
	return s.db.NewWriteBatch()
}

// rollsBackWithoutJournal reports whether a rollback restores the family under
// tag without its state journal entries: it deletes the rows after the target
// by slot, or the load writes the tombstones that stand for the earlier rows.
// The reward output rows are listed because a load of them writes no entry,
// and a tombstone or absent entry per row could not restore a row folded after
// the target, which the load omits; a rollback below a load keeps them as
// loaded.
func rollsBackWithoutJournal(tag byte) bool {
	switch tag {
	case NonceTag, EpochTag, SnapshotTag, SummaryTag, NetworkStateTag,
		DonationTag, PParamsTag, PParamsSlotTag, RewardRecordTag,
		RewardStakeInputTag, RewardPoolInputTag, RewardSnapshotTag,
		RewardPotsTag, RewardDerivedTag, RewardCursorTag,
		RewardOutputTag, RewardEpochTag, RewardRoundTag, RewardPostSnapTag:
		return true
	}
	return false
}

// journaledFamilyTags are the row families a rollback restores through the
// state journal and that a load writes with no entries before the tip. The
// DRep dormancy counter and the reward block counts are not listed: each is a
// row every set holds, so it would mark every load, and a rollback below a
// load is accepted today.
var journaledFamilyTags = []byte{
	AccountTag, AccountPoolTag, AccountDRepTag, DRepTag, DRepLinkTag,
	PoolTag, PoolHistoryTag, StakeHistoryTag, DRepDepositTag,
	OpCertTag, GenesisDelegationTag, MIRCertTag, CommitteeMemberTag,
	CommitteeQuorumTag, ConstitutionTag, HotAuthTag, ResignTag, ProposalTag,
	VoteTag, PParamUpdateTag, ImportBaselineTag, WitnessTag, ClosureUtxoTag,
	DelegationTag,
}
