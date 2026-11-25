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
	"encoding/binary"
	"errors"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// Estimated cost of one output in a transaction, deliberately above the
// real one: a plan that is too coarse only makes more transactions, while one
// that is too fine fails the commit. The entry cost counts the output row
// and its running-sum update; the byte cost adds the key, the journal
// operation and the prior value a removed output records.
const (
	outputEntries = 2
	keyBytes      = refLen + 1
	sumBytes      = 64
	journalBytes  = refLen + 5
	// removedBytes stands for the prior value a removed output keeps in the
	// journal, which is not known until the removal is applied.
	removedBytes = 512
	// perPointEntries covers a point's journal record and sequence number
	// and the batch's count row.
	perPointEntries = 4
	// rowBytes is the estimate for a family row that does not state its
	// size.
	rowBytes = 512
	// familyEntriesPerRow covers a row and its journal entry.
	familyEntriesPerRow = 3
)

// txnBudget returns how many entries and bytes one transaction of a chunked
// batch may hold: half of Badger's limit, since the plan is an estimate.
func (s *Store) txnBudget() (entries, bytes int64) {
	entries, bytes = s.db.MaxBatchCount(), s.db.MaxBatchSize()
	if s.budgetEntries > 0 {
		entries = s.budgetEntries
	}
	if s.budgetBytes > 0 {
		bytes = s.budgetBytes
	}
	return max(entries/2, 1), max(bytes/2, 1)
}

// cost is a plan's estimate of one transaction's weight.
type cost struct {
	entries, bytes int64
}

func (c cost) add(o cost) cost {
	return cost{c.entries + o.entries, c.bytes + o.bytes}
}

func (c cost) fits(entries, bytes int64) bool {
	return c.entries <= entries && c.bytes <= bytes
}

// pointUnit is the changes of one point, which a journal record covers and
// so a transaction never splits.
type pointUnit struct {
	changes []Change
	cost    cost
}

func pointUnits(changes []Change) []pointUnit {
	var units []pointUnit
	for i := 0; i < len(changes); {
		j := i + 1
		for j < len(changes) && samePoint(changes[j].Point, changes[i].Point) {
			j++
		}
		u := pointUnit{
			changes: changes[i:j],
			cost:    cost{entries: perPointEntries},
		}
		for _, c := range changes[i:j] {
			for _, o := range c.Added {
				u.cost.entries += outputEntries
				u.cost.bytes += int64(
					keyBytes + len(o.Cbor) + sumBytes + journalBytes,
				)
			}
			for range c.Removed {
				u.cost.entries += outputEntries
				u.cost.bytes += int64(
					keyBytes + sumBytes + journalBytes + removedBytes,
				)
			}
		}
		units = append(units, u)
		i = j
	}
	return units
}

// familyCost estimates the batch's rows other than UTxO changes, which all
// go in the last transaction with the tip. The snapshot and reward block ops
// are not in it: opsCost weighs them, since a delete is as heavy as the rows
// it finds.
func familyCost(b LedgerBatch) cost {
	rows := len(b.Epochs) + len(b.Summaries) +
		len(b.NetworkStates) + len(b.Donations) + len(b.PParams) +
		len(b.Nonces)
	c := cost{
		entries: int64(rows*familyEntriesPerRow) + 8,
		bytes:   int64(rows * rowBytes),
	}
	c = c.add(accountsCost(b.Accounts)).add(recordsCost(b.RewardRecords))
	if b.EraSwitch != nil {
		c.entries++
	}
	for _, r := range b.Rows {
		c.entries += familyEntriesPerRow
		c.bytes += int64(len(r.Key) + len(r.Value) + rowBytes)
	}
	return c
}

// takeUnits returns how many of units, from the front, a transaction takes
// within the budget. It takes at least one: a point's changes are bounded by
// the block that made them, so the budget is never smaller than one point.
func takeUnits(units []pointUnit, base cost, entries, bytes int64) int {
	c := base
	n := 0
	for _, u := range units {
		if n > 0 && !c.add(u.cost).fits(entries, bytes) {
			break
		}
		c = c.add(u.cost)
		n++
	}
	return n
}

// applyChunked applies batch in one transaction when it fits Badger's limit,
// and otherwise in several.
//
// A chunked batch writes its UTxO changes, point by point and each point with
// its own journal record, in transactions that leave the tip alone. The last
// transaction writes the remaining changes, every other row of the batch and
// the tip. So the tip, and the state it stands for, only moves when the whole
// batch is in, and the journal reaches back through every point written.
//
// A failure mark, written in the first transaction and cleared in the last,
// covers the time between: a set that stopped there, by error or by crash,
// starts with the mark, and the rewind it triggers undoes the journal
// records past the tip, which Rollback does whether or not they are below
// it. Reads that begin while chunks are being written wait for the last one,
// so a reader never sees part of a batch.
func (s *Store) applyChunked(
	tip *ocommon.Point,
	batch LedgerBatch,
	res *ApplyResult,
	pre *priorMark,
) error {
	units := pointUnits(batch.Changes)
	entries, bytes := s.txnBudget()
	// Rows too many for one transaction go first, in transactions of their
	// own, with the batch's rollback ahead of them: a row's put is independent
	// of the batch's other writes, and a rewind of the failure mark undoes
	// whatever landed.
	rows := []RowPut{}
	var (
		accounts     []AccountPut
		records      []RewardRecord
		leadRollback *uint64
	)
	accounts, records = []AccountPut{}, []RewardRecord{}
	mark := failSlot(tip, batch)
	// A batch without a tip, changes or slotted rows writes no failure mark:
	// no journaled commit lies before slot 0, so startup could never rewind
	// a mark there.
	unmarked := mark == 0 && tip == nil && len(batch.Changes) == 0
	var rowsTotal cost
	for _, r := range batch.Rows {
		rowsTotal = rowsTotal.add(rowCost(r))
	}
	rowsTotal = rowsTotal.add(accountsCost(batch.Accounts)).
		add(recordsCost(batch.RewardRecords))
	opsTotal, err := s.opsCost(batch.SnapshotOps, batch.RewardBlocks)
	if err != nil {
		return err
	}
	// Snapshot and reward block ops that do not fit beside the family rows go
	// in transactions of their own, in order, after the batch's rollback. A
	// delete among them runs in as many transactions as its rows need. The
	// rows go ahead of them as well, since a reward block op reads the rows
	// the batch wrote before it.
	extractOps := len(batch.SnapshotOps)+len(batch.RewardBlocks) > 0
	if extractOps {
		stripped := stripOps(batch)
		if !rowsTotal.fits(entries, bytes) {
			stripped.Rows, stripped.Accounts, stripped.RewardRecords =
				nil, nil, nil
		}
		extractOps = !opsTotal.add(familyCost(stripped)).fits(entries, bytes)
	}
	if extractOps || !rowsTotal.fits(entries, bytes) {
		rows, leadRollback = batch.Rows, batch.DeleteNoncesAfter
		accounts, records = batch.Accounts, batch.RewardRecords
		batch.Rows, batch.DeleteNoncesAfter = nil, nil
		batch.Accounts, batch.RewardRecords = nil, nil
	}
	var (
		snaps  = []SnapshotOp{}
		blocks = []RewardBlockOp{}
		family cost
	)
	if extractOps {
		snaps, blocks = batch.SnapshotOps, batch.RewardBlocks
		batch = stripOps(batch)
		family = familyCost(batch)
	} else {
		family = familyCost(batch).add(opsTotal)
	}
	var (
		chain  = chainState{advances: tip != nil}
		chunks int
		locked bool
		// familyAlone keeps the family rows out of any transaction that
		// also holds a point, after one that did failed to commit.
		familyAlone bool
		prior       uint64
		hadPrior    bool
	)
	defer func() {
		if locked {
			s.gate.Unlock()
		}
	}()
	for {
		// The last transaction takes the family rows and every point left
		// when all of them fit together; any other takes as many points as
		// fit.
		last := len(rows) == 0 && len(accounts) == 0 && len(records) == 0 &&
			len(snaps) == 0 && len(blocks) == 0 &&
			(len(units) == 0 ||
				(!familyAlone && family.add(sumUnits(units)).fits(entries, bytes)))
		n := len(units)
		var (
			rowChunk     []RowPut
			accountChunk []AccountPut
			recordChunk  []RewardRecord
			snapChunk    []SnapshotOp
			blockChunk   []RewardBlockOp
			// bulk deletes up to limit rows of the delete op at the
			// front of snaps or blocks and reports whether more remain.
			bulk      func(txn *badger.Txn, limit int64) (bool, error)
			bulkLimit int64
			bulkMore  bool
		)
		switch {
		case len(accounts) > 0:
			n = 0
			accountChunk = accounts[:takeCount(
				len(accounts), accountCost(), entries, bytes,
			)]
		case len(rows) > 0:
			n = 0
			rowChunk = rows[:takeRows(rows, entries, bytes)]
		case len(records) > 0:
			n = 0
			recordChunk = records[:takeCount(
				len(records), recordCost(), entries, bytes,
			)]
		case len(snaps) > 0:
			n = 0
			if k := takeSnapshotOps(snaps, entries, bytes); k > 0 {
				snapChunk = snaps[:k]
			} else if chunks == 0 {
				// The batch's rollback goes first, alone.
				snapChunk = snaps[:0]
			} else {
				op := snaps[0]
				bulkLimit = max(entries/snapshotDeleteEntries, 1)
				bulk = func(txn *badger.Txn, limit int64) (bool, error) {
					return deleteSnapshotLimited(txn, op, limit)
				}
			}
		case len(blocks) > 0:
			n = 0
			if k := takeRewardBlockOps(blocks, entries, bytes); k > 0 {
				blockChunk = blocks[:k]
			} else if chunks == 0 {
				blockChunk = blocks[:0]
			} else {
				below := blocks[0].PruneBelow
				bulkLimit = max(entries/rewardDeleteEntries, 1)
				bulk = func(txn *badger.Txn, limit int64) (bool, error) {
					return pruneRewardBlocksLimited(txn, below, limit)
				}
			}
		case !last:
			n = takeUnits(units, cost{}, entries, bytes)
		}
		leading := rowChunk != nil || accountChunk != nil ||
			recordChunk != nil || snapChunk != nil || blockChunk != nil ||
			bulk != nil
		taken := units[:n]
		var changes []Change
		for _, u := range taken {
			changes = append(changes, u.changes...)
		}
		if chunks == 0 && last {
			// One transaction: no gate, mark or chunk.
			trial := *res
			err := s.update(func(txn *badger.Txn) error {
				if pre != nil {
					if err := restoreFailedMark(txn, pre.slot, pre.had); err != nil {
						return err
					}
				}
				return s.applyLedgerTxn(
					txn,
					tip,
					batch,
					changes,
					&chain,
					&trial,
				)
			})
			if errors.Is(err, badger.ErrTxnTooBig) && n > 0 &&
				(entries > 1 || bytes > 1) {
				entries, bytes = max(entries/2, 1), max(bytes/2, 1)
				continue
			}
			if err == nil {
				*res = trial
			}
			return err
		}
		if !locked {
			s.gate.Lock()
			locked = true
		}
		next, nextTip := LedgerBatch{}, (*ocommon.Point)(nil)
		if leading {
			next.Rows, next.Accounts, next.RewardRecords =
				rowChunk, accountChunk, recordChunk
			next.SnapshotOps, next.RewardBlocks = snapChunk, blockChunk
			if chunks == 0 {
				next.DeleteNoncesAfter = leadRollback
			}
		}
		if last {
			next, nextTip = batch, tip
			next.Changes = nil
		}
		first := chunks == 0
		trial, trialChain := *res, chain
		err := s.update(func(txn *badger.Txn) error {
			var err error
			if first {
				if unmarked {
					prior, hadPrior, err = readFailed(txn)
				} else {
					prior, hadPrior, err = markInFlight(txn, mark)
				}
				if err != nil {
					return err
				}
				if pre != nil {
					// The mark found is the rollback's own.
					prior, hadPrior = pre.slot, pre.had
				}
			}
			if last {
				if err := restoreFailedMark(txn, prior, hadPrior); err != nil {
					return err
				}
			}
			if bulk != nil {
				var err error
				if bulkMore, err = bulk(txn, bulkLimit); err != nil {
					return err
				}
			}
			return s.applyLedgerTxn(
				txn,
				nextTip,
				next,
				changes,
				&trialChain,
				&trial,
			)
		})
		if errors.Is(err, badger.ErrTxnTooBig) &&
			max(
				len(rowChunk), len(accountChunk), len(recordChunk),
				len(snapChunk), len(blockChunk), int(bulkLimit),
			) > 1 {
			entries, bytes = max(entries/2, 1), max(bytes/2, 1)
			continue
		}
		if errors.Is(err, badger.ErrTxnTooBig) && n > 0 {
			switch {
			case n > 1 && (entries > 1 || bytes > 1):
				entries, bytes = max(entries/2, 1), max(bytes/2, 1)
				continue
			case last:
				familyAlone = true
				continue
			}
		}
		if err != nil {
			return err
		}
		*res, chain = trial, trialChain
		units = units[n:]
		rows = rows[len(rowChunk):]
		accounts = accounts[len(accountChunk):]
		records = records[len(recordChunk):]
		snaps = snaps[len(snapChunk):]
		blocks = blocks[len(blockChunk):]
		if bulk != nil && !bulkMore {
			if len(snaps) > 0 && len(snapChunk) == 0 {
				snaps = snaps[1:]
			} else {
				blocks = blocks[1:]
			}
		}
		chunks++
		if last {
			return nil
		}
		if s.afterChunk != nil {
			if err := s.afterChunk(chunks); err != nil {
				return err
			}
		}
	}
}

// accountEntries is the estimate for one account put: its journal entry and
// the account row, with the pool and DRep index keys it moves.
const accountEntries = 6

func accountCost() cost {
	return cost{entries: accountEntries, bytes: rowBytes}
}

func recordCost() cost {
	return cost{entries: familyEntriesPerRow, bytes: rowBytes}
}

func accountsCost(puts []AccountPut) cost {
	c := accountCost()
	return cost{
		entries: c.entries * int64(len(puts)),
		bytes:   c.bytes * int64(len(puts)),
	}
}

func recordsCost(recs []RewardRecord) cost {
	c := recordCost()
	return cost{
		entries: c.entries * int64(len(recs)),
		bytes:   c.bytes * int64(len(recs)),
	}
}

// takeCount returns how many of n items, each of cost unit, a transaction
// takes within the budget, at least one.
func takeCount(n int, unit cost, entries, bytes int64) int {
	fit := min(entries/max(unit.entries, 1), bytes/max(unit.bytes, 1))
	return int(max(min(int64(n), fit), 1))
}

// rowCost estimates the weight of one row put.
func rowCost(r RowPut) cost {
	return cost{
		entries: familyEntriesPerRow,
		bytes:   int64(len(r.Key) + len(r.Value) + rowBytes),
	}
}

// takeRows returns how many of rows, from the front, a transaction takes
// within the budget, at least one.
func takeRows(rows []RowPut, entries, bytes int64) int {
	var c cost
	n := 0
	for _, r := range rows {
		if n > 0 && !c.add(rowCost(r)).fits(entries, bytes) {
			break
		}
		c = c.add(rowCost(r))
		n++
	}
	return n
}

// failSlot is the slot a chunked batch's failure mark records: the first
// change's, else the tip's, else the lowest slot a row carries. A row with no
// slot, such as a prefix wipe, says nothing, and a batch where nothing
// carries one writes no mark.
func failSlot(tip *ocommon.Point, batch LedgerBatch) uint64 {
	switch {
	case len(batch.Changes) > 0:
		return batch.Changes[0].Point.Slot
	case tip != nil:
		return tip.Slot
	}
	var lowest uint64
	note := func(slot uint64) {
		if slot > 0 && (lowest == 0 || slot < lowest) {
			lowest = slot
		}
	}
	for _, r := range batch.Rows {
		note(r.Slot)
	}
	for _, a := range batch.Accounts {
		note(a.Slot)
	}
	for _, r := range batch.RewardRecords {
		note(r.Slot)
	}
	for _, op := range batch.RewardBlocks {
		note(op.Slot)
	}
	return lowest
}

func sumUnits(units []pointUnit) cost {
	var c cost
	for _, u := range units {
		c = c.add(u.cost)
	}
	return c
}

// markInFlight records slot as a failed commit so an interrupted chunked
// batch is rewound at startup. It returns the mark it found, which
// restoreFailedMark puts back. An existing mark at or below slot already
// covers the batch and is left as it is.
func markInFlight(txn *badger.Txn, slot uint64) (uint64, bool, error) {
	cur, ok, err := readFailed(txn)
	if err != nil {
		return 0, false, err
	}
	if ok && cur <= slot {
		return cur, true, nil
	}
	return cur, ok, txn.Set(failedKey, binary.BigEndian.AppendUint64(nil, slot))
}

// restoreFailedMark returns the failure mark to what markInFlight found.
func restoreFailedMark(txn *badger.Txn, prior uint64, had bool) error {
	if !had {
		return txn.Delete(failedKey)
	}
	return txn.Set(failedKey, binary.BigEndian.AppendUint64(nil, prior))
}

// stripOps is batch without the ops opsCost weighs.
func stripOps(batch LedgerBatch) LedgerBatch {
	batch.SnapshotOps, batch.RewardBlocks = nil, nil
	return batch
}
