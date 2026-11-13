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
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// UtxoSetWriter keeps a durable copy of the live UTxO set in step with the
// ledger.
//
// ApplyUtxoChanges receives, once per ledger commit, every UTxO change the
// commit made, in order, and must apply them atomically. tip is the ledger
// point the commit left the ledger at; it is nil for a commit that changed
// the set without moving the ledger, such as the Allegra AVVM removal. Every
// block applied reports its point, even one with no transactions. The call
// is made synchronously on the committing goroutine, after the commit and
// before the next block is validated, so the copy's recorded tip is always a
// point at which it equals the ledger's live set. A copy whose tip trails the
// ledger's is caught up by ResumeUtxoSetFrom.
type UtxoSetWriter interface {
	ApplyUtxoChanges(tip *ocommon.Point, changes []UtxoEvent) error
}

// UtxoSetRollbacker rolls a live UTxO set back from its own history, for
// LedgerStateConfig.UtxoSetOnly, where the metadata store cannot supply a
// rollback's change. It returns the net change as a rollback UtxoEvent.
type UtxoSetRollbacker interface {
	RollbackUtxoSet(point ocommon.Point) (UtxoEvent, error)
	// PreviewUtxoSetRollback returns the change RollbackUtxoSet(point)
	// would make, without making it.
	PreviewUtxoSetRollback(point ocommon.Point) (UtxoEvent, error)
}

// errLiveUtxoSetUnavailable reports a read of a live UTxO set that stopped
// following the ledger when nothing else holds the UTxO set.
var errLiveUtxoSetUnavailable = errors.New(
	"live UTxO set stopped following the ledger; restart to resume it",
)

// utxoSetBatch is one commit's changes for the live UTxO set.
type utxoSetBatch struct {
	// txn is the write transaction staging the batch, nil for a batch
	// applied at once; seq orders batches by when they began staging.
	txn ledgerdb.Txn
	seq uint64
	tip *ocommon.Point
	// rollbackTo, for UtxoSetOnly, rolls the set back to a point from its
	// journal before changes apply.
	rollbackTo *ocommon.Point
	// rollbackRefs are the stake credentials of the outputs the rollback
	// changes, whose live reward aggregate the truncation must refresh.
	rollbackRefs []models.StakeCredentialRef
	changes      []UtxoEvent
	// overlay is each changed output's last staged state, and stake each
	// stake sum's staged change, for reads in the same transaction.
	overlay map[utxoref.Key]utxoSetOverlayEntry
	stake   map[string]int64
}

// utxoChangesWanted reports whether anything consumes UTxO changes: a
// ledger.utxo subscriber's bus or the live UTxO set.
func (ls *LedgerState) utxoChangesWanted() bool {
	return ls.config.EventBus != nil || ls.config.UtxoSet != nil
}

// stageUtxoSet adds changes, and tip when it is not nil, to txn's batch for
// the live UTxO set, which is applied once txn commits. Callers stage before
// the commit, in the order the changes apply. Without a txn the batch is
// applied at once.
func (ls *LedgerState) stageUtxoSet(
	txn ledgerdb.Txn,
	tip *ocommon.Point,
	changes ...UtxoEvent,
) {
	if ls.config.UtxoSet == nil {
		return
	}
	if txn == nil {
		ls.applyUtxoSet(&utxoSetBatch{tip: tip, changes: changes})
		return
	}
	ls.utxoSetMu.Lock()
	if ls.utxoSetBatches == nil {
		ls.utxoSetBatches = make(map[ledgerdb.Txn]*utxoSetBatch)
	}
	batch, ok := ls.utxoSetBatches[txn]
	var meta types.Txn
	if !ok {
		ls.utxoSetSeq++
		batch = &utxoSetBatch{txn: txn, seq: ls.utxoSetSeq}
		ls.utxoSetBatches[txn] = batch
		if dbTxn := dbadapter.Unwrap(txn); dbTxn != nil {
			meta = dbTxn.Metadata()
		}
		if meta != nil {
			if ls.utxoSetByMeta == nil {
				ls.utxoSetByMeta = make(map[types.Txn]*utxoSetBatch)
			}
			ls.utxoSetByMeta[meta] = batch
		}
	}
	batch.changes = append(batch.changes, changes...)
	if tip != nil {
		t := *tip
		batch.tip = &t
	}
	ls.utxoSetMu.Unlock()
	if r, readable := ls.config.UtxoSet.(UtxoSetReader); readable {
		if err := ls.stageUtxoOverlay(r, txn, batch, changes); err != nil {
			ls.utxoSetFailed.Store(true)
			ls.config.Logger.Error(
				"live UTxO set read failed while staging; it stops following the ledger until startup resumes it",
				"component", "ledger",
				"error", err,
			)
		}
	}
	if ok {
		return
	}
	// The batch is complete before the commit, so one callback applies it.
	// A committed batch stays in the maps until it is applied: another
	// write can start between the commit and the apply, and must see the
	// set as the commit left it (see utxoSetBatchesSeenBy). A rolled-back
	// one is dropped when the transaction finishes.
	txn.OnFinish(func() {
		if !txn.IsCommitted() {
			ls.forgetUtxoSetBatch(batch)
		}
	})
	txn.AfterCommit(func() { ls.applyUtxoSet(batch) })
}

// forgetUtxoSetBatch removes a finished transaction's batch from the maps.
func (ls *LedgerState) forgetUtxoSetBatch(batch *utxoSetBatch) {
	if batch.txn == nil {
		return
	}
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	if ls.utxoSetBatches[batch.txn] == batch {
		delete(ls.utxoSetBatches, batch.txn)
	}
	for meta, b := range ls.utxoSetByMeta {
		if b == batch {
			delete(ls.utxoSetByMeta, meta)
		}
	}
}

// utxoSetBatchesSeenBy returns the batches a reader sees over the set's
// applied state, oldest first: own, the reader's open transaction's batch
// (nil for none), and every batch whose transaction has committed but has
// not reached the set yet. The set is applied after the commit, so a write
// starting in between would otherwise pair metadata that holds the commit
// with a set that does not. The caller holds utxoSetApplyMu for reading.
func (ls *LedgerState) utxoSetBatchesSeenBy(own *utxoSetBatch) []*utxoSetBatch {
	ls.utxoSetMu.Lock()
	others := make([]*utxoSetBatch, 0, len(ls.utxoSetBatches))
	for _, b := range ls.utxoSetBatches {
		if b != own {
			others = append(others, b)
		}
	}
	ls.utxoSetMu.Unlock()
	// IsCommitted is asked outside utxoSetMu: it waits for a commit in
	// progress, which must not hold up staging in other transactions.
	seen := make([]*utxoSetBatch, 0, len(others)+1)
	for _, b := range others {
		if b.txn.IsCommitted() {
			seen = append(seen, b)
		}
	}
	if own != nil {
		seen = append(seen, own)
	}
	slices.SortFunc(seen, func(a, b *utxoSetBatch) int {
		return cmp.Compare(a.seq, b.seq)
	})
	return seen
}

// stagedStake sums the stake deltas of batches for the keys keep accepts.
func (ls *LedgerState) stagedStake(
	batches []*utxoSetBatch,
	keep func(key []byte) bool,
) map[string]int64 {
	staged := make(map[string]int64)
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	for _, b := range batches {
		for key, d := range b.stake {
			if keep([]byte(key)) {
				staged[key] += d
			}
		}
	}
	return staged
}

// applyUtxoSet applies one commit's batch to the live UTxO set. After a
// failure it applies nothing more, leaving the set at the last commit it
// took for startup to resume from.
func (ls *LedgerState) applyUtxoSet(batch *utxoSetBatch) {
	if rolledBack := ls.applyAndForgetUtxoSet(batch); rolledBack != nil {
		ls.publishUtxoEvent(ls.lifecycleContext(), *rolledBack) //nolint:contextcheck // the ledger lifecycle bounds an after-commit publish
	}
}

// applyAndForgetUtxoSet applies batch and drops it from the maps as one step
// for readers, which merge committed batches over the set.
func (ls *LedgerState) applyAndForgetUtxoSet(batch *utxoSetBatch) *UtxoEvent {
	ls.utxoSetApplyMu.Lock()
	defer ls.utxoSetApplyMu.Unlock()
	defer ls.forgetUtxoSetBatch(batch)
	return ls.applyUtxoSetLocked(batch)
}

// applyUtxoSetLocked is applyUtxoSet under utxoSetApplyMu. It returns the
// change a rollback made, for publishing once the lock is released.
func (ls *LedgerState) applyUtxoSetLocked(batch *utxoSetBatch) *UtxoEvent {
	w := ls.config.UtxoSet
	if w == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if batch.tip == nil && batch.rollbackTo == nil && len(batch.changes) == 0 {
		return nil
	}
	var rolledBack *UtxoEvent
	if batch.rollbackTo != nil {
		rb, ok := w.(UtxoSetRollbacker)
		if !ok {
			ls.utxoSetFailed.Store(true)
			ls.config.Logger.Error(
				"live UTxO set cannot roll back from its own history",
				"component", "ledger",
			)
			return nil
		}
		evt, err := rb.RollbackUtxoSet(*batch.rollbackTo)
		if err != nil {
			ls.utxoSetFailed.Store(true)
			ls.config.Logger.Error(
				"live UTxO set rollback failed; it stops following the ledger until startup resumes it",
				"component", "ledger",
				"error", err,
				"slot", batch.rollbackTo.Slot,
			)
			return nil
		}
		rolledBack = &evt
	}
	if batch.tip == nil && len(batch.changes) == 0 {
		return rolledBack
	}
	if err := w.ApplyUtxoChanges(batch.tip, batch.changes); err != nil {
		ls.utxoSetFailed.Store(true)
		attrs := []any{"component", "ledger", "error", err}
		if batch.tip != nil {
			attrs = append(attrs, "slot", batch.tip.Slot)
		}
		ls.config.Logger.Error(
			"live UTxO set update failed; it stops following the ledger until startup resumes it",
			attrs...,
		)
	}
	return rolledBack
}

// ResumeUtxoSetFrom rolls the ledger back to point, the tip of a live UTxO
// set that trails the ledger, so every block after it is applied again and
// reaches the set. The set equals the ledger's live set as of point, and the
// rollback's own change, which turns the ledger's set at its tip back into
// the set at point, leaves a set already at point unchanged.
//
// It reports false, changing nothing, when point is not an applied block on
// the primary chain below the ledger tip, or when the rollback would cross the
// Mithril trust boundary or the consumed-UTxO prune floor; the caller then
// rebuilds the set instead. Call it before the ledger starts.
func (ls *LedgerState) ResumeUtxoSetFrom(
	ctx context.Context,
	point ocommon.Point,
) (bool, error) {
	if len(point.Hash) == 0 {
		return false, nil
	}
	tip, err := ls.ledgerDB().GetTip(nil)
	if err != nil {
		return false, fmt.Errorf("read ledger tip: %w", err)
	}
	if point.Slot >= tip.Point.Slot {
		return false, nil
	}
	onChain, err := ls.primaryChainContainsPoint(ctx, point)
	if err != nil {
		return false, fmt.Errorf("check live UTxO set tip on the chain: %w", err)
	}
	if !onChain {
		return false, nil
	}
	ls.RLock()
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return false, nil
	}
	below, _, err := ls.rollbackBelowConsumedUtxoPruneFloor(point)
	if err != nil {
		return false, fmt.Errorf("determine consumed UTxO prune floor: %w", err)
	}
	if below {
		return false, nil
	}
	if err := ls.rollback(ctx, point); err != nil {
		// A committed rollback has moved the ledger and the set together.
		if _, ok := errors.AsType[*rollbackCommittedError](err); ok {
			return true, err
		}
		return false, err
	}
	return true, nil
}

// Read modes for the live UTxO set, selected by
// LedgerStateConfig.UtxoSetReads. Reads fall back to the metadata store
// whatever the mode once an update to the set has failed.
const (
	// UtxoSetReadsOff resolves validation's inputs from the metadata store.
	UtxoSetReadsOff = ""
	// UtxoSetReadsVerify resolves them from the metadata store and also
	// from the live UTxO set, and counts every answer that differs.
	UtxoSetReadsVerify = "verify"
	// UtxoSetReadsSet resolves them from the live UTxO set.
	UtxoSetReadsSet = "set"
)

// UtxoSetReader reads the live UTxO set. A UtxoSetWriter that also
// implements it can serve validation's reads.
type UtxoSetReader interface {
	// Get returns the CBOR of the live output txID#index.
	Get(txID []byte, index uint32) ([]byte, bool, error)
	// Sum returns the committed stake sum for a key built by
	// CredentialStakeKey or PointerStakeKey, as UtxoStakeOf classifies
	// live outputs.
	Sum(key []byte) (uint64, error)
}

// utxoSetOverlayEntry is the last staged state of one output in an open
// transaction's batch.
type utxoSetOverlayEntry struct {
	cbor []byte
	live bool
}

// stageUtxoOverlay records changes in the batch's overlay and stake deltas,
// which reads in the same transaction see before the set does. Each change's
// stake effect is taken against the output's value as txn sees it, so an
// output created and spent in one transaction nets to nothing.
func (ls *LedgerState) stageUtxoOverlay(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	b *utxoSetBatch,
	changes []UtxoEvent,
) error {
	unstake := func(hash []byte, idx uint32) error {
		cbor, live, err := ls.utxoSetGet(r, txn, hash, idx)
		if err != nil || !live {
			return err
		}
		ls.utxoSetMu.Lock()
		b.addStake(cbor, -1)
		ls.utxoSetMu.Unlock()
		return nil
	}
	for _, c := range changes {
		for _, ref := range c.Removed {
			if err := unstake(ref.Hash, ref.Idx); err != nil {
				return err
			}
			ls.utxoSetMu.Lock()
			b.setOverlay(utxoSetKey(ref.Hash, ref.Idx), utxoSetOverlayEntry{})
			ls.utxoSetMu.Unlock()
		}
		for _, o := range c.Added {
			if err := unstake(o.Ref.Hash, o.Ref.Idx); err != nil {
				return err
			}
			ls.utxoSetMu.Lock()
			b.addStake(o.Cbor, 1)
			b.setOverlay(
				utxoSetKey(o.Ref.Hash, o.Ref.Idx),
				utxoSetOverlayEntry{cbor: o.Cbor, live: true},
			)
			ls.utxoSetMu.Unlock()
		}
	}
	return nil
}

func (b *utxoSetBatch) setOverlay(k utxoref.Key, e utxoSetOverlayEntry) {
	if b.overlay == nil {
		b.overlay = make(map[utxoref.Key]utxoSetOverlayEntry)
	}
	b.overlay[k] = e
}

// addStake adds sign times an output's lovelace to its stake sum's delta.
// The caller holds utxoSetMu.
func (b *utxoSetBatch) addStake(cbor []byte, sign int64) {
	key, lovelace, ok := UtxoStakeOf(cbor)
	if !ok {
		return
	}
	if b.stake == nil {
		b.stake = make(map[string]int64)
	}
	b.stake[string(key)] += sign * int64(lovelace) //nolint:gosec // lovelace fits in int64
}

// CredentialUtxoStake returns the lovelace in live outputs naming the stake
// credential (tag, hash), as txn sees the live UTxO set. It needs a live UTxO
// set that is a UtxoSetReader.
func (ls *LedgerState) CredentialUtxoStake(
	txn ledgerdb.Txn,
	tag uint8,
	hash []byte,
) (uint64, error) {
	r, ok := ls.config.UtxoSet.(UtxoSetReader)
	if !ok {
		return 0, errors.New("no readable live UTxO set")
	}
	return ls.utxoStakeSeenBy(r, txn, CredentialStakeKey(tag, hash))
}

// utxoStakeSeenBy returns a stake sum (see CredentialStakeKey) as txn sees
// the live UTxO set: the set's applied sum plus the changes of txn and of
// committed transactions not yet applied (see utxoSetBatchesSeenBy).
func (ls *LedgerState) utxoStakeSeenBy(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	key []byte,
) (uint64, error) {
	ls.utxoSetMu.Lock()
	var b *utxoSetBatch
	if txn != nil {
		b = ls.utxoSetBatches[txn]
	}
	ls.utxoSetMu.Unlock()
	return ls.stakeWithBatch(r, b, key)
}

// stakeWithBatch is a stake sum from the set's applied state plus the staged
// change of b, which may be nil, and of every committed batch not yet
// applied.
func (ls *LedgerState) stakeWithBatch(
	r UtxoSetReader,
	b *utxoSetBatch,
	key []byte,
) (uint64, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	committed, err := r.Sum(key)
	if err != nil {
		return 0, err
	}
	delta := ls.stagedStake(ls.utxoSetBatchesSeenBy(b), func(k []byte) bool {
		return bytes.Equal(k, key)
	})[string(key)]
	total := int64(committed) + delta //nolint:gosec // lovelace fits in int64
	if total < 0 {
		return 0, fmt.Errorf("stake sum %x is negative as staged", key)
	}
	return uint64(total), nil
}

// utxoSetReader returns the live UTxO set's reader and the read mode, or
// UtxoSetReadsOff when reads must use the metadata store.
func (ls *LedgerState) utxoSetReader() (UtxoSetReader, string) {
	if ls.config.UtxoSetOnly {
		// Nothing else holds the UTxO set, so a failed set is reported by
		// utxoSetGet rather than routed around.
		if r, ok := ls.config.UtxoSet.(UtxoSetReader); ok {
			return r, UtxoSetReadsSet
		}
	}
	mode := ls.config.UtxoSetReads
	if mode == UtxoSetReadsOff || ls.utxoSetFailed.Load() {
		return nil, UtxoSetReadsOff
	}
	r, ok := ls.config.UtxoSet.(UtxoSetReader)
	if !ok {
		return nil, UtxoSetReadsOff
	}
	return r, mode
}

// utxoSetGet reads txID#index as txn sees the live UTxO set: the changes txn
// and committed transactions not yet applied have staged over the set.
func (ls *LedgerState) utxoSetGet(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	// r is nil only when reads are off, which callers handle before this.
	if r == nil || ls.config.UtxoSetOnly && ls.utxoSetFailed.Load() {
		return nil, false, errLiveUtxoSetUnavailable
	}
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	return ls.getThroughBatches(r, own, txID, index)
}

// getThroughBatches reads txID#index from the newest batch reader own sees
// that changed it, else from the set's applied state.
func (ls *LedgerState) getThroughBatches(
	r UtxoSetReader,
	own *utxoSetBatch,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	batches := ls.utxoSetBatchesSeenBy(own)
	k := utxoSetKey(txID, index)
	ls.utxoSetMu.Lock()
	for _, b := range slices.Backward(batches) {
		if entry, hit := b.overlay[k]; hit {
			ls.utxoSetMu.Unlock()
			return entry.cbor, entry.live, nil
		}
	}
	ls.utxoSetMu.Unlock()
	return r.Get(txID, index)
}

// utxoByRefForValidation resolves txID#index for validation from the store
// the read mode selects.
func (ls *LedgerState) utxoByRefForValidation(
	ctx context.Context,
	txID []byte,
	index uint32,
	txn ledgerdb.Txn,
) (*models.Utxo, error) {
	r, mode := ls.utxoSetReader()
	if mode == UtxoSetReadsOff {
		return ls.ledgerDB().UtxoByRef(ctx, txID, index, txn)
	}
	setCbor, setFound, setErr := ls.utxoSetGet(r, txn, txID, index)
	if mode == UtxoSetReadsSet {
		if setErr != nil {
			return nil, setErr
		}
		if !setFound {
			return nil, database.ErrUtxoNotFound
		}
		return &models.Utxo{TxId: txID, OutputIdx: index, Cbor: setCbor}, nil
	}
	utxo, err := ls.ledgerDB().UtxoByRef(ctx, txID, index, txn)
	if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
		return utxo, err
	}
	var storeCbor []byte
	if utxo != nil && err == nil {
		storeCbor = utxo.Cbor
	}
	ls.compareUtxoSetRead(
		txID, index, storeCbor, err == nil, setCbor, setFound, setErr,
	)
	return utxo, err
}

// utxosByRefsForValidation resolves refs for validation from the store the
// read mode selects, leaving out refs that are not live.
func (ls *LedgerState) utxosByRefsForValidation(
	ctx context.Context,
	refs []models.UtxoId,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	r, mode := ls.utxoSetReader()
	if mode == UtxoSetReadsOff {
		return ls.ledgerDB().UtxosByRefs(ctx, refs, txn)
	}
	fromSet := make([]models.Utxo, 0, len(refs))
	type setAnswer struct {
		cbor  []byte
		found bool
		err   error
	}
	answers := make([]setAnswer, len(refs))
	for i, ref := range refs {
		c, ok, err := ls.utxoSetGet(r, txn, ref.Hash, ref.Idx)
		answers[i] = setAnswer{cbor: c, found: ok, err: err}
		if mode == UtxoSetReadsSet {
			if err != nil {
				return nil, err
			}
			if ok {
				fromSet = append(fromSet, models.Utxo{
					TxId:      ref.Hash,
					OutputIdx: ref.Idx,
					Cbor:      c,
				})
			}
		}
	}
	if mode == UtxoSetReadsSet {
		return fromSet, nil
	}
	rows, err := ls.ledgerDB().UtxosByRefs(ctx, refs, txn)
	if err != nil {
		return rows, err
	}
	byRef := make(map[utxoref.Key][]byte, len(rows))
	for i := range rows {
		byRef[utxoSetKey(rows[i].TxId, rows[i].OutputIdx)] = rows[i].Cbor
	}
	for i, ref := range refs {
		storeCbor, storeFound := byRef[utxoSetKey(ref.Hash, ref.Idx)]
		a := answers[i]
		ls.compareUtxoSetRead(
			ref.Hash, ref.Idx, storeCbor, storeFound, a.cbor, a.found, a.err,
		)
	}
	return rows, nil
}

// compareUtxoSetRead counts a verify-mode read and reports when the live
// UTxO set's answer differs from the metadata store's.
func (ls *LedgerState) compareUtxoSetRead(
	txID []byte,
	index uint32,
	storeCbor []byte,
	storeFound bool,
	setCbor []byte,
	setFound bool,
	setErr error,
) {
	ls.metrics.utxoSetReadsVerified.Inc()
	if setErr == nil && storeFound == setFound &&
		(!storeFound || bytes.Equal(storeCbor, setCbor)) {
		return
	}
	n := ls.utxoSetReadMismatches.Add(1)
	ls.metrics.utxoSetReadMismatches.Inc()
	// The first mismatches say the most; later ones are sampled.
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set read differs from the metadata store",
		"component", "ledger",
		"utxo", fmt.Sprintf("%x#%d", txID, index),
		"store_found", storeFound,
		"set_found", setFound,
		"set_error", setErr,
		"mismatches", n,
	)
}

func utxoSetKey(txID []byte, index uint32) utxoref.Key {
	return utxoref.Key{TxId: lcommon.NewBlake2b256(txID), Index: index}
}

// LiveUtxoCbor implements types.LiveUtxoSource for the metadata store in
// UtxoSetOnly mode: it reads txID#index as the ledger transaction writing in
// meta sees the live UTxO set.
func (ls *LedgerState) LiveUtxoCbor(
	meta types.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	r, ok := ls.config.UtxoSet.(UtxoSetReader)
	if !ok {
		return nil, false, errors.New("no readable live UTxO set")
	}
	if ls.utxoSetFailed.Load() {
		return nil, false, errLiveUtxoSetUnavailable
	}
	return ls.getThroughBatches(r, ls.utxoSetBatchFor(meta), txID, index)
}

// utxoSetBatchFor returns the batch of the write transaction metadata
// transaction meta belongs to, nil for none.
func (ls *LedgerState) utxoSetBatchFor(meta types.Txn) *utxoSetBatch {
	if meta == nil {
		return nil
	}
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	return ls.utxoSetByMeta[meta]
}

// stageUtxoSetRollback makes txn's batch roll the live UTxO set back to point
// from its journal once txn commits, and record point as its tip.
func (ls *LedgerState) stageUtxoSetRollback(
	txn ledgerdb.Txn,
	point ocommon.Point,
) {
	if txn == nil {
		ls.applyUtxoSet(&utxoSetBatch{rollbackTo: &point, tip: &point})
		return
	}
	// The batch's tip is applied after the rollback, so its journal record
	// lands after the records the rollback undoes.
	ls.stageUtxoSet(txn, &point)
	ls.utxoSetMu.Lock()
	b := ls.utxoSetBatches[txn]
	if b != nil {
		p := point
		b.rollbackTo = &p
	}
	ls.utxoSetMu.Unlock()
	// The metadata truncation that follows re-reads stake, so the
	// rolled-back set is staged in the overlay, though only the journal
	// rollback after the commit changes the set.
	rb, ok := ls.config.UtxoSet.(UtxoSetRollbacker)
	r, readable := ls.config.UtxoSet.(UtxoSetReader)
	if b == nil || !ok || !readable {
		return
	}
	preview, err := rb.PreviewUtxoSetRollback(point)
	if err == nil {
		var refs []models.StakeCredentialRef
		refs, err = ls.rollbackStakeRefs(r, txn, preview)
		ls.utxoSetMu.Lock()
		b.rollbackRefs = refs
		ls.utxoSetMu.Unlock()
	}
	if err == nil {
		err = ls.stageUtxoOverlay(r, txn, b, []UtxoEvent{preview})
	}
	if err != nil {
		ls.utxoSetFailed.Store(true)
		ls.config.Logger.Error(
			"live UTxO set rollback preview failed; it stops following the ledger until startup resumes it",
			"component", "ledger",
			"error", err,
			"slot", point.Slot,
		)
	}
}

// LiveUtxoStake implements types.LiveUtxoSource: the lovelace in live
// outputs naming the stake credential (tag, hash), as the ledger transaction
// writing in meta sees the live UTxO set.
func (ls *LedgerState) LiveUtxoStake(
	meta types.Txn,
	tag uint8,
	hash []byte,
) (uint64, error) {
	r, ok := ls.config.UtxoSet.(UtxoSetReader)
	if !ok {
		return 0, errors.New("no readable live UTxO set")
	}
	if ls.utxoSetFailed.Load() {
		return 0, errLiveUtxoSetUnavailable
	}
	return ls.stakeWithBatch(r, ls.utxoSetBatchFor(meta), CredentialStakeKey(tag, hash))
}

// EachLiveUtxoStake implements types.LiveUtxoSource: every stake
// credential's lovelace in live outputs as the ledger transaction writing in
// meta sees the live UTxO set.
func (ls *LedgerState) EachLiveUtxoStake(
	meta types.Txn,
	fn func(tag uint8, hash []byte, lovelace uint64) error,
) error {
	sums, ok := ls.config.UtxoSet.(UtxoSetSums)
	if !ok {
		return errors.New("the live UTxO set keeps no stake sums")
	}
	if ls.utxoSetFailed.Load() {
		return errLiveUtxoSetUnavailable
	}
	isCredential := func(key []byte) bool {
		_, _, ok := ParseCredentialStakeKey(key)
		return ok
	}
	type stakeSum struct {
		key []byte
		n   int64
	}
	var totals []stakeSum
	// The sums are collected under the lock and fn runs after it, since fn
	// may read the set again.
	err := func() error {
		ls.utxoSetApplyMu.RLock()
		defer ls.utxoSetApplyMu.RUnlock()
		staged := ls.stagedStake(
			ls.utxoSetBatchesSeenBy(ls.utxoSetBatchFor(meta)), isCredential,
		)
		if err := sums.EachSum(func(key []byte, n uint64) error {
			if !isCredential(key) {
				return nil
			}
			d := staged[string(key)]
			delete(staged, string(key))
			totals = append(totals, stakeSum{
				key: bytes.Clone(key),
				n:   int64(n) + d, //nolint:gosec // lovelace fits in int64
			})
			return nil
		}); err != nil {
			return err
		}
		for key, d := range staged {
			totals = append(totals, stakeSum{key: []byte(key), n: d})
		}
		return nil
	}()
	if err != nil {
		return err
	}
	for _, t := range totals {
		if t.n < 0 {
			return fmt.Errorf("stake sum %x is negative as staged", t.key)
		}
		if t.n == 0 {
			continue
		}
		tag, hash, _ := ParseCredentialStakeKey(t.key)
		if err := fn(tag, hash, uint64(t.n)); err != nil {
			return err
		}
	}
	return nil
}

// rollbackStakeRefs returns the stake credentials of the outputs a rollback
// change adds and removes, reading removed outputs before the change.
func (ls *LedgerState) rollbackStakeRefs(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	change UtxoEvent,
) ([]models.StakeCredentialRef, error) {
	var refs []models.StakeCredentialRef
	add := func(cbor []byte) {
		key, _, ok := UtxoStakeOf(cbor)
		if !ok {
			return
		}
		if tag, hash, ok := ParseCredentialStakeKey(key); ok {
			refs = append(refs, models.NewStakeCredentialRef(tag, hash))
		}
	}
	for _, o := range change.Added {
		add(o.Cbor)
	}
	for _, ref := range change.Removed {
		cbor, live, err := ls.utxoSetGet(r, txn, ref.Hash, ref.Idx)
		if err != nil {
			return nil, err
		}
		if live {
			add(cbor)
		}
	}
	return refs, nil
}

// refreshUtxoSetRollbackStake refreshes, after the metadata truncation in
// txn, the live reward aggregate of the credentials txn's rollback changes,
// which the metadata store finds through utxo rows when it has them.
func (ls *LedgerState) refreshUtxoSetRollbackStake(
	txn ledgerdb.Txn,
	slot uint64,
) error {
	if !ls.config.UtxoSetOnly || txn == nil {
		return nil
	}
	ls.utxoSetMu.Lock()
	var refs []models.StakeCredentialRef
	if b := ls.utxoSetBatches[txn]; b != nil {
		refs = b.rollbackRefs
	}
	ls.utxoSetMu.Unlock()
	if len(refs) == 0 {
		return nil
	}
	return ls.ledgerDB().RefreshRewardLiveStakeRefs(refs, slot, txn)
}

// UtxoSetIterator walks a live UTxO set's committed outputs.
type UtxoSetIterator interface {
	EachUtxo(fn func(txID []byte, index uint32, cbor []byte) error) error
}

// iterateLiveUtxos calls fn for every live output as txn sees the UTxO set:
// the metadata store's rows, or with UtxoSetOnly the live UTxO set with txn's
// staged changes over it. Rows passed to fn carry only the reference and the
// output CBOR in UtxoSetOnly mode.
func (ls *LedgerState) iterateLiveUtxos(
	ctx context.Context,
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	if !ls.config.UtxoSetOnly {
		return ls.ledgerDB().IterateLiveUtxos(ctx, txn, fn)
	}
	it, ok := ls.config.UtxoSet.(UtxoSetIterator)
	if !ok {
		return errors.New("the live UTxO set cannot be iterated")
	}
	if ls.utxoSetFailed.Load() {
		return errLiveUtxoSetUnavailable
	}
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	// Overlay entries replace the set's by key, so a batch applied while the
	// set is walked is still counted once; the lock is only needed to pick
	// the batches.
	ls.utxoSetApplyMu.RLock()
	batches := ls.utxoSetBatchesSeenBy(own)
	overlay := make(map[utxoref.Key]utxoSetOverlayEntry)
	ls.utxoSetMu.Lock()
	for _, b := range batches {
		maps.Copy(overlay, b.overlay)
	}
	ls.utxoSetMu.Unlock()
	ls.utxoSetApplyMu.RUnlock()
	if err := it.EachUtxo(func(txID []byte, index uint32, cbor []byte) error {
		if _, staged := overlay[utxoSetKey(txID, index)]; staged {
			return nil
		}
		return fn(&models.Utxo{TxId: txID, OutputIdx: index, Cbor: cbor})
	}); err != nil {
		return err
	}
	for k, e := range overlay {
		if !e.live {
			continue
		}
		if err := fn(&models.Utxo{
			TxId:      k.TxId.Bytes(),
			OutputIdx: k.Index,
			Cbor:      e.cbor,
		}); err != nil {
			return err
		}
	}
	return nil
}

// liveUtxoLovelace is the lovelace in every live output as txn sees the UTxO
// set.
func (ls *LedgerState) liveUtxoLovelace(
	ctx context.Context,
	txn ledgerdb.Txn,
) (uint64, error) {
	if !ls.config.UtxoSetOnly {
		return ls.ledgerDB().GetLiveUtxoLovelace(txn)
	}
	var total uint64
	err := ls.iterateLiveUtxos(ctx, txn, func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err)
		}
		amount, err := models.CheckedUint64FromBigInt(out.Amount())
		if err != nil {
			return err
		}
		if total > ^uint64(0)-amount {
			return errors.New("live UTxO lovelace overflows uint64")
		}
		total += amount
		return nil
	})
	return total, err
}

// utxoIncludingSpent resolves txID#index whether or not it is still live,
// returning nil for an output that never existed. With UtxoSetOnly a spent
// output is read from the body of the transaction that produced it: a valid
// transaction's outputs, or an invalid one's collateral return at the index
// after its outputs, as the metadata store records them.
func (ls *LedgerState) utxoIncludingSpent(
	ctx context.Context,
	txn ledgerdb.Txn,
	txID []byte,
	index uint32,
) (*models.Utxo, error) {
	if !ls.config.UtxoSetOnly {
		return ls.ledgerDB().UtxoByRefIncludingSpent(ctx, txID, index, txn)
	}
	live, err := ls.utxoByRefForValidation(ctx, txID, index, txn)
	if err == nil {
		return live, nil
	}
	if !errors.Is(err, database.ErrUtxoNotFound) {
		return nil, err
	}
	rows, err := ls.ledgerDB().GetTransactionsByHashes(ctx, [][]byte{txID}, txn)
	if err != nil {
		return nil, fmt.Errorf("find producer of %x#%d: %w", txID, index, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	bodyCbor, err := ls.ledgerDB().ResolveTxCbor(txn, txID)
	if err != nil {
		return nil, fmt.Errorf("resolve producer of %x#%d: %w", txID, index, err)
	}
	body, err := gledger.NewTransactionBodyFromCbor(
		uint(rows[0].Type), //nolint:gosec // era ID fits in uint
		bodyCbor,
	)
	if err != nil {
		return nil, fmt.Errorf("decode producer of %x#%d: %w", txID, index, err)
	}
	outputs := body.Outputs()
	var out lcommon.TransactionOutput
	switch {
	case rows[0].Valid && int(index) < len(outputs):
		out = outputs[index]
	case !rows[0].Valid && int(index) == len(outputs):
		out = body.CollateralReturn()
	}
	if out == nil {
		return nil, nil
	}
	return &models.Utxo{TxId: txID, OutputIdx: index, Cbor: outputCbor(out)}, nil
}

// utxosByAddress returns the live outputs paid to any of addrs, at most limit
// of them when limit is positive. With UtxoSetOnly it scans the live UTxO set
// and matches each output's exact address.
func (ls *LedgerState) utxosByAddress(
	ctx context.Context,
	addrs []gledger.Address,
	limit int,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	if !ls.config.UtxoSetOnly {
		return ls.ledgerDB().UtxosByAddress(ctx, addrs, limit, txn)
	}
	return utxosByAddressFrom(addrs, limit, func(fn func(*models.Utxo) error) error {
		return ls.iterateLiveUtxos(ctx, txn, fn)
	})
}

// utxosByAddressFrom returns up to limit outputs each yields that pay to one
// of addrs.
func utxosByAddressFrom(
	addrs []gledger.Address,
	limit int,
	each func(func(*models.Utxo) error) error,
) ([]models.Utxo, error) {
	want := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		raw, err := a.Bytes()
		if err != nil {
			return nil, err
		}
		want[string(raw)] = struct{}{}
	}
	var ret []models.Utxo
	errLimit := errors.New("limit reached")
	err := each(func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err)
		}
		raw, err := out.Address().Bytes()
		if err != nil {
			return err
		}
		if _, ok := want[string(raw)]; !ok {
			return nil
		}
		ret = append(ret, *u)
		if limit > 0 && len(ret) >= limit {
			return errLimit
		}
		return nil
	})
	if errors.Is(err, errLimit) {
		err = nil
	}
	return ret, err
}

// UtxoSetSums walks a live UTxO set's committed stake sums.
type UtxoSetSums interface {
	EachSum(fn func(key []byte, lovelace uint64) error) error
}

// LivePointerStakes implements types.LiveUtxoSource: the lovelace at each
// pointer position as the ledger transaction writing in meta sees the live
// UTxO set.
func (ls *LedgerState) LivePointerStakes(meta types.Txn) ([]types.PointerStake, error) {
	sums, ok := ls.config.UtxoSet.(UtxoSetSums)
	if !ok {
		return nil, errors.New("the live UTxO set keeps no stake sums")
	}
	if ls.utxoSetFailed.Load() {
		return nil, errLiveUtxoSetUnavailable
	}
	isPointer := func(key []byte) bool {
		_, _, _, ok := ParsePointerStakeKey(key)
		return ok
	}
	ls.utxoSetApplyMu.RLock()
	totals := ls.stagedStake(
		ls.utxoSetBatchesSeenBy(ls.utxoSetBatchFor(meta)), isPointer,
	)
	err := sums.EachSum(func(key []byte, n uint64) error {
		if isPointer(key) {
			totals[string(key)] += int64(n) //nolint:gosec // lovelace fits in int64
		}
		return nil
	})
	ls.utxoSetApplyMu.RUnlock()
	if err != nil {
		return nil, err
	}
	ret := make([]types.PointerStake, 0, len(totals))
	for key, n := range totals {
		if n < 0 {
			return nil, fmt.Errorf("pointer stake %x is negative as staged", key)
		}
		if n == 0 {
			continue
		}
		slot, txIndex, certIndex, _ := ParsePointerStakeKey([]byte(key))
		ret = append(ret, types.PointerStake{
			Slot: slot, TxIndex: txIndex, CertIndex: certIndex,
			Lovelace: uint64(n),
		})
	}
	return ret, nil
}

// ErrUtxoAsOfUnavailable reports a UTxO query pinned to a point the live
// UTxO set's history no longer reaches, when the set is the only copy.
var ErrUtxoAsOfUnavailable = errors.New(
	"UTxO queries before the live UTxO set's history are not available when the set is the only copy",
)

// UtxoSetHistory opens views of a live UTxO set as it was at an earlier
// point, for LedgerStateConfig.UtxoSetOnly. UtxoSetAt returns an error
// wrapping ErrUtxoAsOfUnavailable when its history does not reach point, and
// a view of the set as it is for a point at or past its tip.
type UtxoSetHistory interface {
	UtxoSetAt(point ocommon.Point) (UtxoSetView, error)
}

// UtxoSetView is a read-only view of a live UTxO set at one point, read from
// one snapshot. Close releases it.
type UtxoSetView interface {
	Get(txID []byte, index uint32) ([]byte, bool, error)
	EachUtxo(fn func(txID []byte, index uint32, cbor []byte) error) error
	Close()
}

// liveUtxoView opens the live UTxO set as of at for a UTxO query pinned to at
// when the set is the only copy. It returns nil otherwise, and the caller
// answers from its usual source.
func (ls *LedgerState) liveUtxoView(at QueryPoint) (UtxoSetView, error) {
	if !ls.config.UtxoSetOnly || !at.pinned() {
		return nil, nil
	}
	if ls.utxoSetFailed.Load() {
		return nil, errLiveUtxoSetUnavailable
	}
	h, ok := ls.config.UtxoSet.(UtxoSetHistory)
	if !ok {
		return nil, ErrUtxoAsOfUnavailable
	}
	return h.UtxoSetAt(ocommon.Point{Slot: at.Slot, Hash: at.Hash})
}

// eachInView adapts v to the iterator utxosByAddressFrom takes.
func eachInView(v UtxoSetView) func(func(*models.Utxo) error) error {
	return func(fn func(*models.Utxo) error) error {
		return v.EachUtxo(func(txID []byte, index uint32, cbor []byte) error {
			return fn(&models.Utxo{TxId: txID, OutputIdx: index, Cbor: cbor})
		})
	}
}

// utxoExists reports whether txID#index is live, without reading its output:
// a metadata row, or with UtxoSetOnly the live UTxO set.
func (ls *LedgerState) utxoExists(
	ctx context.Context,
	txID []byte,
	index uint32,
) (bool, error) {
	if !ls.config.UtxoSetOnly {
		return ls.ledgerDB().UtxoExists(ctx, txID, index, nil)
	}
	r, _ := ls.utxoSetReader()
	if r == nil {
		return false, errors.New("no readable live UTxO set")
	}
	_, live, err := ls.utxoSetGet(r, nil, txID, index)
	return live, err
}
