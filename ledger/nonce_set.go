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
	"fmt"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// nonceBatchOps is what one staged batch does to the block nonces, in the
// order the set applies it: a rollback, a prune, then the batch's nonces.
type nonceBatchOps struct {
	rollbackTo *uint64
	prune      uint64
	nonces     []models.BlockNonce
}

// nonceOpsOf copies batch's nonce changes. The caller holds utxoSetMu.
func nonceOpsOf(batch *utxoSetBatch) nonceBatchOps {
	ops := nonceBatchOps{
		prune:  batch.pruneNoncesBefore,
		nonces: slices.Clone(batch.nonces),
	}
	lower := func(slot uint64) {
		if ops.rollbackTo == nil || slot < *ops.rollbackTo {
			ops.rollbackTo = &slot
		}
	}
	if batch.rollbackTo != nil {
		lower(batch.rollbackTo.Slot)
	}
	for _, c := range batch.changes {
		if c.Cause == UtxoChangeRollback {
			lower(c.Point.Slot)
		}
	}
	return ops
}

func nonceKeyOf(slot uint64, hash []byte) string {
	return fmt.Sprintf("%d/%x", slot, hash)
}

// nonceView applies the nonce changes of the batches txn sees to the set's
// committed nonces in [start, end).
type nonceView map[string]models.BlockNonce

func (v nonceView) apply(ops nonceBatchOps, start, end uint64) {
	for k, n := range v {
		if (ops.rollbackTo != nil && n.Slot > *ops.rollbackTo) ||
			(n.Slot < ops.prune && !n.IsCheckpoint) {
			delete(v, k)
		}
	}
	for _, n := range ops.nonces {
		if n.Slot >= start && n.Slot < end {
			k := nonceKeyOf(n.Slot, n.Hash)
			// A rewrite keeps the checkpoint flag, as the stores do.
			n.IsCheckpoint = n.IsCheckpoint || v[k].IsCheckpoint
			v[k] = n
		}
	}
}

func (v nonceView) sorted() []models.BlockNonce {
	out := make([]models.BlockNonce, 0, len(v))
	for _, n := range v {
		out = append(out, n)
	}
	slices.SortFunc(out, func(a, b models.BlockNonce) int {
		if c := cmp.Compare(a.Slot, b.Slot); c != 0 {
			return c
		}
		return bytes.Compare(a.Hash, b.Hash)
	})
	return out
}

// nonceSetReader returns the live UTxO set's nonce reader and the read mode,
// or UtxoSetReadsOff when nonce reads stay on the metadata store.
func (ls *LedgerState) nonceSetReader(
	txn ledgerdb.Txn,
) (BlockNonceReader, string) {
	_, mode := ls.utxoSetReader()
	if mode == UtxoSetReadsOff {
		return nil, UtxoSetReadsOff
	}
	r, ok := ls.readSet(txn).(BlockNonceReader)
	if !ok {
		return nil, UtxoSetReadsOff
	}
	if !ls.nonceSetLoaded.Load() {
		// Startup loads the nonces before the ledger runs; until then, and
		// for a set that cannot say, reads stay on the metadata store.
		loaded, err := r.NoncesLoaded()
		if err != nil || !loaded {
			return nil, UtxoSetReadsOff
		}
		ls.nonceSetLoaded.Store(true)
	}
	return r, mode
}

// setNoncesInSlotRange reads the nonces in [start, end) as txn sees them:
// the set's committed nonces with the batches it sees applied over them.
func (ls *LedgerState) setNoncesInSlotRange(
	r BlockNonceReader,
	txn ledgerdb.Txn,
	start, end uint64,
) ([]models.BlockNonce, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	batches := ls.utxoSetBatchesSeenBy(ls.pinOf(txn), own)
	ls.utxoSetMu.Lock()
	ops := make([]nonceBatchOps, 0, len(batches))
	for _, b := range batches {
		ops = append(ops, nonceOpsOf(b))
	}
	ls.utxoSetMu.Unlock()
	committed, err := r.BlockNoncesInSlotRange(start, end)
	if err != nil {
		return nil, err
	}
	v := make(nonceView, len(committed))
	for _, n := range committed {
		v[nonceKeyOf(n.Slot, n.Hash)] = n
	}
	for _, o := range ops {
		v.apply(o, start, end)
	}
	return v.sorted(), nil
}

// blockNonce reads the nonce of the block at point from the store the read
// mode selects.
func (ls *LedgerState) blockNonce(
	point ocommon.Point,
	txn ledgerdb.Txn,
) ([]byte, error) {
	r, mode := ls.nonceSetReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if ls.familyDrained(nodesettings.FamilyNonces) {
			return nil, drainedUnserved(nodesettings.FamilyNonces)
		}
		return ls.ledgerDB().GetBlockNonce(point, txn)
	}
	for attempt := 1; ; attempt++ {
		g := ls.beginVerifyRead(txn)
		ns, setErr := ls.setNoncesInSlotRange(r, txn, point.Slot, point.Slot+1)
		var setNonce []byte
		setFound := false
		for _, n := range ns {
			if bytes.Equal(n.Hash, point.Hash) {
				setNonce, setFound = n.Nonce, true
			}
		}
		if mode == UtxoSetReadsSet {
			if setErr != nil {
				return nil, setErr
			}
			return setNonce, nil
		}
		stored, err := ls.ledgerDB().GetBlockNonce(point, txn)
		if err != nil {
			return nil, err
		}
		storedFound := len(stored) > 0
		same := setErr == nil && setFound == storedFound &&
			(!storedFound || bytes.Equal(stored, setNonce))
		if !same && ls.verifyReadRaced(g) {
			if attempt < verifyReadAttempts {
				continue
			}
			return stored, nil
		}
		ls.compareNonceRead(
			fmt.Sprintf("block %d/%x", point.Slot, point.Hash), same, setErr,
		)
		return stored, nil
	}
}

// blockNoncesInSlotRange reads the nonces in [start, end) from the store the
// read mode selects.
func (ls *LedgerState) blockNoncesInSlotRange(
	start, end uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	r, mode := ls.nonceSetReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if ls.familyDrained(nodesettings.FamilyNonces) {
			return nil, drainedUnserved(nodesettings.FamilyNonces)
		}
		return ls.ledgerDB().GetBlockNoncesInSlotRange(start, end, txn)
	}
	for attempt := 1; ; attempt++ {
		g := ls.beginVerifyRead(txn)
		fromSet, setErr := ls.setNoncesInSlotRange(r, txn, start, end)
		if mode == UtxoSetReadsSet {
			return fromSet, setErr
		}
		stored, err := ls.ledgerDB().GetBlockNoncesInSlotRange(start, end, txn)
		if err != nil {
			return nil, err
		}
		// The metadata store orders by slot alone; the set also by hash.
		sortedStored := nonceView{}
		for _, n := range stored {
			sortedStored[nonceKeyOf(n.Slot, n.Hash)] = n
		}
		same := setErr == nil && sameNonces(sortedStored.sorted(), fromSet)
		if !same && ls.verifyReadRaced(g) {
			if attempt < verifyReadAttempts {
				continue
			}
			return stored, nil
		}
		what := fmt.Sprintf("slots [%d, %d)", start, end)
		if !same && setErr == nil {
			what += " " + nonceDiff(sortedStored.sorted(), fromSet) + " " +
				ls.nonceOverlayReport(r, txn, start, end)
		}
		ls.compareNonceRead(what, same, setErr)
		return stored, nil
	}
}

// latestBlockNonce reads the highest-slot nonce from the store the read mode
// selects.
func (ls *LedgerState) latestBlockNonce(
	txn ledgerdb.Txn,
) (models.BlockNonce, bool, error) {
	r, mode := ls.nonceSetReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if ls.familyDrained(nodesettings.FamilyNonces) {
			return models.BlockNonce{}, false, drainedUnserved(
				nodesettings.FamilyNonces,
			)
		}
		return ls.ledgerDB().GetLatestBlockNonce(txn)
	}
	for attempt := 1; ; attempt++ {
		g := ls.beginVerifyRead(txn)
		latest, ok, setErr := ls.setLatestNonce(r, txn)
		if mode == UtxoSetReadsSet {
			return latest, ok, setErr
		}
		stored, storedOK, err := ls.ledgerDB().GetLatestBlockNonce(txn)
		if err != nil {
			return models.BlockNonce{}, false, err
		}
		same := setErr == nil && ok == storedOK &&
			(!ok || sameNonces(
				[]models.BlockNonce{stored}, []models.BlockNonce{latest},
			))
		if !same && ls.verifyReadRaced(g) {
			if attempt < verifyReadAttempts {
				continue
			}
			return stored, storedOK, nil
		}
		ls.compareNonceRead("latest", same, setErr)
		return stored, storedOK, nil
	}
}

// setLatestNonce is the highest-slot nonce as txn sees the set. Staged
// batches only add nonces at or above the committed latest, or roll back
// below it, so the committed latest bounds where to look.
func (ls *LedgerState) setLatestNonce(
	r BlockNonceReader,
	txn ledgerdb.Txn,
) (models.BlockNonce, bool, error) {
	committed, ok, err := r.LatestBlockNonce()
	if err != nil {
		return models.BlockNonce{}, false, err
	}
	start := uint64(0)
	if ok {
		start = committed.Slot
	}
	ns, err := ls.setNoncesInSlotRange(r, txn, start, ^uint64(0))
	if err == nil && len(ns) == 0 && start > 0 {
		// A staged rollback removed every nonce from start up. Pruning
		// keeps the retained nonces to about a stability window plus the
		// checkpoints, so reading them all is cheap.
		ns, err = ls.setNoncesInSlotRange(r, txn, 0, ^uint64(0))
	}
	if err != nil || len(ns) == 0 {
		return models.BlockNonce{}, false, err
	}
	return ns[len(ns)-1], true, nil
}

// nonceOverlayReport describes, for a mismatch report, what the set read
// rested on: the committed nonces in [start, end) now, and the staged
// batches txn sees with their nonces, prunes and rollbacks.
func (ls *LedgerState) nonceOverlayReport(
	r BlockNonceReader,
	txn ledgerdb.Txn,
	start, end uint64,
) string {
	committed, err := r.BlockNoncesInSlotRange(start, end)
	ls.utxoSetApplyMu.RLock()
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	batches := ls.utxoSetBatchesSeenBy(ls.pinOf(txn), own)
	ls.utxoSetMu.Lock()
	all := len(ls.utxoSetBatches)
	staged := make([]string, 0, len(batches))
	for _, b := range batches {
		o := nonceOpsOf(b)
		rb := "-"
		if o.rollbackTo != nil {
			rb = strconv.FormatUint(*o.rollbackTo, 10)
		}
		staged = append(staged, fmt.Sprintf("seq=%d own=%t nonces=%d prune=%d rollback=%s",
			b.seq, b == own, len(o.nonces), o.prune, rb))
	}
	ls.utxoSetMu.Unlock()
	ls.utxoSetApplyMu.RUnlock()
	return fmt.Sprintf("committed_now=%d committed_err=%v batches_seen=%v batches_all=%d own_found=%t",
		len(committed), err, staged, all, own != nil)
}

// nonceDiff describes up to three entries only the metadata store holds and
// three only the set holds, for a mismatch report.
func nonceDiff(stored, set []models.BlockNonce) string {
	key := func(n models.BlockNonce) string {
		return fmt.Sprintf("%d/%x=%x/%t", n.Slot, n.Hash, n.Nonce, n.IsCheckpoint)
	}
	in := func(ns []models.BlockNonce) map[string]struct{} {
		m := make(map[string]struct{}, len(ns))
		for _, n := range ns {
			m[key(n)] = struct{}{}
		}
		return m
	}
	only := func(ns []models.BlockNonce, other map[string]struct{}) []string {
		var out []string
		for _, n := range ns {
			if _, ok := other[key(n)]; !ok && len(out) < 3 {
				out = append(out, key(n))
			}
		}
		return out
	}
	return fmt.Sprintf("store_only=%v set_only=%v (store %d, set %d)",
		only(stored, in(set)), only(set, in(stored)), len(stored), len(set))
}

func sameNonces(a, b []models.BlockNonce) bool {
	return slices.EqualFunc(a, b, func(x, y models.BlockNonce) bool {
		return x.Slot == y.Slot && bytes.Equal(x.Hash, y.Hash) &&
			bytes.Equal(x.Nonce, y.Nonce) && x.IsCheckpoint == y.IsCheckpoint
	})
}

func (ls *LedgerState) compareNonceRead(what string, same bool, setErr error) {
	if !ls.verifying() {
		return
	}
	ls.metrics.nonceSetReadsVerified.Inc()
	if same {
		return
	}
	n := ls.nonceSetReadMismatches.Add(1)
	ls.metrics.nonceSetReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set nonce read differs from the metadata store",
		"component", "ledger",
		"read", what,
		"set_error", setErr,
		"mismatches", n,
	)
}

// verifyReadGuard records what a verify read's set view rests on: the
// batch sequence and the other transactions' batches it left out because
// they had not committed. A verify read reads the set and then the metadata
// store; a write that commits in between shows only in the metadata store
// and is not a disagreement. A read of a pinned transaction (see
// pinSetReads) reads both as of the pin, so no write can race it.
type verifyReadGuard struct {
	txn     ledgerdb.Txn
	seq     uint64
	pending []*utxoSetBatch
	pinned  bool
}

// utxoSetPin is the live UTxO set as of the moment a read transaction was
// pinned: a snapshot of the set's applied state, and the batches that had
// committed but not yet reached the set. Reads of the transaction see the
// snapshot under those batches and nothing that committed after. set is nil
// when no snapshot could be taken; the transaction's reads then stay on the
// metadata store.
type utxoSetPin struct {
	set     UtxoSetWriter
	batches []*utxoSetBatch
}

// pinSetReads makes txn, a read transaction whose metadata snapshot is
// already fixed, read the live UTxO set as of now until it finishes. The
// snapshot and the batch list are taken under utxoSetApplyMu, which a batch
// holds exclusively while it is applied, so a batch is in exactly one of them.
func (ls *LedgerState) pinSetReads(txn ledgerdb.Txn) {
	if _, mode := ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return
	}
	pin := &utxoSetPin{}
	release := func() {}
	if sn, ok := ls.readSet(txn).(UtxoSetSnapshotter); ok {
		ls.utxoSetApplyMu.RLock()
		set, rel, err := sn.SnapshotUtxoSet()
		if err == nil {
			pin.set, release = set, rel
			pin.batches = ls.utxoSetBatchesSeenBy(nil, nil)
		}
		ls.utxoSetApplyMu.RUnlock()
		if err != nil {
			ls.config.Logger.Warn(
				"live UTxO set snapshot failed; pinned reads use the store",
				"component", "ledger",
				"error", err,
			)
		}
	}
	var meta types.Txn
	if dbTxn := dbadapter.Unwrap(txn); dbTxn != nil {
		meta = dbTxn.Metadata()
	}
	ls.utxoSetMu.Lock()
	if ls.utxoSetPins == nil {
		ls.utxoSetPins = make(map[ledgerdb.Txn]*utxoSetPin)
	}
	ls.utxoSetPins[txn] = pin
	if meta != nil {
		if ls.utxoSetPinsByMeta == nil {
			ls.utxoSetPinsByMeta = make(map[types.Txn]*utxoSetPin)
		}
		ls.utxoSetPinsByMeta[meta] = pin
	}
	ls.utxoSetMu.Unlock()
	txn.OnFinish(func() {
		ls.utxoSetMu.Lock()
		delete(ls.utxoSetPins, txn)
		if meta != nil {
			delete(ls.utxoSetPinsByMeta, meta)
		}
		ls.utxoSetMu.Unlock()
		release()
	})
}

// pinOf returns the pin of txn, nil for a transaction that is not pinned.
func (ls *LedgerState) pinOf(txn ledgerdb.Txn) *utxoSetPin {
	if txn == nil {
		return nil
	}
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	return ls.utxoSetPins[txn]
}

// pinOfMeta returns the pin of the metadata transaction meta.
func (ls *LedgerState) pinOfMeta(meta types.Txn) *utxoSetPin {
	if meta == nil {
		return nil
	}
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	return ls.utxoSetPinsByMeta[meta]
}

// readSet returns the live UTxO set txn reads: its pin's snapshot, nil when
// the pin has none, or the set itself for a transaction that is not pinned.
func (ls *LedgerState) readSet(txn ledgerdb.Txn) UtxoSetWriter {
	if pin := ls.pinOf(txn); pin != nil {
		return pin.set
	}
	return ls.config.UtxoSet
}

// readSetMeta is readSet for a metadata transaction.
func (ls *LedgerState) readSetMeta(meta types.Txn) UtxoSetWriter {
	if pin := ls.pinOfMeta(meta); pin != nil {
		return pin.set
	}
	return ls.config.UtxoSet
}

// verifying reports whether reads are in verify mode. Only verify mode
// compares: set mode answers from the set where it can and trusts it, a read
// it does not serve is the store's, and only verify mode holds the guard
// that tells a disagreement from a write racing the comparison.
func (ls *LedgerState) verifying() bool {
	_, mode := ls.utxoSetReader()
	return mode == UtxoSetReadsVerify
}

// verifyGuard begins a verify read guard in verify mode, and returns an
// unused zero guard otherwise so the reads that never compare skip its
// lock.
func (ls *LedgerState) verifyGuard(txn ledgerdb.Txn) verifyReadGuard {
	if _, mode := ls.utxoSetReader(); mode != UtxoSetReadsVerify {
		return verifyReadGuard{txn: txn}
	}
	return ls.beginVerifyRead(txn)
}

// verifyReadAttempts bounds how often a verify read is retried when a
// write commits under it; past that, the comparison is skipped.
const verifyReadAttempts = 3

func (ls *LedgerState) beginVerifyRead(txn ledgerdb.Txn) verifyReadGuard {
	if ls.pinOf(txn) != nil {
		return verifyReadGuard{txn: txn, pinned: true}
	}
	ls.utxoSetMu.Lock()
	var own *utxoSetBatch
	if txn != nil {
		own = ls.utxoSetBatches[txn]
	}
	g := verifyReadGuard{txn: txn, seq: ls.utxoSetSeq}
	others := make([]*utxoSetBatch, 0, len(ls.utxoSetBatches))
	for _, b := range ls.utxoSetBatches {
		if b != own {
			others = append(others, b)
		}
	}
	ls.utxoSetMu.Unlock()
	for _, b := range others {
		if !b.txn.IsCommitted() {
			g.pending = append(g.pending, b)
		}
	}
	return g
}

// verifyReadRaced reports whether a write may have committed between the
// set read and the metadata read guarded by g: a batch was staged since, or
// one left out as uncommitted has committed. Every write stages its batch
// before it commits, so these cover every commit the metadata read can see.
func (ls *LedgerState) verifyReadRaced(g verifyReadGuard) bool {
	if g.pinned {
		return false
	}
	ls.utxoSetMu.Lock()
	seq := ls.utxoSetSeq
	ls.utxoSetMu.Unlock()
	if seq != g.seq {
		return true
	}
	for _, b := range g.pending {
		if b.txn.IsCommitted() {
			return true
		}
	}
	return false
}
