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
	"errors"
	"fmt"
	"slices"

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
// or false when the set does not answer nonce reads.
func (ls *LedgerState) nonceSetReader(
	txn ledgerdb.Txn,
) (BlockNonceReader, bool) {
	_, mode := ls.utxoSetReader()
	if !mode {
		return nil, false
	}
	r, ok := ls.readSet(txn).(BlockNonceReader)
	if !ok {
		return nil, false
	}
	if !ls.nonceSetLoaded.Load() {
		// Startup loads the nonces before the ledger runs; until then the
		// set does not answer.
		loaded, err := r.NoncesLoaded()
		if err != nil || !loaded {
			return nil, false
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
	if !mode || r == nil {
		return nil, ls.setFamilyUnserved(nodesettings.FamilyNonces)
	}
	ns, setErr := ls.setNoncesInSlotRange(r, txn, point.Slot, point.Slot+1)
	if setErr != nil {
		return nil, setErr
	}
	var setNonce []byte
	for _, n := range ns {
		if bytes.Equal(n.Hash, point.Hash) {
			setNonce = n.Nonce
		}
	}
	return setNonce, nil
}

// blockNoncesInSlotRange reads the nonces in [start, end) from the store the
// read mode selects.
func (ls *LedgerState) blockNoncesInSlotRange(
	start, end uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	r, mode := ls.nonceSetReader(txn)
	if !mode || r == nil {
		return nil, ls.setFamilyUnserved(nodesettings.FamilyNonces)
	}
	return ls.setNoncesInSlotRange(r, txn, start, end)
}

// latestBlockNonce reads the highest-slot nonce from the store the read mode
// selects.
func (ls *LedgerState) latestBlockNonce(
	txn ledgerdb.Txn,
) (models.BlockNonce, bool, error) {
	r, mode := ls.nonceSetReader(txn)
	if !mode || r == nil {
		return models.BlockNonce{}, false, ls.setFamilyUnserved(
			nodesettings.FamilyNonces,
		)
	}
	return ls.setLatestNonce(r, txn)
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

// utxoSetPin is the live UTxO set as of the moment a read transaction was
// pinned: a snapshot of the set's applied state, and the batches that had
// committed but not yet reached the set. Reads of the transaction see the
// snapshot under those batches and nothing that committed after. set is nil
// when no snapshot could be taken; reads served by the set then fail.
type utxoSetPin struct {
	set     UtxoSetWriter
	batches []*utxoSetBatch
}

// pinSetReadsResult pins the set snapshot and committed batches until txn
// finishes. atCommitAnchor requires the coordinated database snapshot barrier.
func (ls *LedgerState) pinSetReadsResult(
	txn ledgerdb.Txn, atCommitAnchor bool,
) error {
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	set := ls.readSet(txn)
	if _, ok := set.(UtxoSetSnapshotter); !ok {
		return errors.New("query views need UTxO snapshots")
	}
	if _, ok := set.(UtxoSetHistory); !ok {
		return errors.New("query views need UTxO history")
	}
	pin := &utxoSetPin{}
	release := func() {}
	var snapshotErr error
	if sn, ok := set.(UtxoSetSnapshotter); ok {
		err := func() error {
			if atCommitAnchor {
				if !ls.utxoSetApplyMu.TryRLock() {
					return errQueryViewSetPending
				}
			} else {
				ls.utxoSetApplyMu.RLock()
			}
			defer ls.utxoSetApplyMu.RUnlock()
			set, rel, err := sn.SnapshotUtxoSet()
			if err == nil {
				pin.set, release = set, rel
				if atCommitAnchor {
					pin.batches = ls.utxoSetBatchesAtCommitAnchor()
				} else {
					pin.batches = ls.utxoSetBatchesSeenBy(nil, nil)
				}
				if _, ok := set.(UtxoSetHistory); !ok {
					return errors.New("query view snapshot needs UTxO history")
				}
			}
			return err
		}()
		if errors.Is(err, errQueryViewSetPending) {
			return err
		}
		if err != nil {
			snapshotErr = err
			ls.config.Logger.Warn(
				"live UTxO set snapshot failed",
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
	return snapshotErr
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
