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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"golang.org/x/crypto/blake2b"
)

// leiosSelectionReceipt is ephemeral and cannot survive a ledger publication,
// even when a rollback later returns to the same parent hash.
type leiosSelectionReceipt struct {
	ls           *LedgerState
	generation   uint64
	parent       []byte
	slot         uint64
	blockNumber  uint64
	transactions lcommon.Blake2b256
}

func leiosSelectionDigest(txs [][]byte) lcommon.Blake2b256 {
	digest, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	var length [8]byte
	for _, raw := range txs {
		binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write(raw)
	}
	return lcommon.NewBlake2b256(digest.Sum(nil))
}

func (r *leiosSelectionReceipt) MatchesLeiosAnnouncement(
	ctx context.Context,
	header lcommon.BlockHeader,
	basis []byte,
	txs [][]byte,
) bool {
	if ctx.Err() != nil || header == nil || header.SlotNumber() != r.slot ||
		header.BlockNumber() != r.blockNumber || header.Era().Id != dijkstra.EraIdDijkstra ||
		!bytes.Equal(
			header.PrevHash().Bytes(),
			r.parent,
		) || !bytes.Equal(basis, r.parent) ||
		len(txs) == 0 || leiosSelectionDigest(txs) != r.transactions {
		return false
	}
	certifier, ok := header.(interface{ LeiosCertified() (bool, bool) })
	if !ok {
		return false
	}
	certified, present := certifier.LeiosCertified()
	return present && !certified &&
		r.ls.txValidationSnapshotCurrent(r.generation)
}

func (r *leiosSelectionReceipt) StillValidForParent(
	ctx context.Context,
	parent []byte,
	slot uint64,
) bool {
	return ctx.Err() == nil && slot == r.slot &&
		bytes.Equal(parent, r.parent) &&
		r.ls.txValidationSnapshotCurrent(r.generation)
}

// ReusePreparedLeiosSelection checks the local preparation result carried by
// the forger after the network layer has verified the EB/header binding.
func ReusePreparedLeiosSelection(
	ctx context.Context,
	header lcommon.BlockHeader,
	basis []byte,
	txs [][]byte,
) bool {
	receipt := forging.LeiosValidationReceiptFromContext(ctx)
	return receipt != nil &&
		receipt.MatchesLeiosAnnouncement(ctx, header, basis, txs)
}

// WithPreparedLeiosSelection opens a rollback-only, slot-ticked validation
// session with the same transaction decoding, validation and ordered full
// effects as ValidateLeiosEndorserBlockTransactions. A selected prefix obtains
// a receipt only after the session completed on its original publication.
func (ls *LedgerState) WithPreparedLeiosSelection(
	ctx context.Context,
	parent []byte,
	slot uint64,
	fn func(
		validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
		measure forging.TxRefScriptSizeFunc,
		admit func([]byte) error,
		stillCurrent func() bool,
	) error,
) (forging.LeiosValidationReceipt, error) {
	return ls.WithParallelLeiosSelection(ctx, parent, slot, func(
		prepare func([]byte) (forging.PreparedLeiosTransaction, error),
		validate func(forging.PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
		measure forging.TxRefScriptSizeFunc,
		admit func(forging.PreparedLeiosTransaction) error,
		current func() bool,
	) error {
		var pending *preparedLeiosSelectionTransaction
		decodeValidate := func(raw []byte, consumed map[utxoref.Key]struct{}, created map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error) {
			pending = nil
			token, err := prepare(raw)
			if err != nil {
				return nil, err
			}
			tx, err := validate(token, consumed, created)
			if err == nil {
				pending = token.(*preparedLeiosSelectionTransaction)
			}
			return tx, err
		}
		admitRaw := func(raw []byte) error {
			if pending == nil || !bytes.Equal(raw, pending.raw) {
				return admit(nil)
			}
			token := pending
			pending = nil
			return admit(token)
		}
		return fn(decodeValidate, measure, admitRaw, current)
	})
}

// preparedLeiosSelectionTransaction keeps decoded semantics and immutable wire
// bytes together. Only the ledger's pure constructor can create this token.
type preparedLeiosSelectionTransaction struct {
	owner *LedgerState
	raw   []byte
	tx    lcommon.Transaction
}

func (p *preparedLeiosSelectionTransaction) Transaction() lcommon.Transaction { return p.tx }

func (p *preparedLeiosSelectionTransaction) BlockTransactionCbor() []byte { return p.raw }

func (ls *LedgerState) prepareLeiosSelectionTransaction(
	raw []byte,
) (forging.PreparedLeiosTransaction, error) {
	raw = bytes.Clone(raw)
	tx, err := safedecode.DijkstraBlockTransaction(raw)
	if err != nil {
		return nil, err
	}
	if err := validateTransactionOutputIndexes([]lcommon.Transaction{tx}); err != nil {
		return nil, err
	}
	return &preparedLeiosSelectionTransaction{owner: ls, raw: raw, tx: tx}, nil
}

var errLeiosSelectionPreempted = errors.New(
	"leios selection yielded to a ledger transition",
)

type leiosSelectionCancellation struct{ cancel context.CancelCauseFunc }

// A speculative session can own SQLite's writer while evaluating a script.
// Cancel it before block application waits for that writer, rather than
// waiting for the next transaction-level publication check.
func (ls *LedgerState) leiosSelectionContext(
	parent context.Context,
) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	registration := &leiosSelectionCancellation{cancel: cancel}
	ls.leiosSelectionMu.Lock()
	if ls.leiosSelections == nil {
		ls.leiosSelections = make(map[*leiosSelectionCancellation]struct{})
	}
	ls.leiosSelections[registration] = struct{}{}
	transitioning := ls.txValidationTransitions.Load() != 0
	ls.leiosSelectionMu.Unlock()
	if transitioning {
		cancel(errLeiosSelectionPreempted)
	}
	return ctx, func() {
		ls.leiosSelectionMu.Lock()
		delete(ls.leiosSelections, registration)
		ls.leiosSelectionMu.Unlock()
		cancel(nil)
	}
}

func (ls *LedgerState) cancelLeiosSelectionsForTransition() {
	ls.leiosSelectionMu.Lock()
	cancellations := make([]context.CancelCauseFunc, 0, len(ls.leiosSelections))
	for registration := range ls.leiosSelections {
		cancellations = append(cancellations, registration.cancel)
	}
	ls.leiosSelectionMu.Unlock()
	for _, cancel := range cancellations {
		cancel(errLeiosSelectionPreempted)
	}
}

// WithParallelLeiosSelection owns the ordered validation session while pure
// transaction decoding runs concurrently with serial ordered ledger work.
func (ls *LedgerState) WithParallelLeiosSelection(
	ctx context.Context,
	parent []byte,
	slot uint64,
	fn func(
		prepare func([]byte) (forging.PreparedLeiosTransaction, error),
		validate func(forging.PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
		measure forging.TxRefScriptSizeFunc,
		admit func(forging.PreparedLeiosTransaction) error,
		current func() bool,
	) error,
) (forging.LeiosValidationReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parentCtx := ctx
	ctx, release := ls.leiosSelectionContext(ctx)
	defer release()
	if len(parent) == 0 {
		return nil, forging.ErrTxValidationParentMismatch
	}
	snapshot := ls.txValidationSnapshot()
	if !bytes.Equal(snapshot.tipPoint.Hash, parent) {
		return nil, forging.ErrTxValidationParentMismatch
	}
	var selected [][]byte
	var selectedBytes uint64
	err := ls.withResolvingTxValidationSession(
		ctx,
		parent,
		&slot,
		true,
		true,
		nil,
		func(
			validate forging.TxValidationFunc,
			current func() bool,
			_ func(func() error) (bool, error),
			apply txValidationApplyFunc,
			_ txValidationResolveFunc,
			_ txValidationAppliedFunc,
			measure forging.TxRefScriptSizeFunc,
		) error {
			var validated *preparedLeiosSelectionTransaction
			var fault error
			validatePrepared := func(token forging.PreparedLeiosTransaction, consumed map[utxoref.Key]struct{}, created map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error) {
				validated = nil
				candidate, ok := token.(*preparedLeiosSelectionTransaction)
				if !ok || candidate == nil || candidate.owner != ls {
					fault = errors.New(
						"invalid prepared Leios transaction token",
					)
					return nil, fault
				}
				if err := validate(candidate.tx, consumed, created, nil); err != nil {
					if _, semantic := errors.AsType[*semanticTxValidationError](err); !semantic {
						fault = err
					}
					return nil, err
				}
				validated = candidate
				return candidate.tx, nil
			}
			admit := func(token forging.PreparedLeiosTransaction) (err error) {
				defer func() {
					if err != nil {
						fault = err
					}
				}()
				candidate, ok := token.(*preparedLeiosSelectionTransaction)
				if !ok || candidate == nil || candidate != validated {
					return errors.New(
						"leios admission does not match the last validated candidate",
					)
				}
				validated = nil
				if len(candidate.raw) == 0 ||
					uint64(len(candidate.raw)) > (16<<20)-selectedBytes {
					return fmt.Errorf(
						"%w: selected endorser block exceeds validation byte limit",
						ErrInvalidLeiosEndorserBlockTransactions,
					)
				}
				// The point identifies rollback-only staged offsets, never published CBOR.
				// Validation resolves selected outputs from the ordered in-memory overlay.
				if err := apply(candidate.tx, len(selected), ocommon.NewPoint(slot, parent), uint(dijkstra.EraIdDijkstra), snapshot.tip.BlockNumber+1); err != nil {
					return err
				}
				selected = append(selected, candidate.raw)
				selectedBytes += uint64(len(candidate.raw))
				return nil
			}
			if err := fn(ls.prepareLeiosSelectionTransaction, validatePrepared, measure, admit, current); err != nil {
				return err
			}
			if fault != nil {
				return fault
			}
			if !current() {
				return ErrLeiosValidationParentSuperseded
			}
			return nil
		},
	)
	if parentCtx.Err() != nil {
		return nil, parentCtx.Err()
	}
	if errors.Is(context.Cause(ctx), errLeiosSelectionPreempted) {
		return nil, fmt.Errorf(
			"%w: %w",
			forging.ErrTxValidationParentMismatch,
			ErrLeiosValidationParentSuperseded,
		)
	}
	if errors.Is(err, ErrLeiosValidationParentSuperseded) {
		return nil, fmt.Errorf(
			"%w: %w",
			forging.ErrTxValidationParentMismatch,
			err,
		)
	}
	if err != nil {
		return nil, err
	}
	if !ls.txValidationSnapshotCurrent(snapshot.generation) {
		return nil, forging.ErrTxValidationParentMismatch
	}
	return &leiosSelectionReceipt{
		ls:           ls,
		generation:   snapshot.generation,
		parent:       bytes.Clone(parent),
		slot:         slot,
		blockNumber:  snapshot.tip.BlockNumber + 1,
		transactions: leiosSelectionDigest(selected),
	}, nil
}

var _ forging.PreparedLeiosSelectionProvider = (*LedgerState)(nil)

var _ forging.ParallelLeiosSelectionProvider = (*LedgerState)(nil)
