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

package forging

import (
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// LeiosValidationReceipt attests to a successful ordered selection under the
// same full ledger semantics used to validate an announcement. Implementations
// must reject a changed publication, slot, header context, or transaction bytes.
type LeiosValidationReceipt interface {
	MatchesLeiosAnnouncement(
		context.Context,
		lcommon.BlockHeader,
		[]byte,
		[][]byte,
	) bool
}

// PreparedLeiosSelectionProvider validates candidates at the announcing slot,
// staging full ledger effects only when admit is called. The session is rolled
// back; its receipt is valid only while the ledger publication is unchanged.
// This capability is used only for announcements without a certified closure.
type PreparedLeiosSelectionProvider interface {
	WithPreparedLeiosSelection(context.Context, []byte, uint64, func(
		validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
		measure TxRefScriptSizeFunc,
		admit func([]byte) error,
		stillCurrent func() bool,
	) error) (LeiosValidationReceipt, error)
}

// PreparedLeiosTransaction is a decoded wire transaction owned by its ledger
// provider. Both views are read-only and must not be mutated by callers.
type PreparedLeiosTransaction interface {
	Transaction() lcommon.Transaction
	BlockTransactionCbor() []byte
}

// ParallelLeiosSelectionProvider separates pure decoding from ordered ledger
// work. prepare may be called by concurrent workers; every other callback must
// be called serially, and admit accepts only the last successfully validated
// token. Providers reject tokens they did not prepare themselves.
type ParallelLeiosSelectionProvider interface {
	WithParallelLeiosSelection(context.Context, []byte, uint64, func(
		prepare func([]byte) (PreparedLeiosTransaction, error),
		validate func(PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
		measure TxRefScriptSizeFunc,
		admit func(PreparedLeiosTransaction) error,
		stillCurrent func() bool,
	) error) (LeiosValidationReceipt, error)
}

// LeiosPreparationReceipt checks the publication before an unsigned candidate
// is consumed. Announcement validation additionally checks the complete header
// context and transaction bytes through LeiosValidationReceipt.
type LeiosPreparationReceipt interface {
	LeiosValidationReceipt
	StillValidForParent(context.Context, []byte, uint64) bool
}

type leiosValidationReceiptKey struct{}

// WithLeiosValidationReceipt carries a local ledger-owned validation result to
// the announcement validator. Peer messages never supply this context value.
func WithLeiosValidationReceipt(
	ctx context.Context,
	receipt LeiosValidationReceipt,
) context.Context {
	return context.WithValue(ctx, leiosValidationReceiptKey{}, receipt)
}

// LeiosValidationReceiptFromContext returns a local prepared selection result.
func LeiosValidationReceiptFromContext(
	ctx context.Context,
) LeiosValidationReceipt {
	receipt, _ := ctx.Value(leiosValidationReceiptKey{}).(LeiosValidationReceipt)
	return receipt
}

func selectPreparedLeiosTransactions(
	ctx context.Context,
	txs []MempoolTransaction,
	provider PreparedLeiosSelectionProvider,
	limits leiosSelectionLimits,
	basis leiosSelectionBasis,
) ([]MempoolTransaction, bool, LeiosValidationReceipt, error) {
	selected := make([]MempoolTransaction, 0, len(txs))
	truncated := false
	capacity := leiosEBCapacity{caps: limits.caps}
	if parallel, ok := provider.(ParallelLeiosSelectionProvider); ok {
		return selectParallelPreparedLeiosTransactions(
			ctx,
			txs,
			parallel,
			limits,
			basis,
		)
	}
	receipt, err := provider.WithPreparedLeiosSelection(
		ctx,
		basis.parentHash,
		basis.slot,
		func(
			validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			measure TxRefScriptSizeFunc,
			admit func([]byte) error,
			stillCurrent func() bool,
		) error {
			consumed := make(map[utxoref.Key]struct{})
			created := make(map[utxoref.Key]lcommon.Utxo)
			for _, candidate := range txs {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !stillCurrent() {
					return errTxValidationSnapshotChanged
				}
				if limits.expired() {
					truncated = true
					break
				}
				if !validLeiosTransactionReference(candidate) {
					continue
				}
				raw, err := leiosBlockTransactionCbor(candidate.Cbor)
				if err != nil || !validLeiosTransactionSize(len(raw)) {
					continue
				}
				tx, err := validate(raw, consumed, created)
				if err != nil {
					continue
				}
				size, err := leiosEBTxMeasure(
					tx,
					measure,
					consumed,
					created,
					limits.caps.aggregateBounded,
				)
				if err != nil {
					continue
				}
				if !capacity.admitMeasured(uint64(len(raw)), size) {
					break
				}
				if err := admit(raw); err != nil {
					return err
				}
				selected = append(selected, candidate)
				for _, input := range tx.Consumed() {
					consumed[utxoref.ForInput(input)] = struct{}{}
				}
				for _, output := range tx.Produced() {
					created[utxoref.ForUtxo(output)] = output
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !stillCurrent() {
				return errTxValidationSnapshotChanged
			}
			return nil
		},
	)
	return selected, truncated, receipt, err
}

func selectParallelPreparedLeiosTransactions(
	ctx context.Context,
	txs []MempoolTransaction,
	provider ParallelLeiosSelectionProvider,
	limits leiosSelectionLimits,
	basis leiosSelectionBasis,
) ([]MempoolTransaction, bool, LeiosValidationReceipt, error) {
	selected := make([]MempoolTransaction, 0, len(txs))
	bodies := make([][]byte, 0, len(txs))
	truncated := false
	capacity := leiosEBCapacity{caps: limits.caps}
	receipt, err := provider.WithParallelLeiosSelection(
		ctx,
		basis.parentHash,
		basis.slot,
		func(
			prepare func([]byte) (PreparedLeiosTransaction, error),
			validate func(PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			measure TxRefScriptSizeFunc,
			admit func(PreparedLeiosTransaction) error,
			current func() bool,
		) error {
			preparation := newLeiosCandidatePreparation(txs, 0)
			preparation.expired = limits.expired
			preparation.prepare = func(candidate MempoolTransaction) (result preparedLeiosCandidate) {
				defer func() {
					if recovered := recover(); recovered != nil {
						result.err = fmt.Errorf(
							"prepare leios transaction: %v",
							recovered,
						)
					}
				}()
				result = prepareLeiosCandidateBytes(candidate)
				if result.err != nil {
					return result
				}
				result.prepared, result.err = prepare(result.blockTx)
				if result.err == nil {
					result.tx = result.prepared.Transaction()
					result.blockTx = result.prepared.BlockTransactionCbor()
				}
				return result
			}
			consumed := make(map[utxoref.Key]struct{})
			created := make(map[utxoref.Key]lcommon.Utxo)
			for range txs {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !current() {
					return errTxValidationSnapshotChanged
				}
				if limits.expired() {
					truncated = true
					break
				}
				candidate, err := preparation.next(ctx)
				if errors.Is(err, errLeiosPreparationExpired) {
					truncated = true
					break
				}
				if err != nil {
					return err
				}
				if !current() {
					return errTxValidationSnapshotChanged
				}
				if limits.expired() {
					truncated = true
					break
				}
				if candidate.err != nil {
					continue
				}
				tx, err := validate(candidate.prepared, consumed, created)
				if err != nil {
					continue
				}
				size, err := leiosEBTxMeasure(
					tx,
					measure,
					consumed,
					created,
					limits.caps.aggregateBounded,
				)
				if err != nil {
					continue
				}
				if !capacity.admitMeasured(
					uint64(len(candidate.blockTx)),
					size,
				) {
					break
				}
				if err := admit(candidate.prepared); err != nil {
					return err
				}
				selected = append(selected, candidate.mempool)
				bodies = append(bodies, candidate.blockTx)
				for _, input := range tx.Consumed() {
					consumed[utxoref.ForInput(input)] = struct{}{}
				}
				for _, output := range tx.Produced() {
					created[utxoref.ForUtxo(output)] = output
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !current() {
				return errTxValidationSnapshotChanged
			}
			return nil
		},
	)
	if err == nil && basis.preparedBodies != nil {
		*basis.preparedBodies = bodies
	}
	return selected, truncated, receipt, err
}
