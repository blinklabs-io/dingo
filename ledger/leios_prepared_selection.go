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

// ReusePreparedLeiosSelection checks the local preparation result carried by
// the forger after the network layer has verified the EB/header binding.
func ReusePreparedLeiosSelection(ctx context.Context, header lcommon.BlockHeader, basis []byte, txs [][]byte) bool {
	receipt := forging.LeiosValidationReceiptFromContext(ctx)
	return receipt != nil && receipt.MatchesLeiosAnnouncement(ctx, header, basis, txs)
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
			stillCurrent func() bool,
			_ func(func() error) (bool, error),
			apply txValidationApplyFunc,
			_ txValidationResolveFunc,
			measure forging.TxRefScriptSizeFunc,
		) error {
			var validatedTx lcommon.Transaction
			var validatedRaw []byte
			var validationFault error
			decodeValidate := func(raw []byte, consumed map[utxoref.Key]struct{}, created map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error) {
				validatedTx = nil
				validatedRaw = nil
				tx, err := safedecode.DijkstraBlockTransaction(raw)
				if err != nil {
					return nil, err
				}
				if err := validateTransactionOutputIndexes([]lcommon.Transaction{tx}); err != nil {
					return nil, err
				}
				if err := validate(tx, consumed, created, nil); err != nil {
					if _, semantic := errors.AsType[*semanticTxValidationError](err); !semantic {
						validationFault = err
					}
					return nil, err
				}
				validatedTx = tx
				validatedRaw = bytes.Clone(raw)
				return tx, nil
			}
			admit := func(raw []byte) (err error) {
				defer func() {
					if err != nil {
						validationFault = err
					}
				}()
				if validatedTx == nil || !bytes.Equal(raw, validatedRaw) {
					return errors.New(
						"leios admission does not match the last validated candidate",
					)
				}
				tx := validatedTx
				validatedTx = nil
				if len(raw) == 0 || uint64(len(raw)) > (16<<20)-selectedBytes {
					return fmt.Errorf(
						"%w: selected endorser block exceeds validation byte limit",
						ErrInvalidLeiosEndorserBlockTransactions,
					)
				}
				// The point identifies rollback-only staged offsets, never published CBOR.
				// Validation resolves selected outputs from the ordered in-memory overlay.
				point := ocommon.NewPoint(slot, parent)
				if err := apply(tx, len(selected), point, uint(dijkstra.EraIdDijkstra), snapshot.tip.BlockNumber+1); err != nil {
					return err
				}
				selected = append(selected, bytes.Clone(raw))
				selectedBytes += uint64(len(raw))
				return nil
			}
			if err := fn(decodeValidate, measure, admit, stillCurrent); err != nil {
				return err
			}
			if validationFault != nil {
				return validationFault
			}
			if !stillCurrent() {
				return ErrLeiosValidationParentSuperseded
			}
			return nil
		},
	)
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
