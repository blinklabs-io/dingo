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
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ValidateForgedBlock validates a locally-forged block before it is adopted
// onto the chain and diffused to peers. It runs three checks in order:
//
//  1. Header crypto — VRF proof and KES signature verification (skipped for
//     Byron-era blocks which use PBFT consensus and have no VRF/KES fields).
//  2. Body-hash consistency — verifies that the body hash in the header is
//     non-zero, catching any builder bug that would embed an all-zero hash.
//  3. Per-transaction ledger rules — each transaction in the block is
//     validated against the current UTxO state. An intra-block overlay
//     is maintained so that transactions spending outputs created earlier
//     in the same block are correctly resolved.
//
// A non-nil error means the block is invalid and must not be adopted or
// diffused. This function satisfies the forging.BlockValidator interface.
func (ls *LedgerState) ValidateForgedBlock(
	block ledger.Block,
	_ []byte,
) error {
	if block == nil {
		return errors.New("nil block")
	}

	// 1. Header crypto: VRF proof + KES signature.
	// Byron blocks use PBFT and have no VRF/KES fields; skip them.
	if block.Era().Id != byron.EraIdByron {
		if err := ls.verifyBlockHeaderCrypto(block); err != nil {
			return fmt.Errorf("header crypto: %w", err)
		}
	}

	// 2. Body-hash consistency: a non-Byron block with an all-zero body
	// hash is always invalid — the builder must have committed a real hash.
	if err := ls.validateForgedBodyHash(block); err != nil {
		return err
	}

	// 3. Per-transaction ledger validation with intra-block UTxO overlay.
	if err := ls.validateForgedTxs(block); err != nil {
		return err
	}

	return nil
}

// validateForgedBodyHash checks that the body hash embedded in the header is
// non-zero for non-Byron blocks. gouroboros era parsers validate hash
// agreement during CBOR decode, but we re-check here as defence-in-depth.
func (ls *LedgerState) validateForgedBodyHash(block ledger.Block) error {
	if block.Era().Id == byron.EraIdByron {
		return nil
	}
	bodyHash := block.Header().BlockBodyHash()
	bh := bodyHash.Bytes()
	if len(bh) == 0 {
		return fmt.Errorf(
			"block at slot %d has empty body hash bytes",
			block.SlotNumber(),
		)
	}
	allZero := true
	for _, b := range bh {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return fmt.Errorf(
			"block at slot %d has all-zero body hash",
			block.SlotNumber(),
		)
	}
	return nil
}

// validateForgedTxs validates each transaction in the block against the
// current ledger state using an intra-block UTxO overlay. The overlay
// accumulates outputs produced by earlier transactions so that later
// transactions in the same block can spend them correctly.
func (ls *LedgerState) validateForgedTxs(block ledger.Block) error {
	txs := block.Transactions()
	if len(txs) == 0 {
		return nil
	}

	// consumedUtxos: inputs spent within this block (prevents intra-block
	// double-spend); key: "<txId>:<outputIndex>".
	// createdUtxos: outputs produced within this block and not yet in the
	// persistent UTxO set; key: same format.
	consumedUtxos := make(map[string]struct{}, len(txs)*2)
	createdUtxos := make(map[string]lcommon.Utxo, len(txs)*4)

	for _, tx := range txs {
		if err := ls.ValidateTxWithOverlay(tx, consumedUtxos, createdUtxos); err != nil {
			return fmt.Errorf(
				"tx %s in forged block at slot %d: %w",
				tx.Hash(),
				block.SlotNumber(),
				err,
			)
		}

		// Advance the overlay with this transaction's effects.
		for _, utxo := range tx.Produced() {
			key := fmt.Sprintf(
				"%s:%d",
				utxo.Id.Id().String(),
				utxo.Id.Index(),
			)
			createdUtxos[key] = utxo
		}
		// Use Consumed() instead of Inputs(): for a phase-2 failed Plutus tx
		// the regular inputs are NOT spent; only the collateral inputs are.
		// Inputs() would falsely mark regular inputs as consumed and reject
		// a later tx in the same block that spends those still-valid UTxOs.
		for _, input := range tx.Consumed() {
			key := fmt.Sprintf(
				"%s:%d",
				input.Id().String(),
				input.Index(),
			)
			consumedUtxos[key] = struct{}{}
		}
	}

	return nil
}
