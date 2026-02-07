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

package database

import (
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// safeIntToUint32 converts an int to uint32, clamping to math.MaxUint32 on overflow.
// This is safe because we use this for byte offsets/lengths within blocks that
// are bounded by Cardano's protocol limits (~90KB max block body).
func safeIntToUint32(n int) uint32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n) // #nosec G115: bounds checked above
}

// BlockIngestionResult contains pre-computed offsets for all items in a block.
// This is computed during block ingestion and used to store offset references
// instead of duplicating CBOR data.
type BlockIngestionResult struct {
	// TxOffsets maps transaction hash to its CborOffset within the block.
	// This stores only the transaction body offset for backward compatibility.
	TxOffsets map[[32]byte]CborOffset

	// TxParts maps transaction hash to all 4 component offsets within the block.
	// This enables byte-perfect reconstruction of complete standalone transaction
	// CBOR from the source block by extracting and reassembling:
	// body, witness, is_valid, and metadata (optional).
	TxParts map[[32]byte]TxCborParts

	// UtxoOffsets maps UTxO reference to its CborOffset within the block
	UtxoOffsets map[UtxoRef]CborOffset

	// DatumOffsets maps datum hash to its CborOffset within the block
	DatumOffsets map[[32]byte]CborOffset

	// RedeemerOffsets maps redeemer key to its CborOffset within the block
	RedeemerOffsets map[common.RedeemerKey]CborOffset

	// ScriptOffsets maps script hash to its CborOffset within the block
	ScriptOffsets map[[32]byte]CborOffset
}

// BlockIndexer computes byte offsets for all items within a block.
// It uses gouroboros's ExtractTransactionOffsets for efficient offset extraction.
type BlockIndexer struct {
	blockSlot uint64
	blockHash [32]byte
}

// NewBlockIndexer creates a new BlockIndexer for the given block.
func NewBlockIndexer(slot uint64, hash []byte) *BlockIndexer {
	bi := &BlockIndexer{
		blockSlot: slot,
	}
	copy(bi.blockHash[:], hash)
	return bi
}

// ComputeOffsets extracts byte offsets for all transactions, UTxOs, datums,
// redeemers, and scripts within the block CBOR.
func (bi *BlockIndexer) ComputeOffsets(
	blockCbor []byte,
	block ledger.Block,
) (*BlockIngestionResult, error) {
	if block == nil {
		return nil, errors.New("block cannot be nil")
	}

	result := &BlockIngestionResult{
		TxOffsets:       make(map[[32]byte]CborOffset),
		TxParts:         make(map[[32]byte]TxCborParts),
		UtxoOffsets:     make(map[UtxoRef]CborOffset),
		DatumOffsets:    make(map[[32]byte]CborOffset),
		RedeemerOffsets: make(map[common.RedeemerKey]CborOffset),
		ScriptOffsets:   make(map[[32]byte]CborOffset),
	}

	// Extract transaction-level offsets from block CBOR
	txOffsets, err := common.ExtractTransactionOffsets(blockCbor)
	if err != nil {
		return nil, fmt.Errorf("extract transaction offsets: %w", err)
	}

	// Get transactions from the parsed block
	txs := block.Transactions()
	if len(txs) == 0 {
		return result, nil
	}

	// Verify we have offsets for all transactions
	if len(txOffsets.Transactions) < len(txs) {
		return nil, fmt.Errorf(
			"transaction count mismatch: block has %d transactions but only %d offsets extracted",
			len(txs), len(txOffsets.Transactions),
		)
	}

	// Process each transaction
	for txIdx, tx := range txs {
		txLoc := txOffsets.Transactions[txIdx]
		txHash := tx.Hash()
		var txHashArray [32]byte
		copy(txHashArray[:], txHash.Bytes())

		// Store transaction body offset for backward compatibility.
		result.TxOffsets[txHashArray] = CborOffset{
			BlockSlot:  bi.blockSlot,
			BlockHash:  bi.blockHash,
			ByteOffset: txLoc.Body.Offset,
			ByteLength: txLoc.Body.Length,
		}

		// Store all 4 transaction components for complete CBOR reconstruction.
		//
		// Cardano blocks store transaction components in separate arrays:
		//   [header, [bodies...], [witnesses...], {aux_data_map}, [invalid_txs]]
		//
		// TxParts stores offsets for each component so they can be extracted
		// from the block and reassembled into a complete standalone transaction
		// CBOR: [body, witness, is_valid, aux_data]
		result.TxParts[txHashArray] = TxCborParts{
			BlockSlot:      bi.blockSlot,
			BlockHash:      bi.blockHash,
			BodyOffset:     txLoc.Body.Offset,
			BodyLength:     txLoc.Body.Length,
			WitnessOffset:  txLoc.Witness.Offset,
			WitnessLength:  txLoc.Witness.Length,
			MetadataOffset: txLoc.Metadata.Offset,
			MetadataLength: txLoc.Metadata.Length,
			IsValid:        tx.IsValid(),
		}

		// Extract UTxO output offsets from transaction body
		if err := bi.extractOutputOffsets(blockCbor, txLoc, tx, result); err != nil {
			return nil, fmt.Errorf("extract output offsets for tx %s: %w", txHash.String(), err)
		}

		// Store datum offsets from witness set
		for datumHash, loc := range txLoc.Datums {
			var hashArray [32]byte
			copy(hashArray[:], datumHash[:])
			result.DatumOffsets[hashArray] = CborOffset{
				BlockSlot:  bi.blockSlot,
				BlockHash:  bi.blockHash,
				ByteOffset: loc.Offset,
				ByteLength: loc.Length,
			}
		}

		// Store redeemer offsets from witness set
		for redeemerKey, loc := range txLoc.Redeemers {
			result.RedeemerOffsets[redeemerKey] = CborOffset{
				BlockSlot:  bi.blockSlot,
				BlockHash:  bi.blockHash,
				ByteOffset: loc.Offset,
				ByteLength: loc.Length,
			}
		}

		// Store script offsets from witness set
		for scriptHash, loc := range txLoc.Scripts {
			var hashArray [32]byte
			copy(hashArray[:], scriptHash[:])
			result.ScriptOffsets[hashArray] = CborOffset{
				BlockSlot:  bi.blockSlot,
				BlockHash:  bi.blockHash,
				ByteOffset: loc.Offset,
				ByteLength: loc.Length,
			}
		}
	}

	return result, nil
}

// extractOutputOffsets extracts byte offsets for each transaction output.
// Uses pre-computed output offsets from gouroboros when available,
// with fallback to byte searching for compatibility.
// Returns an error if offsets cannot be computed for any output.
func (bi *BlockIndexer) extractOutputOffsets(
	blockCbor []byte,
	txLoc common.TransactionLocation,
	tx common.Transaction,
	result *BlockIngestionResult,
) error {
	txHash := tx.Hash()
	var txHashArray [32]byte
	copy(txHashArray[:], txHash.Bytes())

	// Get produced UTxOs (includes both regular outputs and collateral return)
	produced := tx.Produced()
	if len(produced) == 0 {
		return nil
	}

	// Use pre-computed output offsets from gouroboros if available
	// This is much faster than byte searching
	if len(txLoc.Outputs) > 0 {
		allOffsetsFound := true
		for _, utxo := range produced {
			outputIdx := int(utxo.Id.Index())

			// For valid transactions, output index maps directly to Outputs array
			// For invalid transactions with collateral return, the index is len(Outputs)
			// which means we need to handle this case specially
			if outputIdx < len(txLoc.Outputs) {
				loc := txLoc.Outputs[outputIdx]
				ref := UtxoRef{
					TxId:      txHashArray,
					OutputIdx: utxo.Id.Index(),
				}
				result.UtxoOffsets[ref] = CborOffset{
					BlockSlot:  bi.blockSlot,
					BlockHash:  bi.blockHash,
					ByteOffset: loc.Offset,
					ByteLength: loc.Length,
				}
			} else {
				// Output index not in pre-computed offsets
				allOffsetsFound = false
			}
		}
		// Only return early if ALL outputs had pre-computed offsets
		if allOffsetsFound {
			return nil
		}
		// Fall through to byte searching for missing outputs
	}

	// Fallback: search for output CBOR within the body (slower but always works)
	// Returns error if offset cannot be computed for any output.
	// We need to track how many times we've seen each unique CBOR pattern
	// to handle duplicate outputs (which are valid in Cardano).
	seenCborCounts := make(map[string]int)

	for _, utxo := range produced {
		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: utxo.Id.Index(),
		}

		outputCbor := utxo.Output.Cbor()

		// Skip if we already have an offset for this output, but still
		// increment seenCborCounts so subsequent duplicate outputs resolve
		// to the correct occurrence
		if _, ok := result.UtxoOffsets[ref]; ok {
			if len(outputCbor) > 0 {
				seenCborCounts[string(outputCbor)]++
			}
			continue
		}
		if len(outputCbor) == 0 {
			return fmt.Errorf(
				"output %d has no CBOR data - cannot compute offset",
				utxo.Id.Index(),
			)
		}

		bodyStart := txLoc.Body.Offset
		bodyEnd := txLoc.Body.Offset + txLoc.Body.Length
		if bodyEnd > safeIntToUint32(len(blockCbor)) {
			return fmt.Errorf(
				"output %d: body end (%d) exceeds block size (%d)",
				utxo.Id.Index(),
				bodyEnd,
				len(blockCbor),
			)
		}

		bodyBytes := blockCbor[bodyStart:bodyEnd]

		// Track which occurrence of this CBOR pattern we need
		cborKey := string(outputCbor)
		targetOccurrence := seenCborCounts[cborKey]
		seenCborCounts[cborKey]++

		// Find the nth occurrence of this CBOR pattern
		offset := findNthCborOccurrence(bodyBytes, outputCbor, targetOccurrence)
		if offset < 0 {
			return fmt.Errorf(
				"output %d: CBOR occurrence %d not found in transaction body",
				utxo.Id.Index(),
				targetOccurrence,
			)
		}

		result.UtxoOffsets[ref] = CborOffset{
			BlockSlot:  bi.blockSlot,
			BlockHash:  bi.blockHash,
			ByteOffset: bodyStart + safeIntToUint32(offset),
			ByteLength: safeIntToUint32(len(outputCbor)),
		}
	}

	return nil
}

// findNthCborOccurrence finds the nth (0-indexed) occurrence of a CBOR byte
// sequence within a larger byte slice. Returns the offset if found, -1 otherwise.
// This handles transactions with duplicate outputs (identical CBOR).
func findNthCborOccurrence(data, target []byte, n int) int {
	if len(target) == 0 || len(data) < len(target) || n < 0 {
		return -1
	}

	count := 0
	for i := 0; i <= len(data)-len(target); i++ {
		match := true
		for j := 0; j < len(target); j++ {
			if data[i+j] != target[j] {
				match = false
				break
			}
		}
		if match {
			if count == n {
				return i
			}
			count++
			// Skip past this match to find non-overlapping occurrences
			i += len(target) - 1
		}
	}
	return -1
}
