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
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosEndorserBlockReferencer is implemented by a block header that references
// a Leios endorser block via its header extension (the Dijkstra
// [eb_hash, eb_size] pair).
type leiosEndorserBlockReferencer interface {
	LeiosEndorserBlockRef() (lcommon.Blake2b256, uint64, bool)
}

// applyEndorserBlock decodes a Leios endorser block's standalone transactions
// and applies them to the ledger ahead of the ranking block that references it,
// so the endorser-resident outputs the ranking block's transactions spend are
// present in the UTxO set.
//
// Endorser-block transactions are not part of any chain block, so — mirroring
// the genesis path (buildGenesisBlockCbor / SetGenesisCbor) — their CBOR is
// persisted as a standalone blob keyed by the endorser block's (slot, hash) and
// referenced by DOFF offsets, after which resolution works through the normal
// TieredCborCache cold-extract path. Crucially, the transactions' ledger
// effects (metadata rows, spent inputs, produced UTxOs) are recorded under the
// RANKING block's point (rbPoint), not the endorser block's: a rollback of the
// ranking block must remove them, and the ranking block is what admits the
// endorser block to the chain.
//
// It returns the number of transactions applied. Decode/build failures happen
// before storage is mutated and callers may treat them as best-effort. Once
// the endorser blob or transaction rows start writing, any error is wrapped in
// leiosEndorserBlockStorageError so callers can abort the outer transaction
// instead of committing a partial endorser-block application.
func (ls *LedgerState) applyEndorserBlock(
	txn *database.Txn,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHashBytes []byte,
	rawTxs []cbor.RawMessage,
) (int, error) {
	if len(rawTxs) == 0 {
		return 0, nil
	}
	if len(ebHashBytes) != lcommon.Blake2b256Size {
		return 0, fmt.Errorf(
			"endorser block hash must be %d bytes, got %d",
			lcommon.Blake2b256Size,
			len(ebHashBytes),
		)
	}
	var ebHash [lcommon.Blake2b256Size]byte
	copy(ebHash[:], ebHashBytes)

	// Decode each standalone endorser transaction, capturing its body CBOR
	// (the first array element) for the transaction-offset entry.
	txs := make([]lcommon.Transaction, 0, len(rawTxs))
	bodyCbors := make([][]byte, 0, len(rawTxs))
	for i, raw := range rawTxs {
		// leios-fetch carries each endorser transaction CBOR-in-CBOR: the
		// tx_list entry is a CBOR byte string wrapping the transaction's own
		// CBOR (LeiosTx = encodeBytes(txCbor)). Unwrap it to the inner
		// transaction bytes before decoding. (A non-byte-string entry — major
		// type != 2 — is already the bare transaction.)
		txCbor := []byte(raw)
		if len(txCbor) > 0 && txCbor[0]>>5 == 2 {
			var inner []byte
			if _, err := cbor.Decode(txCbor, &inner); err != nil {
				return 0, fmt.Errorf("unwrap endorser tx %d: %w", i, err)
			}
			txCbor = inner
		}
		var elems []cbor.RawMessage
		if _, err := cbor.Decode(txCbor, &elems); err != nil {
			return 0, fmt.Errorf("decode endorser tx %d envelope: %w", i, err)
		}
		if len(elems) < 2 {
			return 0, fmt.Errorf(
				"endorser tx %d has %d elements, want >= 2",
				i,
				len(elems),
			)
		}
		txType, err := ledger.DetermineTransactionType(txCbor)
		if err != nil {
			return 0, fmt.Errorf("determine endorser tx %d type: %w", i, err)
		}
		tx, err := ledger.NewTransactionFromCbor(txType, txCbor)
		if err != nil {
			return 0, fmt.Errorf("decode endorser tx %d: %w", i, err)
		}
		txs = append(txs, tx)
		bodyCbors = append(bodyCbors, []byte(elems[0]))
	}

	// Build the endorser-block blob and its offsets, then persist the blob
	// under (ebSlot, ebHash) so cold-extract can resolve the DOFF refs.
	blob, offsets, err := buildEndorserBlockBlob(txs, bodyCbors, ebSlot, ebHash)
	if err != nil {
		return 0, fmt.Errorf("build endorser block blob: %w", err)
	}
	if err := ls.db.SetGenesisCbor(ebSlot, ebHash[:], blob, txn); err != nil {
		return 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("store endorser block blob: %w", err),
		}
	}

	// Apply the endorser transactions as a delta recorded under the ranking
	// block's point (so a rollback removes them), with offsets pointing into
	// the endorser-block blob.
	delta := NewLedgerDelta(rbPoint, uint(dijkstra.EraIdDijkstra), rbBlockNumber)
	defer delta.Release()
	delta.Offsets = offsets
	for i, tx := range txs {
		delta.addTransaction(tx, i)
	}
	if err := delta.apply(ls, txn); err != nil {
		return 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("apply endorser block transactions: %w", err),
		}
	}
	return len(txs), nil
}

type leiosEndorserBlockStorageError struct {
	err error
}

func (e *leiosEndorserBlockStorageError) Error() string {
	return e.err.Error()
}

func (e *leiosEndorserBlockStorageError) Unwrap() error {
	return e.err
}

// buildEndorserBlockBlob lays out a standalone CBOR blob holding, for each
// endorser transaction, its body CBOR followed by each produced output's CBOR,
// recording the byte ranges as DOFF offsets keyed by (ebSlot, ebHash). The blob
// is not a chain block — cold-extract only slices it by offset/length — so a
// flat concatenation with precise offsets is sufficient.
func buildEndorserBlockBlob(
	txs []lcommon.Transaction,
	bodyCbors [][]byte,
	ebSlot uint64,
	ebHash [lcommon.Blake2b256Size]byte,
) ([]byte, *database.BlockIngestionResult, error) {
	var buf bytes.Buffer
	result := &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset, len(txs)),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	writeRange := func(b []byte) (uint32, uint32, error) {
		off := buf.Len()
		if off > math.MaxUint32 || len(b) > math.MaxUint32 {
			return 0, 0, errors.New("endorser block blob offset out of uint32 range")
		}
		buf.Write(b)
		//nolint:gosec // bounds checked above
		return uint32(off), uint32(len(b)), nil
	}
	for i, tx := range txs {
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		off, length, err := writeRange(bodyCbors[i])
		if err != nil {
			return nil, nil, err
		}
		result.TxOffsets[txHash] = database.CborOffset{
			BlockSlot:  ebSlot,
			BlockHash:  ebHash,
			ByteOffset: off,
			ByteLength: length,
		}
		for _, utxo := range tx.Produced() {
			outCbor := utxo.Output.Cbor()
			if len(outCbor) == 0 {
				enc, err := cbor.Encode(utxo.Output)
				if err != nil {
					return nil, nil, fmt.Errorf("encode endorser output: %w", err)
				}
				outCbor = enc
			}
			off, length, err := writeRange(outCbor)
			if err != nil {
				return nil, nil, err
			}
			result.UtxoOffsets[database.UtxoRef{
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}] = database.CborOffset{
				BlockSlot:  ebSlot,
				BlockHash:  ebHash,
				ByteOffset: off,
				ByteLength: length,
			}
		}
	}
	return buf.Bytes(), result, nil
}
