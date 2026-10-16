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
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

type rebuildAddressKey struct {
	payment       string
	paymentScript bool
	tag           uint8
	staking       string
}

func (d *Database) rebuildAddressTransaction(
	ctx context.Context,
	indexed models.Transaction,
) ([]models.AddressTransaction, error) {
	txn := d.Transaction(ctx, false)
	defer txn.Release()
	level, err := d.rebuildTransactionLevel(txn, indexed)
	if err != nil {
		return nil, err
	}
	addresses := make(map[rebuildAddressKey]struct{})
	add := func(payment []byte, script bool, tag uint8, staking []byte) {
		if len(payment) == 0 && len(staking) == 0 {
			return
		}
		addresses[rebuildAddressKey{
			payment:       string(payment),
			paymentScript: script,
			tag:           tag,
			staking:       string(staking),
		}] = struct{}{}
	}
	for _, output := range level.produced {
		model, err := models.UtxoLedgerToModel(output, indexed.Slot)
		if err != nil {
			return nil, fmt.Errorf("decode produced output address: %w", err)
		}
		add(model.PaymentKey, model.PaymentScript, model.CredentialTag, model.StakingKey)
	}
	for _, input := range level.inputs {
		utxo, err := d.utxoStore().GetUtxoIncludingSpent(
			input.Id().Bytes(), input.Index(), txn.Metadata(),
		)
		if err != nil {
			return nil, fmt.Errorf("read source output for input %s: %w", input, err)
		}
		if utxo == nil {
			return nil, fmt.Errorf("source output for input %s is missing", input)
		}
		add(utxo.PaymentKey, utxo.PaymentScript, utxo.CredentialTag, utxo.StakingKey)
	}
	ret := make([]models.AddressTransaction, 0, len(addresses))
	for key := range addresses {
		ret = append(ret, models.AddressTransaction{
			PaymentKey:    []byte(key.payment),
			PaymentScript: key.paymentScript,
			StakingKey:    []byte(key.staking),
			CredentialTag: key.tag,
		})
	}
	return ret, nil
}

// rebuildLevel is one stored transaction row's address-bearing effects: the
// inputs it names (regular, collateral and reference, even when it failed
// phase 2) and the outputs it produced.
type rebuildLevel struct {
	hash     []byte
	inputs   []lcommon.TransactionInput
	produced []lcommon.Utxo
}

// rebuildTransactionLevel locates a stored transaction row by its hash. A row
// is one ledger level, not one block transaction: a valid Dijkstra
// transaction stores each sub-transaction before itself, so a row's
// block_index counts levels and does not index Block.Transactions. A
// certified Leios closure transaction is stored under its ranking block's
// point while its CBOR lives in the endorser block's standalone blob, so its
// stored offset names a different block and only its body can be read back.
func (d *Database) rebuildTransactionLevel(
	txn *Txn,
	indexed models.Transaction,
) (rebuildLevel, error) {
	blobStore := txn.BlobStore()
	if blobStore == nil || txn.Blob() == nil {
		return rebuildLevel{}, errors.New("retained block storage is unavailable")
	}
	txData, err := blobStore.GetTx(txn.Blob(), indexed.Hash)
	if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		return rebuildLevel{}, fmt.Errorf("read transaction offset: %w", err)
	}
	if err == nil && IsTxOffsetStorage(txData) {
		offset, err := DecodeTxOffset(txData)
		if err != nil {
			return rebuildLevel{}, fmt.Errorf("decode transaction offset: %w", err)
		}
		if offset.BlockSlot != indexed.Slot ||
			!bytes.Equal(offset.BlockHash[:], indexed.BlockHash) {
			return d.rebuildStandaloneLevel(txn, indexed)
		}
	}
	blockCbor, blockMetadata, err := blobStore.GetBlock(
		txn.Blob(), indexed.Slot, indexed.BlockHash,
	)
	if err != nil {
		return rebuildLevel{}, fmt.Errorf(
			"read retained block at slot %d hash %x: %w",
			indexed.Slot, indexed.BlockHash, err,
		)
	}
	block, err := models.DecodeBlockCbor(blockMetadata.Type, blockCbor)
	if err != nil {
		return rebuildLevel{}, fmt.Errorf("decode retained block: %w", err)
	}
	for _, transaction := range block.Transactions() {
		for _, level := range rebuildLevels(transaction) {
			if bytes.Equal(level.hash, indexed.Hash) {
				return level, nil
			}
		}
	}
	return rebuildLevel{}, fmt.Errorf(
		"transaction %x is not in retained block at slot %d",
		indexed.Hash, indexed.Slot,
	)
}

// rebuildLevels expands a block transaction into the rows the ledger stores
// for it, as ledger.TransactionLevelsForApply does (this package cannot
// import it): a valid Dijkstra transaction's sub-transactions in encoded
// order, then the transaction itself. An invalid transaction applies only
// its collateral, so its sub-transactions store no row.
func rebuildLevels(transaction lcommon.Transaction) []rebuildLevel {
	var ret []rebuildLevel
	if transaction.IsValid() {
		for _, body := range lcommon.SubTransactionBodiesFromTransaction(transaction) {
			ret = append(ret, rebuildBodyLevel(body, true))
		}
	}
	level := rebuildBodyLevel(transaction, transaction.IsValid())
	if !transaction.IsValid() {
		level.produced = transaction.Produced()
	}
	return append(ret, level)
}

// rebuildBodyLevel reads one level from its body. A valid level produces its
// outputs; an invalid one produces only its collateral return, which takes
// the index after the last output.
func rebuildBodyLevel(body lcommon.TransactionBody, valid bool) rebuildLevel {
	id := body.Id()
	level := rebuildLevel{hash: id.Bytes()}
	level.inputs = append(level.inputs, body.Inputs()...)
	level.inputs = append(level.inputs, body.Collateral()...)
	level.inputs = append(level.inputs, body.ReferenceInputs()...)
	outputs := body.Outputs()
	if !valid {
		if collateralReturn := body.CollateralReturn(); collateralReturn != nil {
			level.produced = []lcommon.Utxo{{
				Id: shelley.ShelleyTransactionInput{
					TxId:        id,
					OutputIndex: uint32(len(outputs)), //nolint:gosec // bounded by the ledger's 16-bit output index
				},
				Output: collateralReturn,
			}}
		}
		return level
	}
	for index, output := range outputs {
		level.produced = append(level.produced, lcommon.Utxo{
			Id: shelley.ShelleyTransactionInput{
				TxId:        id,
				OutputIndex: uint32(index), //nolint:gosec // bounded by the ledger's 16-bit output index
			},
			Output: output,
		})
	}
	return level
}

// rebuildStandaloneLevel reads a closure transaction row from its body, the
// only part the endorser block's standalone blob keeps. Endorser blocks are
// Dijkstra-era; a sub-transaction row's body has its own shape, tried when the
// enclosing transaction's does not decode.
func (d *Database) rebuildStandaloneLevel(
	txn *Txn,
	indexed models.Transaction,
) (rebuildLevel, error) {
	bodyCbor, err := d.CborCache().ResolveTxCbor(txn, indexed.Hash)
	if err != nil {
		return rebuildLevel{}, fmt.Errorf(
			"read standalone transaction %x body: %w", indexed.Hash, err,
		)
	}
	var body lcommon.TransactionBody
	if txBody, err := dijkstra.NewDijkstraTransactionBodyFromCbor(bodyCbor); err == nil {
		body = txBody
	} else {
		var subBody dijkstra.DijkstraSubTransactionBody
		if _, subErr := cbor.DecodeExact(bodyCbor, &subBody); subErr != nil {
			return rebuildLevel{}, fmt.Errorf(
				"decode standalone transaction %x body: %w",
				indexed.Hash, errors.Join(err, subErr),
			)
		}
		body = &subBody
	}
	level := rebuildBodyLevel(body, indexed.Valid)
	if !bytes.Equal(level.hash, indexed.Hash) {
		return rebuildLevel{}, fmt.Errorf(
			"standalone transaction body hashes to %x, not %x",
			level.hash, indexed.Hash,
		)
	}
	return level, nil
}
