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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	blobStore := txn.BlobStore()
	if blobStore == nil || txn.Blob() == nil {
		return nil, errors.New("retained block storage is unavailable")
	}
	blockCbor, blockMetadata, err := blobStore.GetBlock(
		txn.Blob(), indexed.Slot, indexed.BlockHash,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"read retained block at slot %d hash %x: %w",
			indexed.Slot, indexed.BlockHash, err,
		)
	}
	block, err := models.DecodeBlockCbor(blockMetadata.Type, blockCbor)
	if err != nil {
		return nil, fmt.Errorf("decode retained block: %w", err)
	}
	transactions := block.Transactions()
	if uint64(indexed.BlockIndex) >= uint64(len(transactions)) {
		return nil, fmt.Errorf(
			"block index %d is outside retained block transaction count %d",
			indexed.BlockIndex, len(transactions),
		)
	}
	transaction := transactions[indexed.BlockIndex]
	if !bytes.Equal(transaction.Hash().Bytes(), indexed.Hash) {
		return nil, fmt.Errorf(
			"retained block transaction hash at index %d does not match metadata",
			indexed.BlockIndex,
		)
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
	for _, output := range transaction.Produced() {
		model, err := models.UtxoLedgerToModel(output, indexed.Slot)
		if err != nil {
			return nil, fmt.Errorf("decode produced output address: %w", err)
		}
		add(model.PaymentKey, model.PaymentScript, model.CredentialTag, model.StakingKey)
	}
	inputs := append([]lcommon.TransactionInput{}, transaction.Inputs()...)
	inputs = append(inputs, transaction.Collateral()...)
	inputs = append(inputs, transaction.ReferenceInputs()...)
	for _, input := range inputs {
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
