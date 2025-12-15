// Copyright 2025 Blink Labs Software
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
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (d *Database) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	// Build index map for invalid transactions with collateral returns
	var realIndexMap map[lcommon.Blake2b256]uint32
	collateralReturn := tx.CollateralReturn()
	if !tx.IsValid() && collateralReturn != nil {
		realIndexMap = make(map[lcommon.Blake2b256]uint32)
		for idx, out := range tx.Outputs() {
			if out != nil && idx <= int(^uint32(0)) {
				outputHash := lcommon.NewBlake2b256(out.Cbor())
				//nolint:gosec // G115: idx bounds already checked above
				realIndexMap[outputHash] = uint32(idx)
			}
		}
	}
	// Always store Produced UTxOs (outputs or collateral return)
	for _, utxo := range tx.Produced() {
		// For invalid transactions, use real index from CBOR matching
		outputIdx := utxo.Id.Index()
		if realIndexMap != nil {
			outputHash := lcommon.NewBlake2b256(utxo.Output.Cbor())
			if realIdx, ok := realIndexMap[outputHash]; ok {
				outputIdx = realIdx
			}
		}
		// Add UTxO to blob DB
		key := UtxoBlobKey(
			utxo.Id.Id().Bytes(),
			outputIdx,
		)
		err := txn.Blob().Set(key, utxo.Output.Cbor())
		if err != nil {
			return err
		}
	}
	err := d.metadata.SetTransaction(
		tx,
		point,
		idx,
		certDeposits,
		txn.Metadata(),
	)
	if err != nil {
		return err
	}

	// Protocol parameter updates
	if updateEpoch > 0 && tx.IsValid() {
		for genesisHash, update := range pparamUpdates {
			err := d.SetPParamUpdate(
				genesisHash.Bytes(),
				update.Cbor(),
				point.Slot,
				updateEpoch,
				txn,
			)
			if err != nil {
				return fmt.Errorf("set pparam update: %w", err)
			}
		}
	}

	return nil
}
