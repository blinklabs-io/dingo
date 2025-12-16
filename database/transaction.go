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

	"github.com/blinklabs-io/dingo/database/types"
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
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}

	var realIndexMap map[lcommon.Blake2b256]uint32
	if !tx.IsValid() && tx.CollateralReturn() != nil {
		realIndexMap = make(map[lcommon.Blake2b256]uint32)
		for i, out := range tx.Outputs() {
			if out != nil && i <= int(^uint32(0)) {
				outputHash := lcommon.NewBlake2b256(out.Cbor())
				//nolint:gosec // index bounds already checked above
				realIndexMap[outputHash] = uint32(i)
			}
		}
	}

	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}

	for _, utxo := range tx.Produced() {
		outputIdx := utxo.Id.Index()
		if realIndexMap != nil {
			outputHash := lcommon.NewBlake2b256(utxo.Output.Cbor())
			if realIdx, ok := realIndexMap[outputHash]; ok {
				outputIdx = realIdx
			}
		}
		key := UtxoBlobKey(utxo.Id.Id().Bytes(), outputIdx)
		if err := blob.Set(blobTxn, key, utxo.Output.Cbor()); err != nil {
			return err
		}
	}

	if err := d.metadata.SetTransaction(tx, point, idx, certDeposits, txn.Metadata()); err != nil {
		return err
	}

	if updateEpoch > 0 && tx.IsValid() {
		for genesisHash, update := range pparamUpdates {
			if err := d.SetPParamUpdate(genesisHash.Bytes(), update.Cbor(), point.Slot, updateEpoch, txn); err != nil {
				return fmt.Errorf("set pparam update: %w", err)
			}
		}
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}
