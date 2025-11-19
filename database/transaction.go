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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// SetTransaction persists transaction to blob storage (UTXOs) and metadata storage (certificates).
func (d *Database) SetTransaction(
	point ocommon.Point,
	tx lcommon.Transaction,
	idx uint32,
	deposits map[int]uint64,
	txn *Txn,
) (err error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer func() {
			if err != nil {
				txn.Rollback() //nolint:errcheck
			} else {
				txn.Commit() //nolint:errcheck
			}
		}()
	}
	for _, utxo := range tx.Produced() {
		// Add UTxO to blob DB
		key := UtxoBlobKey(
			utxo.Id.Id().Bytes(),
			utxo.Id.Index(),
		)
		if err = txn.Blob().Set(key, utxo.Output.Cbor()); err != nil {
			return err
		}
	}

	err = d.metadata.SetTransaction(
		point,
		tx,
		idx,
		deposits,
		txn.Metadata(),
	)
	if err != nil {
		return err
	}

	return nil
}
