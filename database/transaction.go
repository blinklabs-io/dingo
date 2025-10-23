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

func (d *Database) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	for u, utxo := range tx.Produced() {
		// Add UTxO to blob DB
		key := UtxoBlobKey(
			utxo.Id.Id().Bytes(),
			uint32(u), //nolint:gosec
		)
		err := txn.Blob().Set(key, utxo.Output.Cbor())
		if err != nil {
			return err
		}
	}
	return d.metadata.SetTransaction(tx, point, idx, txn.Metadata())
}
