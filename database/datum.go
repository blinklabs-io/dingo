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
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GetDatum returns the datum for a given Blake2b256 hash
func (d *Database) GetDatumByHash(
	hash lcommon.Blake2b256,
	txn *Txn,
) ([]byte, error) {
	var datum models.Datum
	var err error

	if txn == nil {
		datum, err = d.metadata.GetDatum(hash, nil)
	} else {
		datum, err = d.metadata.GetDatum(hash, txn.Metadata())
	}
	if err != nil {
		return nil, err
	}
	return datum.RawDatum, nil
}

// SetDatum saves the raw datum into the database by computing the hash before inserting.
func (d *Database) SetDatum(
	rawDatum []byte,
	addedSlot uint64,
	txn *Txn,
) error {
	// Compute Blake2b-256 hash
	datumHash := lcommon.Blake2b256Hash(rawDatum)

	if txn == nil {
		return d.metadata.SetDatum(datumHash, rawDatum, addedSlot, nil)
	} else {
		return d.metadata.SetDatum(datumHash, rawDatum, addedSlot, txn.Metadata())
	}
}
