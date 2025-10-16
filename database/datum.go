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
)

type Datum struct {
	ID        uint   `gorm:"primarykey"`
	Hash      []byte `gorm:"index;not null;unique"`
	RawDatum  []byte `gorm:"not null"`
	AddedSlot uint64 `gorm:"not null"`
}

// GetDatum returns the datum for a given Blake2b256 hash
func (d *Database) GetDatumByHash(
	hash lcommon.Blake2b256,
	txn *Txn,
) ([]byte, error) {
	tmpDatum := Datum{}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	datum, err := d.metadata.GetDatum(hash, txn.Metadata())
	if err != nil {
		return nil, err
	}
	tmpDatum = Datum(datum)
	return tmpDatum.RawDatum, nil
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
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.SetDatum(datumHash, rawDatum, addedSlot, txn.Metadata())
}
