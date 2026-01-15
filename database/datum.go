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
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var ErrDatumNotFound = errors.New("datum not found")

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
	}
	return d.metadata.SetDatum(datumHash, rawDatum, addedSlot, txn.Metadata())
}

// GetDatum retrieves a datum by its hash.
func (d *Database) GetDatum(
	hash []byte,
	txn *Txn,
) (*models.Datum, error) {
	if len(hash) == 0 {
		return nil, ErrDatumNotFound
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	tmpHash := lcommon.NewBlake2b256(hash)
	ret, err := d.metadata.GetDatum(tmpHash, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, ErrDatumNotFound
	}
	return ret, nil
}
