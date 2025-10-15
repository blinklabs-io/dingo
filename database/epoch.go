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
	"github.com/blinklabs-io/dingo/database/models"
)

func (d *Database) GetEpochLatest(txn *Txn) (*models.Epoch, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.GetEpochLatest(txn.Metadata())
}

func (d *Database) GetEpochsByEra(
	eraId uint,
	txn *Txn,
) ([]models.Epoch, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.GetEpochsByEra(eraId, txn.Metadata())
}

func (d *Database) GetEpochs(txn *Txn) ([]models.Epoch, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.GetEpochs(txn.Metadata())
}

func (d *Database) SetEpoch(
	slot, epoch uint64,
	nonce []byte,
	era, slotLength, lengthInSlots uint,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.SetEpoch(
		slot,
		epoch,
		nonce,
		era,
		slotLength,
		lengthInSlots,
		txn.Metadata(),
	)
}
