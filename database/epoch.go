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

func (d *Database) GetEpoch(
	epochId uint64,
	txn *Txn,
) (*models.Epoch, error) {
	if txn == nil {
		return d.metadata.GetEpoch(epochId, nil)
	}
	return txn.db.metadata.GetEpoch(epochId, txn.Metadata())
}

func (d *Database) GetEpochsByEra(
	eraId uint,
	txn *Txn,
) ([]models.Epoch, error) {
	if txn == nil {
		return d.metadata.GetEpochsByEra(eraId, nil)
	}
	return txn.db.metadata.GetEpochsByEra(eraId, txn.Metadata())
}

func (d *Database) GetEpochs(txn *Txn) ([]models.Epoch, error) {
	if txn == nil {
		return d.metadata.GetEpochs(nil)
	}
	return txn.db.metadata.GetEpochs(txn.Metadata())
}

func (d *Database) SetEpoch(
	slot, epoch uint64,
	nonce []byte,
	era, slotLength, lengthInSlots uint,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.SetEpoch(
			slot,
			epoch,
			nonce,
			era,
			slotLength,
			lengthInSlots,
			nil,
		)
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
