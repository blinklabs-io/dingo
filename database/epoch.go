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

	"github.com/blinklabs-io/dingo/database/models"
)

func (d *Database) GetEpoch(
	epochId uint64,
	txn *Txn,
) (*models.Epoch, error) {
	if txn == nil {
		return d.epochStore().GetEpoch(epochId, nil)
	}
	return txn.db.epochStore().GetEpoch(epochId, txn.Metadata())
}

func (d *Database) GetEpochsByEra(
	eraId uint,
	txn *Txn,
) ([]models.Epoch, error) {
	if txn == nil {
		return d.epochStore().GetEpochsByEra(eraId, nil)
	}
	return txn.db.epochStore().GetEpochsByEra(eraId, txn.Metadata())
}

// EpochBySlot returns the persisted epoch containing slot: the one with
// the greatest StartSlot <= slot. Unlike ledger.LedgerState.SlotToEpoch
// (which additionally consults the live hard-fork/era-transition summary
// so it can also reason about slots in the future), this only walks the
// persisted epoch table, with no dependency on genesis config or a live
// LedgerState -- sufficient for every caller here, since a truncate or
// rollback target is always at or before the already-committed tip, so
// the epoch containing it has always already been persisted.
func EpochBySlot(d *Database, slot uint64, txn *Txn) (models.Epoch, error) {
	epoch, err := d.GetEpochBySlot(slot, txn)
	if err != nil {
		return models.Epoch{}, fmt.Errorf("get epoch by slot: %w", err)
	}
	if epoch == nil {
		return models.Epoch{}, fmt.Errorf(
			"slot %d is outside the known epoch range", slot,
		)
	}
	return *epoch, nil
}

func (d *Database) GetEpochs(txn *Txn) ([]models.Epoch, error) {
	if txn == nil {
		return d.epochStore().GetEpochs(nil)
	}
	return txn.db.epochStore().GetEpochs(txn.Metadata())
}

func (d *Database) GetEpochBySlot(
	slot uint64,
	txn *Txn,
) (*models.Epoch, error) {
	if txn == nil {
		return d.epochStore().GetEpochBySlot(slot, nil)
	}
	return txn.db.epochStore().GetEpochBySlot(slot, txn.Metadata())
}

func (d *Database) DeleteEpochsAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		return d.epochStore().DeleteEpochsAfterSlot(slot, nil)
	}
	return txn.db.epochStore().DeleteEpochsAfterSlot(
		slot,
		txn.Metadata(),
	)
}

func (d *Database) SetEpoch(
	slot, epoch uint64,
	nonce, evolvingNonce, candidateNonce, lastEpochBlockNonce []byte,
	era, slotLength, lengthInSlots uint,
	txn *Txn,
) error {
	if txn == nil {
		return d.epochStore().SetEpoch(
			slot,
			epoch,
			nonce,
			evolvingNonce,
			candidateNonce,
			lastEpochBlockNonce,
			era,
			slotLength,
			lengthInSlots,
			nil,
		)
	}
	return d.epochStore().SetEpoch(
		slot,
		epoch,
		nonce,
		evolvingNonce,
		candidateNonce,
		lastEpochBlockNonce,
		era,
		slotLength,
		lengthInSlots,
		txn.Metadata(),
	)
}
