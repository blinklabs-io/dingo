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

type Epoch struct {
	ID uint `gorm:"primarykey"`
	// NOTE: we would normally use this as the primary key, but GORM doesn't
	// like a primary key value of 0
	EpochId       uint64 `gorm:"uniqueIndex"`
	StartSlot     uint64
	Nonce         []byte
	EraId         uint
	SlotLength    uint
	LengthInSlots uint
}

func (Epoch) TableName() string {
	return "epoch"
}

func GetEpochLatest(db *Database) (Epoch, error) {
	return db.GetEpochLatest(nil)
}

func (d *Database) GetEpochLatest(txn *Txn) (Epoch, error) {
	tmpEpoch := Epoch{}
	if txn == nil {
		latestEpoch, err := d.metadata.GetEpochLatest(nil)
		if err != nil {
			return tmpEpoch, err
		}
		tmpEpoch = Epoch(latestEpoch)
	} else {
		latestEpoch, err := d.metadata.GetEpochLatest(txn.Metadata())
		if err != nil {
			return tmpEpoch, err
		}
		tmpEpoch = Epoch(latestEpoch)
	}
	return tmpEpoch, nil
}

func GetEpochsByEra(db *Database, eraId uint) ([]Epoch, error) {
	return db.GetEpochsByEra(eraId, nil)
}

func (d *Database) GetEpochsByEra(eraId uint, txn *Txn) ([]Epoch, error) {
	tmpEpochs := []Epoch{}
	if txn == nil {
		epochs, err := d.metadata.GetEpochsByEra(eraId, nil)
		if err != nil {
			return tmpEpochs, err
		}
		for _, epoch := range epochs {
			tmpEpochs = append(tmpEpochs, Epoch(epoch))
		}
	} else {
		epochs, err := txn.DB().metadata.GetEpochsByEra(eraId, txn.Metadata())
		if err != nil {
			return tmpEpochs, err
		}
		for _, epoch := range epochs {
			tmpEpochs = append(tmpEpochs, Epoch(epoch))
		}
	}
	return tmpEpochs, nil
}

func (d *Database) SetEpoch(
	slot, epoch uint64,
	nonce []byte,
	era, slotLength, lengthInSlots uint,
	txn *Txn,
) error {
	if txn == nil {
		err := d.metadata.SetEpoch(slot, epoch, nonce, era, slotLength, lengthInSlots, nil)
		if err != nil {
			return err
		}
	} else {
		err := d.metadata.SetEpoch(slot, epoch, nonce, era, slotLength, lengthInSlots, txn.Metadata())
		if err != nil {
			return err
		}
	}
	return nil
}
