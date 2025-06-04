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

type BlockNonce struct {
	ID           uint `gorm:"primarykey"`
	Hash         []byte
	Slot         uint64
	Nonce        []byte
	IsCheckpoint bool
}

func (BlockNonce) TableName() string {
	return "block_nonce"
}

func (d *Database) SetBlockNonce(
	blockHash []byte,
	slotNumber uint64,
	nonce []byte,
	isCheckpoint bool,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.SetBlockNonce(
			blockHash,
			slotNumber,
			nonce,
			isCheckpoint,
			nil,
		)
	}
	return d.metadata.SetBlockNonce(
		blockHash,
		slotNumber,
		nonce,
		isCheckpoint,
		txn.Metadata(),
	)
}

func (d *Database) GetBlockNonce(
	blockHash []byte,
	slotNumber uint64,
	txn *Txn,
) ([]byte, error) {
	if txn == nil {
		return d.metadata.GetBlockNonce(blockHash, slotNumber, nil)
	}
	return d.metadata.GetBlockNonce(blockHash, slotNumber, txn.Metadata())
}

// DeleteBlockNoncesBeforeSlot removes all block_nonces older than the given slot number
func (d *Database) DeleteBlockNoncesBeforeSlot(
	slotNumber uint64,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.DeleteBlockNoncesBeforeSlot(slotNumber, nil)
	}
	return d.metadata.DeleteBlockNoncesBeforeSlot(slotNumber, txn.Metadata())
}

// DeleteBlockNoncesBeforeSlotWithoutCheckpoints removes non-checkpoint block_nonces older than the given slot number
func (d *Database) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
	slotNumber uint64,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(slotNumber, nil)
	}
	return d.metadata.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(slotNumber, txn.Metadata())
}
