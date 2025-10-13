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

package models

import (
	"github.com/blinklabs-io/gouroboros/ledger"
)

type Utxo struct {
	ID            uint   `gorm:"primarykey"`
	TxId          []byte `gorm:"index:tx_id_output_idx"`
	OutputIdx     uint32 `gorm:"index:tx_id_output_idx"`
	AddedSlot     uint64 `gorm:"index"`
	DeletedSlot   uint64 `gorm:"index"`
	PaymentKey    []byte `gorm:"index"`
	StakingKey    []byte `gorm:"index"`
	Amount        uint64 `gorm:"index"`
	TransactionID *uint  `gorm:"index"`
	Assets        []Asset
	Cbor          []byte `gorm:"-"` // This is here for convenience but not represented in the metadata DB
}

func (u *Utxo) TableName() string {
	return "utxo"
}

func (u *Utxo) Decode() (ledger.TransactionOutput, error) {
	return ledger.NewTransactionOutputFromCbor(u.Cbor)
}
