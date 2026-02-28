// Copyright 2026 Blink Labs Software
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

// AddressTransaction maps an address (payment and/or staking key) to a
// transaction that references it as an input/output participant.
type AddressTransaction struct {
	ID            uint   `gorm:"primaryKey"`
	PaymentKey    []byte `gorm:"index:idx_addr_tx_payment;size:28"`
	StakingKey    []byte `gorm:"index:idx_addr_tx_staking;size:28"`
	TransactionID uint   `gorm:"index"`
	Slot          uint64 `gorm:"index"`
	TxIndex       uint32
}

func (AddressTransaction) TableName() string {
	return "address_transaction"
}
