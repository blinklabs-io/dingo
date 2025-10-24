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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Utxo represents an unspent transaction output
type Utxo struct {
	TransactionID *uint  `gorm:"index"`
	TxId          []byte `gorm:"index:tx_id_output_idx"`
	PaymentKey    []byte `gorm:"index"`
	StakingKey    []byte `gorm:"index"`
	Assets        []Asset
	Cbor          []byte `gorm:"-"` // This is here for convenience but not represented in the metadata DB
	SpentAtTxId   []byte `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DeletedSlot   uint64 `gorm:"index"`
	Amount        uint64 `gorm:"index"`
	OutputIdx     uint32 `gorm:"index:tx_id_output_idx"`
}

func (u *Utxo) TableName() string {
	return "utxo"
}

func (u *Utxo) Decode() (ledger.TransactionOutput, error) {
	return ledger.NewTransactionOutputFromCbor(u.Cbor)
}

func UtxoLedgerToModel(
	utxo ledger.Utxo,
	slot uint64,
) Utxo {
	outAddr := utxo.Output.Address()
	ret := Utxo{
		TxId:       utxo.Id.Id().Bytes(),
		PaymentKey: outAddr.PaymentKeyHash().Bytes(),
		StakingKey: outAddr.StakeKeyHash().Bytes(),
		Cbor:       utxo.Output.Cbor(),
		AddedSlot:  slot,
		Amount:     utxo.Output.Amount(),
		OutputIdx:  utxo.Id.Index(),
	}

	if multiAssetOutput, ok := utxo.Output.(interface {
		MultiAsset() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput]
	}); ok {
		if multiAsset := multiAssetOutput.MultiAsset(); multiAsset != nil {
			ret.Assets = ConvertMultiAssetToModels(multiAsset)
		}
	}

	return ret
}

// UtxoSlot allows providing a slot number with a ledger.Utxo object
type UtxoSlot struct {
	Utxo ledger.Utxo
	Slot uint64
}
