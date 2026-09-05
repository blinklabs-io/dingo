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

package kupo

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func TestSpendingTransactionDetailsUseCanonicalInputOrder(t *testing.T) {
	inputA1 := models.Utxo{TxId: repeatedByte(0x11), OutputIdx: 1}
	inputA2 := models.Utxo{TxId: repeatedByte(0x11), OutputIdx: 2}
	inputB0 := models.Utxo{TxId: repeatedByte(0x22), OutputIdx: 0}
	details := newSpendingTransactionDetails(&models.Transaction{
		// Metadata hydration orders these by UTxO row ID, which is not the
		// canonical spend-purpose order used by the ledger and Kupo.
		Inputs: []models.Utxo{inputB0, inputA2, inputA1},
		Redeemers: []models.Redeemer{
			{Tag: uint8(lcommon.RedeemerTagMint), Index: 1, Data: []byte{0x01}},
			{
				Tag:   uint8(lcommon.RedeemerTagSpend),
				Index: 1,
				Data:  []byte{0xd8, 0x79, 0x80},
			},
		},
	})

	if got := details.inputIndexes[utxoReference(inputA1)]; got != 0 {
		t.Fatalf("first canonical input index = %d, want 0", got)
	}
	if got := details.inputIndexes[utxoReference(inputA2)]; got != 1 {
		t.Fatalf("second canonical input index = %d, want 1", got)
	}
	if got := details.inputIndexes[utxoReference(inputB0)]; got != 2 {
		t.Fatalf("third canonical input index = %d, want 2", got)
	}
	if got := details.redeemers[1]; got != "d87980" {
		t.Fatalf("spend redeemer = %q, want d87980", got)
	}
	if len(details.redeemers) != 1 {
		t.Fatalf("spend redeemer count = %d, want 1", len(details.redeemers))
	}
}

func repeatedByte(value byte) []byte {
	ret := make([]byte, 32)
	for i := range ret {
		ret[i] = value
	}
	return ret
}
