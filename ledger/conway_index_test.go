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

package ledger

import (
	"encoding/hex"
	"math/big"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// TestConwayInvalidTxProducedIndexes verifies that the specific Conway transaction
// (842a18e8b3...) is invalid and produces only the collateral return at index 1,
// using a fixed CBOR hex payload
func TestConwayInvalidTxProducedIndexes(t *testing.T) {
	// preview tx: 842a18e8b3280cf769f7a50551525b60820ea74ac8d3223e78939bd36e8185cc
	const txHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

	txBytes, err := hex.DecodeString(txHex)
	if err != nil {
		t.Fatalf("decode tx hex: %v", err)
	}

	// Build transaction from CBOR
	tx, err := gledger.NewTransactionFromCbor(uint(conway.EraIdConway), txBytes)
	if err != nil {
		t.Fatalf("NewTransactionFromCbor: %v", err)
	}

	// Check produced collateral return only
	prod := tx.Produced()
	if len(prod) != 1 {
		t.Fatalf(
			"expected 1 produced UTxO (collateral return), got %d",
			len(prod),
		)
	}
	if got := prod[0].Id.Index(); got != 1 {
		t.Fatalf("expected collateral return index 1, got %d", got)
	}
	if amt := prod[0].Output.Amount(); amt.Cmp(big.NewInt(7000000)) != 0 {
		t.Fatalf("expected collateral return amount 7000000, got %s", amt)
	}

	// Outputs() should list the regular outputs (not part of Produced for invalid tx)
	outs := tx.Outputs()
	if len(outs) != 1 {
		t.Fatalf("expected 1 regular output, got %d", len(outs))
	}
	if amt := outs[0].Amount(); amt.Sign() <= 0 {
		t.Fatalf("expected positive regular output amount, got %s", amt)
	}
}
