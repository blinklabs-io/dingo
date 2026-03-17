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

package txpump

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// conwayEraID is the Conway era ID (6) used in the LocalTxSubmission
// MsgSubmitTx message. Babbage is era 5; Conway is era 6.
const conwayEraID uint16 = 6

// MinFee is a fixed minimum fee applied to every transaction.  In a real node
// the fee is computed precisely; here we use a conservative constant so that
// the test transactions are always valid with respect to the minimum-fee rule
// on the devnet configuration (minFeeA * txSize + minFeeB).
const MinFee uint64 = 200_000

// minSendAmount is the smallest output we will create for the recipient so the
// UTxO is above the typical minimum-ADA requirement.
const minSendAmount uint64 = 1_000_000

// txBodyInput is the CBOR representation of a transaction input.
// Cardano encodes inputs as 2-element arrays: [txHash, txIndex].
type txBodyInput struct {
	_    cbor.StructAsArray
	Hash []byte
	Idx  uint32
}

// txBodyOutput is the CBOR representation of a transaction output.
// Pre-Babbage (Shelley/Mary/Alonzo/Babbage compat) format: [address, value].
type txBodyOutput struct {
	_       cbor.StructAsArray
	Address []byte
	Amount  uint64
}

// txBody maps to the Conway transaction body (CBOR map).
// Only the fields needed for a simple payment are populated.
//
// Key 0 = inputs  (set of transaction inputs)
// Key 1 = outputs (array of transaction outputs)
// Key 2 = fee
type txBody struct {
	Inputs  []txBodyInput  `cbor:"0,keyasint"`
	Outputs []txBodyOutput `cbor:"1,keyasint"`
	Fee     uint64         `cbor:"2,keyasint"`
}

// conwayTx is the top-level transaction structure.
// Conway encodes a transaction as a 4-element array:
//
//	[txBody, witnessSet, isValid, auxiliaryData]
//
// For test transactions the witness set is empty, isValid is true, and
// auxiliary data is null.
type conwayTx struct {
	_       cbor.StructAsArray
	Body    txBody
	Witness map[any]any // empty map
	IsValid bool
	AuxData any // nil
}

// PaymentParams holds the parameters for a simple ADA payment transaction.
type PaymentParams struct {
	// Inputs are the UTxOs being spent.
	Inputs []UTxO

	// ToAddr is the bech32/raw bytes of the recipient address.
	ToAddr []byte

	// ChangeAddr is the address that receives the change output.
	ChangeAddr []byte

	// SendAmount is the lovelace amount to send to ToAddr.
	SendAmount uint64

	// Change is the lovelace amount to return to ChangeAddr.
	Change uint64
}

// BuildPayment constructs a minimal CBOR-encoded Conway transaction from the
// provided payment parameters.  The transaction has no witnesses so it will be
// rejected by any node that validates signatures, but it exercises the
// submission path end-to-end for testing purposes.
func BuildPayment(p PaymentParams) ([]byte, error) {
	if len(p.Inputs) == 0 {
		return nil, errors.New("payment: at least one input required")
	}
	if len(p.ToAddr) == 0 {
		return nil, errors.New("payment: recipient address must not be empty")
	}
	if p.SendAmount < minSendAmount {
		return nil, fmt.Errorf(
			"payment: send amount %d is below minimum %d",
			p.SendAmount, minSendAmount,
		)
	}

	inputs := make([]txBodyInput, 0, len(p.Inputs))
	var totalInput uint64
	for _, u := range p.Inputs {
		hashBytes, err := hex.DecodeString(u.TxHash)
		if err != nil {
			return nil, fmt.Errorf(
				"payment: invalid tx hash %q: %w", u.TxHash, err,
			)
		}
		if len(hashBytes) != 32 {
			return nil, fmt.Errorf(
				"payment: tx hash %q must be exactly 32 bytes, got %d",
				u.TxHash, len(hashBytes),
			)
		}
		inputs = append(inputs, txBodyInput{Hash: hashBytes, Idx: u.Index})
		if u.Amount > math.MaxUint64-totalInput {
			return nil, errors.New("payment: total input overflow")
		}
		totalInput += u.Amount
	}

	if p.Change > 0 && len(p.ChangeAddr) == 0 {
		return nil, errors.New(
			"payment: non-zero change requires a change address",
		)
	}

	outputs := []txBodyOutput{
		{Address: p.ToAddr, Amount: p.SendAmount},
	}
	if p.Change > 0 {
		outputs = append(
			outputs,
			txBodyOutput{Address: p.ChangeAddr, Amount: p.Change},
		)
	}
	expectedTotal := p.SendAmount
	if p.Change > math.MaxUint64-expectedTotal {
		return nil, errors.New("payment: outputs total overflow")
	}
	expectedTotal += p.Change
	if MinFee > math.MaxUint64-expectedTotal {
		return nil, errors.New("payment: outputs plus fee overflow")
	}
	expectedTotal += MinFee
	if totalInput != expectedTotal {
		return nil, fmt.Errorf(
			"payment: inputs total %d does not equal outputs+fee %d",
			totalInput,
			expectedTotal,
		)
	}

	body := txBody{
		Inputs:  inputs,
		Outputs: outputs,
		Fee:     MinFee,
	}

	tx := conwayTx{
		Body:    body,
		Witness: map[any]any{},
		IsValid: true,
		AuxData: nil,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("payment: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}
