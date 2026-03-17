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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// alwaysSucceedsScriptHex is a minimal Plutus V2 always-succeeds script
// encoded in CBOR hex.  It is a double-CBOR-wrapped byte string as required
// by the Cardano ledger for script serialisation.
//
// The inner bytes represent a minimal untyped Plutus Core program that
// immediately returns unit regardless of its arguments.
const alwaysSucceedsScriptHex = "4e4d01000033222220051200120011"

// alwaysSucceedsScript returns the raw bytes of the always-succeeds script.
// Returns nil if the hex constant cannot be decoded (should never happen with
// a correct even-length hex string).
func alwaysSucceedsScript() []byte {
	b, err := hex.DecodeString(alwaysSucceedsScriptHex)
	if err != nil {
		// alwaysSucceedsScriptHex must be a valid even-length hex string;
		// if decode fails the constant is malformed.
		slog.Default().Error(
			"alwaysSucceedsScriptHex decode failed",
			"err", err,
		)
		return nil
	}
	return b
}

// scriptAddressFromHash builds a 29-byte Cardano enterprise address for a
// script payment credential.  The network byte is 0x70 (mainnet script
// enterprise) but for devnet testing the exact discriminant byte does not
// affect the CBOR structure test.
func scriptAddressFromHash(scriptHash []byte) []byte {
	addr := make([]byte, 29)
	addr[0] = 0x70 // script enterprise address discriminant
	copy(addr[1:], scriptHash)
	return addr
}

// datumHash computes a 32-byte SHA-256 hash of the provided datum bytes.
// On Cardano the datum hash used in pre-Babbage outputs is the Blake2b-256
// hash of the CBOR-encoded datum; for Antithesis testing we use SHA-256 as a
// stand-in since node signature validation is not enforced.
func datumHash(datum []byte) []byte {
	h := sha256.Sum256(datum)
	return h[:]
}

// txBodyWithDatumHash extends the basic tx body with datum hashes on outputs.
// It uses a map-based encoding so we can include datum_hash (key 2 on the
// output map — not to be confused with the top-level fee key).
//
// For simplicity we represent outputs as raw CBOR so we can embed the
// optional datum_hash field without defining a full post-Babbage output type.
//
// Key 0 = inputs
// Key 1 = outputs  (array of map-encoded outputs)
// Key 2 = fee
type txBodyWithScriptOutput struct {
	Inputs  []txBodyInput     `cbor:"0,keyasint"`
	Outputs []cbor.RawMessage `cbor:"1,keyasint"`
	Fee     uint64            `cbor:"2,keyasint"`
}

// conwayTxWithScriptOutput is a Conway transaction targeting a script address.
type conwayTxWithScriptOutput struct {
	_       cbor.StructAsArray
	Body    txBodyWithScriptOutput
	Witness map[any]any
	IsValid bool
	AuxData any
}

// scriptOutput is a map-encoded Babbage/Conway output that can carry an
// optional datum hash.
//
// Key 0 = address
// Key 1 = value (lovelace only)
// Key 2 = datum_option (pair [0, datumHash] for hash-based datum)
type scriptOutputDatumHash struct {
	Address     []byte `cbor:"0,keyasint"`
	Amount      uint64 `cbor:"1,keyasint"`
	DatumOption []any  `cbor:"2,keyasint"`
}

// BuildPlutusLockTx constructs a minimal CBOR-encoded Conway transaction that
// sends ADA to a script address with an embedded datum hash.  The transaction
// has no witnesses and will be rejected by a live node, but exercises the full
// script-output submission path for Antithesis testing.
func BuildPlutusLockTx(
	inputs []UTxO,
	scriptHash []byte,
	amount uint64,
	fee uint64,
	changeAddr []byte,
) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, errors.New("plutus_lock: at least one input required")
	}
	if len(scriptHash) != 28 {
		return nil, fmt.Errorf(
			"plutus_lock: script hash must be exactly 28 bytes, got %d",
			len(scriptHash),
		)
	}
	if amount < minSendAmount {
		return nil, fmt.Errorf(
			"plutus_lock: amount %d is below minimum %d",
			amount, minSendAmount,
		)
	}

	bodyInputs := make([]txBodyInput, 0, len(inputs))
	for _, u := range inputs {
		hashBytes, err := hex.DecodeString(u.TxHash)
		if err != nil {
			return nil, fmt.Errorf(
				"plutus_lock: invalid tx hash %q: %w", u.TxHash, err,
			)
		}
		bodyInputs = append(bodyInputs, txBodyInput{Hash: hashBytes, Idx: u.Index})
	}

	var total uint64
	for _, u := range inputs {
		total += u.Amount
	}
	spent := amount + fee
	var change uint64
	if total > spent {
		change = total - spent
	}

	scriptAddr := scriptAddressFromHash(scriptHash)

	// Datum: a simple CBOR integer 0 as the locked datum.
	datumBytes, err := cbor.Encode(uint64(0))
	if err != nil {
		return nil, fmt.Errorf("plutus_lock: datum encoding failed: %w", err)
	}
	dHash := datumHash(datumBytes)

	// Encode the script output as a map with datum_option [0, hash].
	scriptOut := scriptOutputDatumHash{
		Address:     scriptAddr,
		Amount:      amount,
		DatumOption: []any{uint64(0), dHash},
	}
	scriptOutBytes, err := cbor.Encode(scriptOut)
	if err != nil {
		return nil, fmt.Errorf(
			"plutus_lock: script output encoding failed: %w", err,
		)
	}

	outputs := []cbor.RawMessage{cbor.RawMessage(scriptOutBytes)}

	if change > 0 && len(changeAddr) > 0 {
		changeOut := scriptOutputDatumHash{
			Address: changeAddr,
			Amount:  change,
		}
		changeOutBytes, err := cbor.Encode(changeOut)
		if err != nil {
			return nil, fmt.Errorf(
				"plutus_lock: change output encoding failed: %w", err,
			)
		}
		outputs = append(outputs, cbor.RawMessage(changeOutBytes))
	}

	body := txBodyWithScriptOutput{
		Inputs:  bodyInputs,
		Outputs: outputs,
		Fee:     fee,
	}

	tx := conwayTxWithScriptOutput{
		Body:    body,
		Witness: map[any]any{},
		IsValid: true,
		AuxData: nil,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("plutus_lock: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}

// txBodyWithRedeemer extends the basic tx body with a redeemer map (key 5 in
// witness set is where redeemers live in real Conway, but for the tx body
// structure we keep this simple).
//
// Key 0 = inputs
// Key 1 = outputs
// Key 2 = fee
type txBodyUnlock struct {
	Inputs  []txBodyInput  `cbor:"0,keyasint"`
	Outputs []txBodyOutput `cbor:"1,keyasint"`
	Fee     uint64         `cbor:"2,keyasint"`
}

// witnessWithScript is a Conway witness set that carries a Plutus V2 script
// and a redeemer.
//
// Key 3 = plutus_v2_scripts (array of script bytes)
// Key 5 = redeemers (map of redeemer_key -> redeemer_value)
type witnessWithScript struct {
	PlutusV2Scripts [][]byte    `cbor:"3,keyasint"`
	Redeemers       map[any]any `cbor:"5,keyasint"`
}

// conwayTxWithScript is the top-level Conway transaction with script witness.
type conwayTxWithScript struct {
	_       cbor.StructAsArray
	Body    txBodyUnlock
	Witness witnessWithScript
	IsValid bool
	AuxData any
}

// redeemerKey identifies which input the redeemer applies to.
// [tag, index] where tag 0 = spend.
type redeemerKey struct {
	_     cbor.StructAsArray
	Tag   uint32
	Index uint32
}

// redeemerValue is the redeemer data and execution units.
// [data, [memory, steps]].
type redeemerValue struct {
	_       cbor.StructAsArray
	Data    any
	ExUnits []uint64
}

// BuildPlutusUnlockTx constructs a minimal CBOR-encoded Conway transaction
// that spends from a script address with an inline redeemer.  The transaction
// carries the script in the witness set.  It will be rejected by a live node
// (no real validation), but exercises the full script-spend submission path
// for Antithesis testing.
func BuildPlutusUnlockTx(
	inputs []UTxO,
	scriptBytes []byte,
	fee uint64,
	changeAddr []byte,
) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, errors.New("plutus_unlock: at least one input required")
	}
	if len(scriptBytes) == 0 {
		scriptBytes = alwaysSucceedsScript()
	}

	bodyInputs := make([]txBodyInput, 0, len(inputs))
	for _, u := range inputs {
		hashBytes, err := hex.DecodeString(u.TxHash)
		if err != nil {
			return nil, fmt.Errorf(
				"plutus_unlock: invalid tx hash %q: %w", u.TxHash, err,
			)
		}
		bodyInputs = append(bodyInputs, txBodyInput{Hash: hashBytes, Idx: u.Index})
	}

	var total uint64
	for _, u := range inputs {
		total += u.Amount
	}
	var change uint64
	if total > fee {
		change = total - fee
	}

	var outputs []txBodyOutput
	if change > 0 && len(changeAddr) > 0 {
		outputs = append(outputs, txBodyOutput{Address: changeAddr, Amount: change})
	}

	body := txBodyUnlock{
		Inputs:  bodyInputs,
		Outputs: outputs,
		Fee:     fee,
	}

	// Redeemer: spend tag (0), input index (0), data = unit (null), ExUnits.
	// Key 5 of the witness set must be a CBOR map of redeemer_key => redeemer_value.
	rKey := redeemerKey{Tag: 0, Index: 0}
	rVal := redeemerValue{Data: nil, ExUnits: []uint64{200_000, 700_000_000}}

	witness := witnessWithScript{
		PlutusV2Scripts: [][]byte{scriptBytes},
		Redeemers:       map[any]any{rKey: rVal},
	}

	tx := conwayTxWithScript{
		Body:    body,
		Witness: witness,
		IsValid: true,
		AuxData: nil,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("plutus_unlock: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}
