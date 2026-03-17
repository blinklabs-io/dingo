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
	"math"

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
func alwaysSucceedsScript() []byte {
	b, err := hex.DecodeString(alwaysSucceedsScriptHex)
	if err != nil {
		panic("alwaysSucceedsScriptHex is malformed: " + err.Error())
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
// Key 3 = ttl
type txBodyWithScriptOutput struct {
	Inputs  []txBodyInput     `cbor:"0,keyasint"`
	Outputs []cbor.RawMessage `cbor:"1,keyasint"`
	Fee     uint64            `cbor:"2,keyasint"`
	TTL     uint64            `cbor:"3,keyasint,omitempty"`
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

	bodyInputs, err := decodeInputs("plutus_lock", inputs)
	if err != nil {
		return nil, err
	}

	total := sumInputs(inputs)
	if amount > math.MaxUint64-fee {
		return nil, errors.New("plutus_lock: amount + fee overflows uint64")
	}
	spent := amount + fee
	if total < spent {
		return nil, fmt.Errorf("plutus_lock: inputs (%d) insufficient for amount+fee (%d)", total, spent)
	}
	change := total - spent
	if change > 0 && len(changeAddr) == 0 {
		return nil, errors.New("plutus_lock: change requires a change address")
	}

	// Datum: a simple CBOR integer 0 as the locked datum.
	datumBytes, err := cbor.Encode(uint64(0))
	if err != nil {
		return nil, fmt.Errorf("plutus_lock: datum encoding failed: %w", err)
	}

	scriptOut := scriptOutputDatumHash{
		Address:     scriptAddressFromHash(scriptHash),
		Amount:      amount,
		DatumOption: []any{uint64(0), datumHash(datumBytes)},
	}
	scriptOutBytes, err := cbor.Encode(scriptOut)
	if err != nil {
		return nil, fmt.Errorf("plutus_lock: script output encoding failed: %w", err)
	}

	outputs := []cbor.RawMessage{cbor.RawMessage(scriptOutBytes)}

	if change > 0 {
		changeOut := scriptOutputDatumHash{Address: changeAddr, Amount: change}
		changeOutBytes, err := cbor.Encode(changeOut)
		if err != nil {
			return nil, fmt.Errorf("plutus_lock: change output encoding failed: %w", err)
		}
		outputs = append(outputs, cbor.RawMessage(changeOutBytes))
	}

	tx := conwayTxWithScriptOutput{
		Body: txBodyWithScriptOutput{
			Inputs:  bodyInputs,
			Outputs: outputs,
			Fee:     fee,
			TTL:     maxTTL,
		},
		Witness: map[any]any{},
		IsValid: true,
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
// Key 3 = ttl
type txBodyUnlock struct {
	Inputs  []txBodyInput  `cbor:"0,keyasint"`
	Outputs []txBodyOutput `cbor:"1,keyasint"`
	Fee     uint64         `cbor:"2,keyasint"`
	TTL     uint64         `cbor:"3,keyasint,omitempty"`
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
		if len(scriptBytes) == 0 {
			return nil, errors.New("plutus_unlock: alwaysSucceedsScript returned nil")
		}
	}

	bodyInputs, err := decodeInputs("plutus_unlock", inputs)
	if err != nil {
		return nil, err
	}

	total := sumInputs(inputs)
	if total < fee {
		return nil, fmt.Errorf("plutus_unlock: inputs (%d) insufficient for fee (%d)", total, fee)
	}
	change := total - fee
	if change > 0 && len(changeAddr) == 0 {
		return nil, errors.New("plutus_unlock: change requires a change address")
	}

	var outputs []txBodyOutput
	if change > 0 {
		outputs = append(outputs, txBodyOutput{Address: changeAddr, Amount: change})
	}

	tx := conwayTxWithScript{
		Body: txBodyUnlock{
			Inputs:  bodyInputs,
			Outputs: outputs,
			Fee:     fee,
			TTL:     maxTTL,
		},
		Witness: witnessWithScript{
			PlutusV2Scripts: [][]byte{scriptBytes},
			Redeemers: map[any]any{
				redeemerKey{Tag: 0, Index: 0}: redeemerValue{
					ExUnits: []uint64{200_000, 700_000_000},
				},
			},
		},
		IsValid: true,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("plutus_unlock: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}
