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

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// alwaysSucceedsScriptHex is a PlutusV3 script, as carried in a witness set
// (a CBOR byte string wrapping the flat program), for
// (program 1.1.0 (lam ctx (con unit ()))): it accepts any script context.
const alwaysSucceedsScriptHex = "450101002499"

// plutusV3 is the Plutus language version index used by protocol parameter
// cost models and language views.
const plutusV3 uint = 2

// plutusLockAmount is the lovelace locked at the script address. It leaves
// change above the minimum output after the unlock fee.
const plutusLockAmount uint64 = 2_000_000

// plutusUnlockFee covers the unlock transaction's size and script execution.
const plutusUnlockFee uint64 = 400_000

// plutusUnlockExUnits is the execution budget declared for the always-succeeds
// script; it is far above what the script uses.
var plutusUnlockExUnits = common.ExUnits{Memory: 100_000, Steps: 100_000_000}

// alwaysSucceedsScript returns the always-succeeds PlutusV3 script bytes.
func alwaysSucceedsScript() []byte {
	b, _ := hex.DecodeString(alwaysSucceedsScriptHex)
	return b
}

// alwaysSucceedsScriptHash returns the ledger hash of the always-succeeds
// script: Blake2b-224 over the PlutusV3 language tag and the script bytes.
func alwaysSucceedsScriptHash() []byte {
	return common.PlutusV3Script(alwaysSucceedsScript()).Hash().Bytes()
}

// scriptAddressFromHash builds a testnet enterprise address whose payment
// credential is the script hash (header 0x70: script, no stake, network 0).
func scriptAddressFromHash(scriptHash []byte) []byte {
	addr := make([]byte, 29)
	addr[0] = 0x70
	copy(addr[1:], scriptHash)
	return addr
}

// plutusOutput is a map-encoded Babbage/Conway output.
//
// Key 0 = address
// Key 1 = value (lovelace only)
// Key 2 = datum_option ([1, #6.24(datum)] for an inline datum)
type plutusOutput struct {
	Address     []byte `cbor:"0,keyasint"`
	Amount      uint64 `cbor:"1,keyasint"`
	DatumOption []any  `cbor:"2,keyasint,omitempty"`
}

// txBodyPlutus is a Conway transaction body for the Plutus workload.
//
// Key 0  = inputs
// Key 1  = outputs
// Key 2  = fee
// Key 11 = script_data_hash
// Key 13 = collateral inputs
type txBodyPlutus struct {
	Inputs         cbor.Set       `cbor:"0,keyasint"`
	Outputs        []plutusOutput `cbor:"1,keyasint"`
	Fee            uint64         `cbor:"2,keyasint"`
	ScriptDataHash []byte         `cbor:"11,keyasint,omitempty"`
	Collateral     cbor.Set       `cbor:"13,keyasint,omitempty"`
}

// conwayTxPlutus is the top-level Conway transaction for the Plutus workload.
type conwayTxPlutus struct {
	cbor.StructAsArray
	Body    txBodyPlutus
	Witness map[any]any
	IsValid bool
	AuxData any
}

func plutusInputs(label string, utxos []UTxO) (cbor.Set, uint64, error) {
	set := make(cbor.Set, 0, len(utxos))
	var total uint64
	for _, u := range utxos {
		hashBytes, err := hex.DecodeString(u.TxHash)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"%s: invalid tx hash %q: %w", label, u.TxHash, err,
			)
		}
		if len(hashBytes) != 32 {
			return nil, 0, fmt.Errorf(
				"%s: tx hash %q has unexpected length %d",
				label, u.TxHash, len(hashBytes),
			)
		}
		set = append(set, txBodyInput{Hash: hashBytes, Idx: u.Index})
		total += u.Amount
	}
	return set, total, nil
}

func encodePlutusTx(label string, body txBodyPlutus, witness map[any]any, keys []*UTxOKey) ([]byte, error) {
	bodyBytes, err := cbor.Encode(body)
	if err != nil {
		return nil, fmt.Errorf("%s: body encoding failed: %w", label, err)
	}
	for k, v := range BuildWitnessMap(bodyBytes, keys...) {
		witness[k] = v
	}
	txBytes, err := cbor.Encode(conwayTxPlutus{
		Body:    body,
		Witness: witness,
		IsValid: true,
		AuxData: nil,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: CBOR encoding failed: %w", label, err)
	}
	return txBytes, nil
}

// BuildPlutusLockTx constructs a signed Conway transaction that sends amount
// to the script address for scriptHash with an inline datum, returning any
// change to changeAddr.
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
	inputSet, total, err := plutusInputs("plutus_lock", inputs)
	if err != nil {
		return nil, err
	}
	if total < amount+fee {
		return nil, fmt.Errorf(
			"plutus_lock: total input %d cannot cover amount %d + fee %d",
			total, amount, fee,
		)
	}
	change := total - amount - fee

	// Inline datum: the integer 0, tagged as embedded CBOR.
	datumBytes, err := cbor.Encode(uint64(0))
	if err != nil {
		return nil, fmt.Errorf("plutus_lock: datum encoding failed: %w", err)
	}
	outputs := []plutusOutput{{
		Address: scriptAddressFromHash(scriptHash),
		Amount:  amount,
		DatumOption: []any{
			uint64(1),
			cbor.Tag{Number: 24, Content: datumBytes},
		},
	}}
	if change > 0 {
		if len(changeAddr) == 0 {
			return nil, errors.New(
				"plutus_lock: non-zero change requires a change address",
			)
		}
		if change < minSendAmount {
			return nil, fmt.Errorf(
				"plutus_lock: change %d is below the minimum output %d",
				change, minSendAmount,
			)
		}
		outputs = append(outputs, plutusOutput{Address: changeAddr, Amount: change})
	}

	keys := make([]*UTxOKey, 0, len(inputs))
	for _, u := range inputs {
		keys = append(keys, u.SigningKey)
	}
	return encodePlutusTx("plutus_lock", txBodyPlutus{
		Inputs:  inputSet,
		Outputs: outputs,
		Fee:     fee,
	}, map[any]any{}, keys)
}

// BuildPlutusUnlockTx constructs a signed Conway transaction that spends the
// always-succeeds script output locked, pledging collateral (a key-locked
// UTxO whose key signs the transaction) and returning the rest to changeAddr.
// costModel is the protocol's PlutusV3 cost model, which the script data hash
// commits to.
func BuildPlutusUnlockTx(
	locked UTxO,
	collateral UTxO,
	costModel []int64,
	fee uint64,
	changeAddr []byte,
) ([]byte, error) {
	if len(costModel) == 0 {
		return nil, errors.New("plutus_unlock: PlutusV3 cost model required")
	}
	if !collateral.SigningKey.canSign() {
		return nil, errors.New("plutus_unlock: collateral must be key-locked")
	}
	inputSet, total, err := plutusInputs("plutus_unlock", []UTxO{locked})
	if err != nil {
		return nil, err
	}
	collateralSet, collateralTotal, err := plutusInputs(
		"plutus_unlock", []UTxO{collateral},
	)
	if err != nil {
		return nil, err
	}
	// The ledger requires collateral of at least collateralPercentage (150%)
	// of the fee.
	if collateralTotal < fee*3/2 {
		return nil, fmt.Errorf(
			"plutus_unlock: collateral %d does not cover 150%% of fee %d",
			collateralTotal, fee,
		)
	}
	if total < fee+minSendAmount {
		return nil, fmt.Errorf(
			"plutus_unlock: locked amount %d cannot cover fee %d and a change output",
			total, fee,
		)
	}
	if len(changeAddr) == 0 {
		return nil, errors.New("plutus_unlock: change address required")
	}

	// Conway redeemers map: {[spend, 0]: [data, ex_units]}. The datum is
	// inline, so the script data hash covers only redeemers and language
	// views.
	redeemerKey, err := cbor.Encode([]uint64{uint64(common.RedeemerTagSpend), 0})
	if err != nil {
		return nil, fmt.Errorf("plutus_unlock: redeemer encoding failed: %w", err)
	}
	redeemerValue, err := cbor.Encode([]any{
		uint64(0),
		[]int64{plutusUnlockExUnits.Memory, plutusUnlockExUnits.Steps},
	})
	if err != nil {
		return nil, fmt.Errorf("plutus_unlock: redeemer encoding failed: %w", err)
	}
	redeemers := append([]byte{0xa1}, redeemerKey...)
	redeemers = append(redeemers, redeemerValue...)

	langViews, err := common.EncodeLangViews(
		map[uint]struct{}{plutusV3: {}},
		map[uint][]int64{plutusV3: costModel},
	)
	if err != nil {
		return nil, fmt.Errorf("plutus_unlock: language views: %w", err)
	}
	scriptDataHash := common.Blake2b256Hash(append(append([]byte{}, redeemers...), langViews...))

	return encodePlutusTx("plutus_unlock", txBodyPlutus{
		Inputs:         inputSet,
		Outputs:        []plutusOutput{{Address: changeAddr, Amount: total - fee}},
		Fee:            fee,
		ScriptDataHash: scriptDataHash.Bytes(),
		Collateral:     collateralSet,
	}, map[any]any{
		uint64(5): cbor.RawMessage(redeemers),
		uint64(7): [][]byte{alwaysSucceedsScript()},
	}, []*UTxOKey{collateral.SigningKey})
}
