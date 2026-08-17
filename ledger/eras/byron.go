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

package eras

import (
	"crypto/sha3"
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var ByronEraDesc = EraDesc{
	Id:              byron.EraIdByron,
	Name:            byron.EraNameByron,
	MinMajorVersion: 0,
	MaxMajorVersion: 1,
	EpochLengthFunc: EpochLengthByron,
	ValidateTxFunc:  ValidateTxByron,
}

// ByronProtocolMagicProvider supplies the protocol magic from the active
// Byron genesis configuration. It must come from ledger state rather than
// being inferred from the Shelley network ID because private networks can use
// custom Byron protocol magic values.
type ByronProtocolMagicProvider interface {
	ByronProtocolMagic() (uint32, error)
}

func EpochLengthByron(
	nodeConfig *cardano.CardanoNodeConfig,
) (uint, uint, error) {
	byronGenesis := nodeConfig.ByronGenesis()
	if byronGenesis == nil {
		return 0, 0, errors.New("unable to get byron genesis")
	}
	// These are known to be within uint range
	// #nosec G115
	return uint(byronGenesis.BlockVersionData.SlotDuration),
		uint(byronGenesis.ProtocolConsts.K * 10),
		nil
}

// Byron validation error types

// InputSetEmptyByronError is returned when a Byron transaction
// has no inputs.
type InputSetEmptyByronError struct{}

func (InputSetEmptyByronError) Error() string {
	return "transaction has no inputs"
}

// OutputSetEmptyByronError is returned when a Byron transaction
// has no outputs.
type OutputSetEmptyByronError struct{}

func (OutputSetEmptyByronError) Error() string {
	return "transaction has no outputs"
}

// OutputNotPositiveByronError is returned when a Byron transaction
// output has a non-positive value.
type OutputNotPositiveByronError struct {
	Index  int
	Amount *big.Int
}

func (e OutputNotPositiveByronError) Error() string {
	return fmt.Sprintf(
		"output %d has non-positive value: %s",
		e.Index,
		e.Amount.String(),
	)
}

// DuplicateInputByronError is returned when a Byron transaction
// contains duplicate inputs.
type DuplicateInputByronError struct {
	TxId  string
	Index uint32
}

func (e DuplicateInputByronError) Error() string {
	return fmt.Sprintf(
		"duplicate input: %s#%d",
		e.TxId,
		e.Index,
	)
}

// BadInputsByronError is returned when a Byron transaction
// references inputs that do not exist in the UTxO set.
type BadInputsByronError struct {
	Inputs []lcommon.TransactionInput
}

func (e BadInputsByronError) Error() string {
	return fmt.Sprintf(
		"inputs not found in UTxO set: %d bad input(s)",
		len(e.Inputs),
	)
}

// ValueNotConservedByronError is returned when a Byron
// transaction's consumed value does not equal its produced
// value plus fee (sum of inputs != sum of outputs + fee).
type ValueNotConservedByronError struct {
	Consumed *big.Int
	Produced *big.Int
}

func (e ValueNotConservedByronError) Error() string {
	return fmt.Sprintf(
		"value not conserved: consumed %s != produced %s",
		e.Consumed.String(),
		e.Produced.String(),
	)
}

// ValidateTxByron performs structural and UTxO-aware
// validation on Byron transactions. Structural rules always
// run. UTxO-aware rules (input existence, value conservation,
// witness signatures) run when a LedgerState is provided.
func ValidateTxByron(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	errs := make([]error, 0)
	// Structural rules (no ledger state needed)
	for _, validationFunc := range byronValidationRules {
		errs = append(
			errs,
			validationFunc(tx),
		)
	}
	// UTxO-aware rules (require ledger state)
	if ls != nil {
		for _, validationFunc := range byronUtxoValidationRules {
			errs = append(
				errs,
				validationFunc(tx, slot, ls, pp),
			)
		}
	}
	return errors.Join(errs...)
}

// byronValidationRuleFunc is a function that validates a Byron
// transaction against a specific structural rule.
type byronValidationRuleFunc func(tx lcommon.Transaction) error

var byronValidationRules = []byronValidationRuleFunc{
	byronValidateInputsNotEmpty,
	byronValidateOutputsNotEmpty,
	byronValidateOutputsPositive,
	byronValidateNoDuplicateInputs,
}

// byronUtxoValidationRules require ledger state and run only
// when a LedgerState is provided.
var byronUtxoValidationRules = []lcommon.UtxoValidationRuleFunc{
	byronValidateBadInputs,
	byronValidateValueConserved,
	byronValidateWitnesses,
}

// byronValidateInputsNotEmpty ensures that the transaction has at
// least one input.
func byronValidateInputsNotEmpty(
	tx lcommon.Transaction,
) error {
	if len(tx.Inputs()) == 0 {
		return InputSetEmptyByronError{}
	}
	return nil
}

// byronValidateOutputsNotEmpty ensures that the transaction has at
// least one output.
func byronValidateOutputsNotEmpty(
	tx lcommon.Transaction,
) error {
	if len(tx.Outputs()) == 0 {
		return OutputSetEmptyByronError{}
	}
	return nil
}

// byronValidateOutputsPositive ensures that all outputs have
// positive values.
func byronValidateOutputsPositive(
	tx lcommon.Transaction,
) error {
	zero := new(big.Int)
	for i, output := range tx.Outputs() {
		amount := output.Amount()
		if amount == nil || amount.Cmp(zero) <= 0 {
			if amount == nil {
				amount = new(big.Int)
			}
			return OutputNotPositiveByronError{
				Index:  i,
				Amount: amount,
			}
		}
	}
	return nil
}

// byronValidateNoDuplicateInputs ensures that there are no
// duplicate inputs in the transaction.
func byronValidateNoDuplicateInputs(
	tx lcommon.Transaction,
) error {
	seen := make(map[string]struct{})
	for _, input := range tx.Inputs() {
		key := fmt.Sprintf("%s#%d", input.Id(), input.Index())
		if _, exists := seen[key]; exists {
			return DuplicateInputByronError{
				TxId:  input.Id().String(),
				Index: input.Index(),
			}
		}
		seen[key] = struct{}{}
	}
	return nil
}

// byronValidateBadInputs ensures that all inputs reference
// UTxOs that exist in the ledger state.
func byronValidateBadInputs(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	_ lcommon.ProtocolParameters,
) error {
	var badInputs []lcommon.TransactionInput
	for _, input := range tx.Inputs() {
		if _, err := ls.UtxoById(input); err != nil {
			badInputs = append(badInputs, input)
		}
	}
	if len(badInputs) == 0 {
		return nil
	}
	return BadInputsByronError{Inputs: badInputs}
}

// byronValidateValueConserved ensures that the consumed value
// (sum of input UTxO amounts) equals the produced value (sum
// of output amounts). In Byron the fee is implicit: it is the
// difference between consumed and produced. We verify that
// consumed >= produced (i.e. the implicit fee is non-negative).
func byronValidateValueConserved(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	_ lcommon.ProtocolParameters,
) error {
	consumed := new(big.Int)
	for _, input := range tx.Inputs() {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			// Bad inputs are caught by byronValidateBadInputs
			continue
		}
		if utxo.Output == nil {
			continue
		}
		if amount := utxo.Output.Amount(); amount != nil {
			consumed.Add(consumed, amount)
		}
	}
	produced := new(big.Int)
	for _, output := range tx.Outputs() {
		if amount := output.Amount(); amount != nil {
			produced.Add(produced, amount)
		}
	}
	// In Byron the fee is implicit (consumed - produced).
	// Consumed must be >= produced for a valid transaction.
	if consumed.Cmp(produced) < 0 {
		return ValueNotConservedByronError{
			Consumed: consumed,
			Produced: produced,
		}
	}
	return nil
}

// byronValidateWitnesses verifies the cryptographic
// signatures on vkey and bootstrap witnesses.
func byronValidateWitnesses(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	_ lcommon.ProtocolParameters,
) error {
	// Verify vkey witness signatures
	if err := lcommon.ValidateVKeyWitnesses(tx); err != nil {
		return err
	}
	// Byron redeem witnesses are constructor 2 values whose fields are
	// wrapped in CBOR tag 24. Older gouroboros releases preserve these raw
	// values but do not expose them through TransactionWitnessSet.
	var redeemWitnesses []lcommon.VkeyWitness
	var bootstrapWitnesses []byronBootstrapWitness
	if byronTx, ok := tx.(*byron.ByronTransaction); ok {
		redeemWitnesses = byronRedeemWitnesses(byronTx.Twit)
		bootstrapWitnesses = byronBootstrapWitnesses(byronTx.Twit)
		if len(redeemWitnesses) > 0 || len(bootstrapWitnesses) > 0 {
			txHash := tx.Hash()
			protocolMagicProvider, ok := ls.(ByronProtocolMagicProvider)
			if !ok {
				return errors.New("ledger state does not provide Byron protocol magic")
			}
			protocolMagic, err := protocolMagicProvider.ByronProtocolMagic()
			if err != nil {
				return fmt.Errorf("get Byron protocol magic: %w", err)
			}
			var redeemMessage []byte
			if len(redeemWitnesses) > 0 {
				redeemMessage, err = byronSignatureMessage(
					0x02,
					protocolMagic,
					txHash,
				)
				if err != nil {
					return err
				}
			}
			var bootstrapMessage []byte
			if len(bootstrapWitnesses) > 0 {
				bootstrapMessage, err = byronSignatureMessage(
					0x01,
					protocolMagic,
					txHash,
				)
				if err != nil {
					return err
				}
			}
			for _, witness := range redeemWitnesses {
				if err := lcommon.VerifyVKeySignature(
					witness.Vkey,
					witness.Signature,
					redeemMessage,
				); err != nil {
					return lcommon.NewValidationError(
						lcommon.ValidationErrorTypeTransaction,
						"invalid vkey signature",
						map[string]any{"err": err.Error()},
						err,
					)
				}
			}
			for _, witness := range bootstrapWitnesses {
				if err := lcommon.VerifyVKeySignature(
					witness.PublicKey,
					witness.Signature,
					bootstrapMessage,
				); err != nil {
					return lcommon.NewValidationError(
						lcommon.ValidationErrorTypeTransaction,
						"invalid bootstrap signature",
						map[string]any{"err": err.Error()},
						err,
					)
				}
			}
		}
	}
	// Verify bootstrap witness signatures
	if len(bootstrapWitnesses) == 0 {
		if err := lcommon.ValidateBootstrapWitnesses(tx); err != nil {
			return err
		}
	}
	// Verify each input has a matching witness
	if len(redeemWitnesses) == 0 && len(bootstrapWitnesses) == 0 {
		return lcommon.ValidateInputVKeyWitnesses(tx, ls)
	}
	return validateByronInputWitnesses(tx, ls, redeemWitnesses, bootstrapWitnesses)
}

func byronSignatureMessage(
	tag byte,
	protocolMagic uint32,
	txHash lcommon.Blake2b256,
) ([]byte, error) {
	magicCbor, err := cbor.Encode(protocolMagic)
	if err != nil {
		return nil, fmt.Errorf("encode Byron protocol magic: %w", err)
	}
	// Byron signatures use a domain tag, the CBOR-encoded protocol magic, and
	// the CBOR bytestring encoding of TxSigData (the transaction body hash).
	message := append([]byte{tag}, magicCbor...)
	message = append(message, 0x58, 0x20)
	return append(message, txHash[:]...), nil
}

type byronBootstrapWitness struct {
	PublicKey []byte
	Signature []byte
	ChainCode []byte
}

func byronWitnessPayloads(
	witnesses []cbor.Value,
	expectedConstructor uint64,
) [][][]byte {
	var ret [][][]byte
	for _, witness := range witnesses {
		fields, ok := witness.Value().([]any)
		if !ok || len(fields) != 2 {
			continue
		}
		ctor, ok := fields[0].(uint64)
		if !ok || ctor != expectedConstructor {
			continue
		}
		wrapped, ok := fields[1].(cbor.WrappedCbor)
		if !ok {
			continue
		}
		var witnessFields [][]byte
		if _, err := cbor.Decode(wrapped.Bytes(), &witnessFields); err != nil {
			continue
		}
		ret = append(ret, witnessFields)
	}
	return ret
}

func byronBootstrapWitnesses(
	witnesses []cbor.Value,
) []byronBootstrapWitness {
	var ret []byronBootstrapWitness
	for _, witnessFields := range byronWitnessPayloads(
		witnesses,
		lcommon.ByronAddressTypePubkey,
	) {
		if len(witnessFields) != 2 || len(witnessFields[0]) != 64 ||
			len(witnessFields[1]) != 64 {
			continue
		}
		ret = append(ret, byronBootstrapWitness{
			PublicKey: witnessFields[0][:32],
			ChainCode: witnessFields[0][32:],
			Signature: witnessFields[1],
		})
	}
	return ret
}

func byronRedeemWitnesses(
	witnesses []cbor.Value,
) []lcommon.VkeyWitness {
	var ret []lcommon.VkeyWitness
	for _, witnessFields := range byronWitnessPayloads(
		witnesses,
		lcommon.ByronAddressTypeRedeem,
	) {
		if len(witnessFields) != 2 {
			continue
		}
		ret = append(ret, lcommon.VkeyWitness{
			Vkey:      witnessFields[0],
			Signature: witnessFields[1],
		})
	}
	return ret
}

func validateByronInputWitnesses(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	redeemWitnesses []lcommon.VkeyWitness,
	byronBootstrapWitnesses []byronBootstrapWitness,
) error {
	provided := make(map[lcommon.Blake2b224]struct{})
	if witnesses := tx.Witnesses(); witnesses != nil {
		for _, witness := range witnesses.Vkey() {
			provided[lcommon.Blake2b224Hash(witness.Vkey)] = struct{}{}
		}
	}
	for _, witness := range redeemWitnesses {
		provided[lcommon.Blake2b224Hash(witness.Vkey)] = struct{}{}
	}

	var bootstrapWitnesses []lcommon.BootstrapWitness
	if witnesses := tx.Witnesses(); witnesses != nil {
		bootstrapWitnesses = witnesses.Bootstrap()
	}
	for _, input := range tx.Inputs() {
		utxo, err := ls.UtxoById(input)
		if err != nil || utxo.Output == nil {
			continue
		}
		addr := utxo.Output.Address()
		payload, ok := addr.PayloadPayload().(lcommon.AddressPayloadKeyHash)
		if !ok {
			continue
		}
		if _, ok := provided[payload.Hash]; ok {
			continue
		}
		if addr.Type() == lcommon.AddressTypeByron {
			if addr.ByronType() == lcommon.ByronAddressTypeRedeem {
				matched := false
				for _, witness := range redeemWitnesses {
					redeemAddr, err := lcommon.NewByronAddressRedeem(
						witness.Vkey,
						addr.ByronAttr(),
					)
					if err == nil && redeemAddr.PaymentKeyHash() == payload.Hash {
						matched = true
						break
					}
				}
				if matched {
					continue
				}
			}
			matched := false
			for _, witness := range bootstrapWitnesses {
				addrRoot, err := byronAddressRoot(witness)
				if err == nil && addrRoot == payload.Hash {
					matched = true
					break
				}
			}
			if matched {
				continue
			}
			attrs, err := cbor.Encode(addr.ByronAttr())
			if err != nil {
				continue
			}
			for _, witness := range byronBootstrapWitnesses {
				addrRoot, err := byronAddressRootForParts(
					witness.PublicKey,
					witness.ChainCode,
					attrs,
				)
				if err == nil && addrRoot == payload.Hash {
					matched = true
					break
				}
			}
			if matched {
				continue
			}
			addressType := "bootstrap"
			if addr.ByronType() == lcommon.ByronAddressTypeRedeem {
				addressType = "redeem"
			}
			return lcommon.NewValidationError(
				lcommon.ValidationErrorTypeTransaction,
				fmt.Sprintf("missing %s witness for Byron input", addressType),
				map[string]any{
					"input":        input.String(),
					"keyhash":      payload.Hash.String(),
					"address_type": addressType,
				},
				nil,
			)
		}
		return lcommon.NewValidationError(
			lcommon.ValidationErrorTypeTransaction,
			"missing vkey witness for input",
			map[string]any{
				"input":   input.String(),
				"keyhash": payload.Hash.String(),
			},
			nil,
		)
	}
	return nil
}

func byronAddressRoot(
	witness lcommon.BootstrapWitness,
) (lcommon.Blake2b224, error) {
	return byronAddressRootForParts(
		witness.PublicKey,
		witness.ChainCode,
		witness.Attributes,
	)
}

func byronAddressRootForParts(
	publicKey []byte,
	chainCode []byte,
	attributes []byte,
) (lcommon.Blake2b224, error) {
	if len(publicKey) != 32 {
		return lcommon.Blake2b224{}, fmt.Errorf(
			"invalid Byron pubkey size: expected 32 bytes, got %d",
			len(publicKey),
		)
	}
	if len(chainCode) != 32 {
		return lcommon.Blake2b224{}, fmt.Errorf(
			"invalid Byron chain code size: expected 32 bytes, got %d",
			len(chainCode),
		)
	}
	if len(attributes) == 0 {
		attributes = []byte{0xa0}
	}
	// Encode the public-key address structure through its CBOR package so the
	// Byron address-root shape stays explicit.
	pubkey := make([]byte, 0, len(publicKey)+len(chainCode))
	pubkey = append(pubkey, publicKey...)
	pubkey = append(pubkey, chainCode...)
	root, err := cbor.Encode([]any{
		uint64(lcommon.ByronAddressTypePubkey),
		[]any{uint64(lcommon.ByronAddressTypePubkey), pubkey},
		cbor.RawMessage(attributes),
	})
	if err != nil {
		return lcommon.Blake2b224{}, fmt.Errorf("encode Byron address root: %w", err)
	}
	hash := sha3.Sum256(root)
	return lcommon.Blake2b224Hash(hash[:]), nil
}
