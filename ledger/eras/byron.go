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
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var ByronEraDesc = EraDesc{
	Id:              byron.EraIdByron,
	Name:            byron.EraNameByron,
	EpochLengthFunc: EpochLengthByron,
	ValidateTxFunc:  ValidateTxByron,
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
	// Verify bootstrap witness signatures
	if err := lcommon.ValidateBootstrapWitnesses(tx); err != nil {
		return err
	}
	// Verify each input has a matching witness
	if err := lcommon.ValidateInputVKeyWitnesses(tx, ls); err != nil {
		return err
	}
	return nil
}
