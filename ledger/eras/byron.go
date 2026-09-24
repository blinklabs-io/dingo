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
	"strconv"

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
	return epochLengthByronGenesis(byronGenesis)
}

func epochLengthByronGenesis(
	byronGenesis *byron.ByronGenesis,
) (uint, uint, error) {
	if byronGenesis.BlockVersionData.SlotDuration < 0 {
		return 0, 0, fmt.Errorf(
			"byron genesis: slotDuration must not be negative, got %d",
			byronGenesis.BlockVersionData.SlotDuration,
		)
	}
	// K is also validated at genesis load (config/cardano's
	// validateSecurityParameters), by internal/node/load.go's
	// loadSecurityParamForConfig, and by this package's own
	// StabilityWindowForEra -- so a non-positive k should never reach a
	// production call here. This guard is the same defense-in-depth as the
	// SlotDuration check above: this specific uint conversion had no local
	// guard of its own before either fix.
	if byronGenesis.ProtocolConsts.K <= 0 {
		return 0, 0, fmt.Errorf(
			"byron genesis: security parameter (protocolConsts.k) must be positive, got %d",
			byronGenesis.ProtocolConsts.K,
		)
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

// OutputNotPositiveByronError is retained for API compatibility.
//
// Deprecated: Byron consensus permits zero-value outputs, so validation no
// longer returns this error.
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

// OutputNegativeByronError is returned when a Byron transaction output has a
// negative value.
type OutputNegativeByronError struct {
	Index  int
	Amount *big.Int
}

func (e OutputNegativeByronError) Error() string {
	return fmt.Sprintf(
		"output %d has negative value: %s",
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

// FeeTooLowByronError is returned when a Byron transaction's implicit fee is
// below the fee required by the Byron genesis fee policy.
type FeeTooLowByronError struct {
	Actual   *big.Int
	Required *big.Int
	Size     uint64
}

func (e FeeTooLowByronError) Error() string {
	return fmt.Sprintf(
		"fee %s is below Byron minimum %s for transaction size %d",
		e.Actual.String(),
		e.Required.String(),
		e.Size,
	)
}

// NetworkMagicMismatchByronError is returned when a Byron transaction pays to
// an address belonging to a different network than the one this node is
// validating for. Expected and Actual are nil for the mainnet encoding, which
// carries no network-magic attribute at all, and non-nil for a testnet or
// custom network, which carries its magic explicitly.
type NetworkMagicMismatchByronError struct {
	OutputIndex int
	Expected    *uint32
	Actual      *uint32
}

func (e NetworkMagicMismatchByronError) Error() string {
	return fmt.Sprintf(
		"output %d network magic %s does not match expected %s",
		e.OutputIndex,
		byronNetworkMagicString(e.Actual),
		byronNetworkMagicString(e.Expected),
	)
}

// byronNetworkMagicString renders a Byron address network attribute, naming
// the absent case rather than printing a bare nil: absence is meaningful here,
// it is how a mainnet address is encoded.
func byronNetworkMagicString(magic *uint32) string {
	if magic == nil {
		return "mainnet (no network attribute)"
	}
	return strconv.FormatUint(uint64(*magic), 10)
}

// ByronFeePolicyProvider supplies the active Byron genesis fee policy. Both
// values are scaled by 10^9.
type ByronFeePolicyProvider interface {
	ByronFeePolicy() (summand int64, multiplier int64, err error)
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
	byronValidateOutputsNonNegative,
	byronValidateNoDuplicateInputs,
}

// byronUtxoValidationRules require ledger state and run only
// when a LedgerState is provided.
var byronUtxoValidationRules = []lcommon.UtxoValidationRuleFunc{
	byronValidateBadInputs,
	byronValidateValueConserved,
	byronValidateOutputNetwork,
	byronValidateMinFee,
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

// byronValidateOutputsNonNegative ensures that output amounts are not
// negative. Zero-value outputs are valid in Byron.
func byronValidateOutputsNonNegative(
	tx lcommon.Transaction,
) error {
	for i, output := range tx.Outputs() {
		amount := output.Amount()
		if amount != nil && amount.Sign() < 0 {
			return OutputNegativeByronError{
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

// byronValidateOutputNetwork enforces the Byron reference's validateTxOutNM:
// every output address must belong to the network this node validates for,
// derived from the active protocol magic.
//
// Byron encodes that membership asymmetrically. A mainnet address carries no
// network-magic attribute at all, while a testnet or custom-network address
// carries its magic explicitly, so the expected value is an absence on
// mainnet and a specific number everywhere else. Comparing the magic itself,
// rather than a mainnet/testnet flag, is what keeps two distinct custom
// networks from being treated as interchangeable: an address minted for magic
// 42 is invalid on the network whose magic is 43, even though both are
// "testnet" by any boolean reading. Address.NetworkId() collapses exactly
// that distinction, which is why this reads the attribute directly.
func byronValidateOutputNetwork(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	_ lcommon.ProtocolParameters,
) error {
	provider, ok := ls.(ByronProtocolMagicProvider)
	if !ok {
		// Lightweight ledger-state implementations used by structural callers
		// do not necessarily expose chain configuration. The production
		// LedgerState does, and enforces the rule there.
		return nil
	}
	protocolMagic, err := provider.ByronProtocolMagic()
	if err != nil {
		return fmt.Errorf("get Byron protocol magic: %w", err)
	}
	var expected *uint32
	if protocolMagic != byron.MainnetProtocolMagic {
		expected = &protocolMagic
	}
	for idx, output := range tx.Outputs() {
		addr := output.Address()
		if addr.Type() != lcommon.AddressTypeByron {
			// A Byron output always decodes to a Byron address; the
			// lightweight outputs structural tests build do not carry one at
			// all, and there is no network to compare for those.
			continue
		}
		actual := addr.ByronAttr().Network
		if byronNetworkMagicEqual(expected, actual) {
			continue
		}
		return NetworkMagicMismatchByronError{
			OutputIndex: idx,
			Expected:    expected,
			Actual:      actual,
		}
	}
	return nil
}

// byronNetworkMagicEqual compares two Byron address network attributes by
// value, treating absence as its own case rather than as zero.
func byronNetworkMagicEqual(expected, actual *uint32) bool {
	if expected == nil || actual == nil {
		return expected == nil && actual == nil
	}
	return *expected == *actual
}

// byronValidateMinFee enforces the Byron genesis fee policy. Byron fees are
// implicit, so the consumed-minus-produced value computed by the conservation
// rule is the transaction fee.
func byronValidateMinFee(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	_ lcommon.ProtocolParameters,
) error {
	provider, ok := ls.(ByronFeePolicyProvider)
	if !ok {
		// Lightweight ledger-state implementations used by structural callers do
		// not necessarily expose chain configuration. The production
		// LedgerState does, and validates the policy there.
		return nil
	}
	summand, multiplier, err := provider.ByronFeePolicy()
	if err != nil {
		return fmt.Errorf("get Byron fee policy: %w", err)
	}
	if multiplier < 0 || summand < 0 {
		return fmt.Errorf(
			"invalid Byron fee policy: multiplier %d summand %d",
			multiplier,
			summand,
		)
	}
	size := TxSizeForFee(tx)
	required := new(big.Int).Mul(
		big.NewInt(multiplier),
		new(big.Int).SetUint64(size),
	)
	required.Add(required, big.NewInt(summand))
	const feeDivisor = int64(1_000_000_000)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(required, big.NewInt(feeDivisor), remainder)
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	required = quotient

	actual := new(big.Int)
	// The Byron reference exempts a transaction from the minimum fee when its
	// complete input UTxO consists of redeem addresses (isRedeemUTxO). A single
	// non-redeem input is enough to require the normal fee, and an input that
	// cannot be resolved is not evidence of a redeem address, so both clear the
	// exemption. An empty input set is not vacuously redeem-only.
	redeemOnly := len(tx.Inputs()) > 0
	for _, input := range tx.Inputs() {
		utxo, lookupErr := ls.UtxoById(input)
		if lookupErr != nil || utxo.Output == nil {
			redeemOnly = false
			continue
		}
		addr := utxo.Output.Address()
		if addr.Type() != lcommon.AddressTypeByron ||
			addr.ByronType() != lcommon.ByronAddressTypeRedeem {
			redeemOnly = false
		}
		if amount := utxo.Output.Amount(); amount != nil {
			actual.Add(actual, amount)
		}
	}
	for _, output := range tx.Outputs() {
		if amount := output.Amount(); amount != nil {
			actual.Sub(actual, amount)
		}
	}
	if redeemOnly {
		// Redemption still must conserve value, so a negative implicit fee
		// remains a failure against a zero requirement.
		required = big.NewInt(0)
	}
	if actual.Cmp(required) < 0 {
		return FeeTooLowByronError{
			Actual:   actual,
			Required: required,
			Size:     size,
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
	// ByronTransaction's upstream verifier requires protocol magic to be
	// injected from a decoded block header. Here, Dingo validates its raw Byron
	// witnesses below using the active ledger state's protocol magic instead.
	if _, ok := tx.(*byron.ByronTransaction); !ok {
		if err := lcommon.ValidateVKeyWitnesses(tx); err != nil {
			return err
		}
	}
	// Decode raw Byron witness values because bootstrap verification needs the
	// chain-code half of a constructor-0 witness, which VkeyWitness omits.
	var redeemWitnesses []lcommon.VkeyWitness
	var bootstrapWitnesses []byronBootstrapWitness
	if byronTx, ok := tx.(*byron.ByronTransaction); ok {
		var err error
		bootstrapWitnesses, redeemWitnesses, err = byronDecodeWitnesses(
			byronTx.Twit,
		)
		if err != nil {
			return lcommon.NewValidationError(
				lcommon.ValidationErrorTypeTransaction,
				"invalid byron transaction witness",
				map[string]any{"err": err.Error()},
				err,
			)
		}
		if len(redeemWitnesses) > 0 || len(bootstrapWitnesses) > 0 {
			protocolMagicProvider, ok := ls.(ByronProtocolMagicProvider)
			if !ok {
				return errors.New(
					"ledger state does not provide Byron protocol magic",
				)
			}
			protocolMagic, err := protocolMagicProvider.ByronProtocolMagic()
			if err != nil {
				return fmt.Errorf("get Byron protocol magic: %w", err)
			}
			byronTx, ok := tx.(*byron.ByronTransaction)
			if !ok {
				return errors.New("unexpected Byron transaction implementation")
			}
			if err := byronTx.ValidateVKeyWitnesses(protocolMagic); err != nil {
				return lcommon.NewValidationError(
					lcommon.ValidationErrorTypeTransaction,
					"invalid Byron vkey witness",
					map[string]any{"err": err.Error()},
					err,
				)
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
	return validateByronInputWitnesses(
		tx,
		ls,
		redeemWitnesses,
		bootstrapWitnesses,
	)
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
	txPayload, err := cbor.Encode(txHash[:])
	if err != nil {
		return nil, fmt.Errorf("encode Byron transaction signing payload: %w", err)
	}
	message := append([]byte{tag}, magicCbor...)
	return append(message, txPayload...), nil
}

type byronBootstrapWitness struct {
	PublicKey []byte
	Signature []byte
	ChainCode []byte
}

// byronDecodeWitnesses strictly decodes every entry of a Byron transaction's
// Twit list. The reference sum type (Cardano.Chain.UTxO.TxWitness) has
// exactly two live constructors -- VKWitness (0) and RedeemWitness (2) --
// and no catch-all case: a witness with an unrecognized constructor, or a
// malformed payload for a known constructor, invalidates the whole
// transaction rather than being dropped from the returned witnesses. A
// transaction with a genuinely valid witness followed by such an entry must
// not have its valid witness accepted while the invalid one is discarded,
// since the witness proof covers the raw bytes of both.
func byronDecodeWitnesses(
	witnesses []cbor.Value,
) ([]byronBootstrapWitness, []lcommon.VkeyWitness, error) {
	var bootstrap []byronBootstrapWitness
	var redeem []lcommon.VkeyWitness
	for idx, witness := range witnesses {
		fields, ok := witness.Value().([]any)
		if !ok || len(fields) != 2 {
			return nil, nil, fmt.Errorf(
				"witness %d: not a 2-element TxInWitness", idx,
			)
		}
		ctor, ok := fields[0].(uint64)
		if !ok {
			return nil, nil, fmt.Errorf(
				"witness %d: constructor tag is not an unsigned integer",
				idx,
			)
		}
		wrapped, ok := fields[1].(cbor.WrappedCbor)
		if !ok {
			return nil, nil, fmt.Errorf(
				"witness %d: payload is not tag-24-wrapped CBOR", idx,
			)
		}
		var witnessFields [][]byte
		wrappedBytes := wrapped.Bytes()
		consumed, err := cbor.Decode(wrappedBytes, &witnessFields)
		if err != nil || consumed != len(wrappedBytes) {
			return nil, nil, fmt.Errorf(
				"witness %d: failed to decode tag-24 payload", idx,
			)
		}
		switch ctor {
		case lcommon.ByronAddressTypePubkey:
			if len(witnessFields) != 2 || len(witnessFields[0]) != 64 ||
				len(witnessFields[1]) != 64 {
				return nil, nil, fmt.Errorf(
					"witness %d: malformed VKWitness fields", idx,
				)
			}
			bootstrap = append(bootstrap, byronBootstrapWitness{
				PublicKey: witnessFields[0][:32],
				ChainCode: witnessFields[0][32:],
				Signature: witnessFields[1],
			})
		case lcommon.ByronAddressTypeRedeem:
			if len(witnessFields) != 2 || len(witnessFields[0]) != 32 ||
				len(witnessFields[1]) != 64 {
				return nil, nil, fmt.Errorf(
					"witness %d: malformed RedeemWitness fields", idx,
				)
			}
			redeem = append(redeem, lcommon.VkeyWitness{
				Vkey:      witnessFields[0],
				Signature: witnessFields[1],
			})
		default:
			return nil, nil, fmt.Errorf(
				"witness %d: unknown TxInWitness constructor %d", idx, ctor,
			)
		}
	}
	return bootstrap, redeem, nil
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
					if err == nil &&
						redeemAddr.PaymentKeyHash() == payload.Hash {
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
		return lcommon.Blake2b224{}, fmt.Errorf(
			"encode Byron address root: %w",
			err,
		)
	}
	hash := sha3.Sum256(root)
	return lcommon.Blake2b224Hash(hash[:]), nil
}
