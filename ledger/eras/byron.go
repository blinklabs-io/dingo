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

// TxTooLargeByronError is returned when a serialized Byron TxAux exceeds the
// active ppMaxTxSize (TxValidationTxTooLarge).
type TxTooLargeByronError struct {
	Size uint64
	Max  uint64
}

func (e TxTooLargeByronError) Error() string {
	return fmt.Sprintf(
		"transaction size %d exceeds maximum %d",
		e.Size,
		e.Max,
	)
}

// UnknownAttributesByronError is returned when the unknown transaction
// attributes reach the 128-byte limit (TxValidationUnknownAttributes).
type UnknownAttributesByronError struct {
	Size int
}

func (e UnknownAttributesByronError) Error() string {
	return fmt.Sprintf(
		"unknown transaction attributes are %d bytes, limit is %d",
		e.Size,
		byronMaxUnknownAttributesSize-1,
	)
}

// UnknownAddressAttributesByronError is returned when the unknown attributes
// of an output address reach the 128-byte limit
// (TxValidationUnknownAddressAttributes).
type UnknownAddressAttributesByronError struct {
	OutputIndex int
	Size        int
}

func (e UnknownAddressAttributesByronError) Error() string {
	return fmt.Sprintf(
		"output %d address has %d bytes of unknown attributes, limit is %d",
		e.OutputIndex,
		e.Size,
		byronMaxUnknownAttributesSize-1,
	)
}

// LovelaceBoundByronError is returned when a Byron balance or minimum fee
// leaves the Lovelace domain [0, 45e15] (TxValidationLovelaceError).
type LovelaceBoundByronError struct {
	Balance string
	Value   *big.Int
}

func (e LovelaceBoundByronError) Error() string {
	return fmt.Sprintf(
		"%s %s exceeds the maximum Lovelace value %d",
		e.Balance,
		e.Value.String(),
		byronMaxLovelace,
	)
}

// WitnessWrongKeyByronError is returned when the witness at an input's
// position does not authorize that input's address
// (TxValidationWitnessWrongKey).
type WitnessWrongKeyByronError struct {
	InputIndex int
	Input      string
}

func (e WitnessWrongKeyByronError) Error() string {
	return fmt.Sprintf(
		"witness %d does not authorize input %s",
		e.InputIndex,
		e.Input,
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

// ByronProtocolParametersProvider supplies the Byron protocol parameters in
// effect for a transaction validated outside block application: those adopted
// as of the ledger tip, or the genesis parameters before any update has been
// adopted. Block application passes the parameters adopted for the block's own
// epoch as the pparams argument instead.
type ByronProtocolParametersProvider interface {
	ByronProtocolParameters() (*ByronProtocolParameters, error)
}

// byronProtocolParameters returns the parameters a Byron rule validates
// against, or nil when neither pp nor ls carries them.
func byronProtocolParameters(
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (*ByronProtocolParameters, error) {
	if params, ok := pp.(*ByronProtocolParameters); ok && params != nil {
		return params, nil
	}
	provider, ok := ls.(ByronProtocolParametersProvider)
	if !ok {
		// Lightweight ledger-state implementations used by structural
		// callers do not necessarily expose chain configuration. The
		// production LedgerState does, and enforces the rules there.
		return nil, nil
	}
	params, err := provider.ByronProtocolParameters()
	if err != nil {
		return nil, fmt.Errorf("get Byron protocol parameters: %w", err)
	}
	return params, nil
}

const (
	// byronMaxLovelace is maxLovelaceVal: every Byron Lovelace value,
	// including an aggregate balance, must stay at or below it.
	byronMaxLovelace = 45_000_000_000_000_000
	// byronMaxUnknownAttributesSize is the exclusive bound on
	// unknownAttributesLength: the reference requires the sum to be < 128.
	byronMaxUnknownAttributesSize = 128
	// byronFeePolicyScale is the 10^9 scale of genesis fee coefficients.
	byronFeePolicyScale = 1_000_000_000
)

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
	byronValidateUnknownAttributes,
}

// byronUtxoValidationRules require ledger state and run only
// when a LedgerState is provided.
var byronUtxoValidationRules = []lcommon.UtxoValidationRuleFunc{
	byronValidateBadInputs,
	byronValidateValueConserved,
	byronValidateOutputNetwork,
	byronValidateMaxTxSize,
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

// byronValidateUnknownAttributes enforces unknownAttributesLength < 128 for
// the transaction attributes and, independently, for every output address.
// The length is the sum of the raw attribute values the decoder does not
// interpret; recognized address attributes do not count.
func byronValidateUnknownAttributes(
	tx lcommon.Transaction,
) error {
	if byronTx, ok := tx.(*byron.ByronTransaction); ok &&
		len(byronTx.Body.Attributes) > 0 {
		// TxAttributes interprets no key, so every value is unknown.
		var attrs map[uint8][]byte
		if _, err := cbor.Decode(byronTx.Body.Attributes, &attrs); err != nil {
			return fmt.Errorf("decode transaction attributes: %w", err)
		}
		size := 0
		for _, value := range attrs {
			size += len(value)
		}
		if size >= byronMaxUnknownAttributesSize {
			return UnknownAttributesByronError{Size: size}
		}
	}
	for idx, output := range tx.Outputs() {
		addr := output.Address()
		if addr.Type() != lcommon.AddressTypeByron {
			continue
		}
		size := 0
		for _, value := range addr.ByronAttr().Unparsed {
			size += len(value)
		}
		if size >= byronMaxUnknownAttributesSize {
			return UnknownAddressAttributesByronError{
				OutputIndex: idx,
				Size:        size,
			}
		}
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

// byronInputBalance sums the resolved input UTxO once per distinct input.
// The reference restricts the UTxO to the input set (Set.fromList txInputs
// <| utxo), so a repeated input contributes once. redeemOnly reports
// isRedeemUTxO over that restriction; an unresolved input clears it because
// it is no evidence of a redeem address, and an empty input set is not
// vacuously redeem-only.
func byronInputBalance(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
) (balance *big.Int, redeemOnly bool) {
	balance = new(big.Int)
	redeemOnly = len(tx.Inputs()) > 0
	seen := make(map[string]struct{}, len(tx.Inputs()))
	for _, input := range tx.Inputs() {
		key := input.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		utxo, err := ls.UtxoById(input)
		if err != nil || utxo.Output == nil {
			// Bad inputs are caught by byronValidateBadInputs
			redeemOnly = false
			continue
		}
		addr := utxo.Output.Address()
		if addr.Type() != lcommon.AddressTypeByron ||
			addr.ByronType() != lcommon.ByronAddressTypeRedeem {
			redeemOnly = false
		}
		if amount := utxo.Output.Amount(); amount != nil {
			balance.Add(balance, amount)
		}
	}
	return balance, redeemOnly
}

func byronOutputBalance(tx lcommon.Transaction) *big.Int {
	balance := new(big.Int)
	for _, output := range tx.Outputs() {
		if amount := output.Amount(); amount != nil {
			balance.Add(balance, amount)
		}
	}
	return balance
}

// byronBalances returns the bounded output and input balances. The reference
// sums each with sumLovelace, which fails when the total leaves the Lovelace
// domain, before it subtracts them to find the fee.
func byronBalances(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
) (in *big.Int, out *big.Int, redeemOnly bool, err error) {
	out = byronOutputBalance(tx)
	if err := byronCheckLovelace("output balance", out); err != nil {
		return nil, nil, false, err
	}
	in, redeemOnly = byronInputBalance(tx, ls)
	if err := byronCheckLovelace("input balance", in); err != nil {
		return nil, nil, false, err
	}
	return in, out, redeemOnly, nil
}

func byronCheckLovelace(balance string, value *big.Int) error {
	if value.Sign() < 0 || value.Cmp(big.NewInt(byronMaxLovelace)) > 0 {
		return LovelaceBoundByronError{Balance: balance, Value: value}
	}
	return nil
}

// byronValidateValueConserved ensures that the consumed value is at least the
// produced value. In Byron the fee is implicit: it is the difference.
func byronValidateValueConserved(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	_ lcommon.ProtocolParameters,
) error {
	consumed, produced, _, err := byronBalances(tx, ls)
	if err != nil {
		return err
	}
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

// byronValidateMaxTxSize enforces ppMaxTxSize against the serialized TxAux,
// witnesses included, independently of the enclosing block size.
func byronValidateMaxTxSize(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	params, err := byronProtocolParameters(ls, pp)
	if err != nil || params == nil {
		return err
	}
	size := TxSizeForFee(tx)
	if params.MaxTxSize == nil ||
		new(big.Int).SetUint64(size).Cmp(params.MaxTxSize) <= 0 {
		return nil
	}
	return TxTooLargeByronError{Size: size, Max: params.MaxTxSize.Uint64()}
}

// byronValidateMinFee enforces the adopted Byron fee policy. Byron fees are
// implicit, so the input balance minus the output balance is the fee.
func byronValidateMinFee(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	params, err := byronProtocolParameters(ls, pp)
	if err != nil || params == nil {
		return err
	}
	consumed, produced, redeemOnly, err := byronBalances(tx, ls)
	if err != nil {
		return err
	}
	size := TxSizeForFee(tx)
	var required *big.Int
	if redeemOnly {
		// The reference exempts a transaction whose whole input UTxO is
		// redeem addresses (isRedeemUTxO). Redemption still must conserve
		// value, so a negative implicit fee fails against zero.
		required = big.NewInt(0)
	} else {
		required, err = params.MinFee(size)
		if err != nil {
			return err
		}
	}
	actual := new(big.Int).Sub(consumed, produced)
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
	// Byron witnesses have constructor-specific signature domains and key
	// layouts, so they must not pass through the generic witness verifier.
	// Byron redeem witnesses are constructor 2 values whose fields are
	// wrapped in CBOR tag 24, and a VkeyWitness has no room for the
	// chain-code half of a constructor-0 witness that
	// byronAddressRootForParts needs, so this file decodes Twit itself.
	byronTx, ok := tx.(*byron.ByronTransaction)
	if !ok {
		if err := lcommon.ValidateVKeyWitnesses(tx); err != nil {
			return err
		}
		if err := lcommon.ValidateBootstrapWitnesses(tx); err != nil {
			return err
		}
		return lcommon.ValidateInputVKeyWitnesses(tx, ls)
	}
	witnesses, err := byronDecodeWitnesses(byronTx.Twit)
	if err != nil {
		return lcommon.NewValidationError(
			lcommon.ValidationErrorTypeTransaction,
			"invalid byron transaction witness",
			map[string]any{"err": err.Error()},
			err,
		)
	}
	if len(witnesses) == 0 {
		return nil
	}
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
	// Byron witnesses sign the wire-encoded body ID. ByronTransaction.Hash
	// is the canonical ledger ID, which may differ from those bytes.
	txHash := byronTx.WireId()
	messages := make(map[byte][]byte, 2)
	for _, witness := range witnesses {
		tag := byte(0x01)
		failure := "invalid bootstrap signature"
		if witness.redeem {
			tag = 0x02
			failure = "invalid vkey signature"
		}
		message, ok := messages[tag]
		if !ok {
			message, err = byronSignatureMessage(tag, protocolMagic, txHash)
			if err != nil {
				return err
			}
			messages[tag] = message
		}
		if err := lcommon.VerifyVKeySignature(
			witness.publicKey,
			witness.signature,
			message,
		); err != nil {
			return lcommon.NewValidationError(
				lcommon.ValidationErrorTypeTransaction,
				failure,
				map[string]any{"err": err.Error()},
				err,
			)
		}
	}
	return validateByronInputWitnesses(tx, ls, witnesses)
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

// byronTxInWitness is one decoded TxInWitness: a VKWitness carrying an
// extended verification key, or a RedeemWitness carrying a redeem key.
type byronTxInWitness struct {
	redeem    bool
	publicKey []byte
	chainCode []byte
	signature []byte
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
) ([]byronTxInWitness, error) {
	ret := make([]byronTxInWitness, 0, len(witnesses))
	for idx, witness := range witnesses {
		fields, ok := witness.Value().([]any)
		if !ok || len(fields) != 2 {
			return nil, fmt.Errorf(
				"witness %d: not a 2-element TxInWitness", idx,
			)
		}
		ctor, ok := fields[0].(uint64)
		if !ok {
			return nil, fmt.Errorf(
				"witness %d: constructor tag is not an unsigned integer",
				idx,
			)
		}
		wrapped, ok := fields[1].(cbor.WrappedCbor)
		if !ok {
			return nil, fmt.Errorf(
				"witness %d: payload is not tag-24-wrapped CBOR", idx,
			)
		}
		var witnessFields [][]byte
		wrappedBytes := wrapped.Bytes()
		consumed, err := cbor.Decode(wrappedBytes, &witnessFields)
		if err != nil || consumed != len(wrappedBytes) {
			return nil, fmt.Errorf(
				"witness %d: failed to decode tag-24 payload", idx,
			)
		}
		switch ctor {
		case lcommon.ByronAddressTypePubkey:
			if len(witnessFields) != 2 || len(witnessFields[0]) != 64 ||
				len(witnessFields[1]) != 64 {
				return nil, fmt.Errorf(
					"witness %d: malformed VKWitness fields", idx,
				)
			}
			ret = append(ret, byronTxInWitness{
				publicKey: witnessFields[0][:32],
				chainCode: witnessFields[0][32:],
				signature: witnessFields[1],
			})
		case lcommon.ByronAddressTypeRedeem:
			if len(witnessFields) != 2 || len(witnessFields[0]) != 32 ||
				len(witnessFields[1]) != 64 {
				return nil, fmt.Errorf(
					"witness %d: malformed RedeemWitness fields", idx,
				)
			}
			ret = append(ret, byronTxInWitness{
				redeem:    true,
				publicKey: witnessFields[0],
				signature: witnessFields[1],
			})
		default:
			return nil, fmt.Errorf(
				"witness %d: unknown TxInWitness constructor %d", idx, ctor,
			)
		}
	}
	return ret, nil
}

// validateByronInputWitnesses pairs witness i with input i, as the reference
// does with zip addresses witnesses, and requires each witness to authorize
// its own input's address. zip stops at the shorter list, so inputs beyond the
// last witness are not checked and witnesses beyond the last input are
// ignored.
func validateByronInputWitnesses(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	witnesses []byronTxInWitness,
) error {
	for idx, input := range tx.Inputs() {
		if idx >= len(witnesses) {
			break
		}
		utxo, err := ls.UtxoById(input)
		if err != nil || utxo.Output == nil {
			// Bad inputs are caught by byronValidateBadInputs
			continue
		}
		if !byronWitnessAuthorizes(witnesses[idx], utxo.Output.Address()) {
			return WitnessWrongKeyByronError{
				InputIndex: idx,
				Input:      input.String(),
			}
		}
	}
	return nil
}

// byronWitnessAuthorizes implements checkVerKeyAddress and checkRedeemAddress:
// the address must have the witness's spending-data type, and its root must be
// the hash of that spending data with the address's attributes.
func byronWitnessAuthorizes(
	witness byronTxInWitness,
	addr lcommon.Address,
) bool {
	if addr.Type() != lcommon.AddressTypeByron {
		return false
	}
	payload, ok := addr.PayloadPayload().(lcommon.AddressPayloadKeyHash)
	if !ok {
		return false
	}
	attrs, err := byronCanonicalAddressAttributes(addr.ByronAttr())
	if err != nil {
		return false
	}
	if witness.redeem {
		if addr.ByronType() != lcommon.ByronAddressTypeRedeem {
			return false
		}
		redeemAddr, err := lcommon.NewByronAddressRedeem(
			witness.publicKey,
			attrs,
		)
		return err == nil && redeemAddr.PaymentKeyHash() == payload.Hash
	}
	if addr.ByronType() != lcommon.ByronAddressTypePubkey {
		return false
	}
	attrsCbor, err := cbor.Encode(attrs)
	if err != nil {
		return false
	}
	root, err := byronAddressRootForParts(
		witness.publicKey,
		witness.chainCode,
		attrsCbor,
	)
	return err == nil && root == payload.Hash
}

// byronCanonicalAddressAttributes rebuilds address attributes from their
// decoded fields. The reference reconstructs the address from its semantic
// attributes when it checks a witness, so the root it hashes carries the
// canonical attribute encoding rather than the bytes the address arrived with.
// A decoded ByronAddressAttributes re-emits those original bytes, so they are
// dropped here, and the derivation path value, an encoded byte string, is
// re-encoded as well.
func byronCanonicalAddressAttributes(
	attrs lcommon.ByronAddressAttributes,
) (lcommon.ByronAddressAttributes, error) {
	attrs.SetCbor(nil)
	if len(attrs.Payload) > 0 {
		var payload []byte
		consumed, err := cbor.Decode(attrs.Payload, &payload)
		if err != nil || consumed != len(attrs.Payload) {
			return attrs, errors.New("invalid Byron address derivation path")
		}
		attrs.Payload, err = cbor.Encode(payload)
		if err != nil {
			return attrs, err
		}
	}
	return attrs, nil
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
