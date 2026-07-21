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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// ErrExUnitsOverflow is returned when ExUnits
// summation would overflow int64.
var ErrExUnitsOverflow = errors.New(
	"execution units overflow int64",
)

type phase2ValidationSkipper interface {
	SkipPhase2Validation() bool
}

// MinPoolMarginProvider is satisfied by the dingo ledger state to expose the
// CIP-23 minimum pool margin to era validation, mirroring phase2ValidationSkipper.
// Exported so implementers (e.g. *ledger.LedgerView) can assert conformance at
// compile time; a signature drift here would otherwise silently disable the
// CIP-23 pool-margin-floor certificate rule at runtime.
type MinPoolMarginProvider interface {
	MinPoolMargin() *big.Rat
}

// minPoolMarginFromLedgerState returns the CIP-23 minimum pool margin the ledger
// state enforces, or nil when the state does not provide one (feature disabled).
func minPoolMarginFromLedgerState(ls lcommon.LedgerState) *big.Rat {
	provider, ok := ls.(MinPoolMarginProvider)
	if !ok {
		return nil
	}
	return provider.MinPoolMargin()
}

// checkPoolMarginFloor enforces the CIP-23 rule that each pool registration
// certificate's margin (variable fee) is at least minMargin. It is a no-op when
// minMargin is nil (feature disabled). A nil certificate margin is treated as 0.
// Non-pool-registration certificates are ignored.
func checkPoolMarginFloor(certs []lcommon.Certificate, minMargin *big.Rat) error {
	if minMargin == nil {
		return nil
	}
	for _, cert := range certs {
		reg, ok := cert.(*lcommon.PoolRegistrationCertificate)
		if !ok {
			continue
		}
		if reg == nil {
			continue
		}
		margin := reg.Margin.Rat
		if margin == nil {
			margin = new(big.Rat)
		}
		if margin.Cmp(minMargin) < 0 {
			return fmt.Errorf(
				"pool %x margin %s below minimum pool margin %s",
				reg.Operator,
				margin.RatString(),
				minMargin.RatString(),
			)
		}
	}
	return nil
}

type indexedUtxoValidationRule struct {
	index          int
	validationFunc lcommon.UtxoValidationRuleFunc
}

type utxoValidationRuleSkip struct {
	index          int
	validationFunc lcommon.UtxoValidationRuleFunc
	name           string
}

const (
	noUtxoValidationRuleIndex = -1

	// Positions in gouroboros v0.180.1 UtxoValidationRules. Function
	// values are not directly comparable in Go, so setup guards compare
	// function pointers before filtering by index.
	alonzoUtxoValidatePlutusScriptsRuleIndex   = 26
	babbageUtxoValidatePlutusScriptsRuleIndex  = 30
	conwayUtxoValidateFeeTooSmallRuleIndex     = 19
	conwayUtxoValidateExUnitsTooBigRuleIndex   = 34
	conwayUtxoValidatePlutusScriptsRuleIndex   = 38
	dijkstraUtxoValidatePlutusScriptsRuleIndex = 38

	conwayRefScriptCostStride = 25_600
)

func shouldSkipPhase2Validation(
	ls lcommon.LedgerState,
) bool {
	skipper, ok := ls.(phase2ValidationSkipper)
	return ok && skipper.SkipPhase2Validation()
}

func buildIndexedUtxoValidationRules(
	rules []lcommon.UtxoValidationRuleFunc,
	skipIndex int,
	skipValidationFunc lcommon.UtxoValidationRuleFunc,
	skipRuleName string,
) []indexedUtxoValidationRule {
	if skipIndex != noUtxoValidationRuleIndex {
		return buildIndexedUtxoValidationRulesWithSkips(
			rules,
			[]utxoValidationRuleSkip{
				{
					index:          skipIndex,
					validationFunc: skipValidationFunc,
					name:           skipRuleName,
				},
			},
		)
	}
	return buildIndexedUtxoValidationRulesWithSkips(rules, nil)
}

func buildIndexedUtxoValidationRulesWithSkips(
	rules []lcommon.UtxoValidationRuleFunc,
	skips []utxoValidationRuleSkip,
) []indexedUtxoValidationRule {
	skipIndexes := map[int]struct{}{}
	for _, skip := range skips {
		if skip.index == noUtxoValidationRuleIndex {
			continue
		}
		validateUtxoValidationSkipIndex(
			rules,
			skip.index,
			skip.validationFunc,
			skip.name,
		)
		skipIndexes[skip.index] = struct{}{}
	}
	ret := make([]indexedUtxoValidationRule, 0, len(rules))
	for idx, validationFunc := range rules {
		if _, ok := skipIndexes[idx]; ok {
			continue
		}
		ret = append(ret, indexedUtxoValidationRule{
			index:          idx,
			validationFunc: validationFunc,
		})
	}
	return ret
}

func validateUtxoValidationSkipIndex(
	rules []lcommon.UtxoValidationRuleFunc,
	skipIndex int,
	skipValidationFunc lcommon.UtxoValidationRuleFunc,
	skipRuleName string,
) {
	if skipRuleName == "" {
		skipRuleName = "UTxO validation skip rule"
	}
	if skipIndex < 0 {
		panic(fmt.Sprintf(
			"%s has invalid negative hardcoded rule index %d",
			skipRuleName,
			skipIndex,
		))
	}
	if skipIndex >= len(rules) {
		panic(fmt.Sprintf(
			"%s hardcoded rule index %d is outside upstream rules length %d",
			skipRuleName,
			skipIndex,
			len(rules),
		))
	}
	if skipValidationFunc == nil {
		panic(skipRuleName + " expected validation function is nil")
	}
	if utxoValidationRulePtr(rules[skipIndex]) != utxoValidationRulePtr(skipValidationFunc) {
		panic(fmt.Sprintf(
			"%s hardcoded rule index %d no longer resolves to the expected function",
			skipRuleName,
			skipIndex,
		))
	}
}

func utxoValidationRulePtr(fn lcommon.UtxoValidationRuleFunc) uintptr {
	if fn == nil {
		return 0
	}
	return reflect.ValueOf(fn).Pointer()
}

// SafeAddExUnits adds two ExUnits values with
// overflow detection. Returns an error if either
// the Memory or Steps sum would exceed
// math.MaxInt64.
func SafeAddExUnits(
	a, b lcommon.ExUnits,
) (lcommon.ExUnits, error) {
	if a.Memory < 0 || b.Memory < 0 {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: negative memory %d, %d",
			ErrExUnitsOverflow,
			a.Memory,
			b.Memory,
		)
	}
	if a.Steps < 0 || b.Steps < 0 {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: negative steps %d, %d",
			ErrExUnitsOverflow,
			a.Steps,
			b.Steps,
		)
	}
	if a.Memory > 0 && b.Memory > math.MaxInt64-a.Memory {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: memory %d + %d",
			ErrExUnitsOverflow,
			a.Memory,
			b.Memory,
		)
	}
	if a.Steps > 0 && b.Steps > math.MaxInt64-a.Steps {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: steps %d + %d",
			ErrExUnitsOverflow,
			a.Steps,
			b.Steps,
		)
	}
	return lcommon.ExUnits{
		Memory: a.Memory + b.Memory,
		Steps:  a.Steps + b.Steps,
	}, nil
}

// txTypeAlonzo is the first era whose on-wire CBOR
// includes the 1-byte IsValid boolean field.
const txTypeAlonzo = 4

// TxSizeForFee computes the transaction size used in the
// Cardano fee formula. Per the Haskell ledger's
// toCBORForSizeComputation, this is the 3-element CBOR
// encoding [body, witnesses, auxiliary_data] — the
// IsValid boolean is excluded for backward compatibility
// with Mary-era transaction sizes. Since both 0x83 and
// 0x84 are 1-byte CBOR headers, the difference from the
// on-wire 4-element format is exactly the 1-byte IsValid
// field. Pre-Alonzo transactions (Byron through Mary) do
// not contain an IsValid byte, so their full CBOR length
// is the fee-relevant size.
func TxSizeForFee(tx lcommon.Transaction) uint64 {
	fullSize := uint64(len(tx.Cbor()))
	if fullSize > 0 && tx.Type() >= txTypeAlonzo {
		return fullSize - 1
	}
	return fullSize
}

// ValidateTxSize checks that the transaction size does
// not exceed the protocol parameter maximum.
func ValidateTxSize(
	tx lcommon.Transaction,
	maxTxSize uint,
) error {
	size := TxSizeForFee(tx)
	if size > uint64(maxTxSize) {
		return fmt.Errorf(
			"transaction size %d exceeds maximum %d",
			size,
			maxTxSize,
		)
	}
	return nil
}

// ValidateTxExUnits checks that total execution units
// do not exceed the protocol parameter per-transaction
// limits.
func ValidateTxExUnits(
	totalExUnits lcommon.ExUnits,
	maxTxExUnits lcommon.ExUnits,
) error {
	if totalExUnits.Memory > maxTxExUnits.Memory {
		return fmt.Errorf(
			"transaction memory %d exceeds maximum %d",
			totalExUnits.Memory,
			maxTxExUnits.Memory,
		)
	}
	if totalExUnits.Steps > maxTxExUnits.Steps {
		return fmt.Errorf(
			"transaction steps %d exceeds maximum %d",
			totalExUnits.Steps,
			maxTxExUnits.Steps,
		)
	}
	return nil
}

func normalizeScriptDataHashCbor(
	tx lcommon.Transaction,
) (lcommon.Transaction, error) {
	if tx.ScriptDataHash() == nil {
		return tx, nil
	}
	switch tmpTx := tx.(type) {
	case *alonzo.AlonzoTransaction:
		if !alonzoScriptDataHashCborMissing(tmpTx) {
			return tx, nil
		}
		txCbor := tmpTx.Cbor()
		if len(txCbor) == 0 {
			return tx, nil
		}
		return alonzo.NewAlonzoTransactionFromCbor(txCbor)
	case *babbage.BabbageTransaction:
		if !babbageScriptDataHashCborMissing(tmpTx) {
			return tx, nil
		}
		txCbor := tmpTx.Cbor()
		if len(txCbor) == 0 {
			return tx, nil
		}
		return babbage.NewBabbageTransactionFromCbor(txCbor)
	case *conway.ConwayTransaction:
		if !conwayScriptDataHashCborMissing(tmpTx) {
			return tx, nil
		}
		txCbor := tmpTx.Cbor()
		if len(txCbor) == 0 {
			return tx, nil
		}
		return conway.NewConwayTransactionFromCbor(txCbor)
	default:
		return tx, nil
	}
}

func alonzoScriptDataHashCborMissing(tx *alonzo.AlonzoTransaction) bool {
	return (len(tx.WitnessSet.WsRedeemers.Redeemers) > 0 &&
		len(tx.WitnessSet.WsRedeemers.Cbor()) == 0) ||
		(len(tx.WitnessSet.WsPlutusData.Items) > 0 &&
			len(tx.WitnessSet.WsPlutusData.Cbor()) == 0)
}

func babbageScriptDataHashCborMissing(tx *babbage.BabbageTransaction) bool {
	return (len(tx.WitnessSet.WsRedeemers.Redeemers) > 0 &&
		len(tx.WitnessSet.WsRedeemers.Cbor()) == 0) ||
		(len(tx.WitnessSet.WsPlutusData.Items) > 0 &&
			len(tx.WitnessSet.WsPlutusData.Cbor()) == 0)
}

func conwayScriptDataHashCborMissing(tx *conway.ConwayTransaction) bool {
	return (tx.WitnessSet.WsRedeemers.Len() > 0 &&
		len(tx.WitnessSet.WsRedeemers.Cbor()) == 0) ||
		(len(tx.WitnessSet.WsPlutusData.Items()) > 0 &&
			len(tx.WitnessSet.WsPlutusData.Cbor()) == 0)
}

// CalculateMinFee computes the minimum fee for a
// transaction using the Cardano fee formula:
//
//	fee = (minFeeA * txSize) + minFeeB + scriptFee
//
// where (per Alonzo spec txscriptfee):
//
//	scriptFee = ceil(pricesMem * mem + pricesSteps * steps)
//
// Note: a single ceiling is applied over the sum of
// both components, NOT ceil of each added together.
// Script fee arithmetic uses big.Rat to prevent
// overflow and preserve exact rational arithmetic.
func CalculateMinFee(
	txSize uint64,
	exUnits lcommon.ExUnits,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) uint64 {
	// baseFee = minFeeA * txSize + minFeeB
	a := uint64(minFeeA)
	b := uint64(minFeeB)
	var baseFee uint64
	if a != 0 && txSize > (math.MaxUint64-b)/a {
		baseFee = math.MaxUint64
	} else {
		baseFee = a*txSize + b
	}

	var scriptFee uint64
	if pricesMem != nil && pricesSteps != nil {
		// Compute exact rational sum:
		// sum = pricesMem * mem + pricesSteps * steps
		memCost := new(big.Rat).Mul(
			pricesMem,
			new(big.Rat).SetInt64(exUnits.Memory),
		)
		stepCost := new(big.Rat).Mul(
			pricesSteps,
			new(big.Rat).SetInt64(exUnits.Steps),
		)
		sum := new(big.Rat).Add(memCost, stepCost)
		scriptFee = ceilRatToUint64(sum)
	}

	return saturatedAddUint64(baseFee, scriptFee)
}

func saturatedAddUint64(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func ceilRatToUint64(val *big.Rat) uint64 {
	if val == nil {
		return 0
	}
	num := val.Num()
	denom := val.Denom()
	q, r := new(big.Int).DivMod(
		num,
		denom,
		new(big.Int),
	)
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsUint64() {
		return math.MaxUint64
	}
	return q.Uint64()
}

func floorRatToUint64(val *big.Rat) uint64 {
	if val == nil {
		return 0
	}
	q := new(big.Int).Div(val.Num(), val.Denom())
	if q.Sign() < 0 {
		return 0
	}
	if !q.IsUint64() {
		return math.MaxUint64
	}
	return q.Uint64()
}

func calculateTieredRefScriptFee(
	refScriptSize uint64,
	costPerByte *big.Rat,
	stride uint64,
	multiplier *big.Rat,
) uint64 {
	if refScriptSize == 0 || costPerByte == nil || stride == 0 {
		return 0
	}
	if multiplier == nil {
		multiplier = big.NewRat(1, 1)
	}
	remaining := refScriptSize
	currentCostPerByte := new(big.Rat).Set(costPerByte)
	total := new(big.Rat)
	for remaining > 0 {
		chunkSize := min(remaining, stride)
		chunkFee := new(big.Rat).Mul(
			currentCostPerByte,
			new(big.Rat).SetInt(
				new(big.Int).SetUint64(chunkSize),
			),
		)
		total.Add(total, chunkFee)
		remaining -= chunkSize
		currentCostPerByte.Mul(currentCostPerByte, multiplier)
	}
	return floorRatToUint64(total)
}

func CalculateConwayRefScriptFee(
	refScriptSize uint64,
	costPerByte *big.Rat,
) uint64 {
	return calculateTieredRefScriptFee(
		refScriptSize,
		costPerByte,
		conwayRefScriptCostStride,
		big.NewRat(6, 5),
	)
}

func CalculateConwayMinFee(
	txSize uint64,
	exUnits lcommon.ExUnits,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
	refScriptSize uint64,
	refScriptCostPerByte *big.Rat,
) uint64 {
	baseAndExecutionFee := CalculateMinFee(
		txSize,
		exUnits,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
	refScriptFee := CalculateConwayRefScriptFee(
		refScriptSize,
		refScriptCostPerByte,
	)
	return saturatedAddUint64(baseAndExecutionFee, refScriptFee)
}

func ReferencedScriptSize(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
) (uint64, error) {
	utxos, err := referencedScriptUtxos(tx, ls)
	if err != nil {
		return 0, err
	}
	return ReferenceScriptSizeFromUtxos(utxos)
}

func ReferenceScriptSizeFromUtxos(
	utxos []lcommon.Utxo,
) (uint64, error) {
	var total uint64
	seen := make(map[string]struct{}, len(utxos))
	for _, utxo := range utxos {
		if utxo.Output == nil {
			continue
		}
		scriptRef := utxo.Output.ScriptRef()
		if scriptRef == nil {
			continue
		}
		if utxo.Id != nil {
			key := utxo.Id.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		size := uint64(len(scriptRef.RawScriptBytes()))
		if size > math.MaxUint64-total {
			return 0, errors.New("reference script size overflow")
		}
		total += size
	}
	return total, nil
}

func referencedScriptUtxos(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
) ([]lcommon.Utxo, error) {
	inputs := tx.Inputs()
	refInputs := tx.ReferenceInputs()
	if len(inputs) == 0 && len(refInputs) == 0 {
		return nil, nil
	}
	if ls == nil {
		return nil, errors.New(
			"ledger state unavailable for reference script fee calculation",
		)
	}
	utxos := make([]lcommon.Utxo, 0, len(inputs)+len(refInputs))
	for _, input := range inputs {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			return nil, lcommon.InputResolutionError{
				Input: input,
				Err:   err,
			}
		}
		utxos = append(utxos, utxo)
	}
	for _, input := range refInputs {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			return nil, lcommon.ReferenceInputResolutionError{
				Input: input,
				Err:   err,
			}
		}
		utxos = append(utxos, utxo)
	}
	return utxos, nil
}

// DeclaredExUnits returns the total execution units
// declared across all redeemers in a transaction's
// witness set. These are the budgets the transaction
// builder committed to (not the evaluated actuals).
// Returns an error if the summation would overflow
// int64.
func DeclaredExUnits(
	tx lcommon.Transaction,
) (lcommon.ExUnits, error) {
	var total lcommon.ExUnits
	wits := tx.Witnesses()
	if wits == nil {
		return total, nil
	}
	redeemers := wits.Redeemers()
	if redeemers == nil {
		return total, nil
	}
	for _, val := range redeemers.Iter() {
		var err error
		total, err = SafeAddExUnits(total, val.ExUnits)
		if err != nil {
			return lcommon.ExUnits{}, fmt.Errorf(
				"summing redeemer execution units: %w",
				err,
			)
		}
	}
	return total, nil
}

// ValidateTxFee checks that the fee declared in the
// transaction body is at least the calculated minimum
// fee, including both the base fee component and the
// script execution fee component.
//
// The minimum fee formula (Alonzo+ eras), computed by
// CalculateMinFee:
//
//	minFee = (minFeeA * txSize) + minFeeB
//	       + ceil(pricesMem * totalMem
//	            + pricesSteps * totalSteps)
//
// Note: a single ceiling is applied over the combined
// script cost sum, not per-component.
//
// Returns nil if the declared fee is sufficient.
func ValidateTxFee(
	tx lcommon.Transaction,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) error {
	txSize := TxSizeForFee(tx)
	declaredEU, err := DeclaredExUnits(tx)
	if err != nil {
		return fmt.Errorf(
			"calculating declared execution units: %w",
			err,
		)
	}
	minFee := CalculateMinFee(
		txSize,
		declaredEU,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
	txFee := tx.Fee()
	if txFee == nil {
		txFee = new(big.Int)
	}
	minFeeBig := new(big.Int).SetUint64(minFee)
	if txFee.Cmp(minFeeBig) >= 0 {
		return nil
	}
	return fmt.Errorf(
		"transaction fee %d is less than the calculated "+
			"minimum fee %d",
		txFee,
		minFeeBig,
	)
}
