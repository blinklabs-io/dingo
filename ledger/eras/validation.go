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
	"iter"
	"math"
	"math/big"

	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
func checkPoolMarginFloor(
	certs []lcommon.Certificate,
	minMargin *big.Rat,
) error {
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
	id             utxoValidationRuleId
	validationFunc lcommon.UtxoValidationRuleFunc
}

type utxoValidationRuleId string

const (
	utxoValidationRuleConwayFeaturesWithPlutusV1V2 utxoValidationRuleId = "conway-features-with-plutus-v1-v2"
	utxoValidationRuleFeeTooSmall                  utxoValidationRuleId = "fee-too-small"
	utxoValidationRuleMaxTxSize                    utxoValidationRuleId = "max-tx-size"
	utxoValidationRulePlutusScripts                utxoValidationRuleId = "plutus-scripts"
)

type utxoValidationRuleClassifier func(
	lcommon.UtxoValidationRuleFunc,
) bool

type utxoValidationRuleReplacement struct {
	id              utxoValidationRuleId
	classifier      utxoValidationRuleClassifier
	replacementFunc lcommon.UtxoValidationRuleFunc
}

type utxoValidationRuleProbe struct {
	tx lcommon.Transaction
	ls lcommon.LedgerState
	pp lcommon.ProtocolParameters
}

type utxoValidationRuleProbeTx struct {
	lcommon.Transaction
	cbor                 []byte
	fee                  *big.Int
	isValid              bool
	witnesses            lcommon.TransactionWitnessSet
	currentTreasuryValue *big.Int
}

func (t *utxoValidationRuleProbeTx) Cbor() []byte {
	return t.cbor
}

func (*utxoValidationRuleProbeTx) Type() int {
	return 0
}

func (t *utxoValidationRuleProbeTx) Fee() *big.Int {
	return t.fee
}

func (t *utxoValidationRuleProbeTx) IsValid() bool {
	return t.isValid
}

func (t *utxoValidationRuleProbeTx) Witnesses() lcommon.TransactionWitnessSet {
	return t.witnesses
}

func (*utxoValidationRuleProbeTx) Inputs() []lcommon.TransactionInput {
	return nil
}

func (*utxoValidationRuleProbeTx) ReferenceInputs() []lcommon.TransactionInput {
	return nil
}

func (*utxoValidationRuleProbeTx) Certificates() []lcommon.Certificate {
	return nil
}

func (*utxoValidationRuleProbeTx) Withdrawals() map[*lcommon.Address]*big.Int {
	return nil
}

func (*utxoValidationRuleProbeTx) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return nil
}

func (*utxoValidationRuleProbeTx) VotingProcedures() lcommon.VotingProcedures {
	return nil
}

func (*utxoValidationRuleProbeTx) ProposalProcedures() []lcommon.ProposalProcedure {
	return nil
}

func (t *utxoValidationRuleProbeTx) CurrentTreasuryValue() *big.Int {
	return t.currentTreasuryValue
}

type utxoValidationRuleProbeWitnesses struct {
	redeemers       lcommon.TransactionWitnessRedeemers
	plutusV1Scripts []lcommon.PlutusV1Script
}

func (*utxoValidationRuleProbeWitnesses) Vkey() []lcommon.VkeyWitness {
	return nil
}

func (*utxoValidationRuleProbeWitnesses) NativeScripts() []lcommon.NativeScript {
	return nil
}

func (*utxoValidationRuleProbeWitnesses) Bootstrap() []lcommon.BootstrapWitness {
	return nil
}

func (*utxoValidationRuleProbeWitnesses) PlutusData() []lcommon.Datum {
	return nil
}

func (w *utxoValidationRuleProbeWitnesses) PlutusV1Scripts() []lcommon.PlutusV1Script {
	return w.plutusV1Scripts
}

func (*utxoValidationRuleProbeWitnesses) PlutusV2Scripts() []lcommon.PlutusV2Script {
	return nil
}

func (*utxoValidationRuleProbeWitnesses) PlutusV3Scripts() []lcommon.PlutusV3Script {
	return nil
}

func (w *utxoValidationRuleProbeWitnesses) Redeemers() lcommon.TransactionWitnessRedeemers {
	return w.redeemers
}

type utxoValidationRuleProbeRedeemers struct{}

func (utxoValidationRuleProbeRedeemers) Indexes(lcommon.RedeemerTag) []uint {
	return nil
}

func (utxoValidationRuleProbeRedeemers) Value(
	uint,
	lcommon.RedeemerTag,
) lcommon.RedeemerValue {
	return lcommon.RedeemerValue{}
}

func (utxoValidationRuleProbeRedeemers) Iter() iter.Seq2[
	lcommon.RedeemerKey,
	lcommon.RedeemerValue,
] {
	return func(yield func(lcommon.RedeemerKey, lcommon.RedeemerValue) bool) {
		yield(
			lcommon.RedeemerKey{Tag: lcommon.RedeemerTagSpend},
			lcommon.RedeemerValue{},
		)
	}
}

const conwayRefScriptCostStride = 25_600

func shouldSkipPhase2Validation(
	ls lcommon.LedgerState,
) bool {
	skipper, ok := ls.(phase2ValidationSkipper)
	return ok && skipper.SkipPhase2Validation()
}

// validatePlutusOutcome requires the locally evaluated phase-2 result to
// match the transaction's declared validity flag. A failed script is the
// expected outcome for an invalid transaction; every other validation error
// remains a hard failure because it does not establish that script execution
// itself failed.
func validatePlutusOutcome(tx lcommon.Transaction, phase2Err error) error {
	if tx.IsValid() {
		return phase2Err
	}
	if phase2Err == nil {
		return errors.New(
			"transaction declared invalid but Plutus scripts succeeded",
		)
	}
	if _, ok := errors.AsType[conway.PlutusScriptFailedError](phase2Err); ok {
		return nil
	}
	return phase2Err
}

// txHasRedeemers reports whether the transaction carries at least one redeemer.
//
// Redeemers are what drive Plutus phase-2 evaluation: every Plutus script a
// transaction runs needs one, so a transaction without any runs no Plutus
// script. Native scripts are phase-1 and never reach evaluation.
//
// Callers use this to avoid building a Plutus script context (TxInfo) for a
// transaction that has no script to evaluate. That is not merely wasted work:
// the context embeds the transaction's validity interval translated to
// wall-clock time, so building it converts the transaction's TTL through the
// bounded HFC forecast horizon and fails with hardfork.ErrPastHorizon whenever
// the TTL lies past that horizon — rejecting canonical, script-free
// transactions. cardano-ledger only translates the validity interval while
// assembling the context for the Plutus scripts a transaction actually needs
// (Alonzo collectPlutusScriptsWithContext), and ValidateTxConway already
// follows that shape here via conwayTxInfoCache.
func txHasRedeemers(tx lcommon.Transaction) bool {
	witnesses := tx.Witnesses()
	if witnesses == nil {
		return false
	}
	redeemers := witnesses.Redeemers()
	if redeemers == nil {
		return false
	}
	for range redeemers.Iter() {
		return true
	}
	return false
}

// utxoValidationRuleErrorClassifier identifies a rule by the concrete error
// it returns for a controlled transaction. Unrelated rules can require ledger
// state or transaction fields that a probe intentionally does not provide, so
// a panic means only that the rule did not match this classifier.
func utxoValidationRuleErrorClassifier[E error](
	probe utxoValidationRuleProbe,
) utxoValidationRuleClassifier {
	return func(rule lcommon.UtxoValidationRuleFunc) (matched bool) {
		defer func() {
			if recover() != nil {
				matched = false
			}
		}()
		err := rule(probe.tx, 0, probe.ls, probe.pp)
		_, matched = errors.AsType[E](err)
		return matched
	}
}

func feeTooSmallUtxoValidationRuleClassifier(
	pp lcommon.ProtocolParameters,
) utxoValidationRuleClassifier {
	return utxoValidationRuleErrorClassifier[shelley.FeeTooSmallUtxoError](
		utxoValidationRuleProbe{
			tx: &utxoValidationRuleProbeTx{
				cbor: []byte{0x80},
				fee:  new(big.Int),
			},
			pp: pp,
		},
	)
}

func maxTxSizeUtxoValidationRuleClassifier(
	pp lcommon.ProtocolParameters,
) utxoValidationRuleClassifier {
	return utxoValidationRuleErrorClassifier[shelley.MaxTxSizeUtxoError](
		utxoValidationRuleProbe{
			tx: &utxoValidationRuleProbeTx{
				cbor: []byte{0x80},
				fee:  new(big.Int),
			},
			pp: pp,
		},
	)
}

// unsupportedPlutusUtxoValidationRuleClassifier probes for the rule that
// rejects phase-2 execution in an era that does not implement it. That rule
// keys off the presence of a redeemer, so the redeemer alone identifies it. The
// probe also carries a Plutus script so it stays a representative phase-2
// witness set: an unmatched replacement panics during package initialisation,
// so the probe should not depend on the rule ignoring the script.
func unsupportedPlutusUtxoValidationRuleClassifier(
	pp lcommon.ProtocolParameters,
) utxoValidationRuleClassifier {
	return utxoValidationRuleErrorClassifier[lcommon.PlutusScriptValidationUnsupportedError](utxoValidationRuleProbe{
		tx: &utxoValidationRuleProbeTx{
			isValid: true,
			witnesses: &utxoValidationRuleProbeWitnesses{
				redeemers:       utxoValidationRuleProbeRedeemers{},
				plutusV1Scripts: []lcommon.PlutusV1Script{{0x01}},
			},
		},
		pp: pp,
	})
}

func conwayPlutusUtxoValidationRuleClassifier(
	pp lcommon.ProtocolParameters,
) utxoValidationRuleClassifier {
	witnesses := &utxoValidationRuleProbeWitnesses{
		redeemers: utxoValidationRuleProbeRedeemers{},
	}
	return func(rule lcommon.UtxoValidationRuleFunc) (matched bool) {
		defer func() {
			if recover() != nil {
				matched = false
			}
		}()
		validErr := rule(
			&utxoValidationRuleProbeTx{
				isValid:   true,
				witnesses: witnesses,
			},
			0,
			nil,
			pp,
		)
		if _, ok := errors.AsType[conway.ExtraRedeemerError](validErr); !ok {
			return false
		}
		// Phase-2 execution is skipped when a block producer declares the
		// transaction invalid. The phase-1 extraneous-redeemer rule returns
		// the same concrete error for the valid probe but still runs here.
		return rule(
			&utxoValidationRuleProbeTx{witnesses: witnesses},
			0,
			nil,
			pp,
		) == nil
	}
}

// buildIndexedUtxoValidationRules finds each target by stable validation
// behavior and preserves the target's original upstream position. A nil
// replacement removes the target; a non-nil replacement substitutes it in
// place. Invalid, ambiguous, duplicate, or missing metadata fails closed.
func buildIndexedUtxoValidationRules(
	rules []lcommon.UtxoValidationRuleFunc,
	replacements ...utxoValidationRuleReplacement,
) []indexedUtxoValidationRule {
	for idx, rule := range rules {
		if rule == nil {
			panic(fmt.Sprintf(
				"UTxO validation rule at index %d is nil",
				idx,
			))
		}
	}

	replacementIndexById := make(
		map[utxoValidationRuleId]int,
		len(replacements),
	)
	for idx, replacement := range replacements {
		if replacement.id == "" {
			panic("UTxO validation rule replacement has an empty ID")
		}
		if replacement.classifier == nil {
			panic(fmt.Sprintf(
				"UTxO validation rule replacement ID %q has a nil classifier",
				replacement.id,
			))
		}
		if _, ok := replacementIndexById[replacement.id]; ok {
			panic(fmt.Sprintf(
				"UTxO validation rule replacement ID %q is configured more than once",
				replacement.id,
			))
		}
		replacementIndexById[replacement.id] = idx
	}

	matchedRuleIndexById := make(map[utxoValidationRuleId]int, len(replacements))
	ret := make([]indexedUtxoValidationRule, 0, len(rules))
	for idx, validationFunc := range rules {
		var matchedReplacement utxoValidationRuleReplacement
		matchedReplacementFound := false
		for _, replacement := range replacements {
			if !replacement.classifier(validationFunc) {
				continue
			}
			if matchedReplacementFound {
				panic(fmt.Sprintf(
					"UTxO validation rule at index %d matches replacement IDs %q and %q",
					idx,
					matchedReplacement.id,
					replacement.id,
				))
			}
			if previousIdx, ok := matchedRuleIndexById[replacement.id]; ok {
				panic(fmt.Sprintf(
					"UTxO validation rule replacement ID %q matches upstream rules at indexes %d and %d",
					replacement.id,
					previousIdx,
					idx,
				))
			}
			matchedReplacement = replacement
			matchedReplacementFound = true
		}

		var ruleId utxoValidationRuleId
		if matchedReplacementFound {
			matchedRuleIndexById[matchedReplacement.id] = idx
			ruleId = matchedReplacement.id
			validationFunc = matchedReplacement.replacementFunc
			if validationFunc == nil {
				continue
			}
		}
		ret = append(ret, indexedUtxoValidationRule{
			index:          idx,
			id:             ruleId,
			validationFunc: validationFunc,
		})
	}
	for _, replacement := range replacements {
		if _, ok := matchedRuleIndexById[replacement.id]; !ok {
			panic(fmt.Sprintf(
				"UTxO validation rule replacement ID %q was not found in upstream rules",
				replacement.id,
			))
		}
	}
	return ret
}

// buildIndexedUtxoValidationRuleDescriptors preserves the upstream rule IDs
// while allowing Dingo to replace selected validators. The descriptor API is
// authoritative; matching by validator behavior is only needed for older
// gouroboros releases.
func buildIndexedUtxoValidationRuleDescriptors(
	descriptors []lcommon.UtxoValidationRuleDescriptor,
	replacements ...utxoValidationRuleReplacement,
) []indexedUtxoValidationRule {
	matched := make(map[utxoValidationRuleId]bool, len(replacements))
	ret := make([]indexedUtxoValidationRule, 0, len(descriptors))
	for idx, descriptor := range descriptors {
		if descriptor.Validator == nil {
			panic(fmt.Sprintf("UTxO validation rule at index %d is nil", idx))
		}
		ruleID := utxoValidationRuleId(descriptor.Id)
		validationFunc := descriptor.Validator
		for _, replacement := range replacements {
			replacementRuleID := replacement.id
			if replacementRuleID == utxoValidationRuleMaxTxSize {
				// The shared descriptor uses the canonical name introduced
				// with the descriptor API; retain Dingo's internal ID.
				replacementRuleID = utxoValidationRuleId("max-transaction-size")
			}
			if replacementRuleID != ruleID {
				continue
			}
			if matched[replacement.id] {
				panic(fmt.Sprintf("UTxO validation rule replacement ID %q matches multiple upstream rules", replacement.id))
			}
			matched[replacement.id] = true
			validationFunc = replacement.replacementFunc
			break
		}
		if validationFunc != nil {
			ret = append(ret, indexedUtxoValidationRule{
				index:          idx,
				id:             ruleID,
				validationFunc: validationFunc,
			})
		}
	}
	for _, replacement := range replacements {
		if !matched[replacement.id] {
			panic(fmt.Sprintf("UTxO validation rule replacement ID %q was not found in upstream descriptors", replacement.id))
		}
	}
	return ret
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
// is the fee-relevant size — except when the transaction
// was rebuilt from block components, which
// preAlonzoRebuiltWireSize handles.
func TxSizeForFee(tx lcommon.Transaction) uint64 {
	if size, ok := preAlonzoRebuiltWireSize(tx); ok {
		return size
	}
	fullSize := uint64(len(tx.Cbor()))
	if fullSize > 0 && tx.Type() >= txTypeAlonzo {
		return fullSize - 1
	}
	return fullSize
}

// preAlonzoRebuiltWireSize returns the wire size of a Shelley or Allegra
// transaction that was rebuilt from separately decoded components rather than
// decoded from a complete transaction encoding.
//
// ShelleyBlock.Transactions and AllegraBlock.Transactions construct each
// transaction from the block's parallel body, witness-set, and auxiliary-data
// arrays, so the resulting value carries no stored transaction CBOR. The
// current upstream body and witness encoders preserve their component wire
// bytes; this helper explicitly reconstructs the fee-relevant transaction
// size from those bytes and the three-element transaction envelope.
//
// The size is rebuilt from the preserved component bytes: a 1-byte
// definite-length 3-element array header, the body and witness-set bytes as
// they appeared on the wire, and either the auxiliary data bytes or a 1-byte
// CBOR null. A non-empty body or witness-set Cbor() is only ever set by that
// component's UnmarshalCBOR, so these bytes are the ones that were decoded.
//
// Transactions that do carry stored transaction CBOR are left to the caller's
// len(tx.Cbor()), so no size that a node observed on the wire is recomputed
// here. MaryTransactionBody likewise implements MarshalCBOR and returns its
// preserved bytes, so Mary does not need this helper.
func preAlonzoRebuiltWireSize(tx lcommon.Transaction) (uint64, bool) {
	var storedCbor, bodyCbor, witnessCbor []byte
	var auxData lcommon.AuxiliaryData
	var metadata lcommon.TransactionMetadatum
	switch tmpTx := tx.(type) {
	case *shelley.ShelleyTransaction:
		storedCbor = tmpTx.DecodeStoreCbor.Cbor()
		bodyCbor = tmpTx.Body.Cbor()
		witnessCbor = tmpTx.WitnessSet.Cbor()
		auxData = tmpTx.AuxiliaryData()
		metadata = tmpTx.Metadata()
	case *allegra.AllegraTransaction:
		storedCbor = tmpTx.DecodeStoreCbor.Cbor()
		bodyCbor = tmpTx.Body.Cbor()
		witnessCbor = tmpTx.WitnessSet.Cbor()
		auxData = tmpTx.AuxiliaryData()
		metadata = tmpTx.Metadata()
	default:
		return 0, false
	}
	if len(storedCbor) > 0 {
		// Decoded from a complete transaction encoding, so those are the
		// bytes the node received.
		return 0, false
	}
	if len(bodyCbor) == 0 || len(witnessCbor) == 0 {
		return 0, false
	}
	// The third wire element is the auxiliary data, or CBOR null when the
	// transaction has none. Bail out rather than guess when auxiliary data is
	// present but its original bytes are not, so the caller falls back to
	// len(tx.Cbor()).
	auxSize := 1 // CBOR null auxiliary data
	switch {
	case auxData != nil && len(auxData.Cbor()) > 0:
		auxSize = len(auxData.Cbor())
	case auxData != nil || metadata != nil:
		return 0, false
	}
	// 1 byte for the definite-length 3-element array header.
	return uint64(1 + len(bodyCbor) + len(witnessCbor) + auxSize), true
}

// validatePreAlonzoTx runs a pre-Alonzo era's UTxO validation rules and then
// applies Dingo's size and fee checks in place of the upstream fee and
// max-size rules that buildIndexedUtxoValidationRules removed.
//
// Both replacements derive their size from TxSizeForFee. Keeping both checks
// on TxSizeForFee makes the local validation path explicit and consistent with
// the fee calculation, matching cardano-ledger, where validateMaxTxSizeUTxO
// and the minimum-fee calculation both read sizeTxF.
func validatePreAlonzoTx(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
	rules []indexedUtxoValidationRule,
	maxTxSize uint,
	minFeeA uint,
	minFeeB uint,
) error {
	errs := make([]error, 0, len(rules)+2)
	for _, rule := range rules {
		errs = append(errs, rule.validationFunc(tx, slot, ls, pp))
	}
	errs = append(errs, ValidateTxSize(tx, maxTxSize))
	errs = append(errs, ValidateTxFee(tx, minFeeA, minFeeB, nil, nil))
	return errors.Join(errs...)
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
