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
	"runtime"

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

// CommitteeCredentialState is the optional ledger-state capability used by
// Dingo's Conway and Dijkstra validation compositions to preserve key/script
// credential tags. The legacy hash-only LedgerState committee methods cannot
// distinguish credentials that share a hash.
//
// The method set is identical to gouroboros
// ledger/common.CommitteeCredentialState, so the compile-time assertions
// against this interface (ledger.LedgerView, conformance DingoStateProvider)
// also prove those types satisfy the upstream capability. Keep the two in
// sync: the upstream Conway rules type-assert for it and fail closed when the
// assertion misses, so a signature drift here would silently disable committee
// validation rather than fail to build. Once the gouroboros pin exports it,
// alias this to the upstream type instead of redeclaring it.
type CommitteeCredentialState interface {
	CommitteeStateAvailable() (bool, error)
	CommitteeCredentialMember(
		lcommon.Credential,
	) (*lcommon.CommitteeMember, error)
	CommitteeHotCredentialMember(
		lcommon.Credential,
	) (*lcommon.CommitteeMember, error)
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
	validationFunc lcommon.UtxoValidationRuleFunc
}

type utxoValidationRuleSkip struct {
	validationFunc lcommon.UtxoValidationRuleFunc
	name           string
}

const (
	noUtxoValidationRuleIndex = -1

	conwayRefScriptCostStride = 25_600
)

func shouldSkipPhase2Validation(
	ls lcommon.LedgerState,
) bool {
	skipper, ok := ls.(phase2ValidationSkipper)
	return ok && skipper.SkipPhase2Validation()
}

// validateCommitteeCertificates preserves the full cold credential identity
// when the ledger state exposes Dingo's tag-aware capability. Other state
// implementations retain the upstream hash-only behavior.
func validateCommitteeCertificates(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	// Committee certificates belong to the CERTS state transition. A
	// phase-2-invalid transaction only applies its collateral effects, so it
	// must not inspect or reject against committee state.
	if !tx.IsValid() {
		return nil
	}
	state, ok := ls.(CommitteeCredentialState)
	if !ok {
		return conway.UtxoValidateCommitteeCertificates(tx, slot, ls, pp)
	}
	// Availability is resolved on the first lookup rather than up front, so a
	// transaction carrying no committee certificate never pays for the query.
	// The second result reports whether the answer is authoritative; a nil
	// member is only non-membership when it is.
	var availabilityKnown, available bool
	committeeMember := func(
		coldCredential lcommon.Credential,
	) (*lcommon.CommitteeMember, bool, error) {
		if !availabilityKnown {
			resolved, err := state.CommitteeStateAvailable()
			if err != nil {
				return nil, false, err
			}
			available, availabilityKnown = resolved, true
		}
		if !available {
			return nil, false, nil
		}
		member, err := state.CommitteeCredentialMember(coldCredential)
		return member, true, err
	}
	for _, cert := range tx.Certificates() {
		var (
			credential lcommon.Credential
			operation  string
			authorize  bool
		)
		switch c := cert.(type) {
		case *lcommon.AuthCommitteeHotCertificate:
			credential = c.ColdCredential
			operation = "authorize hot key"
			authorize = true
		case *lcommon.ResignCommitteeColdCertificate:
			credential = c.ColdCredential
			operation = "resign"
		default:
			continue
		}
		member, authoritative, err := committeeMember(credential)
		if err != nil {
			// A failed lookup is never authorization: fail closed.
			return conway.CommitteeMemberLookupError{
				Credential: credential.Credential,
				Err:        err,
			}
		}
		if member == nil {
			if !authoritative {
				// Dingo holds no committee state for this snapshot, so
				// non-membership cannot be established. Rejecting here would
				// reject a real genesis committee member, because Dingo does
				// not seed the Conway genesis committee. See
				// LedgerView.CommitteeStateAvailable.
				continue
			}
			return conway.NotCommitteeMemberError{
				Credential: credential.Credential,
				Operation:  operation,
			}
		}
		if authorize && member.Resigned {
			return conway.ResignedCommitteeMemberHotKeyError{
				ColdKey: credential.Credential,
			}
		}
	}
	return nil
}

// validateUnknownVoters preserves the full hot credential identity for
// committee voters. DRep and stake-pool checks intentionally mirror the
// upstream rule so replacing it does not change their behavior.
func validateUnknownVoters(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	// Votes belong to the GOV state transition, which a phase-2-invalid
	// transaction does not apply.
	if !tx.IsValid() {
		return nil
	}
	state, ok := ls.(CommitteeCredentialState)
	if !ok {
		return conway.UtxoValidateUnknownVoters(tx, slot, ls, pp)
	}
	votes := tx.VotingProcedures()
	if len(votes) == 0 {
		return nil
	}
	// Resolved on the first committee voter rather than up front, so a
	// transaction with only DRep or pool votes never pays for the query. The
	// second result reports whether the answer is authoritative.
	var availabilityKnown, available bool
	committeeHotMember := func(
		hotCredential lcommon.Credential,
	) (*lcommon.CommitteeMember, bool, error) {
		if !availabilityKnown {
			resolved, err := state.CommitteeStateAvailable()
			if err != nil {
				return nil, false, err
			}
			available, availabilityKnown = resolved, true
		}
		if !available {
			return nil, false, nil
		}
		member, err := state.CommitteeHotCredentialMember(hotCredential)
		return member, true, err
	}
	for voter := range votes {
		if voter == nil {
			continue
		}
		switch voter.Type {
		case lcommon.VoterTypeDRepKeyHash,
			lcommon.VoterTypeDRepScriptHash:
			registration, err := ls.DRepRegistration(
				lcommon.Blake2b224(voter.Hash),
			)
			if err != nil {
				return err
			}
			if registration == nil {
				return conway.UnknownVoterError{Voter: *voter}
			}
		case lcommon.VoterTypeStakingPoolKeyHash:
			if !ls.IsPoolRegistered(lcommon.PoolKeyHash(voter.Hash)) {
				return conway.UnknownVoterError{Voter: *voter}
			}
		case lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
			lcommon.VoterTypeConstitutionalCommitteeHotScriptHash:
			credentialType := uint(lcommon.CredentialTypeAddrKeyHash)
			if voter.Type ==
				lcommon.VoterTypeConstitutionalCommitteeHotScriptHash {
				credentialType = uint(lcommon.CredentialTypeScriptHash)
			}
			hotCredential := lcommon.Credential{
				CredType:   credentialType,
				Credential: lcommon.Blake2b224(voter.Hash),
			}
			member, authoritative, err := committeeHotMember(hotCredential)
			if err != nil {
				// A failed lookup is never a known voter: fail closed.
				return conway.CommitteeMemberLookupError{
					Credential: hotCredential.Credential,
					Err:        err,
				}
			}
			// An unauthoritative nil member cannot establish an unknown
			// voter. See LedgerView.CommitteeStateAvailable.
			if (member == nil && authoritative) ||
				(member != nil && member.Resigned) {
				return conway.UnknownVoterError{Voter: *voter}
			}
		default:
			return conway.UnknownVoterError{Voter: *voter}
		}
	}
	return nil
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

func buildIndexedUtxoValidationRules(
	rules []lcommon.UtxoValidationRuleFunc,
	skipValidationFunc lcommon.UtxoValidationRuleFunc,
	skipRuleName string,
) []indexedUtxoValidationRule {
	return buildIndexedUtxoValidationRulesWithSkips(rules, []utxoValidationRuleSkip{{
		validationFunc: skipValidationFunc,
		name:           skipRuleName,
	}})
}

func buildIndexedUtxoValidationRulesWithSkips(
	rules []lcommon.UtxoValidationRuleFunc,
	skips []utxoValidationRuleSkip,
) []indexedUtxoValidationRule {
	skipIndexes := map[int]struct{}{}
	for _, skip := range skips {
		resolvedIndex := resolveUtxoValidationSkipIndex(
			rules,
			skip.validationFunc,
			skip.name,
		)
		skipIndexes[resolvedIndex] = struct{}{}
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

func resolveUtxoValidationSkipIndex(
	rules []lcommon.UtxoValidationRuleFunc,
	skipValidationFunc lcommon.UtxoValidationRuleFunc,
	skipRuleName string,
) int {
	if skipRuleName == "" {
		skipRuleName = "UTxO validation skip rule"
	}
	if skipValidationFunc == nil {
		panic(skipRuleName + " expected validation function is nil")
	}
	targetName := utxoValidationRuleName(skipValidationFunc)
	found := noUtxoValidationRuleIndex
	for index, rule := range rules {
		if utxoValidationRuleName(rule) != targetName {
			continue
		}
		if found != noUtxoValidationRuleIndex {
			panic(fmt.Sprintf(
				"%s resolves to multiple upstream rule indexes %d and %d",
				skipRuleName,
				found,
				index,
			))
		}
		found = index
	}
	if found == noUtxoValidationRuleIndex {
		panic(
			skipRuleName +
				" expected function is absent from upstream validation rules",
		)
	}
	return found
}

func utxoValidationRuleName(fn lcommon.UtxoValidationRuleFunc) string {
	if fn == nil {
		return ""
	}
	pc := reflect.ValueOf(fn).Pointer()
	if runtimeFn := runtime.FuncForPC(pc); runtimeFn != nil {
		return runtimeFn.Name()
	}
	return fmt.Sprintf("%x", pc)
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
// max-size rules that buildIndexedUtxoValidationRulesWithSkips removed.
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
