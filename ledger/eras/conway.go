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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
)

var ConwayEraDesc = EraDesc{
	Id:                      conway.EraIdConway,
	Name:                    conway.EraNameConway,
	MinMajorVersion:         conway.MinProtocolVersionConway,
	MaxMajorVersion:         conway.MaxProtocolVersionConway,
	DecodePParamsFunc:       DecodePParamsConway,
	DecodePParamsUpdateFunc: DecodePParamsUpdateConway,
	PParamsUpdateFunc:       PParamsUpdateConway,
	HardForkFunc:            HardForkConway,
	EpochLengthFunc:         EpochLengthShelley,
	CalculateEtaVFunc:       CalculateEtaVConway,
	CertDepositFunc:         CertDepositConway,
	ValidateTxFunc:          ValidateTxConway,
	EvaluateTxFunc:          EvaluateTxConway,
}

func DecodePParamsConway(data []byte) (lcommon.ProtocolParameters, error) {
	var ret conway.ConwayProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateConway(data []byte) (any, error) {
	var ret conway.ConwayProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateConway(
	currentPParams lcommon.ProtocolParameters,
	pparamsUpdate any,
) (lcommon.ProtocolParameters, error) {
	conwayPParams, ok := currentPParams.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	conwayPParamsUpdate, ok := pparamsUpdate.(conway.ConwayProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	conwayPParams.Update(&conwayPParamsUpdate)
	return conwayPParams, nil
}

func HardForkConway(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	babbagePParams, ok := prevPParams.(*babbage.BabbageProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := conway.UpgradePParams(*babbagePParams)
	conwayGenesis := nodeConfig.ConwayGenesis()
	if err := ret.UpdateFromGenesis(conwayGenesis); err != nil {
		return nil, err
	}
	// Bump ProtocolVersion.Major to Conway's range when entering the
	// era. gouroboros's UpgradePParams carries the prior major
	// forward unchanged; schedule-driven transitions would otherwise
	// reach Conway with major still at Babbage's value.
	if ret.ProtocolVersion.Major < conway.MinProtocolVersionConway {
		ret.ProtocolVersion.Major = conway.MinProtocolVersionConway
	}
	return &ret, nil
}

func CalculateEtaVConway(
	nodeConfig *cardano.CardanoNodeConfig,
	prevBlockNonce []byte,
	block ledger.Block,
) ([]byte, error) {
	if len(prevBlockNonce) == 0 {
		tmpNonce, err := hex.DecodeString(nodeConfig.ShelleyGenesisHash)
		if err != nil {
			return nil, err
		}
		prevBlockNonce = tmpNonce
	}
	h, ok := block.Header().(*conway.ConwayBlockHeader)
	if !ok {
		return nil, errors.New("unexpected block type")
	}
	// Praos (Babbage+) uses domain separation + double hash,
	// unlike TPraos which uses the raw VRF output directly.
	vrfNonce := praosVRFNonceValue(h.Body.VrfResult.Output)
	tmpNonce, err := lcommon.CalculateRollingNonce(
		prevBlockNonce,
		vrfNonce,
	)
	if err != nil {
		return nil, err
	}
	return tmpNonce.Bytes(), nil
}

func CertDepositConway(
	cert lcommon.Certificate,
	pp lcommon.ProtocolParameters,
) (uint64, error) {
	tmpPparams, ok := pp.(*conway.ConwayProtocolParameters)
	if !ok {
		return 0, ErrIncompatibleProtocolParams
	}
	switch cert.(type) {
	case *lcommon.PoolRegistrationCertificate:
		return uint64(tmpPparams.PoolDeposit), nil
	case *lcommon.RegistrationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.RegistrationDrepCertificate:
		return uint64(tmpPparams.DRepDeposit), nil
	case *lcommon.StakeRegistrationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.StakeRegistrationDelegationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.VoteRegistrationDelegationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	default:
		return 0, nil
	}
}

func ValidateTxConway(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	tmpPparams, ok := pp.(*conway.ConwayProtocolParameters)
	if !ok {
		return ErrIncompatibleProtocolParams
	}
	normalizedTx, err := normalizeScriptDataHashCbor(tx)
	if err != nil {
		return fmt.Errorf("normalize script data hash CBOR: %w", err)
	}
	tx = normalizedTx
	// Validate TX through ledger validation rules (Phase-1).
	// These must run even for isValid=false transactions, which still
	// require valid structure, fees, and UTxO references for collateral.
	errs := []error{}
	for _, validationRule := range conwayValidationRules(ls) {
		err = validationRule.validationFunc(tx, slot, ls, pp)
		if err != nil {
			errs = append(
				errs,
				fmt.Errorf(
					"conway utxo validation rule %d: %w",
					validationRule.index,
					err,
				),
			)
		}
	}
	if err := ValidateTxFeeConway(tx, ls, tmpPparams); err != nil &&
		(!isInputResolutionError(err) || len(errs) == 0) {
		errs = append(
			errs,
			fmt.Errorf("conway fee validation: %w", err),
		)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	plutusCtx, err := newConwayPlutusValidationContext(tx, ls)
	if err != nil {
		return fmt.Errorf("conway plutus redeemer validation: %w", err)
	}
	// Required redeemers are a Conway UTXOW/phase-1 condition, so this stays
	// before phase-2 skips.
	if err := validateConwayRequiredPlutusRedeemers(
		tx,
		plutusCtx.scriptInputs,
		plutusCtx.inputs,
		plutusCtx.assetMint,
		plutusCtx.redeemers,
	); err != nil {
		return fmt.Errorf("conway plutus redeemer validation: %w", err)
	}
	// Skip script evaluation (Phase-2) if TX is marked as not valid.
	// These transactions failed script validation on-chain; collateral
	// is consumed instead of regular inputs.
	if !tx.IsValid() {
		return nil
	}
	if shouldSkipPhase2Validation(ls) {
		return nil
	}
	if err := validateTxPlutusConwayWithContext(
		tx,
		ls,
		tmpPparams,
		plutusCtx,
		false,
	); err != nil {
		return fmt.Errorf("conway plutus validation: %w", err)
	}
	return nil
}

var (
	conwayUtxoValidationRules       = buildConwayValidationRules()
	conwayPhase1UtxoValidationRules = conwayUtxoValidationRules
)

func conwayValidationRules(
	ls lcommon.LedgerState,
) []indexedUtxoValidationRule {
	if shouldSkipPhase2Validation(ls) {
		return conwayPhase1UtxoValidationRules
	}
	return conwayUtxoValidationRules
}

func buildConwayValidationRules() []indexedUtxoValidationRule {
	skips := []utxoValidationRuleSkip{
		{
			index:          conwayUtxoValidateFeeTooSmallRuleIndex,
			validationFunc: conway.UtxoValidateFeeTooSmallUtxo,
			name:           "conway.UtxoValidateFeeTooSmallUtxo",
		},
		{
			index:          conwayUtxoValidatePlutusScriptsRuleIndex,
			validationFunc: conway.UtxoValidatePlutusScripts,
			name:           "conway.UtxoValidatePlutusScripts",
		},
	}
	return buildIndexedUtxoValidationRulesWithSkips(
		conway.UtxoValidationRules,
		skips,
	)
}

func isInputResolutionError(err error) bool {
	return errors.Is(err, lcommon.ErrInputResolution) ||
		errors.Is(err, lcommon.ErrReferenceInputResolution)
}

type conwayPlutusValidationContext struct {
	scriptInputs  conwayScriptInputs
	inputs        []lcommon.TransactionInput
	assetMint     lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
	redeemers     lcommon.TransactionWitnessRedeemers
	witnessDatums map[lcommon.Blake2b256]*lcommon.Datum
}

func newConwayPlutusValidationContext(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
) (conwayPlutusValidationContext, error) {
	var ret conwayPlutusValidationContext
	scriptInputs, err := resolveConwayScriptInputs(tx, ls, true)
	if err != nil {
		return ret, err
	}
	ret.scriptInputs = scriptInputs
	witnesses := tx.Witnesses()
	ret.witnessDatums = make(map[lcommon.Blake2b256]*lcommon.Datum)
	if witnesses != nil {
		ret.redeemers = witnesses.Redeemers()
		plutusData := witnesses.PlutusData()
		ret.witnessDatums = make(
			map[lcommon.Blake2b256]*lcommon.Datum,
			len(plutusData),
		)
		for i := range plutusData {
			datum := plutusData[i]
			ret.witnessDatums[datum.Hash()] = &datum
		}
	}
	ret.inputs = script.SortInputs(tx.Inputs())
	assetMint := tx.AssetMint()
	if assetMint != nil {
		ret.assetMint = *assetMint
	}
	return ret, nil
}

func ValidateTxPlutusConway(
	tx lcommon.Transaction,
	_ uint64,
	ls lcommon.LedgerState,
	pp *conway.ConwayProtocolParameters,
) error {
	return validateTxPlutusConway(tx, ls, pp, true)
}

func validateTxPlutusConway(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *conway.ConwayProtocolParameters,
	validateRequiredRedeemers bool,
) error {
	if !tx.IsValid() {
		return nil
	}
	plutusCtx, err := newConwayPlutusValidationContext(tx, ls)
	if err != nil {
		return err
	}
	return validateTxPlutusConwayWithContext(
		tx,
		ls,
		pp,
		plutusCtx,
		validateRequiredRedeemers,
	)
}

func validateTxPlutusConwayWithContext(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *conway.ConwayProtocolParameters,
	plutusCtx conwayPlutusValidationContext,
	validateRequiredRedeemers bool,
) error {
	if validateRequiredRedeemers {
		if err := validateConwayRequiredPlutusRedeemers(
			tx,
			plutusCtx.scriptInputs,
			plutusCtx.inputs,
			plutusCtx.assetMint,
			plutusCtx.redeemers,
		); err != nil {
			return err
		}
	}
	if plutusCtx.redeemers == nil {
		return nil
	}
	hasRedeemers := false
	for range plutusCtx.redeemers.Iter() {
		hasRedeemers = true
		break
	}
	if !hasRedeemers {
		return nil
	}
	txInfos := newConwayTxInfoCache(
		ls,
		tx,
		plutusCtx.scriptInputs.resolvedAllInputs,
	)
	for redeemerKey, redeemerValue := range plutusCtx.redeemers.Iter() {
		purpose, ok := buildConwayScriptPurpose(
			redeemerKey,
			plutusCtx.scriptInputs.resolvedInputsMap,
			plutusCtx.inputs,
			plutusCtx.assetMint,
			tx.Certificates(),
			tx.Withdrawals(),
			tx.VotingProcedures(),
			tx.ProposalProcedures(),
			plutusCtx.witnessDatums,
		)
		if !ok {
			return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
		}
		if purpose == nil {
			return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
		}
		switch p := purpose.(type) {
		case script.ScriptPurposeSpending:
			if p.Input.Output != nil {
				addr := p.Input.Output.Address()
				if (addr.Type() & lcommon.AddressTypeScriptBit) == 0 {
					return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
				}
			}
		case script.ScriptPurposeCertifying:
			if p.ScriptHash() == (lcommon.ScriptHash{}) {
				return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
			}
		case script.ScriptPurposeRewarding:
			if p.StakeCredential.CredType != lcommon.CredentialTypeScriptHash {
				return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
			}
		case script.ScriptPurposeProposing:
			if p.ScriptHash() == (lcommon.ScriptHash{}) {
				return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
			}
		case script.ScriptPurposeVoting:
			// ScriptPurposeVoting.ScriptHash() returns the voter hash even for
			// key-hash voters, so an empty-hash check (as used for the other
			// purposes above) cannot distinguish them. A redeemer pointing at a
			// key-hash voter requires no script and is therefore extraneous.
			if !conwayVoterRequiresScriptRedeemer(&p.Voter) {
				return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
			}
		}
		scriptHash := purpose.ScriptHash()
		plutusScript, ok := plutusCtx.scriptInputs.scripts[scriptHash]
		if !ok {
			return lcommon.MissingScriptWitnessesError{
				ScriptHash: scriptHash,
			}
		}
		if !isConwayPlutusScript(plutusScript) {
			return conway.ExtraRedeemerError{RedeemerKey: redeemerKey}
		}
		var datum data.PlutusData
		var spendInput lcommon.TransactionInput
		if spendPurpose, ok := purpose.(script.ScriptPurposeSpending); ok {
			datum = spendPurpose.Datum
			spendInput = spendPurpose.Input.Id
		}
		switch plutusScript.(type) {
		case lcommon.PlutusV1Script, lcommon.PlutusV2Script:
			if _, isSpend := purpose.(script.ScriptPurposeSpending); isSpend && datum == nil {
				return conway.MissingDatumForSpendingScriptError{
					ScriptHash: scriptHash,
					Input:      spendInput,
				}
			}
		}
		redeemer := script.Redeemer{
			Tag:     redeemerKey.Tag,
			Index:   redeemerKey.Index,
			Data:    redeemerValue.Data.Data,
			ExUnits: redeemerValue.ExUnits,
		}
		_, execErr, err := evaluateConwayPlutusScript(
			plutusScript,
			purpose,
			redeemer,
			datum,
			redeemerValue.ExUnits,
			pp,
			txInfos,
			true,
		)
		if err != nil {
			return err
		}
		if execErr != nil {
			return conway.PlutusScriptFailedError{
				ScriptHash: scriptHash,
				Tag:        redeemerKey.Tag,
				Index:      redeemerKey.Index,
				Err:        execErr,
			}
		}
	}
	return nil
}

func validateConwayRequiredPlutusRedeemers(
	tx lcommon.Transaction,
	scriptInputs conwayScriptInputs,
	inputs []lcommon.TransactionInput,
	assetMint lcommon.MultiAsset[lcommon.MultiAssetTypeMint],
	redeemers lcommon.TransactionWitnessRedeemers,
) error {
	present := make(map[lcommon.RedeemerKey]struct{})
	if redeemers != nil {
		for redeemerKey := range redeemers.Iter() {
			present[redeemerKey] = struct{}{}
		}
	}
	checkRequired := func(
		key lcommon.RedeemerKey,
		purpose script.ScriptPurpose,
	) error {
		if purpose == nil {
			return nil
		}
		scriptHash := purpose.ScriptHash()
		if scriptHash == (lcommon.ScriptHash{}) {
			return nil
		}
		availableScript, ok := scriptInputs.scripts[scriptHash]
		if !ok {
			return lcommon.MissingScriptWitnessesError{
				ScriptHash: scriptHash,
			}
		}
		if !isConwayPlutusScript(availableScript) {
			return nil
		}
		if _, ok := present[key]; ok {
			return nil
		}
		return conway.MissingRedeemerForScriptError{
			ScriptHash: scriptHash,
			Tag:        key.Tag,
			Index:      key.Index,
		}
	}
	for idx, input := range inputs {
		utxo, ok := scriptInputs.resolvedInputsMap[input.String()]
		if !ok || utxo.Output == nil {
			continue
		}
		addr := utxo.Output.Address()
		if (addr.Type() & lcommon.AddressTypeScriptBit) == 0 {
			continue
		}
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTagSpend,
			Index: uint32(idx),
		}
		if err := checkRequired(
			key,
			script.ScriptPurposeSpending{Input: utxo},
		); err != nil {
			return err
		}
	}
	policies := assetMint.Policies()
	slices.SortFunc(policies, func(a, b lcommon.Blake2b224) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})
	for idx, policy := range policies {
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTagMint,
			Index: uint32(idx),
		}
		if err := checkRequired(
			key,
			script.ScriptPurposeMinting{PolicyId: policy},
		); err != nil {
			return err
		}
	}
	for idx, cert := range tx.Certificates() {
		if !conwayCertificateRequiresScriptRedeemer(cert) {
			continue
		}
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTagCert,
			Index: uint32(idx),
		}
		if err := checkRequired(
			key,
			script.ScriptPurposeCertifying{
				Index:       uint32(idx),
				Certificate: cert,
			},
		); err != nil {
			return err
		}
	}
	withdrawalAddrs := sortedConwayWithdrawalAddresses(tx.Withdrawals())
	for idx, addr := range withdrawalAddrs {
		if (addr.Type() & lcommon.AddressTypeScriptBit) == 0 {
			continue
		}
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTagReward,
			Index: uint32(idx),
		}
		if err := checkRequired(
			key,
			script.ScriptPurposeRewarding{
				StakeCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeScriptHash,
					Credential: addr.StakeKeyHash(),
				},
			},
		); err != nil {
			return err
		}
	}
	voters := sortedConwayVoters(tx.VotingProcedures())
	for idx, voter := range voters {
		if !conwayVoterRequiresScriptRedeemer(voter) {
			continue
		}
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTagVoting,
			Index: uint32(idx),
		}
		if err := checkRequired(
			key,
			script.ScriptPurposeVoting{Voter: *voter},
		); err != nil {
			return err
		}
	}
	for idx, proposal := range tx.ProposalProcedures() {
		if proposal == nil || proposal.GovAction() == nil {
			continue
		}
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTagProposing,
			Index: uint32(idx),
		}
		if err := checkRequired(
			key,
			script.ScriptPurposeProposing{
				Index:             uint32(idx),
				ProposalProcedure: proposal,
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func conwayCertificateRequiresScriptRedeemer(
	cert lcommon.Certificate,
) bool {
	var cred *lcommon.Credential
	switch c := cert.(type) {
	case *lcommon.StakeDeregistrationCertificate:
		cred = &c.StakeCredential
	case *lcommon.StakeDelegationCertificate:
		cred = c.StakeCredential
	case *lcommon.RegistrationCertificate:
		cred = &c.StakeCredential
	case *lcommon.DeregistrationCertificate:
		cred = &c.StakeCredential
	case *lcommon.VoteDelegationCertificate:
		cred = &c.StakeCredential
	case *lcommon.StakeVoteDelegationCertificate:
		cred = &c.StakeCredential
	case *lcommon.StakeRegistrationDelegationCertificate:
		cred = &c.StakeCredential
	case *lcommon.VoteRegistrationDelegationCertificate:
		cred = &c.StakeCredential
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		cred = &c.StakeCredential
	case *lcommon.AuthCommitteeHotCertificate:
		cred = &c.ColdCredential
	case *lcommon.ResignCommitteeColdCertificate:
		cred = &c.ColdCredential
	case *lcommon.RegistrationDrepCertificate:
		cred = &c.DrepCredential
	case *lcommon.DeregistrationDrepCertificate:
		cred = &c.DrepCredential
	case *lcommon.UpdateDrepCertificate:
		cred = &c.DrepCredential
	default:
		return false
	}
	return cred != nil && cred.CredType == lcommon.CredentialTypeScriptHash
}

func isConwayPlutusScript(script lcommon.Script) bool {
	switch script.(type) {
	case lcommon.PlutusV1Script,
		lcommon.PlutusV2Script,
		lcommon.PlutusV3Script:
		return true
	default:
		return false
	}
}

func sortedConwayWithdrawalAddresses(
	withdrawals map[*lcommon.Address]*big.Int,
) []*lcommon.Address {
	ret := make([]*lcommon.Address, 0, len(withdrawals))
	for addr := range withdrawals {
		ret = append(ret, addr)
	}
	slices.SortFunc(ret, func(a, b *lcommon.Address) int {
		aBytes, aErr := a.Bytes()
		bBytes, bErr := b.Bytes()
		if aErr != nil || bErr != nil {
			return strings.Compare(a.String(), b.String())
		}
		return bytes.Compare(aBytes, bBytes)
	})
	return ret
}

func sortedConwayVoters(
	votingProcedures lcommon.VotingProcedures,
) []*lcommon.Voter {
	ret := make([]*lcommon.Voter, 0, len(votingProcedures))
	for voter := range votingProcedures {
		ret = append(ret, voter)
	}
	slices.SortFunc(ret, func(a, b *lcommon.Voter) int {
		tagA := conwayVoterRedeemerOrder(a)
		tagB := conwayVoterRedeemerOrder(b)
		if tagA == tagB {
			return bytes.Compare(a.Hash[:], b.Hash[:])
		}
		return tagA - tagB
	})
	return ret
}

func conwayVoterRedeemerOrder(voter *lcommon.Voter) int {
	switch voter.Type {
	case lcommon.VoterTypeConstitutionalCommitteeHotScriptHash:
		return 0
	case lcommon.VoterTypeConstitutionalCommitteeHotKeyHash:
		return 1
	case lcommon.VoterTypeDRepScriptHash:
		return 2
	case lcommon.VoterTypeDRepKeyHash:
		return 3
	case lcommon.VoterTypeStakingPoolKeyHash:
		return 4
	default:
		return -1
	}
}

func conwayVoterRequiresScriptRedeemer(voter *lcommon.Voter) bool {
	return voter.Type == lcommon.VoterTypeConstitutionalCommitteeHotScriptHash ||
		voter.Type == lcommon.VoterTypeDRepScriptHash
}

type conwayScriptInputs struct {
	resolvedInputs          []lcommon.Utxo
	resolvedReferenceInputs []lcommon.Utxo
	resolvedAllInputs       []lcommon.Utxo
	resolvedInputsMap       map[string]lcommon.Utxo
	scripts                 map[lcommon.ScriptHash]lcommon.Script
}

func resolveConwayScriptInputs(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	includeNativeScripts bool,
) (conwayScriptInputs, error) {
	ret := conwayScriptInputs{
		resolvedInputs: make([]lcommon.Utxo, 0, len(tx.Inputs())),
		resolvedReferenceInputs: make(
			[]lcommon.Utxo,
			0,
			len(tx.ReferenceInputs()),
		),
		resolvedInputsMap: make(
			map[string]lcommon.Utxo,
			len(tx.Inputs())+len(tx.ReferenceInputs()),
		),
	}
	for _, input := range tx.Inputs() {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			return ret, lcommon.InputResolutionError{
				Input: input,
				Err:   err,
			}
		}
		ret.resolvedInputs = append(ret.resolvedInputs, utxo)
		ret.resolvedInputsMap[input.String()] = utxo
	}
	for _, input := range tx.ReferenceInputs() {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			return ret, lcommon.ReferenceInputResolutionError{
				Input: input,
				Err:   err,
			}
		}
		ret.resolvedReferenceInputs = append(ret.resolvedReferenceInputs, utxo)
		ret.resolvedInputsMap[input.String()] = utxo
	}
	ret.resolvedAllInputs = make(
		[]lcommon.Utxo,
		0,
		len(ret.resolvedInputs)+len(ret.resolvedReferenceInputs),
	)
	ret.resolvedAllInputs = append(ret.resolvedAllInputs, ret.resolvedInputs...)
	ret.resolvedAllInputs = append(
		ret.resolvedAllInputs,
		ret.resolvedReferenceInputs...,
	)
	ret.scripts = collectConwayAvailableScripts(
		tx.Witnesses(),
		ret.resolvedAllInputs,
		includeNativeScripts,
	)
	return ret, nil
}

func collectConwayAvailableScripts(
	witnesses lcommon.TransactionWitnessSet,
	resolvedInputs []lcommon.Utxo,
	includeNativeScripts bool,
) map[lcommon.ScriptHash]lcommon.Script {
	ret := make(map[lcommon.ScriptHash]lcommon.Script)
	if witnesses != nil {
		for _, tmpScript := range witnesses.PlutusV1Scripts() {
			ret[tmpScript.Hash()] = tmpScript
		}
		for _, tmpScript := range witnesses.PlutusV2Scripts() {
			ret[tmpScript.Hash()] = tmpScript
		}
		for _, tmpScript := range witnesses.PlutusV3Scripts() {
			ret[tmpScript.Hash()] = tmpScript
		}
		if includeNativeScripts {
			for _, tmpScript := range witnesses.NativeScripts() {
				ret[tmpScript.Hash()] = tmpScript
			}
		}
	}
	for _, utxo := range resolvedInputs {
		if utxo.Output == nil {
			continue
		}
		scriptRef := utxo.Output.ScriptRef()
		if scriptRef == nil {
			continue
		}
		ret[scriptRef.Hash()] = scriptRef
	}
	return ret
}

type conwayTxInfoCache struct {
	ls             lcommon.LedgerState
	tx             lcommon.Transaction
	resolvedInputs []lcommon.Utxo
	txInfoV1       script.TxInfoV1
	txInfoV2       script.TxInfoV2
	txInfoV3       script.TxInfoV3
	txInfoV1Built  bool
	txInfoV2Built  bool
	txInfoV3Built  bool
}

func newConwayTxInfoCache(
	ls lcommon.LedgerState,
	tx lcommon.Transaction,
	resolvedInputs []lcommon.Utxo,
) *conwayTxInfoCache {
	return &conwayTxInfoCache{
		ls:             ls,
		tx:             tx,
		resolvedInputs: resolvedInputs,
	}
}

func (c *conwayTxInfoCache) v1() (script.TxInfoV1, error) {
	if !c.txInfoV1Built {
		txInfo, err := script.NewTxInfoV1FromTransaction(
			c.ls,
			c.tx,
			c.resolvedInputs,
		)
		if err != nil {
			return script.TxInfoV1{}, conway.ScriptContextConstructionError{
				Err: err,
			}
		}
		c.txInfoV1 = txInfo
		c.txInfoV1Built = true
	}
	return c.txInfoV1, nil
}

func (c *conwayTxInfoCache) v2() (script.TxInfoV2, error) {
	if !c.txInfoV2Built {
		txInfo, err := script.NewTxInfoV2FromTransaction(
			c.ls,
			c.tx,
			c.resolvedInputs,
		)
		if err != nil {
			return script.TxInfoV2{}, conway.ScriptContextConstructionError{
				Err: err,
			}
		}
		c.txInfoV2 = txInfo
		c.txInfoV2Built = true
	}
	return c.txInfoV2, nil
}

func (c *conwayTxInfoCache) v3() (script.TxInfoV3, error) {
	if !c.txInfoV3Built {
		txInfo, err := script.NewTxInfoV3FromTransaction(
			c.ls,
			c.tx,
			c.resolvedInputs,
		)
		if err != nil {
			return script.TxInfoV3{}, conway.ScriptContextConstructionError{
				Err: err,
			}
		}
		c.txInfoV3 = txInfo
		c.txInfoV3Built = true
	}
	return c.txInfoV3, nil
}

// restrictiveEnormousBudget is used when evaluating Plutus scripts in
// restrictive (phase-2 ledger validation) mode. Instead of limiting the CEK
// machine to the declared redeemer budget during execution — which causes
// intermediate slippage-batch flush failures — we run with a virtually
// unlimited budget and compare the consumed amount against the declared budget
// after execution. This mirrors cardano-node's restrictingEnormous semantics:
// the machine runs unconstrained; at the end, usedBudget (which excludes the
// final unbudgeted step batch, per SkipFinalSlippageFlush) must fit within the
// declared redeemer budget.
//
// math.MaxInt64/2 avoids overflow in ExBudget arithmetic (consumed = enormous - remaining).
const restrictiveEnormousBudget = int64(math.MaxInt64 / 2)

func evaluateConwayPlutusScript(
	plutusScript lcommon.Script,
	purpose script.ScriptPurpose,
	redeemer script.Redeemer,
	datum data.PlutusData,
	budget lcommon.ExUnits,
	pp *conway.ConwayProtocolParameters,
	txInfos *conwayTxInfoCache,
	skipFinalSlippageFlush bool,
) (lcommon.ExUnits, error, error) {
	// In restrictive mode (skipFinalSlippageFlush=true), pass an enormous
	// budget to gouroboros/plutigo so intermediate slippage-batch flushes
	// never exhaust the budget mid-execution. After execution we compare the
	// consumed amount (final batch already excluded by SkipFinalSlippageFlush)
	// against the declared redeemer budget. This matches cardano-node's
	// restrictingEnormous mode. In exact mode the declared budget is used as
	// the machine limit (unchanged behavior).
	evalBudget := budget
	if skipFinalSlippageFlush {
		evalBudget = lcommon.ExUnits{
			Steps:  restrictiveEnormousBudget,
			Memory: restrictiveEnormousBudget,
		}
	}
	switch s := plutusScript.(type) {
	case lcommon.PlutusV3Script:
		txInfoV3, err := txInfos.v3()
		if err != nil {
			return lcommon.ExUnits{}, nil, err
		}
		ctx := script.NewScriptContextV3(txInfoV3, redeemer, purpose)
		evalContext, err := cek.NewEvalContext(
			lang.LanguageVersionV3,
			cek.ProtoVersion{
				Major: pp.ProtocolVersion.Major,
				Minor: pp.ProtocolVersion.Minor,
			},
			pp.CostModels[2],
		)
		if err != nil {
			return lcommon.ExUnits{}, nil, fmt.Errorf("build evaluation context: %w", err)
		}
		evalContext.SkipFinalSlippageFlush = skipFinalSlippageFlush
		usedBudget, err := s.Evaluate(
			ctx.ToPlutusData(),
			evalBudget,
			evalContext,
		)
		if err != nil {
			return lcommon.ExUnits{}, err, nil
		}
		if skipFinalSlippageFlush && (usedBudget.Steps > budget.Steps || usedBudget.Memory > budget.Memory) {
			return usedBudget, fmt.Errorf(
				"script exceeded declared budget: used (%d cpu, %d mem), declared (%d cpu, %d mem)",
				usedBudget.Steps, usedBudget.Memory, budget.Steps, budget.Memory,
			), nil
		}
		return usedBudget, nil, nil
	case lcommon.PlutusV2Script:
		txInfoV2, err := txInfos.v2()
		if err != nil {
			return lcommon.ExUnits{}, nil, err
		}
		ctx := script.NewScriptContextV1V2(txInfoV2, purpose)
		evalContext, err := cek.NewEvalContext(
			lang.LanguageVersionV2,
			cek.ProtoVersion{
				Major: pp.ProtocolVersion.Major,
				Minor: pp.ProtocolVersion.Minor,
			},
			pp.CostModels[1],
		)
		if err != nil {
			return lcommon.ExUnits{}, nil, fmt.Errorf("build evaluation context: %w", err)
		}
		evalContext.SkipFinalSlippageFlush = skipFinalSlippageFlush
		usedBudget, err := s.Evaluate(
			datum,
			redeemer.Data,
			ctx.ToPlutusData(),
			evalBudget,
			evalContext,
		)
		if err != nil {
			return lcommon.ExUnits{}, err, nil
		}
		if skipFinalSlippageFlush && (usedBudget.Steps > budget.Steps || usedBudget.Memory > budget.Memory) {
			return usedBudget, fmt.Errorf(
				"script exceeded declared budget: used (%d cpu, %d mem), declared (%d cpu, %d mem)",
				usedBudget.Steps, usedBudget.Memory, budget.Steps, budget.Memory,
			), nil
		}
		return usedBudget, nil, nil
	case lcommon.PlutusV1Script:
		txInfoV1, err := txInfos.v1()
		if err != nil {
			return lcommon.ExUnits{}, nil, err
		}
		ctx := script.NewScriptContextV1V2(txInfoV1, purpose)
		evalContext, err := cek.NewEvalContext(
			lang.LanguageVersionV1,
			cek.ProtoVersion{
				Major: pp.ProtocolVersion.Major,
				Minor: pp.ProtocolVersion.Minor,
			},
			pp.CostModels[0],
		)
		if err != nil {
			return lcommon.ExUnits{}, nil, fmt.Errorf("build evaluation context: %w", err)
		}
		evalContext.SkipFinalSlippageFlush = skipFinalSlippageFlush
		usedBudget, err := s.Evaluate(
			datum,
			redeemer.Data,
			ctx.ToPlutusData(),
			evalBudget,
			evalContext,
		)
		if err != nil {
			return lcommon.ExUnits{}, err, nil
		}
		if skipFinalSlippageFlush && (usedBudget.Steps > budget.Steps || usedBudget.Memory > budget.Memory) {
			return usedBudget, fmt.Errorf(
				"script exceeded declared budget: used (%d cpu, %d mem), declared (%d cpu, %d mem)",
				usedBudget.Steps, usedBudget.Memory, budget.Steps, budget.Memory,
			), nil
		}
		return usedBudget, nil, nil
	default:
		return lcommon.ExUnits{}, nil, fmt.Errorf(
			"unimplemented script type: %T",
			plutusScript,
		)
	}
}

func buildConwayScriptPurpose(
	redeemerKey lcommon.RedeemerKey,
	resolvedInputs map[string]lcommon.Utxo,
	inputs []lcommon.TransactionInput,
	mint lcommon.MultiAsset[lcommon.MultiAssetTypeMint],
	certificates []lcommon.Certificate,
	withdrawals map[*lcommon.Address]*big.Int,
	votes lcommon.VotingProcedures,
	proposalProcedures []lcommon.ProposalProcedure,
	witnessDatums map[lcommon.Blake2b256]*lcommon.Datum,
) (purpose script.ScriptPurpose, ok bool) {
	defer func() {
		if recover() != nil {
			purpose = nil
			ok = false
		}
	}()
	purpose = script.BuildScriptPurpose(
		redeemerKey,
		resolvedInputs,
		inputs,
		mint,
		certificates,
		withdrawals,
		votes,
		proposalProcedures,
		witnessDatums,
	)
	return purpose, purpose != nil
}

func ValidateTxFeeConway(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *conway.ConwayProtocolParameters,
) error {
	txSize := TxSizeForFee(tx)
	declaredEU, err := DeclaredExUnits(tx)
	if err != nil {
		return fmt.Errorf(
			"calculating declared execution units: %w",
			err,
		)
	}
	refScriptSize, err := ReferencedScriptSize(tx, ls)
	if err != nil {
		return fmt.Errorf(
			"calculating reference script size: %w",
			err,
		)
	}
	var pricesMem, pricesSteps *big.Rat
	if pp.ExecutionCosts.MemPrice != nil {
		pricesMem = pp.ExecutionCosts.MemPrice.ToBigRat()
	}
	if pp.ExecutionCosts.StepPrice != nil {
		pricesSteps = pp.ExecutionCosts.StepPrice.ToBigRat()
	}
	var refScriptCostPerByte *big.Rat
	if pp.MinFeeRefScriptCostPerByte != nil {
		refScriptCostPerByte = pp.MinFeeRefScriptCostPerByte.ToBigRat()
	}
	minFee := CalculateConwayMinFee(
		txSize,
		declaredEU,
		pp.MinFeeA,
		pp.MinFeeB,
		pricesMem,
		pricesSteps,
		refScriptSize,
		refScriptCostPerByte,
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

func EvaluateTxConway(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	tmpPparams, ok := pp.(*conway.ConwayProtocolParameters)
	if !ok {
		return 0, lcommon.ExUnits{}, nil, ErrIncompatibleProtocolParams
	}
	scriptInputs, err := resolveConwayScriptInputs(tx, ls, true)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	// Evaluate scripts
	var retTotalExUnits lcommon.ExUnits
	retRedeemerExUnits := make(map[lcommon.RedeemerKey]lcommon.ExUnits)
	txInfos := newConwayTxInfoCache(
		ls,
		tx,
		scriptInputs.resolvedAllInputs,
	)
	txInfoV3, err := txInfos.v3()
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	for _, redeemerPair := range txInfoV3.Redeemers {
		purpose := redeemerPair.Key
		if purpose == nil {
			return 0, lcommon.ExUnits{}, nil, errors.New(
				"script purpose is nil",
			)
		}
		redeemer := redeemerPair.Value
		// Lookup script from redeemer purpose
		tmpScript := scriptInputs.scripts[purpose.ScriptHash()]
		if tmpScript == nil {
			return 0, lcommon.ExUnits{}, nil, errors.New(
				"could not find needed script",
			)
		}
		var datum data.PlutusData
		if tmp, ok := purpose.(script.ScriptPurposeSpending); ok {
			datum = tmp.Datum
		}
		usedBudget, execErr, err := evaluateConwayPlutusScript(
			tmpScript,
			purpose,
			redeemer,
			datum,
			tmpPparams.MaxTxExUnits,
			tmpPparams,
			txInfos,
			false,
		)
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, err
		}
		if execErr != nil {
			return 0, lcommon.ExUnits{}, nil, execErr
		}
		retTotalExUnits.Steps += usedBudget.Steps
		retTotalExUnits.Memory += usedBudget.Memory
		retRedeemerExUnits[lcommon.RedeemerKey{
			Tag:   redeemer.Tag,
			Index: redeemer.Index,
		}] = usedBudget
	}
	// Calculate fee based on TX size and calculated ExUnits
	txSize := TxSizeForFee(tx)
	var pricesMem, pricesSteps *big.Rat
	if tmpPparams.ExecutionCosts.MemPrice != nil {
		pricesMem = tmpPparams.ExecutionCosts.MemPrice.ToBigRat()
	}
	if tmpPparams.ExecutionCosts.StepPrice != nil {
		pricesSteps = tmpPparams.ExecutionCosts.StepPrice.ToBigRat()
	}
	fee := CalculateMinFee(
		txSize,
		retTotalExUnits,
		tmpPparams.MinFeeA,
		tmpPparams.MinFeeB,
		pricesMem,
		pricesSteps,
	)
	refScriptSize, err := ReferenceScriptSizeFromUtxos(
		scriptInputs.resolvedAllInputs,
	)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	var refScriptCostPerByte *big.Rat
	if tmpPparams.MinFeeRefScriptCostPerByte != nil {
		refScriptCostPerByte = tmpPparams.MinFeeRefScriptCostPerByte.ToBigRat()
	}
	fee = saturatedAddUint64(
		fee,
		CalculateConwayRefScriptFee(
			refScriptSize,
			refScriptCostPerByte,
		),
	)
	return fee, retTotalExUnits, retRedeemerExUnits, nil
}
