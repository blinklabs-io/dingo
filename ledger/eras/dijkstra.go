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

package eras

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

var DijkstraEraDesc = EraDesc{
	Id:                      gdijkstra.EraIdDijkstra,
	Name:                    gdijkstra.EraNameDijkstra,
	MinMajorVersion:         gdijkstra.MinProtocolVersionDijkstra,
	MaxMajorVersion:         gdijkstra.MaxProtocolVersionDijkstra,
	DecodePParamsFunc:       DecodePParamsDijkstra,
	DecodePParamsUpdateFunc: DecodePParamsUpdateDijkstra,
	PParamsUpdateFunc:       PParamsUpdateDijkstra,
	ParamUpdateHasPlutusV2CostModelFunc: func(u any) bool {
		upd, ok := u.(gdijkstra.DijkstraProtocolParameterUpdate)
		if !ok {
			return false
		}
		return paramUpdateHasPlutusV2CostModel(upd.CostModels)
	},
	HardForkFunc:      HardForkDijkstra,
	EpochLengthFunc:   EpochLengthShelley,
	CalculateEtaVFunc: CalculateEtaVDijkstra,
	CertDepositFunc:   CertDepositDijkstra,
	ValidateTxFunc:    ValidateTxDijkstra,
	EvaluateTxFunc:    EvaluateTxDijkstra,
}

func DecodePParamsDijkstra(data []byte) (lcommon.ProtocolParameters, error) {
	var ret gdijkstra.DijkstraProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateDijkstra(data []byte) (any, error) {
	var ret gdijkstra.DijkstraProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateDijkstra(
	currentPParams lcommon.ProtocolParameters,
	pparamsUpdate any,
) (lcommon.ProtocolParameters, error) {
	dijkstraPParams, ok := currentPParams.(*gdijkstra.DijkstraProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	dijkstraPParamsUpdate, ok := pparamsUpdate.(gdijkstra.DijkstraProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	// ParameterChange must never change protocol version; only
	// HardForkInitiation may (dingo#4439). Transaction validation already
	// rejects a ParameterChange carrying key 14 before it can be persisted,
	// so this only guards an already-stored malformed proposal that reaches
	// enactment some other way (e.g. replay of pre-fix data).
	dijkstraPParamsUpdate.ProtocolVersion = nil
	if err := dijkstraPParams.ApplyUpdate(&dijkstraPParamsUpdate); err != nil {
		return nil, err
	}
	return dijkstraPParams, nil
}

func HardForkDijkstra(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	conwayPParams, ok := prevPParams.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: *conwayPParams,
	}
	ret.CostModels = cloneCostModels(ret.CostModels)
	if nodeConfig != nil {
		dijkstraGenesis := nodeConfig.DijkstraGenesis()
		if !isEmptyDijkstraGenesis(dijkstraGenesis) {
			if err := ret.UpdateFromGenesis(dijkstraGenesis); err != nil {
				return nil, err
			}
			keepConwayGovernanceParams(
				&ret,
				conwayPParams,
				&dijkstraGenesis.ConwayGenesis,
			)
		}
	}
	applyDijkstraRefScriptDefaults(&ret)
	if ret.ProtocolVersion.Major < gdijkstra.MinProtocolVersionDijkstra {
		ret.ProtocolVersion.Major = gdijkstra.MinProtocolVersionDijkstra
	}
	return &ret, nil
}

// keepConwayGovernanceParams restores the Conway governance parameters that
// conway.UpdateFromGenesis copies unconditionally. A Dijkstra genesis that
// sets only Dijkstra fields leaves them zero, which would otherwise wipe the
// deposits, lifetimes and committee bounds carried over from Conway.
func keepConwayGovernanceParams(
	p *gdijkstra.DijkstraProtocolParameters,
	prev *conway.ConwayProtocolParameters,
	genesis *conway.ConwayGenesis,
) {
	if genesis.MinCommitteeSize == 0 {
		p.MinCommitteeSize = prev.MinCommitteeSize
	}
	if genesis.CommitteeTermLimit == 0 {
		p.CommitteeTermLimit = prev.CommitteeTermLimit
	}
	if genesis.GovActionValidityPeriod == 0 {
		p.GovActionValidityPeriod = prev.GovActionValidityPeriod
	}
	if genesis.GovActionDeposit == 0 {
		p.GovActionDeposit = prev.GovActionDeposit
	}
	if genesis.DRepDeposit == 0 {
		p.DRepDeposit = prev.DRepDeposit
	}
	if genesis.DRepInactivityPeriod == 0 {
		p.DRepInactivityPeriod = prev.DRepInactivityPeriod
	}
}

// applyDijkstraRefScriptDefaults fills reference-script parameters the genesis
// left unset with the fixed Conway values. A zero stride makes the tiered
// reference-script fee calculation fail, and zero size limits reject every
// transaction that consumes a reference script.
func applyDijkstraRefScriptDefaults(p *gdijkstra.DijkstraProtocolParameters) {
	if p.RefScriptCostStride == 0 {
		p.RefScriptCostStride = uint32(conway.RefScriptCostStride)
	}
	if p.RefScriptCostMultiplier == nil {
		p.RefScriptCostMultiplier = &cbor.Rat{Rat: big.NewRat(6, 5)}
	}
	if p.MaxRefScriptSizePerTx == 0 {
		p.MaxRefScriptSizePerTx = uint32(conway.MaxRefScriptSizePerTx)
	}
	if p.MaxRefScriptSizePerBlock == 0 {
		p.MaxRefScriptSizePerBlock = uint32(conway.MaxRefScriptSizePerBlock)
	}
}

func isEmptyDijkstraGenesis(genesis *gdijkstra.DijkstraGenesis) bool {
	if genesis == nil {
		return true
	}
	if genesis.MaxRefScriptSizePerBlock != 0 ||
		genesis.MaxRefScriptSizePerTx != 0 ||
		genesis.RefScriptCostStride != 0 ||
		genesis.RefScriptCostMultiplier != nil ||
		genesis.CommitteeStakeCoverage != nil ||
		genesis.QuorumStakeThreshold != nil ||
		genesis.MaxPledgeLeverage != nil ||
		genesis.MinPoolMargin != nil ||
		len(genesis.PlutusV4CostModel) > 0 ||
		genesis.LeiosAnnouncementPeriodLength != 0 ||
		genesis.LeiosVotePeriodLength != 0 ||
		genesis.LeiosDiffusionPeriodLength != 0 ||
		genesis.LeiosCommitteeSize != 0 ||
		genesis.LeiosQuorumStakeThreshold != nil ||
		genesis.MaxEndorserBlockReferencesSize != 0 ||
		genesis.MaxEndorserBlockTxsSize != 0 ||
		genesis.MaxEndorserBlockExUnits != (lcommon.ExUnits{}) ||
		genesis.MaxRefScriptSizePerEndorserBlock != 0 {
		return false
	}
	return isEmptyConwayGenesis(&genesis.ConwayGenesis)
}

func isEmptyConwayGenesis(genesis *conway.ConwayGenesis) bool {
	if genesis == nil {
		return true
	}
	return genesis.PoolVotingThresholds.CommitteeNormal == nil &&
		genesis.PoolVotingThresholds.CommitteeNoConfidence == nil &&
		genesis.PoolVotingThresholds.HardForkInitiation == nil &&
		genesis.PoolVotingThresholds.MotionNoConfidence == nil &&
		genesis.PoolVotingThresholds.PpSecurityGroup == nil &&
		genesis.DRepVotingThresholds.MotionNoConfidence == nil &&
		genesis.DRepVotingThresholds.CommitteeNormal == nil &&
		genesis.DRepVotingThresholds.CommitteeNoConfidence == nil &&
		genesis.DRepVotingThresholds.UpdateToConstitution == nil &&
		genesis.DRepVotingThresholds.HardForkInitiation == nil &&
		genesis.DRepVotingThresholds.PpNetworkGroup == nil &&
		genesis.DRepVotingThresholds.PpEconomicGroup == nil &&
		genesis.DRepVotingThresholds.PpTechnicalGroup == nil &&
		genesis.DRepVotingThresholds.PpGovGroup == nil &&
		genesis.DRepVotingThresholds.TreasuryWithdrawal == nil &&
		genesis.MinCommitteeSize == 0 &&
		genesis.CommitteeTermLimit == 0 &&
		genesis.GovActionValidityPeriod == 0 &&
		genesis.GovActionDeposit == 0 &&
		genesis.DRepDeposit == 0 &&
		genesis.DRepInactivityPeriod == 0 &&
		genesis.MinFeeRefScriptCostPerByte == nil &&
		len(genesis.PlutusV3CostModel) == 0 &&
		genesis.Constitution.Anchor.DataHash == "" &&
		genesis.Constitution.Anchor.Url == "" &&
		genesis.Constitution.Script == "" &&
		len(genesis.Committee.Members) == 0 &&
		genesis.Committee.Threshold == nil &&
		len(genesis.Delegs) == 0 &&
		len(genesis.InitialDReps) == 0
}

func CalculateEtaVDijkstra(
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
	h, ok := block.Header().(*gdijkstra.DijkstraBlockHeader)
	if !ok {
		return nil, errors.New("unexpected block type")
	}
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

func CertDepositDijkstra(
	cert lcommon.Certificate,
	pp lcommon.ProtocolParameters,
) (uint64, error) {
	tmpPparams, ok := pp.(*gdijkstra.DijkstraProtocolParameters)
	// The nil check is part of the guard, not redundant with it: a typed-nil
	// *DijkstraProtocolParameters satisfies the assertion, so testing only ok
	// lets every case below dereference nil. CertDepositConway has always
	// spelled it this way; this one had not.
	if !ok || tmpPparams == nil {
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

func ValidateTxDijkstra(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	tmpPparams, ok := pp.(*gdijkstra.DijkstraProtocolParameters)
	if !ok || tmpPparams == nil {
		return ErrIncompatibleProtocolParams
	}
	normalizedTx, err := normalizeScriptDataHashCbor(tx)
	if err != nil {
		return fmt.Errorf("normalize script data hash CBOR: %w", err)
	}
	tx = normalizedTx
	errs := []error{}
	// Phase-1 rules apply to every transaction regardless of its declared
	// phase-2 result. A declared-invalid transaction still has to prove its
	// collateral, redeemers, and other UTXO invariants.
	for _, validationRule := range dijkstraPhase1ValidationRules() {
		err = validationRule.validationFunc(tx, slot, ls, pp)
		if err != nil {
			errs = append(
				errs,
				fmt.Errorf(
					"dijkstra utxo validation rule %d: %w",
					validationRule.index,
					err,
				),
			)
		}
	}
	// Pool registration is an ENTITIES transition, so its operator-configured
	// margin floor applies only when the transaction's body effects are valid.
	if tx.IsValid() {
		if err := checkPoolMarginFloor(
			tx.Certificates(),
			minPoolMarginFromLedgerState(ls),
		); err != nil {
			errs = append(errs, err)
		}
	}
	if err := validateParameterChangeExcludesProtocolVersion(tx, slot, ls, pp); err != nil {
		errs = append(
			errs,
			fmt.Errorf("dijkstra parameter-change validation: %w", err),
		)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	// Applied before the skip-phase-2 shortcut below, and before delegating
	// to gdijkstra.UtxoValidatePlutusScripts, so a transaction using a
	// synthetic-cost-model PlutusV2 script cannot slip through either path
	// -- see dijkstraSyntheticV2CostModelGuard.
	if syntheticV2CostModelInEffect(ls) {
		if err := dijkstraSyntheticV2CostModelGuard(
			tx,
			slot,
			ls,
			tmpPparams,
		); err != nil {
			return err
		}
	}
	if shouldSkipPhase2Validation(ls) {
		return nil
	}

	// gdijkstra.UtxoValidatePlutusScripts deliberately skips transactions whose
	// IsValid flag is false. Re-evaluate with a copied concrete transaction set
	// valid so phase-2 is independent of the block producer's declaration. The
	// concrete *DijkstraTransaction is essential: the upstream evaluator uses it
	// for Dijkstra guarding redeemers and sub-transaction witness scripts.
	phase2Tx, err := dijkstraTransactionForPhase2(tx)
	if err != nil {
		return err
	}
	if err := validateDijkstraPlutusV4ReferenceInputOverlap(phase2Tx, ls); err != nil {
		return err
	}
	phase2Err := gdijkstra.UtxoValidatePlutusScripts(
		phase2Tx,
		slot,
		ls,
		tmpPparams,
	)
	return validatePlutusOutcome(tx, phase2Err)
}

// dijkstraSyntheticV2CostModelGuard rejects a transaction that uses --
// directly witnesses, or resolves via a reference script or a Dijkstra
// sub-transaction -- a PlutusV2 script while HardForkBabbage's fabricated
// default is still the only PlutusV2 cost model in force; see
// ErrNoCostModelForPlutusV2. ValidateTxDijkstra delegates phase-2 validation
// entirely to gdijkstra.UtxoValidatePlutusScripts, and skips it outright
// when phase-2 validation is disabled -- neither path knows about Dingo's
// synthetic marker. Rather than reimplementing gdijkstra's reference-script
// and sub-transaction script resolution locally, this reuses its own
// exported UtxoValidateCostModelsPresent (the same NoCostModel rule
// Babbage/Conway's local phase-2 evaluation encodes by hand) against a
// pruned copy of pp with the PlutusV2 entry (map key 1) removed.
func dijkstraSyntheticV2CostModelGuard(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp *gdijkstra.DijkstraProtocolParameters,
) error {
	prunedCostModels := make(map[uint][]int64, len(pp.CostModels))
	for version, model := range pp.CostModels {
		if version == 1 {
			continue
		}
		prunedCostModels[version] = model
	}
	prunedPParams := *pp
	prunedPParams.CostModels = prunedCostModels
	err := gdijkstra.UtxoValidateCostModelsPresent(tx, slot, ls, &prunedPParams)
	if err == nil {
		return nil
	}
	var missing lcommon.MissingCostModelError
	if errors.As(err, &missing) && missing.Version == 1 {
		return fmt.Errorf(
			"dijkstra plutus validation: %w",
			ErrNoCostModelForPlutusV2,
		)
	}
	return err
}

var dijkstraPhase1UtxoValidationRules = buildDijkstraValidationRules()

func buildDijkstraValidationRules() []indexedUtxoValidationRule {
	// Skips are resolved by upstream rule Id, never by validation function.
	// Dijkstra reimplements several rules that Conway owned in earlier
	// releases, so the package a rule's function lives in is not stable
	// either.
	skipRuleIds := []lcommon.UtxoValidationRuleId{
		lcommon.UtxoValidationRulePlutusScripts,
		lcommon.UtxoValidationRuleCommitteeCertificates,
		lcommon.UtxoValidationRuleUnknownVoters,
	}
	descriptors := gdijkstra.UtxoValidationRuleDescriptors()
	indexes := make([]int, len(skipRuleIds))
	for i := range skipRuleIds {
		indexes[i] = resolveUtxoValidationSkipIndex(
			descriptors, gdijkstra.UtxoValidationRules, skipRuleIds[i],
		)
	}
	ret := buildIndexedUtxoValidationRulesWithSkips(
		descriptors,
		gdijkstra.UtxoValidationRules,
		skipRuleIds,
	)
	ret = append(ret,
		indexedUtxoValidationRule{
			index:          indexes[1],
			validationFunc: validateCommitteeCertificates,
		},
		indexedUtxoValidationRule{
			index:          indexes[2],
			validationFunc: validateUnknownVoters,
		},
	)
	slices.SortFunc(ret, func(a, b indexedUtxoValidationRule) int {
		return a.index - b.index
	})
	return ret
}

func dijkstraPhase1ValidationRules() []indexedUtxoValidationRule {
	return dijkstraPhase1UtxoValidationRules
}

func dijkstraTransactionForPhase2(
	tx lcommon.Transaction,
) (*gdijkstra.DijkstraTransaction, error) {
	dijkstraTx, ok := tx.(*gdijkstra.DijkstraTransaction)
	if !ok || dijkstraTx == nil {
		return nil, fmt.Errorf(
			"dijkstra phase-2 validation requires *dijkstra.DijkstraTransaction, got %T",
			tx,
		)
	}
	phase2Tx := *dijkstraTx
	phase2Tx.TxIsValid = true
	return &phase2Tx, nil
}

type dijkstraPlutusScriptLevel struct {
	body       lcommon.TransactionBody
	witnesses  lcommon.TransactionWitnessSet
	resolved   []lcommon.Utxo
	overlap    lcommon.TransactionInput
	hasOverlap bool
}

// validateDijkstraPlutusV4ReferenceInputOverlap supplies the Plutus V4
// context-construction check missing from gOuroboros v0.208.0. It checks each
// transaction level only when that level has a redeemer for an available V4
// script, matching the script execution boundary in the ledger rules.
func validateDijkstraPlutusV4ReferenceInputOverlap(
	tx *gdijkstra.DijkstraTransaction,
	ls lcommon.LedgerState,
) error {
	subTransactions := tx.Body.TxSubTransactions.Items()
	levels := make([]dijkstraPlutusScriptLevel, 0, len(subTransactions)+1)
	for index := range subTransactions {
		levels = append(levels, dijkstraPlutusScriptLevel{
			body:      &subTransactions[index].Body,
			witnesses: subTransactions[index].WitnessSet,
		})
	}
	levels = append(levels, dijkstraPlutusScriptLevel{
		body:      &tx.Body,
		witnesses: tx.WitnessSet,
	})
	hasOverlap := false
	for levelIndex := range levels {
		level := &levels[levelIndex]
		level.overlap, level.hasOverlap = dijkstraReferenceInputOverlap(level.body)
		hasOverlap = hasOverlap || level.hasOverlap
	}
	if !hasOverlap {
		return nil
	}
	if !dijkstraLevelsHaveRedeemers(levels) {
		return nil
	}
	if ls == nil {
		return errors.New("ledger state is required for Dijkstra script validation")
	}

	available := make(map[lcommon.ScriptHash]lcommon.Script)
	for levelIndex := range levels {
		level := &levels[levelIndex]
		inputs, referenceInputs, err := resolveDijkstraScriptLevelInputs(
			level.body,
			ls,
		)
		if err != nil {
			return err
		}
		level.resolved = script.ConcatResolvedInputs(inputs, referenceInputs)
		maps.Copy(available, script.PlutusWitnessScripts(level.witnesses))
		for _, utxo := range level.resolved {
			if utxo.Output == nil {
				continue
			}
			candidate := utxo.Output.ScriptRef()
			if candidate == nil {
				continue
			}
			if _, ok := lcommon.PlutusScriptVersion(candidate); ok {
				available[candidate.Hash()] = candidate
			}
		}
	}

	for _, level := range levels {
		if level.witnesses == nil || level.witnesses.Redeemers() == nil {
			continue
		}
		redeemers := make(map[lcommon.RedeemerKey]struct{})
		for key := range level.witnesses.Redeemers().Iter() {
			redeemers[key] = struct{}{}
		}
		if !level.hasOverlap {
			continue
		}
		for _, needed := range script.ScriptPurposes(level.body, level.resolved) {
			if _, ok := redeemers[needed.Key]; !ok {
				continue
			}
			candidate, ok := available[needed.Purpose.ScriptHash()]
			if !ok {
				continue
			}
			version, ok := lcommon.PlutusScriptVersion(candidate)
			if !ok || version != 3 {
				continue
			}
			return conway.ScriptContextConstructionError{
				Err: fmt.Errorf(
					"plutus V4 reference input %s is also a regular input",
					level.overlap.String(),
				),
			}
		}
	}
	return nil
}

func dijkstraLevelsHaveRedeemers(levels []dijkstraPlutusScriptLevel) bool {
	for _, level := range levels {
		if level.witnesses == nil || level.witnesses.Redeemers() == nil {
			continue
		}
		for range level.witnesses.Redeemers().Iter() {
			return true
		}
	}
	return false
}

func resolveDijkstraScriptLevelInputs(
	body lcommon.TransactionBody,
	ls lcommon.LedgerState,
) (inputs, referenceInputs []lcommon.Utxo, err error) {
	inputs = make([]lcommon.Utxo, 0, len(body.Inputs()))
	for _, input := range body.Inputs() {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			return nil, nil, lcommon.InputResolutionError{
				Input: input,
				Err:   err,
			}
		}
		inputs = append(inputs, utxo)
	}
	referenceInputs = make(
		[]lcommon.Utxo,
		0,
		len(body.ReferenceInputs()),
	)
	for _, input := range body.ReferenceInputs() {
		utxo, err := ls.UtxoById(input)
		if err != nil {
			return nil, nil, lcommon.ReferenceInputResolutionError{
				Input: input,
				Err:   err,
			}
		}
		referenceInputs = append(referenceInputs, utxo)
	}
	return inputs, referenceInputs, nil
}

func dijkstraReferenceInputOverlap(
	body lcommon.TransactionBody,
) (lcommon.TransactionInput, bool) {
	inputs := make(map[string]struct{}, len(body.Inputs()))
	for _, input := range body.Inputs() {
		inputs[input.String()] = struct{}{}
	}
	for _, input := range body.ReferenceInputs() {
		if _, ok := inputs[input.String()]; ok {
			return input, true
		}
	}
	return nil, false
}

// EvaluateTxDijkstra runs every Plutus redeemer of a Dijkstra transaction,
// at the top level and in each sub-transaction, with the per-transaction
// execution limit as each script's budget. The returned total and fee cover
// every level. The per-redeemer map is keyed by tag and index, which name a
// redeemer only within its own level, so it carries the top-level redeemers.
func EvaluateTxDijkstra(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	tmpPparams, ok := pp.(*gdijkstra.DijkstraProtocolParameters)
	if !ok || tmpPparams == nil {
		return 0, lcommon.ExUnits{}, nil, ErrIncompatibleProtocolParams
	}
	dijkstraTx, ok := tx.(*gdijkstra.DijkstraTransaction)
	if !ok || dijkstraTx == nil {
		return EvaluateTxConway(tx, ls, &tmpPparams.ConwayProtocolParameters)
	}
	if syntheticV2CostModelInEffect(ls) {
		if err := dijkstraSyntheticV2CostModelGuard(
			tx, 0, ls, tmpPparams,
		); err != nil {
			return 0, lcommon.ExUnits{}, nil, err
		}
	}
	if err := gdijkstra.UtxoValidateCostModelsPresent(
		tx, 0, ls, tmpPparams,
	); err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	results, err := gdijkstra.EvaluatePlutusScripts(
		dijkstraTx,
		ls,
		tmpPparams,
		tmpPparams.MaxTxExUnits,
	)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	var total lcommon.ExUnits
	redeemers := make(map[lcommon.RedeemerKey]lcommon.ExUnits)
	for _, result := range results {
		total, err = SafeAddExUnits(total, result.ExUnits)
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, fmt.Errorf(
				"aggregate execution units: %w",
				err,
			)
		}
		if result.SubTransactionIndex == nil {
			redeemers[result.Key] = result.ExUnits
		}
	}
	fee, err := dijkstraEvaluationFee(tx, ls, tmpPparams, total)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	return fee, total, redeemers, nil
}

// dijkstraEvaluationFee is the Dijkstra minimum fee for tx with exUnits as
// its execution units. As in the Dijkstra minimum-fee rule, only top-level
// reference scripts are charged, at the protocol's tiered stride and
// multiplier.
func dijkstraEvaluationFee(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *gdijkstra.DijkstraProtocolParameters,
	exUnits lcommon.ExUnits,
) (uint64, error) {
	var pricesMem, pricesSteps *big.Rat
	if pp.ExecutionCosts.MemPrice != nil {
		pricesMem = pp.ExecutionCosts.MemPrice.ToBigRat()
	}
	if pp.ExecutionCosts.StepPrice != nil {
		pricesSteps = pp.ExecutionCosts.StepPrice.ToBigRat()
	}
	fee := CalculateMinFee(
		TxSizeForFee(tx),
		exUnits,
		pp.MinFeeA,
		pp.MinFeeB,
		pricesMem,
		pricesSteps,
	)
	refScriptSize, err := lcommon.ConsumedReferenceScriptSize(tx, ls)
	if err != nil {
		return 0, err
	}
	var costPerByte, multiplier *big.Rat
	if pp.MinFeeRefScriptCostPerByte != nil {
		costPerByte = pp.MinFeeRefScriptCostPerByte.ToBigRat()
	}
	if pp.RefScriptCostMultiplier != nil {
		multiplier = pp.RefScriptCostMultiplier.ToBigRat()
	}
	return saturatedAddUint64(
		fee,
		calculateTieredRefScriptFee(
			refScriptSize,
			costPerByte,
			uint64(pp.RefScriptCostStride),
			multiplier,
		),
	), nil
}
