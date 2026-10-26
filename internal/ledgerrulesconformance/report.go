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

package ledgerrulesconformance

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/conformance"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockconformance "github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
)

const (
	schemaVersion                     = "1"
	maxConcurrentLedgerFixtureWorkers = 4
)

type Report struct {
	SchemaVersion  string            `json:"version"`
	Scenario       string            `json:"scenario"`
	GeneratedAt    string            `json:"generated_at"`
	Implementation map[string]string `json:"implementation"`
	Totals         Totals            `json:"-"`
	Scenarios      []CaseOutcome     `json:"scenarios"`
}

type Totals struct {
	Passed int `json:"passed"`
	Failed int `json:"failed"`
	Total  int `json:"total"`
}

type CaseOutcome struct {
	ID     string `json:"id"`
	Result string `json:"result"`
}

type Failure struct {
	ID       string
	Expected string
	Observed string
}

type fixture struct {
	Network            string          `json:"network"`
	EraHistory         json.RawMessage `json:"era_history"`
	ProtocolParameters json.RawMessage `json:"protocol_parameters"`
	InitialState       initialStateDoc `json:"initial_state"`
	Point              struct {
		Slot             uint64 `json:"slot"`
		TransactionIndex uint32 `json:"transaction_index"`
	} `json:"point"`
	Transaction string          `json:"transaction"`
	Expected    json.RawMessage `json:"expected"`
}

type initialStateDoc struct {
	UTxO               []utxoDoc                `json:"utxo"`
	Pools              []string                 `json:"pools"`
	Accounts           []accountDoc             `json:"accounts"`
	DReps              []drepDoc                `json:"dreps"`
	Committee          []committeeDoc           `json:"committee"`
	Proposals          [][]json.RawMessage      `json:"proposals"`
	ProposalRoots      map[string]proposalIDDoc `json:"proposals_roots"`
	GovernanceActivity *struct {
		ConsecutiveDormantEpochs uint64 `json:"consecutive_dormant_epochs"`
	} `json:"governance_activity"`
	Pots *struct {
		Treasury *uint64 `json:"treasury"`
		Reserves *uint64 `json:"reserves"`
	} `json:"pots"`
	GuardrailScript *string `json:"guardrail_script"`
}

type utxoDoc struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

type accountDoc struct {
	Credential string `json:"credential"`
	Deposit    uint64 `json:"deposit"`
	Rewards    uint64 `json:"rewards"`
	Pool       *struct {
		ID string `json:"id"`
	} `json:"pool"`
	DRep *struct {
		ID string `json:"id"`
	} `json:"drep"`
}

type drepDoc struct {
	Credential string `json:"credential"`
	Deposit    uint64 `json:"deposit"`
	ValidUntil uint64 `json:"valid_until"`
}

type committeeDoc struct {
	ColdCredential string          `json:"cold_credential"`
	Status         json.RawMessage `json:"status"`
	ValidUntil     *uint64         `json:"valid_until"`
}

type proposalIDDoc struct {
	TransactionID string `json:"transaction_id"`
	ProposalIndex uint32 `json:"proposal_index"`
}

type expectedDoc struct {
	Predicate       string `json:"predicate"`
	Description     string `json:"description"`
	DecodingFailure bool   `json:"decoding_failure"`
}

type eraHistoryDoc struct {
	StabilityWindow uint64 `json:"stability_window"`
	Eras            []struct {
		Start struct {
			Slot  uint64 `json:"slot"`
			Epoch uint64 `json:"epoch"`
		} `json:"start"`
		End *struct {
			Slot  uint64 `json:"slot"`
			Epoch uint64 `json:"epoch"`
		} `json:"end"`
		Params struct {
			EpochSizeSlots uint64 `json:"epoch_size_slots"`
		} `json:"params"`
	} `json:"eras"`
}

type ratioDoc struct {
	Numerator   int64 `json:"numerator"`
	Denominator int64 `json:"denominator"`
}

type exUnitsDoc struct {
	Memory int64 `json:"mem"`
	Steps  int64 `json:"steps"`
}

type protocolParametersDoc struct {
	MinFeeCoefficient             uint     `json:"min_fee_coefficient"`
	MinFeeConstant                uint     `json:"min_fee_constant"`
	MaxBlockBodySize              uint     `json:"max_block_body_size"`
	MaxBlockHeaderSize            uint     `json:"max_block_header_size"`
	MaxTransactionSize            uint     `json:"max_transaction_size"`
	MinUtxoDepositCoefficient     uint64   `json:"min_utxo_deposit_coefficient"`
	StakeCredentialDeposit        uint     `json:"stake_credential_deposit"`
	StakePoolDeposit              uint     `json:"stake_pool_deposit"`
	StakePoolRetirementEpochBound uint     `json:"stake_pool_retirement_epoch_bound"`
	DesiredNumberOfStakePools     uint     `json:"desired_number_of_stake_pools"`
	StakePoolPledgeInfluence      ratioDoc `json:"stake_pool_pledge_influence"`
	MonetaryExpansion             ratioDoc `json:"monetary_expansion"`
	TreasuryExpansion             ratioDoc `json:"treasury_expansion"`
	Version                       struct {
		Major uint `json:"major"`
		Minor uint `json:"minor"`
	} `json:"version"`
	MinStakePoolCost uint64 `json:"min_stake_pool_cost"`
	PlutusCostModels struct {
		V1 []int64 `json:"plutus_v1"`
		V2 []int64 `json:"plutus_v2"`
		V3 []int64 `json:"plutus_v3"`
	} `json:"plutus_cost_models"`
	ScriptExecutionPrices struct {
		MemPrice  ratioDoc `json:"mem_price"`
		StepPrice ratioDoc `json:"step_price"`
	} `json:"script_execution_prices"`
	MaxExecutionUnitsPerTransaction exUnitsDoc `json:"max_execution_units_per_transaction"`
	MaxExecutionUnitsPerBlock       exUnitsDoc `json:"max_execution_units_per_block"`
	MaxValueSize                    uint       `json:"max_value_size"`
	// Conway fixes the per-transaction reference-script limit, so Dingo parses
	// but does not apply this value.
	MaxReferenceScriptsSize   *uint64 `json:"max_reference_scripts_size"`
	CollateralPercentage      uint    `json:"collateral_percentage"`
	MaxCollateralInputs       uint    `json:"max_collateral_inputs"`
	StakePoolVotingThresholds struct {
		MotionNoConfidence      ratioDoc `json:"motion_no_confidence"`
		CommitteeNormal         ratioDoc `json:"committee_normal"`
		CommitteeNoConfidence   ratioDoc `json:"committee_no_confidence"`
		HardForkInitiation      ratioDoc `json:"hard_fork_initiation"`
		SecurityVotingThreshold ratioDoc `json:"security_voting_threshold"`
	} `json:"stake_pool_voting_thresholds"`
	DRepVotingThresholds struct {
		MotionNoConfidence    ratioDoc `json:"motion_no_confidence"`
		CommitteeNormal       ratioDoc `json:"committee_normal"`
		CommitteeNoConfidence ratioDoc `json:"committee_no_confidence"`
		UpdateConstitution    ratioDoc `json:"update_constitution"`
		HardForkInitiation    ratioDoc `json:"hard_fork_initiation"`
		PpNetworkGroup        ratioDoc `json:"pp_network_group"`
		PpEconomicGroup       ratioDoc `json:"pp_economic_group"`
		PpTechnicalGroup      ratioDoc `json:"pp_technical_group"`
		PpGovernanceGroup     ratioDoc `json:"pp_governance_group"`
		TreasuryWithdrawal    ratioDoc `json:"treasury_withdrawal"`
	} `json:"delegate_representative_voting_thresholds"`
	ConstitutionalCommitteeMinSize       uint   `json:"constitutional_committee_min_size"`
	ConstitutionalCommitteeMaxTermLength uint64 `json:"constitutional_committee_max_term_length"`
	GovernanceActionLifetime             uint64 `json:"governance_action_lifetime"`
	GovernanceActionDeposit              uint64 `json:"governance_action_deposit"`
	DelegateRepresentativeDeposit        uint64 `json:"delegate_representative_deposit"`
	DelegateRepresentativeMaxIdleTime    uint64 `json:"delegate_representative_max_idle_time"`
	MinFeeReferenceScripts               struct {
		Base ratioDoc `json:"base"`
	} `json:"min_fee_reference_scripts"`
}

// GenerateReport evaluates every versioned ledger-rule fixture against
// Dingo's Conway transaction validator and returns schema-compatible results.
func GenerateReport(
	root string,
	generatedAt time.Time,
) (Report, []Failure, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return Report{}, nil, fmt.Errorf(
			"resolve ledger fixture directory: %w",
			err,
		)
	}
	files, err := filepath.Glob(filepath.Join(root, "scenarios", "*.json"))
	if err != nil {
		return Report{}, nil, fmt.Errorf("find ledger fixtures: %w", err)
	}
	if len(files) == 0 {
		return Report{}, nil, errors.New(
			"ledger fixture directory contains no scenarios",
		)
	}
	sort.Strings(files)

	report := Report{
		SchemaVersion:  schemaVersion,
		Scenario:       "ledger-rules",
		GeneratedAt:    generatedAt.UTC().Format(time.RFC3339),
		Implementation: map[string]string{"name": "dingo"},
		Scenarios:      make([]CaseOutcome, 0, len(files)),
	}
	failures := make([]Failure, 0)
	type indexedResult struct {
		index   int
		outcome CaseOutcome
		failure *Failure
		err     error
	}
	workerCount := min(
		maxConcurrentLedgerFixtureWorkers,
		runtime.GOMAXPROCS(0),
		len(files),
	)
	jobs := make(chan int, len(files))
	results := make(chan indexedResult, len(files))
	for index := range files {
		jobs <- index
	}
	close(jobs)

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				path := files[index]
				outcome, failure, err := evaluateFixture(root, path)
				results <- indexedResult{
					index:   index,
					outcome: outcome,
					failure: failure,
					err:     err,
				}
			}
		}()
	}
	workers.Wait()
	close(results)

	orderedResults := make([]indexedResult, len(files))
	for result := range results {
		orderedResults[result.index] = result
	}
	for _, result := range orderedResults {
		if result.err != nil {
			return Report{}, failures, result.err
		}
		report.Scenarios = append(report.Scenarios, result.outcome)
		report.Totals.Total++
		if result.failure == nil {
			report.Totals.Passed++
		} else {
			report.Totals.Failed++
			failures = append(failures, *result.failure)
		}
	}
	return report, failures, nil
}

func evaluateFixture(
	root, path string,
) (CaseOutcome, *Failure, error) {
	manager, err := conformance.NewDingoStateManager()
	if err != nil {
		return CaseOutcome{}, nil, fmt.Errorf(
			"create Dingo ledger state manager: %w",
			err,
		)
	}
	outcome, failure, err := runFixture(root, path, manager)
	closeErr := manager.Close()
	if err != nil {
		return CaseOutcome{}, nil, fmt.Errorf("run fixture %s: %w", path, err)
	}
	if closeErr != nil {
		return CaseOutcome{}, nil, fmt.Errorf(
			"close ledger state manager after %s: %w",
			path,
			closeErr,
		)
	}
	return outcome, failure, nil
}

// WriteReport writes the ledger-rules report as indented JSON.
func WriteReport(path string, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode ledger-rules report: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write ledger-rules report: %w", err)
	}
	return nil
}

func runFixture(
	root, path string,
	manager *conformance.DingoStateManager,
) (CaseOutcome, *Failure, error) {
	var fixture fixture
	data, err := os.ReadFile(path)
	if err != nil {
		return CaseOutcome{}, nil, fmt.Errorf("read fixture: %w", err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		return CaseOutcome{}, nil, fmt.Errorf("decode fixture JSON: %w", err)
	}
	id := filepath.Base(path)
	if len(id) < 5 {
		return CaseOutcome{}, nil, fmt.Errorf(
			"fixture name has no five-digit id: %s",
			path,
		)
	}
	id = id[:5]
	for _, digit := range id {
		if digit < '0' || digit > '9' {
			return CaseOutcome{}, nil, fmt.Errorf(
				"fixture name has no five-digit id: %s",
				path,
			)
		}
	}

	expected, err := parseExpected(fixture.Expected)
	if err != nil {
		return CaseOutcome{}, nil, err
	}
	if expected.DecodingFailure {
		transactionCBOR, err := hex.DecodeString(fixture.Transaction)
		if err != nil {
			return CaseOutcome{}, nil, fmt.Errorf(
				"decode transaction hex: %w",
				err,
			)
		}
		if transactionFailsToDecode(transactionCBOR) {
			return CaseOutcome{ID: id, Result: "DecodingFailure"}, nil, nil
		}
		return mismatch(id, expected, "transaction decoded successfully")
	}

	protocolParameters, err := loadProtocolParameters(
		root,
		fixture.ProtocolParameters,
	)
	if err != nil {
		return CaseOutcome{}, nil, err
	}
	eraHistory, err := loadEraHistory(root, fixture.EraHistory)
	if err != nil {
		return CaseOutcome{}, nil, err
	}
	initialState, metadata, err := parseInitialState(
		fixture.InitialState,
		eraHistory,
		fixture.Point.Slot,
	)
	if err != nil {
		return CaseOutcome{}, nil, err
	}
	pp, err := protocolParametersToLedger(protocolParameters)
	if err != nil {
		return CaseOutcome{}, nil, fmt.Errorf(
			"decode protocol parameters: %w",
			err,
		)
	}
	if err := manager.LoadLedgerRulesInitialState(initialState, pp, metadata); err != nil {
		return CaseOutcome{}, nil, fmt.Errorf(
			"seed initial ledger state: %w",
			err,
		)
	}

	transactionCBOR, err := hex.DecodeString(fixture.Transaction)
	if err != nil {
		return CaseOutcome{}, nil, fmt.Errorf("decode transaction hex: %w", err)
	}
	tx, err := conway.NewConwayTransactionFromCbor(transactionCBOR)
	if err != nil {
		return mismatch(
			id,
			expected,
			"transaction decoding failure: "+err.Error(),
		)
	}
	provider := conformance.NewDingoStateProvider(manager)
	provider.SetForecastHorizon(fixture.Point.Slot, eraHistory.StabilityWindow)
	validationErr := eras.ValidateTxConway(
		tx,
		fixture.Point.Slot,
		provider,
		pp,
	)
	if predicate := unregisteredDelegationPredicate(tx, provider, validationErr); validationErr != nil &&
		predicate != "" {
		outcome := CaseOutcome{ID: id, Result: predicate}
		if predicate == expected.Predicate {
			return outcome, nil, nil
		}
		_, failure, err := mismatch(
			id,
			expected,
			"rejected: "+validationErr.Error(),
		)
		return outcome, failure, err
	}
	if expected.Predicate == "" {
		if validationErr == nil {
			return CaseOutcome{ID: id, Result: "pass"}, nil, nil
		}
		return mismatch(id, expected, "rejected: "+validationErr.Error())
	}
	if validationErr == nil {
		return mismatch(id, expected, "transaction accepted")
	}
	if predicateMatches(validationErr, expected) {
		return CaseOutcome{ID: id, Result: expected.Predicate}, nil, nil
	}
	return mismatch(id, expected, "rejected: "+validationErr.Error())
}

// The ledger uses one error type for pool and vote delegation by an
// unregistered credential. NDC distinguishes them by certificate type.
func unregisteredDelegationPredicate(
	tx common.Transaction,
	state common.LedgerState,
	err error,
) string {
	delegationErr, ok := errors.AsType[conway.DelegateUnregisteredStakeCredentialError](
		err,
	)
	if !ok {
		return ""
	}
	registered := state.IsStakeCredentialRegistered(delegationErr.Credential)
	for _, certificate := range tx.Certificates() {
		var credential *common.Credential
		var registration *bool
		var predicate string
		switch cert := certificate.(type) {
		case *common.StakeRegistrationCertificate:
			credential, registration = &cert.StakeCredential, new(true)
		case *common.RegistrationCertificate:
			credential, registration = &cert.StakeCredential, new(true)
		case *common.StakeRegistrationDelegationCertificate:
			credential, registration = &cert.StakeCredential, new(true)
		case *common.VoteRegistrationDelegationCertificate:
			credential, registration = &cert.StakeCredential, new(true)
		case *common.StakeVoteRegistrationDelegationCertificate:
			credential, registration = &cert.StakeCredential, new(true)
		case *common.StakeDeregistrationCertificate:
			credential, registration = &cert.StakeCredential, new(false)
		case *common.DeregistrationCertificate:
			credential, registration = &cert.StakeCredential, new(false)
		case *common.StakeDelegationCertificate:
			credential = cert.StakeCredential
			predicate = "StakeCredentialInvalidPoolDelegation"
		case *common.VoteDelegationCertificate:
			credential = &cert.StakeCredential
			predicate = "StakeCredentialInvalidVoteDelegation"
		case *common.StakeVoteDelegationCertificate:
			credential = &cert.StakeCredential
			predicate = "StakeCredentialInvalidVoteDelegation"
		default:
			continue
		}
		if credential == nil ||
			credential.CredType != delegationErr.Credential.CredType ||
			credential.Credential != delegationErr.Credential.Credential {
			continue
		}
		if registration != nil {
			registered = *registration
		} else if !registered {
			return predicate
		}
	}
	return ""
}

func transactionFailsToDecode(transactionCBOR []byte) bool {
	_, err := conway.NewConwayTransactionFromCbor(transactionCBOR)
	return err != nil
}

// unmappedLedgerResult is reported for observations that map to no NDC
// predicate, so the scenario fails without discarding the rest of the report.
const unmappedLedgerResult = "Unknown"

func mismatch(
	id string,
	expected expectedDoc,
	observed string,
) (CaseOutcome, *Failure, error) {
	result, ok := observedLedgerResult(observed)
	if !ok {
		// An unrecognised rejection must not abort the whole report; emit a
		// result that no NDC expectation can equal and keep the Failure.
		result = unmappedLedgerResult
	}
	expectedText := "Pass"
	if expected.DecodingFailure {
		expectedText = "decoding_failure"
	} else if expected.Predicate != "" {
		expectedText = expected.Predicate
		if expected.Description != "" {
			expectedText += ":" + expected.Description
		}
	}
	return CaseOutcome{ID: id, Result: result}, &Failure{
		ID:       id,
		Expected: expectedText,
		Observed: observed,
	}, nil
}

// observedLedgerResult emits the ledger outcome that NDC compares with its
// fixture expectation.
func observedLedgerResult(observed string) (string, bool) {
	switch {
	case observed == "transaction accepted" || observed == "transaction decoded successfully":
		return "pass", true
	case strings.HasPrefix(observed, "transaction decoding failure:"):
		return "DecodingFailure", true
	case strings.HasPrefix(observed, "rejected: "):
		return ledgerPredicateFromError(
			strings.TrimPrefix(observed, "rejected: "),
		)
	default:
		return "", false
	}
}

func ledgerPredicateFromError(errText string) (string, bool) {
	text := strings.ToLower(errText)
	predicates := make([]string, 0, len(ledgerPredicateErrorPatterns))
	for predicate := range ledgerPredicateErrorPatterns {
		predicates = append(predicates, predicate)
	}
	sort.Strings(predicates)

	var result string
	bestPatternLength := 0
	for _, predicate := range predicates {
		reportResult := predicate
		if separator := strings.IndexByte(reportResult, ':'); separator >= 0 {
			reportResult = reportResult[:separator]
		}
		if listed, ok := ndcPredicateAliases[reportResult]; ok {
			reportResult = listed
		}
		for _, pattern := range ledgerPredicateErrorPatterns[predicate] {
			pattern = strings.ToLower(pattern)
			if len(pattern) > bestPatternLength &&
				strings.Contains(text, pattern) {
				result = reportResult
				bestPatternLength = len(pattern)
			}
		}
	}
	return result, result != ""
}

func parseExpected(raw json.RawMessage) (expectedDoc, error) {
	var pass string
	if err := json.Unmarshal(raw, &pass); err == nil {
		if pass != "Pass" {
			return expectedDoc{}, fmt.Errorf("unknown expected result %q", pass)
		}
		return expectedDoc{}, nil
	}
	var expected expectedDoc
	if err := json.Unmarshal(raw, &expected); err != nil {
		return expectedDoc{}, fmt.Errorf("decode expected result: %w", err)
	}
	if expected.DecodingFailure == (expected.Predicate != "") {
		return expectedDoc{}, errors.New(
			"expected result must name one predicate or a decoding failure",
		)
	}
	return expected, nil
}

func predicateMatches(err error, expected expectedDoc) bool {
	if err == nil || expected.Predicate == "" {
		return false
	}
	text := strings.ToLower(fmt.Sprintf("%+v", err))
	predicate := expected.Predicate
	if predicate == "ValidationTagMismatch" && expected.Description != "" {
		predicate += ":" + expected.Description
	}
	patterns, ok := ledgerPredicateErrorPatterns[predicate]
	if !ok {
		patterns = []string{predicate}
	}
	for alias, listed := range ndcPredicateAliases {
		if listed == predicate {
			patterns = append(
				slices.Clone(patterns),
				ledgerPredicateErrorPatterns[alias]...)
		}
	}
	for _, pattern := range patterns {
		if strings.Contains(text, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

// ndcPredicateAliases names the listed predicate that reports a Dingo rule
// which the scenario's closed predicate list lacks. The scenario asks each
// implementation to map its own failures onto that list. Only the report is
// affected: the validator still raises the distinct rule.
//
// UnelectedCommitteeVoters, raised for a committee hot credential whose cold
// credential holds no seat, is reported as VotersDoNotExist, the listed
// predicate for a vote cast by a voter that cannot vote.
var ndcPredicateAliases = map[string]string{
	"UnelectedCommitteeVoters": "VotersDoNotExist",
}

var ledgerPredicateErrorPatterns = map[string][]string{
	"BabbageNonDisjointRefInputs": {
		"non-disjoint reference inputs",
	},
	"BabbageOutputTooSmallUTxO": {"output too small"},
	"BadInputsUTxO": {
		"bad input(s)",
		"failed to resolve input",
		"failed to resolve reference input",
		"utxo not found for collateral input",
	},
	"CommitteeHasPreviouslyResigned": {
		"committee member already resigned",
		"resigned cc member",
	},
	"CommitteeIsUnknown":      {"not a cc member"},
	"ConflictingMetadataHash": {"conflicting metadata hash"},
	"ConwayTreasuryValueMismatch": {
		"current treasury value mismatch",
	},
	"ConwayTxRefScriptsSizeTooBig": {
		"reference-script size per transaction too large",
	},
	"ConwayWdrlNotDelegatedToDRep": {
		"withdrawal is not delegated to a drep",
	},
	"DelegateeDRepNotRegistered": {
		"vote delegation to unregistered drep",
	},
	"DelegateeStakePoolNotRegistered": {
		"delegation to unregistered pool",
	},
	"DisallowedVoters":               {"voting restriction"},
	"DRepAlreadyRegistered":          {"drep already registered"},
	"DRepNotRegistered":              {"drep is not registered"},
	"ExtraneousScriptWitnessesUTXOW": {"extraneous script witness"},
	"FeeTooSmallUTxO": {
		"less than the calculated minimum fee",
	},
	"IncorrectDepositDELEG": {
		"incorrect deposit for certificate type",
	},
	"IncorrectTotalCollateralField": {
		"incorrect total collateral field",
	},
	"InputSetEmptyUTxO":      {"input set empty"},
	"InsufficientCollateral": {"insufficient collateral"},
	"InvalidGuardrailsScriptHash": {
		"invalid guardrails script hash",
	},
	"InvalidPrevGovActionId": {
		"previous governance action",
		"prev gov action",
		"invalid governance action ancestor",
	},
	"InvalidWitnessesUTXOW": {
		"invalid vkey signature",
		"invalid bootstrap signature",
	},
	"MalformedReferenceScripts": {
		"malformed reference scripts",
	},
	"OutsideForecast": {"outside forecast", "OutsideForecast"},
	"MalformedScriptWitnesses": {
		"malformed script witnesses",
	},
	"MaxTxSizeUTxO": {
		"transaction size too large",
	},
	"MissingScriptWitnessesUTXOW": {"missing script witness"},
	"MissingTxBodyMetadataHash": {
		"missing transaction auxiliary data hash",
	},
	"MissingTxMetadata": {
		"missing transaction metadata",
	},
	"MissingVerificationKeyWitnessesUTXOW": {
		"missing vkey witness",
		"missing required vkey witness",
		"missing bootstrap witness",
	},
	"NoCollateralInputs":          {"no collateral inputs"},
	"OutsideValidityIntervalUTxO": {"outside validity interval"},
	"OutputTooBigUTxO": {
		"output value too large",
		"output bootstrap address attributes too big",
	},
	"ProposalCantFollow": {
		"cannot follow current protocol version",
	},
	"ProposalReturnAccountDoesNotExist": {
		"proposal return account does not exist",
	},
	"ProposalProcedureNetworkIdMismatch": {
		"wrong network id in proposal address",
	},
	"ScriptsNotPaidUTxO": {
		"collateral input must be key-locked",
	},
	"StakeCredentialInvalidPoolDelegation": {
		"delegation from unregistered stake credential",
	},
	"StakeKeyHasNonZeroAccountBalance": {
		"stake credential has nonzero reward balance",
	},
	"StakeKeyNotRegistered": {"stake credential is not registered"},
	"StakeKeyRegistered": {
		"stake credential already registered",
	},
	"StakePoolCostTooLowPOOL":         {"below minimum"},
	"StakePoolNotRegisteredOnKeyPOOL": {"stake pool not registered"},
	"StakePoolRetirementWrongEpochPOOL": {
		"retirement epoch",
		"outside allowed range",
	},
	"TreasuryWithdrawalReturnAccountsDoNotExist": {
		"treasury withdrawal return account does not exist",
		"treasury withdrawal return account(s) do not exist",
	},
	"TreasuryWithdrawalsAllZeros": {
		"treasury withdrawals are all zero",
		"treasury withdrawal governance action has zero withdrawal amount",
	},
	"TooManyCollateralInputs": {
		"too many collateral inputs",
	},
	"ValidationTagMismatch:FailedUnexpectedly": {"plutus script failed"},
	"ValidationTagMismatch:PassedUnexpectedly": {
		"transaction declared invalid but plutus scripts succeeded",
		"transaction marked as invalid but has no plutus scripts requiring phase-2 validation",
	},
	"ValueNotConservedUTxO": {"value not conserved"},
	"VotersDoNotExist": {
		"vote cast by unknown voter",
	},
	"UnelectedCommitteeVoters": {
		"committee voter is not elected",
	},
	"VotingOnExpiredGovAction": {
		"voting on expired governance action",
		"vote cast on expired governance action",
	},
	"GovActionsDoNotExist": {
		"vote references unknown governance action id",
	},
	"WithdrawalsNotInRewardsCERTS": {
		"withdrawal from unregistered reward account",
		"incorrect withdrawal amount",
	},
	"WrongNetworkInTxBody": {
		"wrong transaction network id",
	},
	"WrongNetworkInTxOutput": {"wrong network: addr"},
	"WrongNetworkPOOL":       {"reward account network"},
	"WrongNetworkWithdrawal": {"wrong network withdrawals"},
}

func loadProtocolParameters(
	root string,
	raw json.RawMessage,
) (protocolParametersDoc, error) {
	resolved, err := resolveReferencedObject(root, raw)
	if err != nil {
		return protocolParametersDoc{}, fmt.Errorf(
			"load protocol parameters: %w",
			err,
		)
	}
	data, err := json.Marshal(resolved)
	if err != nil {
		return protocolParametersDoc{}, fmt.Errorf(
			"encode protocol parameters: %w",
			err,
		)
	}
	var parameters protocolParametersDoc
	if err := json.Unmarshal(data, &parameters); err != nil {
		return protocolParametersDoc{}, fmt.Errorf(
			"decode protocol parameters: %w",
			err,
		)
	}
	return parameters, nil
}

func protocolParametersToLedger(
	doc protocolParametersDoc,
) (*conway.ConwayProtocolParameters, error) {
	ratPtr := func(value ratioDoc) (*cbor.Rat, error) {
		if value.Denominator <= 0 {
			return nil, errors.New("ratio denominator must be positive")
		}
		return &cbor.Rat{
			Rat: big.NewRat(value.Numerator, value.Denominator),
		}, nil
	}
	rat := func(value ratioDoc) (cbor.Rat, error) {
		converted, err := ratPtr(value)
		if err != nil {
			return cbor.Rat{}, err
		}
		return *converted, nil
	}
	poolThresholds := conway.PoolVotingThresholds{}
	thresholdPairs := []struct {
		target *cbor.Rat
		value  ratioDoc
	}{
		{
			&poolThresholds.MotionNoConfidence,
			doc.StakePoolVotingThresholds.MotionNoConfidence,
		},
		{
			&poolThresholds.CommitteeNormal,
			doc.StakePoolVotingThresholds.CommitteeNormal,
		},
		{
			&poolThresholds.CommitteeNoConfidence,
			doc.StakePoolVotingThresholds.CommitteeNoConfidence,
		},
		{
			&poolThresholds.HardForkInitiation,
			doc.StakePoolVotingThresholds.HardForkInitiation,
		},
		{
			&poolThresholds.PpSecurityGroup,
			doc.StakePoolVotingThresholds.SecurityVotingThreshold,
		},
	}
	for _, pair := range thresholdPairs {
		value, err := rat(pair.value)
		if err != nil {
			return nil, err
		}
		*pair.target = value
	}
	drepThresholds := conway.DRepVotingThresholds{}
	drepPairs := []struct {
		target *cbor.Rat
		value  ratioDoc
	}{
		{
			&drepThresholds.MotionNoConfidence,
			doc.DRepVotingThresholds.MotionNoConfidence,
		},
		{
			&drepThresholds.CommitteeNormal,
			doc.DRepVotingThresholds.CommitteeNormal,
		},
		{
			&drepThresholds.CommitteeNoConfidence,
			doc.DRepVotingThresholds.CommitteeNoConfidence,
		},
		{
			&drepThresholds.UpdateToConstitution,
			doc.DRepVotingThresholds.UpdateConstitution,
		},
		{
			&drepThresholds.HardForkInitiation,
			doc.DRepVotingThresholds.HardForkInitiation,
		},
		{
			&drepThresholds.PpNetworkGroup,
			doc.DRepVotingThresholds.PpNetworkGroup,
		},
		{
			&drepThresholds.PpEconomicGroup,
			doc.DRepVotingThresholds.PpEconomicGroup,
		},
		{
			&drepThresholds.PpTechnicalGroup,
			doc.DRepVotingThresholds.PpTechnicalGroup,
		},
		{
			&drepThresholds.PpGovGroup,
			doc.DRepVotingThresholds.PpGovernanceGroup,
		},
		{
			&drepThresholds.TreasuryWithdrawal,
			doc.DRepVotingThresholds.TreasuryWithdrawal,
		},
	}
	for _, pair := range drepPairs {
		value, err := rat(pair.value)
		if err != nil {
			return nil, err
		}
		*pair.target = value
	}
	a0, err := ratPtr(doc.StakePoolPledgeInfluence)
	if err != nil {
		return nil, fmt.Errorf("pool pledge influence: %w", err)
	}
	rho, err := ratPtr(doc.MonetaryExpansion)
	if err != nil {
		return nil, fmt.Errorf("monetary expansion: %w", err)
	}
	tau, err := ratPtr(doc.TreasuryExpansion)
	if err != nil {
		return nil, fmt.Errorf("treasury expansion: %w", err)
	}
	memPrice, err := ratPtr(doc.ScriptExecutionPrices.MemPrice)
	if err != nil {
		return nil, fmt.Errorf("memory price: %w", err)
	}
	stepPrice, err := ratPtr(doc.ScriptExecutionPrices.StepPrice)
	if err != nil {
		return nil, fmt.Errorf("step price: %w", err)
	}
	refScriptPrice, err := ratPtr(doc.MinFeeReferenceScripts.Base)
	if err != nil {
		return nil, fmt.Errorf("reference script price: %w", err)
	}
	return &conway.ConwayProtocolParameters{
		MinFeeA:            doc.MinFeeCoefficient,
		MinFeeB:            doc.MinFeeConstant,
		MaxBlockBodySize:   doc.MaxBlockBodySize,
		MaxTxSize:          doc.MaxTransactionSize,
		MaxBlockHeaderSize: doc.MaxBlockHeaderSize,
		KeyDeposit:         doc.StakeCredentialDeposit,
		PoolDeposit:        doc.StakePoolDeposit,
		MaxEpoch:           doc.StakePoolRetirementEpochBound,
		NOpt:               doc.DesiredNumberOfStakePools,
		A0:                 a0,
		Rho:                rho,
		Tau:                tau,
		ProtocolVersion: common.ProtocolParametersProtocolVersion{
			Major: doc.Version.Major,
			Minor: doc.Version.Minor,
		},
		MinPoolCost:    doc.MinStakePoolCost,
		AdaPerUtxoByte: doc.MinUtxoDepositCoefficient,
		CostModels: map[uint][]int64{
			0: doc.PlutusCostModels.V1,
			1: doc.PlutusCostModels.V2,
			2: doc.PlutusCostModels.V3,
		},
		ExecutionCosts: common.ExUnitPrice{
			MemPrice:  memPrice,
			StepPrice: stepPrice,
		},
		MaxTxExUnits: common.ExUnits{
			Memory: doc.MaxExecutionUnitsPerTransaction.Memory,
			Steps:  doc.MaxExecutionUnitsPerTransaction.Steps,
		},
		MaxBlockExUnits: common.ExUnits{
			Memory: doc.MaxExecutionUnitsPerBlock.Memory,
			Steps:  doc.MaxExecutionUnitsPerBlock.Steps,
		},
		MaxValueSize:               doc.MaxValueSize,
		CollateralPercentage:       doc.CollateralPercentage,
		MaxCollateralInputs:        doc.MaxCollateralInputs,
		PoolVotingThresholds:       poolThresholds,
		DRepVotingThresholds:       drepThresholds,
		MinCommitteeSize:           doc.ConstitutionalCommitteeMinSize,
		CommitteeTermLimit:         doc.ConstitutionalCommitteeMaxTermLength,
		GovActionValidityPeriod:    doc.GovernanceActionLifetime,
		GovActionDeposit:           doc.GovernanceActionDeposit,
		DRepDeposit:                doc.DelegateRepresentativeDeposit,
		DRepInactivityPeriod:       doc.DelegateRepresentativeMaxIdleTime,
		MinFeeRefScriptCostPerByte: refScriptPrice,
	}, nil
}

func loadEraHistory(root string, raw json.RawMessage) (eraHistoryDoc, error) {
	resolved, err := resolveReferencedObject(root, raw)
	if err != nil {
		return eraHistoryDoc{}, fmt.Errorf("load era history: %w", err)
	}
	data, err := json.Marshal(resolved)
	if err != nil {
		return eraHistoryDoc{}, fmt.Errorf("encode era history: %w", err)
	}
	var history eraHistoryDoc
	if err := json.Unmarshal(data, &history); err != nil {
		return eraHistoryDoc{}, fmt.Errorf("decode era history: %w", err)
	}
	return history, nil
}

func resolveReferencedObject(
	root string,
	raw json.RawMessage,
) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("expected JSON object: %w", err)
	}
	var overrides map[string]json.RawMessage
	if overrideRaw, ok := object["$override"]; ok {
		if err := json.Unmarshal(overrideRaw, &overrides); err != nil {
			return nil, fmt.Errorf("decode $override: %w", err)
		}
	}
	var ref string
	if refRaw, ok := object["$ref"]; ok {
		if err := json.Unmarshal(refRaw, &ref); err != nil {
			return nil, fmt.Errorf("decode $ref: %w", err)
		}
		path, err := resolveFixturePath(root, ref)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read referenced document %q: %w", ref, err)
		}
		if err := json.Unmarshal(data, &object); err != nil {
			return nil, fmt.Errorf(
				"decode referenced document %q: %w",
				ref,
				err,
			)
		}
	}
	delete(object, "$override")
	maps.Copy(object, overrides)
	delete(object, "$ref")
	return object, nil
}

func resolveFixturePath(root, ref string) (string, error) {
	if filepath.IsAbs(ref) {
		return "", fmt.Errorf(
			"referenced fixture path must be relative: %q",
			ref,
		)
	}
	clean := filepath.Clean(filepath.FromSlash(ref))
	if clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("referenced fixture path escapes corpus: %q", ref)
	}
	candidates := []string{filepath.Join(root, clean)}
	if commonPath, ok := strings.CutPrefix(filepath.ToSlash(clean), "common/"); ok {
		candidates = append(candidates, filepath.Join(root, commonPath))
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf(
		"referenced fixture %q was not found under %s",
		ref,
		root,
	)
}

func parseInitialState(
	doc initialStateDoc,
	history eraHistoryDoc,
	slot uint64,
) (*mockconformance.ParsedInitialState, conformance.LedgerRulesMetadata, error) {
	state := &mockconformance.ParsedInitialState{
		Utxos: make(
			map[string]mockconformance.ParsedUtxo,
		),
		StakeRegistrations: make(map[common.Blake2b224]bool),
		StakeRegistrationsByCredential: make(
			map[mockledger.RewardAccountKey]bool,
		),
		RewardAccounts: make(map[common.Blake2b224]uint64),
		RewardAccountBalances: make(
			map[mockledger.RewardAccountKey]uint64,
		),
		StakeCredentialDeposits: make(
			map[mockledger.RewardAccountKey]uint64,
		),
		PoolRegistrations: make(map[common.Blake2b224]bool),
		PoolDelegationsByCredential: make(
			map[mockledger.RewardAccountKey]common.PoolKeyHash,
		),
		CommitteeMembers: make(map[common.Blake2b224]uint64),
		CommitteeMembersByCredential: make(
			map[mockledger.RewardAccountKey]uint64,
		),
		DRepDeposits: make(
			map[mockledger.RewardAccountKey]uint64,
		),
		DRepRegistrationsByCredential: make(
			map[mockledger.RewardAccountKey]bool,
		),
		DRepExpiries: make(
			map[mockledger.RewardAccountKey]uint64,
		),
		DRepDelegations: make(
			map[common.Blake2b224]common.Drep,
		),
		DRepDelegationsByCredential: make(
			map[mockledger.RewardAccountKey]common.Drep,
		),
		HotKeyAuthorizations: make(
			map[common.Blake2b224]common.Blake2b224,
		),
		HotKeyAuthorizationsByCredential: make(
			map[mockledger.RewardAccountKey]common.Credential,
		),
		CommitteeResignations: make(
			map[mockledger.RewardAccountKey]bool,
		),
		Proposals: make(
			map[string]mockconformance.GovActionInfo,
		),
		CostModels: make(map[uint][]int64),
	}
	metadata := conformance.LedgerRulesMetadata{
		GovActionCbor: make(map[string][]byte),
	}
	currentEpoch, err := epochForSlot(history, slot)
	if err != nil {
		return nil, conformance.LedgerRulesMetadata{}, err
	}
	state.CurrentEpoch = currentEpoch
	for _, item := range doc.UTxO {
		inputData, err := hex.DecodeString(item.Input)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode UTxO input hex: %w",
				err,
			)
		}
		var input shelley.ShelleyTransactionInput
		if _, err := cbor.DecodeExact(inputData, &input); err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode UTxO input CBOR: %w",
				err,
			)
		}
		outputData, err := hex.DecodeString(item.Output)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode UTxO output hex: %w",
				err,
			)
		}
		output, err := babbage.NewBabbageTransactionOutputFromCbor(outputData)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode UTxO output CBOR: %w",
				err,
			)
		}
		id := fmt.Sprintf("%x#%d", input.TxId, input.OutputIndex)
		state.Utxos[id] = mockconformance.ParsedUtxo{
			TxHash: append([]byte(nil), input.TxId[:]...),
			Index:  input.OutputIndex,
			Output: output,
		}
	}
	for _, poolID := range doc.Pools {
		hash, err := decodeHash28(poolID)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode pool id: %w",
				err,
			)
		}
		state.PoolRegistrations[hash] = true
	}
	for _, account := range doc.Accounts {
		credential, key, err := decodeCredential(account.Credential)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode stake account credential: %w",
				err,
			)
		}
		state.StakeRegistrationsByCredential[key] = true
		state.RewardAccountBalances[key] = account.Rewards
		state.StakeCredentialDeposits[key] = account.Deposit
		if account.Pool != nil {
			poolHash, err := decodeHash28(account.Pool.ID)
			if err != nil {
				return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
					"decode delegated pool id: %w",
					err,
				)
			}
			state.PoolDelegationsByCredential[key] = common.PoolKeyHash(
				poolHash,
			)
		}
		if account.DRep != nil {
			drepData, err := hex.DecodeString(account.DRep.ID)
			if err != nil {
				return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
					"decode delegated DRep hex: %w",
					err,
				)
			}
			var drep common.Drep
			if _, err := cbor.DecodeExact(drepData, &drep); err != nil {
				return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
					"decode delegated DRep CBOR: %w",
					err,
				)
			}
			state.DRepDelegationsByCredential[key] = drep
		}
		if credential.CredType == common.CredentialTypeAddrKeyHash {
			state.StakeRegistrations[credential.Credential] = true
			state.RewardAccounts[credential.Credential] = account.Rewards
		}
	}
	for _, drep := range doc.DReps {
		credential, key, err := decodeCredential(drep.Credential)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode DRep credential: %w",
				err,
			)
		}
		state.DRepRegistrationsByCredential[key] = true
		state.DRepDeposits[key] = drep.Deposit
		state.DRepExpiries[key] = drep.ValidUntil
		if credential.CredType == common.CredentialTypeAddrKeyHash {
			state.DRepRegistrations = append(
				state.DRepRegistrations,
				credential.Credential,
			)
		}
	}
	for _, member := range doc.Committee {
		cold, key, err := decodeCredential(member.ColdCredential)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode committee cold credential: %w",
				err,
			)
		}
		var expiry uint64
		if member.ValidUntil != nil {
			expiry = *member.ValidUntil
		} else {
			metadata.UnelectedCommitteeCredentials = append(
				metadata.UnelectedCommitteeCredentials,
				cold,
			)
		}
		state.CommitteeMembersByCredential[key] = expiry
		if cold.CredType == common.CredentialTypeAddrKeyHash {
			state.CommitteeMembers[cold.Credential] = expiry
		}
		if len(member.Status) == 0 || string(member.Status) == "null" {
			continue
		}
		var status string
		if err := json.Unmarshal(member.Status, &status); err == nil &&
			status == "resigned" {
			state.CommitteeResignations[key] = true
			continue
		}
		var hot common.Credential
		if err := json.Unmarshal(member.Status, &status); err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode committee status: %w",
				err,
			)
		}
		statusCBOR, err := hex.DecodeString(status)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode committee hot credential hex: %w",
				err,
			)
		}
		if _, err := cbor.DecodeExact(statusCBOR, &hot); err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode committee hot credential CBOR: %w",
				err,
			)
		}
		state.HotKeyAuthorizationsByCredential[key] = hot
		if cold.CredType == common.CredentialTypeAddrKeyHash &&
			hot.CredType == common.CredentialTypeAddrKeyHash {
			state.HotKeyAuthorizations[cold.Credential] = hot.Credential
		}
	}
	for _, proposal := range doc.Proposals {
		if len(proposal) != 3 {
			return nil, conformance.LedgerRulesMetadata{}, errors.New(
				"proposal entry must have three values",
			)
		}
		var id proposalIDDoc
		var kind string
		var expires uint64
		if err := json.Unmarshal(proposal[0], &id); err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode proposal id: %w",
				err,
			)
		}
		if err := json.Unmarshal(proposal[1], &kind); err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode proposal kind: %w",
				err,
			)
		}
		if err := json.Unmarshal(proposal[2], &expires); err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode proposal expiry: %w",
				err,
			)
		}
		txHash, err := decodeHash32(id.TransactionID)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode proposal transaction id: %w",
				err,
			)
		}
		proposalKey := fmt.Sprintf("%x#%d", txHash, id.ProposalIndex)
		state.Proposals[proposalKey] = mockconformance.GovActionInfo{
			ActionType:   proposalActionType(kind),
			ExpiresAfter: expires,
		}
		actionCBOR, err := proposalActionCbor(kind)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"encode proposal %s action: %w", proposalKey, err,
			)
		}
		if len(actionCBOR) > 0 {
			metadata.GovActionCbor[proposalKey] = actionCBOR
		}
	}
	for purpose, id := range doc.ProposalRoots {
		txHash, err := decodeHash32(id.TransactionID)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode %s proposal root: %w",
				purpose,
				err,
			)
		}
		proposalKey := fmt.Sprintf("%x#%d", txHash, id.ProposalIndex)
		root := &proposalKey
		switch purpose {
		case "protocol_parameters":
			state.ProposalRoots.ProtocolParameters = root
		case "hard_fork":
			state.ProposalRoots.HardFork = root
		case "constitutional_committee":
			state.ProposalRoots.ConstitutionalCommittee = root
		case "constitution":
			state.ProposalRoots.Constitution = root
		default:
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"unknown proposal root purpose %q",
				purpose,
			)
		}
	}
	if doc.GuardrailScript != nil && *doc.GuardrailScript != "" {
		guardrail, err := decodeHash28(*doc.GuardrailScript)
		if err != nil {
			return nil, conformance.LedgerRulesMetadata{}, fmt.Errorf(
				"decode guardrail script hash: %w",
				err,
			)
		}
		state.Constitution = &mockconformance.ConstitutionInfo{
			// The scenario projects only the active policy hash. Dingo's
			// persisted constitution also requires a 32-byte anchor hash, so
			// use the canonical empty hash for the unobserved anchor fields.
			AnchorHash: make([]byte, 32),
			PolicyHash: append([]byte(nil), guardrail[:]...),
		}
	}
	if state.Constitution == nil {
		state.Constitution = &mockconformance.ConstitutionInfo{
			AnchorHash: make([]byte, 32),
		}
	}
	if doc.Pots != nil {
		metadata.Treasury = doc.Pots.Treasury
		metadata.Reserves = doc.Pots.Reserves
	}
	if doc.GovernanceActivity != nil {
		dormant := doc.GovernanceActivity.ConsecutiveDormantEpochs
		metadata.DormantDRepEpochs = &dormant
	}
	return state, metadata, nil
}

func proposalActionCbor(kind string) ([]byte, error) {
	switch {
	case strings.HasPrefix(kind, "ProtocolParameters"):
		action := &conway.ConwayParameterChangeGovAction{
			Type:        uint(common.GovActionTypeParameterChange),
			ParamUpdate: conway.ConwayProtocolParameterUpdate{},
		}
		if kind == "ProtocolParameters(security-group)" {
			maxTxSize := uint(16384)
			action.ParamUpdate.MaxTxSize = &maxTxSize
		}
		return cbor.Encode(action)
	case strings.HasPrefix(kind, "HardFork("):
		version := strings.TrimSuffix(
			strings.TrimPrefix(kind, "HardFork("),
			")",
		)
		major, minorText, ok := strings.Cut(version, ".")
		if !ok {
			return nil, fmt.Errorf(
				"invalid hard-fork protocol version %q",
				kind,
			)
		}
		majorVersion, err := strconv.ParseUint(major, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse hard-fork major version: %w", err)
		}
		minorVersion, err := strconv.ParseUint(minorText, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse hard-fork minor version: %w", err)
		}
		action, err := common.NewHardForkInitiationGovAction(
			nil, uint(majorVersion), uint(minorVersion),
		)
		if err != nil {
			return nil, err
		}
		return cbor.Encode(action)
	default:
		return nil, nil
	}
}

func epochForSlot(history eraHistoryDoc, slot uint64) (uint64, error) {
	for _, era := range slices.Backward(history.Eras) {
		if slot < era.Start.Slot {
			continue
		}
		if era.End != nil && slot >= era.End.Slot {
			continue
		}
		if era.Params.EpochSizeSlots == 0 {
			return 0, errors.New("era history has a zero epoch size")
		}
		return era.Start.Epoch + (slot-era.Start.Slot)/era.Params.EpochSizeSlots, nil
	}
	return 0, fmt.Errorf("slot %d is outside the supplied era history", slot)
}

func decodeCredential(
	value string,
) (common.Credential, mockledger.RewardAccountKey, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return common.Credential{}, mockledger.RewardAccountKey{}, err
	}
	var credential common.Credential
	if _, err := cbor.DecodeExact(data, &credential); err != nil {
		return common.Credential{}, mockledger.RewardAccountKey{}, err
	}
	return credential, mockledger.NewRewardAccountKey(credential), nil
}

func decodeHash28(value string) (common.Blake2b224, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return common.Blake2b224{}, err
	}
	return common.NewBlake2b224Checked(data)
}

func decodeHash32(value string) ([32]byte, error) {
	var hash [32]byte
	data, err := hex.DecodeString(value)
	if err != nil {
		return hash, err
	}
	if len(data) != len(hash) {
		return hash, fmt.Errorf(
			"expected 32-byte hash, got %d bytes",
			len(data),
		)
	}
	copy(hash[:], data)
	return hash, nil
}

func proposalActionType(kind string) common.GovActionType {
	switch {
	case strings.HasPrefix(kind, "ProtocolParameters"):
		return common.GovActionTypeParameterChange
	case strings.HasPrefix(kind, "HardFork"):
		return common.GovActionTypeHardForkInitiation
	case kind == "ConstitutionalCommittee":
		return common.GovActionTypeUpdateCommittee
	case kind == "Constitution":
		return common.GovActionTypeNewConstitution
	case kind == "TreasuryWithdrawals":
		return common.GovActionTypeTreasuryWithdrawal
	default:
		return common.GovActionTypeInfo
	}
}
