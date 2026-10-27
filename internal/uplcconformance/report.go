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

package uplcconformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
)

const (
	schemaVersion = "1"

	// mainCorpusDir holds the vectors evaluated as Plutus V3 at protocol
	// version defaultProtocol.
	mainCorpusDir = "conformance"
	// regressionCorpusDir holds vectors whose language and protocol version
	// are declared per case by regressionManifest.
	regressionCorpusDir = "conformance_extra"
	regressionManifest  = "conformance_extra.rs"
	defaultProtocol     = 11
	regressionProtocol  = 10
	// protocol10V3CostModelID names the Plutus V3 parameters shipped with the
	// regression manifest. They model protocol version 10, where the default
	// V3 parameters do not apply.
	protocol10V3CostModelID = "v3-pv10-conformance"
	// machineSlippage is the step slippage the reference vectors were
	// generated with.
	machineSlippage = 200
	// machineCPU and machineMem are the execution budget the reference
	// vectors run under. Vectors that expect "out of budget" depend on it
	// being finite.
	machineCPU = 10_000_000_000
	machineMem = 14_000_000
)

var (
	budgetPattern = regexp.MustCompile(`(?s)cpu:\s*([0-9]+).*?mem:\s*([0-9]+)`)
	slugPattern   = regexp.MustCompile(`[^a-z0-9]+`)
	// regressionCasePattern matches both forms of the manifest macro: a bare
	// name and path, or one that also names the Plutus and protocol version.
	regressionCasePattern = regexp.MustCompile(
		`regression_case!\(\s*([a-z0-9_]+)\s*,\s*"([^"]+)"` +
			`(?:\s*,\s*PlutusVersion::V([123])\s*,\s*PROTOCOL_VERSION_([0-9]+))?\s*\)`,
	)
	ndcCorpusPathPattern = regexp.MustCompile(
		`^protocol-version-([0-9]+)/v([123])/(.+)/[^/]+\.uplc$`,
	)
	protocol10CostsPattern = regexp.MustCompile(
		`(?s)let v3_costs: &\[i64\] = &\[(.*?)\];`,
	)
)

type Report struct {
	SchemaVersion   string            `json:"version"`
	Scenario        string            `json:"scenario"`
	GeneratedAt     string            `json:"generated_at"`
	Implementation  map[string]string `json:"implementation"`
	ProtocolVersion string            `json:"-"`
	CostModels      []CostModel       `json:"-"`
	Totals          Totals            `json:"-"`
	Scenarios       []CaseResult      `json:"scenarios"`
}

type CostModel struct {
	ID              string `json:"id"`
	Description     string `json:"description"`
	PlutusVersion   string `json:"plutus_version"`
	ProtocolVersion uint   `json:"protocol_version"`
	Source          string `json:"source"`
}

type Totals struct {
	Total      int            `json:"total"`
	Passed     int            `json:"passed"`
	Failed     int            `json:"failed"`
	Evaluation DimensionTotal `json:"evaluation"`
	Budget     DimensionTotal `json:"budget"`
}

type DimensionTotal struct {
	Passed int `json:"passed"`
	Failed int `json:"failed"`
	Total  int `json:"total"`
}

type Outcome struct {
	Expected any
	Actual   any
	Passed   *bool
}

// MarshalJSON exposes the observed value; expectations and pass bookkeeping
// are internal to Dingo's report generation.
func (o Outcome) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.Actual)
}

type CaseResult struct {
	ID              string  `json:"id"`
	Title           string  `json:"title"`
	Group           string  `json:"group"`
	PlutusVersion   string  `json:"plutus_version"`
	ProtocolVersion string  `json:"protocol_version"`
	CostModelID     string  `json:"-"`
	Evaluation      Outcome `json:"evaluation"`
	Budget          Outcome `json:"budget"`
	Passed          bool    `json:"-"`
	Error           string  `json:"error,omitempty"`
}

type testCase struct {
	// program is the path of the .uplc program relative to the corpus root.
	program  string
	id       string
	group    string
	title    string
	language lang.LanguageVersion
	protocol uint
	costs    costModelKey
	ndc      bool
}

type costModelKey struct {
	language lang.LanguageVersion
	protocol uint
	// protocol10 selects the regression manifest's Plutus V3 parameters.
	protocol10 bool
}

func (k costModelKey) id() string {
	if k.protocol10 {
		return protocol10V3CostModelID
	}
	return fmt.Sprintf(
		"%s-pv%d-default",
		strings.ToLower(versionName(k.language)),
		k.protocol,
	)
}

type failureStage uint8

const (
	noFailure failureStage = iota
	parseFailure
	evaluationFailure
)

// GenerateReport evaluates the UPLC corpus under root and records evaluation
// and budget conformance as independent dimensions.
//
// root is the directory holding the "conformance" vectors, the
// "conformance_extra" regression vectors and the "conformance_extra.rs"
// manifest that gives each regression vector its Plutus version, protocol
// version and protocol-10 cost parameters. Each vector is a directory holding
// the program, its expected result and its expected budget.
func GenerateReport(root string, generatedAt time.Time) (Report, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return Report{}, fmt.Errorf("resolve corpus directory: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return Report{}, fmt.Errorf("stat corpus directory: %w", err)
	}
	if !info.IsDir() {
		return Report{}, fmt.Errorf("corpus path is not a directory: %s", root)
	}
	corpusFS, err := os.OpenRoot(root)
	if err != nil {
		return Report{}, fmt.Errorf("open corpus directory: %w", err)
	}
	defer corpusFS.Close()

	var cases []testCase
	var protocol10Costs []int64
	corpusCostModels := map[costModelKey][]int64{}
	if info, err := os.Stat(filepath.Join(root, "protocol-version-10")); err == nil && info.IsDir() {
		cases, corpusCostModels, err = discoverNDCCases(root, corpusFS)
		if err != nil {
			return Report{}, err
		}
	} else {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return Report{}, fmt.Errorf("inspect UPLC corpus layout: %w", err)
		}
		cases, err = discoverMainCases(root)
		if err != nil {
			return Report{}, err
		}
		regressions, costs, err := discoverRegressionCases(corpusFS)
		if err != nil {
			return Report{}, err
		}
		cases = append(cases, regressions...)
		protocol10Costs = costs
	}
	if len(cases) == 0 {
		return Report{}, errors.New("corpus contains no UPLC programs")
	}
	sort.Slice(cases, func(i, j int) bool {
		if cases[i].id != cases[j].id {
			return cases[i].id < cases[j].id
		}
		if cases[i].language != cases[j].language {
			return versionName(cases[i].language) < versionName(cases[j].language)
		}
		return cases[i].protocol < cases[j].protocol
	})
	for i := 1; i < len(cases); i++ {
		if sameScenarioIdentity(cases[i], cases[i-1]) {
			return Report{}, fmt.Errorf(
				"duplicate scenario identity %q for %s protocol %d",
				cases[i].id,
				versionName(cases[i].language),
				cases[i].protocol,
			)
		}
	}

	contexts := newEvalContexts(protocol10Costs, corpusCostModels)
	report := Report{
		SchemaVersion:   schemaVersion,
		Scenario:        "uplc-conformance",
		GeneratedAt:     generatedAt.UTC().Format(time.RFC3339),
		Implementation:  map[string]string{"name": "dingo"},
		ProtocolVersion: "mixed",
		Scenarios:       make([]CaseResult, 0, len(cases)),
	}
	used := map[costModelKey]struct{}{}
	for _, testCase := range cases {
		result, err := runCase(corpusFS, contexts, testCase)
		if err != nil {
			return Report{}, fmt.Errorf("scenario %s: %w", testCase.id, err)
		}
		used[testCase.costs] = struct{}{}
		report.Scenarios = append(report.Scenarios, result)
		report.Totals.Total++
		report.Totals.Evaluation.Total++
		if *result.Evaluation.Passed {
			report.Totals.Evaluation.Passed++
		} else {
			report.Totals.Evaluation.Failed++
		}
		if result.Budget.Passed != nil {
			report.Totals.Budget.Total++
			if *result.Budget.Passed {
				report.Totals.Budget.Passed++
			} else {
				report.Totals.Budget.Failed++
			}
		}
		if result.Passed {
			report.Totals.Passed++
		} else {
			report.Totals.Failed++
		}
	}
	report.CostModels = costModels(used)
	return report, nil
}

func sameScenarioIdentity(left, right testCase) bool {
	return left.id == right.id && left.language == right.language &&
		left.protocol == right.protocol
}

func costModels(used map[costModelKey]struct{}) []CostModel {
	models := make([]CostModel, 0, len(used))
	for key := range used {
		model := CostModel{
			ID:              key.id(),
			PlutusVersion:   versionName(key.language),
			ProtocolVersion: key.protocol,
			Source:          "github.com/blinklabs-io/plutigo/cek.NewDefaultEvalContext",
			Description: fmt.Sprintf(
				"Plutigo default Plutus %s cost model at protocol version %d",
				versionName(key.language),
				key.protocol,
			),
		}
		if key.protocol10 {
			model.Source = "github.com/blinklabs-io/plutigo/cek.NewEvalContext"
			model.Description = "Plutus V3 protocol-10 conformance parameters from the regression manifest"
		}
		models = append(models, model)
	}
	sort.Slice(
		models,
		func(i, j int) bool { return models[i].ID < models[j].ID },
	)
	return models
}

// slug lowercases s and replaces every run of non-alphanumeric characters with
// the separator.
func slug(s, separator string) string {
	return strings.Trim(
		slugPattern.ReplaceAllString(strings.ToLower(s), separator),
		separator,
	)
}

// discoverMainCases finds every vector directory under "conformance". The
// scenario id joins the slugged directory names, the group is the parent
// directory, and the title is the vector directory name.
func discoverMainCases(root string) ([]testCase, error) {
	mainRoot := filepath.Join(root, mainCorpusDir)
	var cases []testCase
	err := filepath.WalkDir(
		mainRoot,
		func(p string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return fmt.Errorf("walk %q: %w", p, walkErr)
			}
			if entry.IsDir() || filepath.Ext(p) != ".uplc" {
				return nil
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return fmt.Errorf("make corpus path for %q: %w", p, err)
			}
			rel = filepath.ToSlash(rel)
			dir := strings.TrimPrefix(path.Dir(rel), mainCorpusDir+"/")
			parts := strings.Split(dir, "/")
			segments := make([]string, len(parts))
			for i, part := range parts {
				segments[i] = slug(part, "_")
			}
			group := ""
			if len(parts) > 1 {
				group = strings.Join(parts[:len(parts)-1], "/")
			}
			cases = append(cases, testCase{
				program:  rel,
				id:       strings.Join(segments, "_"),
				group:    group,
				title:    slug(parts[len(parts)-1], " "),
				language: lang.LanguageVersionV3,
				protocol: defaultProtocol,
				costs: costModelKey{
					language: lang.LanguageVersionV3,
					protocol: defaultProtocol,
				},
			})
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("no UPLC programs under %s", mainCorpusDir)
	}
	return cases, nil
}

// discoverNDCCases reads the versioned UPLC corpus and cost models published
// for node-diversity-conformance.
//
// Each vector is evaluated from its textual program. The corpus also carries a
// Flat encoding for most vectors, but the two can disagree and the expected
// results follow the text: term/constr/constr-07 is the text
// "(constr -5 ...)", a parse error, while its Flat file decodes to a valid
// constr with tag 2^64-5, which the reference decoder accepts.
func discoverNDCCases(
	root string,
	corpusFS *os.Root,
) ([]testCase, map[costModelKey][]int64, error) {
	var cases []testCase
	costModels := map[costModelKey][]int64{}
	err := filepath.WalkDir(
		root,
		func(filePath string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return fmt.Errorf("walk %q: %w", filePath, walkErr)
			}
			if entry.IsDir() || filepath.Ext(filePath) != ".uplc" {
				return nil
			}
			rel, err := filepath.Rel(root, filePath)
			if err != nil {
				return fmt.Errorf("make corpus path for %q: %w", filePath, err)
			}
			rel = filepath.ToSlash(rel)
			match := ndcCorpusPathPattern.FindStringSubmatch(rel)
			if match == nil {
				return fmt.Errorf("unexpected NDC Flat corpus path %q", rel)
			}
			protocol, err := strconv.ParseUint(match[1], 10, 32)
			if err != nil || protocol == 0 {
				return fmt.Errorf("invalid protocol version in corpus path %q", rel)
			}
			language, err := languageFromDigit(match[2])
			if err != nil {
				return err
			}
			protocolVersion := uint(protocol)
			casePath := match[3]
			group := path.Dir(casePath)
			if group == "." {
				return fmt.Errorf("NDC Flat vector has no group directory: %q", rel)
			}
			caseName := path.Base(casePath)
			id := slug(casePath+"_"+versionName(language), "_")
			key := costModelKey{language: language, protocol: protocolVersion}
			if _, ok := costModels[key]; !ok {
				modelPath := fmt.Sprintf(
					"cost-models/protocol-version-%d/v%s.json",
					protocolVersion,
					match[2],
				)
				modelContents, err := corpusFS.ReadFile(modelPath)
				if err != nil {
					return fmt.Errorf("read cost model %q: %w", modelPath, err)
				}
				var params []int64
				if err := json.Unmarshal(modelContents, &params); err != nil {
					return fmt.Errorf("decode cost model %q: %w", modelPath, err)
				}
				if len(params) == 0 {
					return fmt.Errorf("cost model %q is empty", modelPath)
				}
				costModels[key] = params
			}
			cases = append(cases, testCase{
				program:  rel,
				id:       id,
				group:    group,
				title:    strings.ReplaceAll(caseName, "-", " "),
				language: language,
				protocol: protocolVersion,
				costs:    key,
				ndc:      true,
			})
			return nil
		},
	)
	if err != nil {
		return nil, nil, err
	}
	if len(cases) == 0 {
		return nil, nil, errors.New("NDC corpus contains no UPLC programs")
	}
	return cases, costModels, nil
}

// discoverRegressionCases reads the regression manifest. Each manifest entry
// is one scenario, so a program shared by several entries is reported once per
// language and protocol version.
func discoverRegressionCases(corpusFS *os.Root) ([]testCase, []int64, error) {
	manifest, err := corpusFS.ReadFile(regressionManifest)
	if err != nil {
		return nil, nil, fmt.Errorf("read regression manifest: %w", err)
	}
	costs, err := parseProtocol10Costs(string(manifest))
	if err != nil {
		return nil, nil, err
	}
	matches := regressionCasePattern.FindAllStringSubmatch(string(manifest), -1)
	if len(matches) == 0 {
		return nil, nil, errors.New("regression manifest declares no cases")
	}
	cases := make([]testCase, 0, len(matches))
	for _, match := range matches {
		name, program := match[1], match[2]
		language := lang.LanguageVersionV3
		protocol := uint(regressionProtocol)
		if match[3] != "" {
			language, err = languageFromDigit(match[3])
			if err != nil {
				return nil, nil, err
			}
			parsed, err := strconv.ParseUint(match[4], 10, 32)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"regression %s protocol version: %w",
					name,
					err,
				)
			}
			protocol = uint(parsed)
		}
		program = path.Clean(program)
		if !strings.HasPrefix(program, regressionCorpusDir+"/") {
			return nil, nil, fmt.Errorf(
				"regression %s path %q is outside %s",
				name,
				program,
				regressionCorpusDir,
			)
		}
		caseDir := path.Dir(
			strings.TrimPrefix(program, regressionCorpusDir+"/"),
		)
		key := costModelKey{language: language, protocol: protocol}
		if language == lang.LanguageVersionV3 && protocol < defaultProtocol {
			key.protocol10 = true
		}
		cases = append(cases, testCase{
			program:  program,
			id:       name,
			group:    path.Dir(caseDir),
			title:    slug(name, " "),
			language: language,
			protocol: protocol,
			costs:    key,
		})
	}
	return cases, costs, nil
}

func parseProtocol10Costs(manifest string) ([]int64, error) {
	match := protocol10CostsPattern.FindStringSubmatch(manifest)
	if match == nil {
		return nil, errors.New(
			"regression manifest has no protocol-10 V3 cost parameters",
		)
	}
	fields := strings.FieldsFunc(match[1], func(r rune) bool {
		return r == ',' || r == ' ' || r == '\n' || r == '\t' || r == '\r'
	})
	costs := make([]int64, 0, len(fields))
	for _, field := range fields {
		value, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"protocol-10 V3 cost parameter %q: %w",
				field,
				err,
			)
		}
		costs = append(costs, value)
	}
	return costs, nil
}

func languageFromDigit(digit string) (lang.LanguageVersion, error) {
	switch digit {
	case "1":
		return lang.LanguageVersionV1, nil
	case "2":
		return lang.LanguageVersionV2, nil
	case "3":
		return lang.LanguageVersionV3, nil
	default:
		return lang.LanguageVersion{}, fmt.Errorf(
			"unsupported Plutus version %q",
			digit,
		)
	}
}

type evalContexts struct {
	protocol10Costs []int64
	costModels      map[costModelKey][]int64
	cache           map[costModelKey]*cek.EvalContext
}

func newEvalContexts(
	protocol10Costs []int64,
	costModels map[costModelKey][]int64,
) *evalContexts {
	return &evalContexts{
		protocol10Costs: protocol10Costs,
		costModels:      costModels,
		cache:           map[costModelKey]*cek.EvalContext{},
	}
}

func (c *evalContexts) get(key costModelKey) (*cek.EvalContext, error) {
	if ctx, ok := c.cache[key]; ok {
		return ctx, nil
	}
	protocol := cek.ProtoVersion{Major: key.protocol}
	var ctx *cek.EvalContext
	if params, ok := c.costModels[key]; ok {
		var err error
		ctx, err = cek.NewEvalContext(key.language, protocol, params)
		if err != nil {
			return nil, fmt.Errorf("build %s evaluation context: %w", key.id(), err)
		}
	} else if key.protocol10 {
		var err error
		ctx, err = cek.NewEvalContext(key.language, protocol, c.protocol10Costs)
		if err != nil {
			return nil, fmt.Errorf(
				"build %s evaluation context: %w",
				key.id(),
				err,
			)
		}
	} else {
		ctx = cek.NewDefaultEvalContext(key.language, protocol)
	}
	c.cache[key] = ctx
	return ctx, nil
}

func readCorpusFile(corpusFS *os.Root, rel string) ([]byte, error) {
	return corpusFS.ReadFile(rel)
}

// readExpectations returns the expected result and the expected budget of a
// program. A vector without an expected budget has none to compare.
func readExpectations(
	corpusFS *os.Root,
	program string,
	ndcCorpus bool,
) (expected, budget string, budgetFound bool, err error) {
	basePath := strings.TrimSuffix(program, filepath.Ext(program))
	expectedPath := program + ".expected"
	budgetPath := program + ".budget.expected"
	if ndcCorpus {
		expectedPath = basePath + ".uplc.expected"
		budgetPath = basePath + ".budget"
	}
	expectedBytes, err := readCorpusFile(corpusFS, expectedPath)
	if err != nil {
		return "", "", false, fmt.Errorf("read expected result: %w", err)
	}
	budgetBytes, err := readCorpusFile(corpusFS, budgetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return strings.TrimSpace(string(expectedBytes)), "", false, nil
		}
		return "", "", false, fmt.Errorf("read expected budget: %w", err)
	}
	return strings.TrimSpace(
			string(expectedBytes),
		), strings.TrimSpace(
			string(budgetBytes),
		), true, nil
}

func runCase(
	corpusFS *os.Root,
	contexts *evalContexts,
	testCase testCase,
) (CaseResult, error) {
	contents, err := readCorpusFile(corpusFS, testCase.program)
	if err != nil {
		return CaseResult{}, fmt.Errorf("read program: %w", err)
	}
	expectedText, expectedBudget, budgetFound, err := readExpectations(
		corpusFS,
		testCase.program,
		testCase.ndc,
	)
	if err != nil {
		return CaseResult{}, err
	}
	evalCtx, err := contexts.get(testCase.costs)
	if err != nil {
		return CaseResult{}, err
	}

	expectedStage := expectedFailure(expectedText)
	evalExpected := expectedText
	budgetMatch := budgetPattern.FindStringSubmatch(expectedBudget)
	// A vector whose expected budget is the largest CPU value exhausts every
	// finite budget, so a run under the machine budget must fail.
	exhaustsBudget := budgetFound && len(budgetMatch) == 3 &&
		budgetMatch[1] == strconv.FormatInt(math.MaxInt64, 10)
	if exhaustsBudget {
		expectedStage = evaluationFailure
		evalExpected = "out of budget"
	}
	if expectedStage == noFailure {
		expectedProgram, err := syn.Parse(expectedText)
		if err != nil {
			return CaseResult{}, fmt.Errorf("parse expected result: %w", err)
		}
		expectedDeBruijn, err := syn.NameToDeBruijn(expectedProgram)
		if err != nil {
			return CaseResult{}, fmt.Errorf("convert expected result: %w", err)
		}
		evalExpected = syn.PrettyTerm[syn.DeBruijn](expectedDeBruijn.Term)
	}

	var result syn.Term[syn.DeBruijn]
	var programVersion lang.LanguageVersion
	var consumed cek.ExBudget
	var stage failureStage
	var runErr error
	result, programVersion, consumed, stage, runErr = evaluate(
		string(contents),
		testCase.language,
		testCase.protocol,
		evalCtx,
		testCase.ndc,
	)
	var evalActual, evaluationObservation string
	switch stage {
	case parseFailure:
		evalActual = "parse error"
		evaluationObservation = "parse/decode error"
	case evaluationFailure:
		evalActual = "evaluation failure"
		evaluationObservation = evalActual
		if cek.IsBudgetError(runErr) {
			evaluationObservation = "out of budget"
		}
	case noFailure:
		evalActual = syn.PrettyTerm[syn.DeBruijn](result)
		evaluationObservation, err = observedProgram(programVersion, result)
		if err != nil {
			return CaseResult{}, err
		}
	}
	evalPassed := compareEvaluation(
		expectedStage,
		evalExpected,
		evalActual,
		stage,
	)
	if exhaustsBudget || expectedText == "out of budget" {
		evalPassed = compareBudgetExhaustion(stage, runErr)
	}
	caseResult := CaseResult{
		ID:              testCase.id,
		Title:           testCase.title,
		Group:           testCase.group,
		PlutusVersion:   versionName(testCase.language),
		ProtocolVersion: fmt.Sprintf("%d.0", testCase.protocol),
		CostModelID:     testCase.costs.id(),
		Evaluation: Outcome{
			Expected: evalExpected,
			Actual:   evaluationObservation,
			Passed:   new(evalPassed),
		},
		Passed: evalPassed,
		Budget: Outcome{},
	}
	if runErr != nil {
		caseResult.Error = runErr.Error()
	}

	if exhaustsBudget {
		caseResult.Budget = Outcome{
			Expected: map[string]string{
				"cpu": budgetMatch[1],
				"mem": budgetMatch[2],
			},
		}
	} else if budgetFound && expectedStage == noFailure {
		if len(budgetMatch) != 3 {
			return CaseResult{}, fmt.Errorf("parse budget expectation %q", expectedBudget)
		}
		wantCPU, wantMem := budgetMatch[1], budgetMatch[2]
		actual := map[string]int64{"cpu": 0, "mem": 0}
		budgetPassed := false
		if stage == noFailure {
			actual["cpu"] = consumed.Cpu
			actual["mem"] = consumed.Mem
			budgetPassed = strconv.FormatInt(actual["cpu"], 10) == wantCPU &&
				strconv.FormatInt(actual["mem"], 10) == wantMem
		}
		caseResult.Budget = Outcome{
			Expected: map[string]string{"cpu": wantCPU, "mem": wantMem},
			Actual:   actual,
			Passed:   new(budgetPassed),
		}
		caseResult.Passed = caseResult.Passed && budgetPassed
	}
	return caseResult, nil
}

// expectedFailure maps a vector's expected result to the stage that must fail.
// The reference vectors record an exhausted budget as an evaluation failure.
func expectedFailure(expected string) failureStage {
	switch expected {
	case "parse error", "parse/decode error":
		return parseFailure
	case "evaluation failure", "out of budget":
		return evaluationFailure
	default:
		return noFailure
	}
}

func compareEvaluation(
	expectedStage failureStage,
	expected, actual string,
	stage failureStage,
) bool {
	if expectedStage != noFailure {
		return expectedStage == stage
	}
	return stage == noFailure && expected == actual
}

func compareBudgetExhaustion(stage failureStage, err error) bool {
	return stage == evaluationFailure && cek.IsBudgetError(err)
}

// evaluate parses, validates and runs a program for the given ledger language
// and protocol version. A program rejected by the syntax parser fails at the
// parse stage; one rejected by protocol-version validation, de Bruijn
// conversion or the machine fails at the evaluation stage.
// evaluate parses and runs a textual program. The scenario's expected results
// treat the execution-time version gate as a decode error, which gateIsDecode
// selects; the earlier corpus layout treats it as an evaluation failure.
func evaluate(
	text string,
	language lang.LanguageVersion,
	protocol uint,
	evalCtx *cek.EvalContext,
	gateIsDecode bool,
) (syn.Term[syn.DeBruijn], lang.LanguageVersion, cek.ExBudget, failureStage, error) {
	if _, err := syn.Parse(text); err != nil {
		return nil, lang.LanguageVersion{}, cek.ExBudget{}, parseFailure, err
	}
	programContext := syn.ProgramContext{
		LedgerLanguage: language,
		ProtocolMajor:  protocol,
	}
	program, err := syn.ParseWithContext(text, programContext)
	if err != nil {
		return nil, lang.LanguageVersion{}, cek.ExBudget{}, evaluationFailure, err
	}
	if err := syn.ValidateTermVersionForExecution(program.Version, programContext); err != nil {
		stage := evaluationFailure
		if gateIsDecode {
			stage = parseFailure
		}
		return nil, program.Version, cek.ExBudget{}, stage, err
	}
	deBruijn, err := syn.NameToDeBruijn(program)
	if err != nil {
		return nil, program.Version, cek.ExBudget{}, evaluationFailure, err
	}
	result, consumed, stage, err := runTerm(deBruijn, language, evalCtx)
	return result, program.Version, consumed, stage, err
}

func runTerm(
	program *syn.Program[syn.DeBruijn],
	language lang.LanguageVersion,
	evalCtx *cek.EvalContext,
) (syn.Term[syn.DeBruijn], cek.ExBudget, failureStage, error) {
	machine := cek.NewMachine[syn.DeBruijn](language, machineSlippage, evalCtx)
	initial := cek.ExBudget{Cpu: machineCPU, Mem: machineMem}
	machine.ExBudget = initial
	term, err := machine.Run(program.Term)
	if err != nil {
		return nil, cek.ExBudget{}, evaluationFailure, err
	}
	return term, initial.Sub(&machine.ExBudget), noFailure, nil
}

func observedProgram(
	version lang.LanguageVersion,
	term syn.Term[syn.DeBruijn],
) (string, error) {
	namedTerm, err := nameNDCResult(term)
	if err != nil {
		return "", err
	}
	prettyTerm := formatNDCPrettyTerm(syn.PrettyTerm[syn.NamedDeBruijn](namedTerm))
	if prettyTerm == "" {
		return "", errors.New("format NDC evaluation result")
	}
	return fmt.Sprintf(
		"(program %d.%d.%d %s)",
		version[0],
		version[1],
		version[2],
		prettyTerm,
	), nil
}

func nameNDCResult(
	term syn.Term[syn.DeBruijn],
) (syn.Term[syn.NamedDeBruijn], error) {
	var binders []syn.NamedDeBruijn
	nextName := 0
	var convert func(syn.Term[syn.DeBruijn]) (syn.Term[syn.NamedDeBruijn], error)
	convert = func(term syn.Term[syn.DeBruijn]) (syn.Term[syn.NamedDeBruijn], error) {
		switch node := term.(type) {
		case *syn.Var[syn.DeBruijn]:
			index := int(node.Name)
			if index < 1 || index > len(binders) {
				return nil, fmt.Errorf("NDC result has unbound variable index %d", index)
			}
			return &syn.Var[syn.NamedDeBruijn]{
				Name: binders[len(binders)-index],
			}, nil
		case *syn.Delay[syn.DeBruijn]:
			inner, err := convert(node.Term)
			if err != nil {
				return nil, err
			}
			return &syn.Delay[syn.NamedDeBruijn]{Term: inner}, nil
		case *syn.Force[syn.DeBruijn]:
			inner, err := convert(node.Term)
			if err != nil {
				return nil, err
			}
			return &syn.Force[syn.NamedDeBruijn]{Term: inner}, nil
		case *syn.Lambda[syn.DeBruijn]:
			binder := syn.NamedDeBruijn{Text: "i", Index: syn.DeBruijn(nextName)}
			nextName++
			binders = append(binders, binder)
			body, err := convert(node.Body)
			binders = binders[:len(binders)-1]
			if err != nil {
				return nil, err
			}
			return &syn.Lambda[syn.NamedDeBruijn]{ParameterName: binder, Body: body}, nil
		case *syn.Apply[syn.DeBruijn]:
			function, err := convert(node.Function)
			if err != nil {
				return nil, err
			}
			argument, err := convert(node.Argument)
			if err != nil {
				return nil, err
			}
			return &syn.Apply[syn.NamedDeBruijn]{Function: function, Argument: argument}, nil
		case syn.Builtin:
			return &node, nil
		case *syn.Builtin:
			return node, nil
		case *syn.Constr[syn.DeBruijn]:
			fields := make([]syn.Term[syn.NamedDeBruijn], len(node.Fields))
			for i, field := range node.Fields {
				converted, err := convert(field)
				if err != nil {
					return nil, err
				}
				fields[i] = converted
			}
			return &syn.Constr[syn.NamedDeBruijn]{Tag: node.Tag, Fields: fields}, nil
		case *syn.Case[syn.DeBruijn]:
			constr, err := convert(node.Constr)
			if err != nil {
				return nil, err
			}
			branches := make([]syn.Term[syn.NamedDeBruijn], len(node.Branches))
			for i, branch := range node.Branches {
				converted, err := convert(branch)
				if err != nil {
					return nil, err
				}
				branches[i] = converted
			}
			return &syn.Case[syn.NamedDeBruijn]{Constr: constr, Branches: branches}, nil
		case syn.Constant:
			return &node, nil
		case *syn.Constant:
			return node, nil
		case syn.Error:
			return &node, nil
		case *syn.Error:
			return node, nil
		default:
			return nil, fmt.Errorf("format unsupported NDC result term %T", term)
		}
	}
	return convert(term)
}

func formatNDCPrettyTerm(term string) string {
	var out strings.Builder
	out.Grow(len(term))
	inString := false
	escaped := false
	for i := 0; i < len(term); {
		if inString && term[i] == '\\' {
			if escaped {
				out.WriteByte(term[i])
				escaped = false
				i++
				continue
			}
			if strings.HasPrefix(term[i:], `\o177`) {
				out.WriteString(`\DEL`)
				i += len(`\o177`)
				continue
			}
			out.WriteByte(term[i])
			escaped = true
			i++
			continue
		}
		if !inString && strings.HasPrefix(term[i:], "i_") {
			end := i + 2
			for end < len(term) && term[end] >= '0' && term[end] <= '9' {
				end++
			}
			if end > i+2 {
				out.WriteString("i-")
				out.WriteString(term[i+2 : end])
				i = end
				continue
			}
		}
		char := term[i]
		out.WriteByte(char)
		if char == '"' && !escaped {
			inString = !inString
		}
		if char != '\\' {
			escaped = false
		}
		i++
	}
	return out.String()
}

func versionName(version lang.LanguageVersion) string {
	switch version {
	case lang.LanguageVersionV1:
		return "V1"
	case lang.LanguageVersionV2:
		return "V2"
	case lang.LanguageVersionV3:
		return "V3"
	case lang.LanguageVersionV4:
		return "V4"
	default:
		return "unknown"
	}
}

// WriteReport writes a stable, indented report to path.
func WriteReport(path string, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode UPLC report: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write UPLC report: %w", err)
	}
	return nil
}
