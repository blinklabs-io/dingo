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
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/plutigo/cek"
)

const (
	referenceAddInteger = `(program 1.0.0
  [ [ (builtin addInteger) (con integer 1) ] (con integer 1) ])
`
	referenceResult = "(program 1.0.0 (con integer 2))"
	// referenceBudget is the V3 protocol-11 cost of referenceAddInteger.
	referenceBudget = "({cpu: 181308\n| mem: 602})"
	generatedAt     = "2026-10-15T12:00:00Z"
)

var testTime = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

// writeVector writes one vector directory under corpusDir.
func writeVector(
	t *testing.T,
	root, corpusDir, group, name, program, expected, budget string,
) string {
	t.Helper()
	dir := filepath.Join(root, corpusDir, filepath.FromSlash(group), name)
	programPath := filepath.Join(dir, name+".uplc")
	writeFile(t, programPath, program)
	writeFile(t, programPath+".expected", expected)
	if budget != "" {
		writeFile(t, programPath+".budget.expected", budget)
	}
	return programPath
}

func writeManifest(t *testing.T, root, cases string) {
	t.Helper()
	writeFile(
		t,
		filepath.Join(root, regressionManifest),
		"let v3_costs: &[i64] = &[1, 2, 3];\n"+cases,
	)
}

func generate(t *testing.T, root string) Report {
	t.Helper()
	report, err := GenerateReport(root, testTime)
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	return report
}

const baselineManifest = `regression_case!(
    baseline_regression,
    "conformance_extra/textual/baseline/baseline/baseline.uplc",
    PlutusVersion::V2,
    PROTOCOL_VERSION_11
);
`

// baselineScenarios counts the two scenarios newCorpus always provides, a
// main vector and a regression vector. Both pass and neither has a budget.
const baselineScenarios = 2

// newCorpus returns a corpus holding only the baseline scenarios, which
// satisfy the generator's requirement for at least one main vector and one
// regression case.
func newCorpus(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeManifest(t, root, baselineManifest)
	addBaselineVectors(t, root)
	return root
}

func addBaselineVectors(t *testing.T, root string) {
	t.Helper()
	writeVector(
		t,
		root,
		mainCorpusDir,
		"baseline",
		"baseline",
		referenceAddInteger,
		referenceResult,
		"",
	)
	writeVector(
		t,
		root,
		regressionCorpusDir,
		"textual/baseline",
		"baseline",
		referenceAddInteger,
		referenceResult,
		"",
	)
}

func scenarioByID(t *testing.T, report Report, id string) CaseResult {
	t.Helper()
	for _, scenario := range report.Scenarios {
		if scenario.ID == id {
			return scenario
		}
	}
	t.Fatalf("scenario %s not found", id)
	return CaseResult{}
}

func TestGenerateReportDerivesScenarioIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeManifest(t, root, baselineManifest+`regression_case!(
    builtin_semantics_addinteger_v2_pv11_regression,
    "conformance_extra/textual/builtin/semantics/addInteger/v2-pv11/v2-pv11.uplc",
    PlutusVersion::V2,
    PROTOCOL_VERSION_11
);
`)
	addBaselineVectors(t, root)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		referenceResult,
		referenceBudget,
	)
	writeVector(
		t,
		root,
		regressionCorpusDir,
		"textual/builtin/semantics/addInteger",
		"v2-pv11",
		referenceAddInteger,
		referenceResult,
		"",
	)

	report := generate(t, root)
	if report.GeneratedAt != generatedAt || report.ProtocolVersion != "mixed" {
		t.Fatalf(
			"report header = %q / %q",
			report.GeneratedAt,
			report.ProtocolVersion,
		)
	}
	want := []CaseResult{
		{
			ID:              "baseline_baseline",
			Title:           "baseline",
			Group:           "baseline",
			PlutusVersion:   "V3",
			ProtocolVersion: 11,
			CostModelID:     "v3-pv11-default",
		},
		{
			ID:              "baseline_regression",
			Title:           "baseline regression",
			Group:           "textual/baseline",
			PlutusVersion:   "V2",
			ProtocolVersion: 11,
			CostModelID:     "v2-pv11-default",
		},
		{
			ID:              "builtin_semantics_addinteger_addinteger_01",
			Title:           "addinteger 01",
			Group:           "builtin/semantics/addInteger",
			PlutusVersion:   "V3",
			ProtocolVersion: 11,
			CostModelID:     "v3-pv11-default",
		},
		{
			ID:              "builtin_semantics_addinteger_v2_pv11_regression",
			Title:           "builtin semantics addinteger v2 pv11 regression",
			Group:           "textual/builtin/semantics/addInteger",
			PlutusVersion:   "V2",
			ProtocolVersion: 11,
			CostModelID:     "v2-pv11-default",
		},
	}
	if len(report.Scenarios) != len(want) {
		t.Fatalf(
			"scenario count = %d, want %d",
			len(report.Scenarios),
			len(want),
		)
	}
	for i, got := range report.Scenarios {
		w := want[i]
		if got.ID != w.ID || got.Title != w.Title || got.Group != w.Group ||
			got.PlutusVersion != w.PlutusVersion || got.ProtocolVersion != w.ProtocolVersion ||
			got.CostModelID != w.CostModelID {
			t.Errorf("scenario %d = %+v, want %+v", i, got, w)
		}
	}
	ids := map[string]bool{}
	for _, model := range report.CostModels {
		ids[model.ID] = true
	}
	if len(report.CostModels) != 2 || !ids["v3-pv11-default"] ||
		!ids["v2-pv11-default"] {
		t.Fatalf("cost models = %+v", report.CostModels)
	}
}

func TestGenerateReportSeparatesEvaluationAndBudget(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		referenceResult,
		referenceBudget,
	)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-02",
		referenceAddInteger,
		referenceResult,
		"({cpu: 1\n| mem: 1})",
	)

	report := generate(t, root)
	if report.Totals.Total != 2+baselineScenarios ||
		report.Totals.Passed != 1+baselineScenarios ||
		report.Totals.Failed != 1 {
		t.Fatalf("totals = %+v", report.Totals)
	}
	if report.Totals.Evaluation != (DimensionTotal{Passed: 2 + baselineScenarios, Total: 2 + baselineScenarios}) {
		t.Fatalf("evaluation totals = %+v", report.Totals.Evaluation)
	}
	if report.Totals.Budget != (DimensionTotal{Passed: 1, Failed: 1, Total: 2}) {
		t.Fatalf("budget totals = %+v", report.Totals.Budget)
	}
	wrong := scenarioByID(
		t,
		report,
		"builtin_semantics_addinteger_addinteger_02",
	)
	if wrong.Evaluation.Passed == nil || !*wrong.Evaluation.Passed ||
		wrong.Passed {
		t.Fatalf(
			"wrong budget must fail the scenario but not evaluation: %+v",
			wrong,
		)
	}
}

func TestGenerateReportKeepsEvaluationFailuresVisible(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		"(program 1.0.0 (con integer 3))",
		"",
	)

	report := generate(t, root)
	if report.Totals.Passed != baselineScenarios || report.Totals.Failed != 1 {
		t.Fatalf("totals = %+v", report.Totals)
	}
	scenario := scenarioByID(
		t,
		report,
		"builtin_semantics_addinteger_addinteger_01",
	)
	if report.Totals.Budget.Total != 0 || scenario.Budget.Passed != nil {
		t.Fatalf("missing budget outcome was counted: %+v", scenario.Budget)
	}
}

func TestGenerateReportExpectedFailures(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"term/parse",
		"bad-syntax",
		"(program 1.0.0 (con integer",
		"parse error",
		"parse error",
	)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"term/free",
		"free-variable",
		"(program 1.0.0 x)",
		"evaluation failure",
		"evaluation failure",
	)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"term/swapped",
		"wrong-stage",
		"(program 1.0.0 x)",
		"parse error",
		"parse error",
	)

	report := generate(t, root)
	passed := map[string]bool{}
	for _, scenario := range report.Scenarios {
		passed[scenario.ID] = scenario.Passed
		if scenario.Budget.Passed != nil {
			t.Errorf("%s: failure vector carries a budget outcome", scenario.ID)
		}
	}
	if !passed["term_parse_bad_syntax"] || !passed["term_free_free_variable"] {
		t.Fatalf("expected failures were not matched: %v", passed)
	}
	if passed["term_swapped_wrong_stage"] {
		t.Fatal("a parse error was expected but the program parses")
	}
}

func TestGenerateReportExhaustedBudgetExpectsFailure(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	program := `(program 1.0.0
  [ [ (force (builtin dropList)) (con integer 9223372036854775807) ]
    (con (list integer) [1]) ])`
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/dropList",
		"dropList-09",
		program,
		"out of budget",
		"({cpu: "+strconv.FormatInt(math.MaxInt64, 10)+"\n| mem: 704})",
	)

	report := generate(t, root)
	scenario := scenarioByID(
		t,
		report,
		"builtin_semantics_droplist_droplist_09",
	)
	if !scenario.Passed || scenario.Evaluation.Expected != "out of budget" {
		t.Fatalf("scenario = %+v", scenario)
	}
	if scenario.Budget.Passed != nil || scenario.Budget.Expected == nil {
		t.Fatalf("budget outcome = %+v", scenario.Budget)
	}
	if report.Totals.Budget.Total != 0 {
		t.Fatalf("budget totals = %+v", report.Totals.Budget)
	}
}

func TestCompareBudgetExhaustionRequiresBudgetError(t *testing.T) {
	t.Parallel()
	if !compareBudgetExhaustion(
		evaluationFailure,
		&cek.BudgetError{Code: cek.ErrCodeBudgetExhausted},
	) {
		t.Fatal("budget exhaustion was not accepted")
	}
	if compareBudgetExhaustion(
		evaluationFailure,
		errors.New("invalid for protocol version"),
	) {
		t.Fatal("protocol validation error was accepted as budget exhaustion")
	}
	if compareBudgetExhaustion(noFailure, nil) {
		t.Fatal("successful evaluation was accepted as budget exhaustion")
	}
}

func TestGenerateReportRegressionEntriesShareAProgram(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	writeManifest(t, root, baselineManifest+`regression_case!(
    program_version_v2_pv10_regression,
    "conformance_extra/textual/program/version/v2/v2.uplc",
    PlutusVersion::V2,
    PROTOCOL_VERSION_10
);
regression_case!(
    program_version_v2_pv11_regression,
    "conformance_extra/textual/program/version/v2/v2.uplc",
    PlutusVersion::V2,
    PROTOCOL_VERSION_11
);
`)
	// A 1.1.0 program is rejected for V2 before protocol version 11.
	program := "(program 1.1.0 (con integer 1))"
	writeVector(
		t,
		root,
		regressionCorpusDir,
		"textual/program/version",
		"v2",
		program,
		"evaluation failure",
		"evaluation failure",
	)

	report := generate(t, root)
	before := scenarioByID(t, report, "program_version_v2_pv10_regression")
	from := scenarioByID(t, report, "program_version_v2_pv11_regression")
	if before.ProtocolVersion != 10 || from.ProtocolVersion != 11 {
		t.Fatalf(
			"protocol versions = %d, %d",
			before.ProtocolVersion,
			from.ProtocolVersion,
		)
	}
	if !before.Passed {
		t.Fatalf("protocol 10 must reject the program: %+v", before)
	}
	if from.Passed {
		t.Fatalf(
			"protocol 11 accepts the program, so the failure expectation must not hold: %+v",
			from,
		)
	}
}

func TestGenerateReportRejectsMissingExpectedResult(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	writeFile(
		t,
		filepath.Join(
			root,
			mainCorpusDir,
			"builtin",
			"addInteger",
			"addInteger-01",
			"addInteger-01.uplc",
		),
		referenceAddInteger,
	)

	if _, err := GenerateReport(root, testTime); err == nil {
		t.Fatal(
			"expected a missing expected-result file to fail report generation",
		)
	}
}

func TestGenerateReportRejectsMissingManifest(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		referenceResult,
		"",
	)

	if _, err := GenerateReport(root, testTime); err == nil {
		t.Fatal(
			"expected a missing regression manifest to fail report generation",
		)
	}
}

func TestGenerateReportRejectsRegressionOutsideCorpus(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		referenceResult,
		"",
	)
	writeManifest(t, root, baselineManifest+`regression_case!(
    escaped_regression,
    "../outside/outside.uplc"
);
`)

	if _, err := GenerateReport(root, testTime); err == nil {
		t.Fatal("expected a manifest path outside conformance_extra to fail")
	}
}

func TestGenerateReportRejectsNormalizedManifestPathOutsideRegressionCorpus(
	t *testing.T,
) {
	t.Parallel()
	root := newCorpus(t)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		referenceResult,
		"",
	)
	writeManifest(t, root, baselineManifest+`regression_case!(
    escaped_regression,
    "conformance_extra/../conformance/builtin/semantics/addInteger/addInteger-01/addInteger-01.uplc"
);
`)
	if _, err := GenerateReport(root, testTime); err == nil {
		t.Fatal("expected normalized path outside conformance_extra to fail")
	}
}

func TestGenerateReportRejectsExpectedResultSymlinkOutsideCorpus(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "corpus")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, root, baselineManifest)
	addBaselineVectors(t, root)
	programPath := writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/semantics/addInteger",
		"addInteger-01",
		referenceAddInteger,
		referenceResult,
		"",
	)
	outside := filepath.Join(t.TempDir(), "outside.expected")
	writeFile(t, outside, referenceResult)
	if err := os.Remove(programPath + ".expected"); err != nil {
		t.Fatalf("remove in-corpus expected result: %v", err)
	}
	if err := os.Symlink(outside, programPath+".expected"); err != nil {
		t.Fatalf("symlink expected result: %v", err)
	}

	if _, err := GenerateReport(root, testTime); err == nil {
		t.Fatal(
			"expected report generation to reject an expected-result symlink outside the corpus",
		)
	}
}

func TestGenerateReportRejectsDuplicateIDs(t *testing.T) {
	t.Parallel()
	root := newCorpus(t)
	// Both directory names slug to the same scenario id.
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/x",
		"add-01",
		referenceAddInteger,
		referenceResult,
		"",
	)
	writeVector(
		t,
		root,
		mainCorpusDir,
		"builtin/x",
		"add_01",
		referenceAddInteger,
		referenceResult,
		"",
	)

	if _, err := GenerateReport(root, testTime); err == nil {
		t.Fatal("expected duplicate scenario ids to fail")
	}
}
