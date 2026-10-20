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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/fxamacker/cbor/v2"
)

func TestGenerateTurboReportMeasuresFlatPrograms(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("UPLC turbo reports are Linux-only")
	}
	root := t.TempDir()
	named, err := syn.Parse(referenceAddInteger)
	if err != nil {
		t.Fatalf("parse source program: %v", err)
	}
	program, err := syn.NameToDeBruijn(named)
	if err != nil {
		t.Fatalf("convert source program: %v", err)
	}
	encoded, err := syn.Encode(program)
	if err != nil {
		t.Fatalf("encode Flat program: %v", err)
	}
	wrapped, err := cbor.Marshal(encoded)
	if err != nil {
		t.Fatalf("wrap Flat program in CBOR: %v", err)
	}
	epochDir := filepath.Join(root, "484")
	if err := os.MkdirAll(epochDir, 0o700); err != nil {
		t.Fatalf("create epoch directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(epochDir, "addInteger-v1.flat"), wrapped, 0o600); err != nil {
		t.Fatalf("write Flat program: %v", err)
	}

	report, err := GenerateTurboReport(
		root,
		2,
		0,
		time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("generate turbo report: %v", err)
	}
	if report.Scenario != "uplc-turbo" {
		t.Fatalf("report scenario = %q, want uplc-turbo", report.Scenario)
	}
	if len(report.Benchmarks) != 1 ||
		report.Benchmarks[0].SizeBytes != len(encoded) {
		t.Fatalf("benchmarks = %+v", report.Benchmarks)
	}
	if report.Benchmarks[0].Bench != "addInteger-v1.flat" {
		t.Fatalf("benchmark ID = %q, want bare corpus filename", report.Benchmarks[0].Bench)
	}
	if report.Benchmarks[0].ProtocolVersion != "8.0" {
		t.Fatalf("protocol version = %q, want 8.0 for epoch 484", report.Benchmarks[0].ProtocolVersion)
	}
	if report.Benchmarks[0].BudgetCPU == nil ||
		report.Benchmarks[0].BudgetMem == nil {
		t.Fatalf("consumed budget is missing: %+v", report.Benchmarks[0])
	}
	// Pin the pre-Chang (protocol 8) variant A costs for this program. The
	// report's published budgets must match the cardano-node reference, and a
	// change to plutigo's default cost model must not silently shift them.
	if *report.Benchmarks[0].BudgetCPU != 321577 ||
		*report.Benchmarks[0].BudgetMem != 602 {
		t.Fatalf(
			"consumed budget = cpu %d mem %d, want cpu 321577 mem 602",
			*report.Benchmarks[0].BudgetCPU,
			*report.Benchmarks[0].BudgetMem,
		)
	}
	reportPath := filepath.Join(t.TempDir(), "report.json")
	if err := WriteTurboReport(reportPath, report); err != nil {
		t.Fatalf("write turbo report: %v", err)
	}
	reportJSON, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read turbo report: %v", err)
	}
	if !json.Valid(reportJSON) {
		t.Fatal("turbo report is not valid JSON")
	}
	reportFields := assertTurboJSONFields(
		t,
		reportJSON,
		"version",
		"scenario",
		"generated_at",
		"implementation",
		"machine",
		"benchmarks",
	)
	var implementation json.RawMessage
	if err := json.Unmarshal(reportFields["implementation"], &implementation); err != nil {
		t.Fatalf("decode implementation: %v", err)
	}
	assertTurboJSONFields(t, implementation, "name")
	var machine json.RawMessage
	if err := json.Unmarshal(reportFields["machine"], &machine); err != nil {
		t.Fatalf("decode machine: %v", err)
	}
	assertTurboJSONFields(t, machine, "os", "architecture", "logical_cores")
	var benchmarks []json.RawMessage
	if err := json.Unmarshal(reportFields["benchmarks"], &benchmarks); err != nil {
		t.Fatalf("decode benchmarks: %v", err)
	}
	if len(benchmarks) != 1 {
		t.Fatalf("benchmarks = %d, want 1", len(benchmarks))
	}
	benchmarkFields := assertTurboJSONFields(
		t,
		benchmarks[0],
		"bench",
		"plutus_version",
		"protocol_version",
		"budget_cpu",
		"budget_mem",
		"cpu_us",
		"mem_bytes",
	)
	for _, field := range []string{"cpu_us", "mem_bytes"} {
		var value float64
		if err := json.Unmarshal(benchmarkFields[field], &value); err != nil {
			t.Fatalf("decode benchmark %s: %v", field, err)
		}
		if value < 0 {
			t.Fatalf("benchmark %s = %v, want non-negative", field, value)
		}
	}
	if bytes.Contains(reportJSON, []byte(`"passed"`)) ||
		bytes.Contains(reportJSON, []byte(`"failed"`)) {
		t.Fatalf(
			"performance report exposes conformance outcomes: %s",
			reportJSON,
		)
	}
}

func assertTurboJSONFields(
	t *testing.T,
	raw json.RawMessage,
	expected ...string,
) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	if len(fields) != len(expected) {
		t.Fatalf("object fields = %v, want exactly %v", fields, expected)
	}
	for _, field := range expected {
		if _, ok := fields[field]; !ok {
			t.Fatalf("object is missing field %q", field)
		}
	}
	return fields
}

func TestMakeTurboBenchmarkSeparatesEvaluationAndFlatTimes(t *testing.T) {
	budget := cek.ExBudget{Cpu: 100, Mem: 20}
	benchmark, err := makeTurboBenchmark(
		turboCase{name: "example-v1.flat", group: "V1", protocolMajor: 8},
		42,
		[]turboSample{
			{
				duration:           3 * time.Millisecond,
				evaluationDuration: 2 * time.Millisecond,
				flatDuration:       time.Millisecond,
				memBytes:           100,
				budget:             budget,
			},
			{
				duration:           7 * time.Millisecond,
				evaluationDuration: 6 * time.Millisecond,
				flatDuration:       time.Millisecond,
				memBytes:           200,
				budget:             budget,
			},
		},
	)
	if err != nil {
		t.Fatalf("make turbo benchmark: %v", err)
	}
	if benchmark.CPUUS == nil || *benchmark.CPUUS != 4_000 ||
		benchmark.FlatUS == nil || *benchmark.FlatUS != 1_000 {
		t.Fatalf(
			"cpu/flat times = %v/%v us, want 4000/1000 us",
			benchmark.CPUUS,
			benchmark.FlatUS,
		)
	}
}

func TestTurboProtocolMajorByEpoch(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		epoch int
		want  int
	}{
		{epoch: 0, want: 8},
		{epoch: 506, want: 8},
		{epoch: 507, want: 9},
		{epoch: 536, want: 9},
		{epoch: 537, want: 10},
		{epoch: 643, want: 10},
		{epoch: 644, want: 11},
	} {
		t.Run(strconv.Itoa(test.epoch), func(t *testing.T) {
			t.Parallel()
			if got := protocolMajorForEpoch(test.epoch); got != test.want {
				t.Fatalf("protocol major = %d, want %d", got, test.want)
			}
		})
	}
}

func TestDiscoverTurboCasesUsesProtocolMajorFromFilename(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{
		"510/script-v1.flat",
		"510/script-v2-p6.flat",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create program directory: %v", err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write program placeholder: %v", err)
		}
	}

	cases, err := discoverTurboCases(root)
	if err != nil {
		t.Fatalf("discover turbo cases: %v", err)
	}
	if len(cases) != 2 {
		t.Fatalf("discovered %d cases, want 2", len(cases))
	}
	if cases[0].protocolMajor != 9 {
		t.Errorf("legacy filename protocol major = %d, want 9", cases[0].protocolMajor)
	}
	if cases[1].protocolMajor != 6 {
		t.Errorf("explicit filename protocol major = %d, want 6", cases[1].protocolMajor)
	}
}

func TestMakeTurboBenchmarkUsesBestThirtySamples(t *testing.T) {
	t.Parallel()
	budget := cek.ExBudget{Cpu: 100, Mem: 20}
	samples := make([]turboSample, 50)
	for i := range samples {
		duration := time.Duration(i+1) * time.Millisecond
		samples[i] = turboSample{
			duration:           duration,
			evaluationDuration: duration,
			flatDuration:       0,
			budget:             budget,
		}
	}
	benchmark, err := makeTurboBenchmark(
		turboCase{name: "example-v1.flat", group: "V1", protocolMajor: 8},
		42,
		samples,
	)
	if err != nil {
		t.Fatalf("make turbo benchmark: %v", err)
	}
	if benchmark.CPUUS == nil || *benchmark.CPUUS != 15_500 {
		t.Fatalf("cpu time = %v us, want mean of best 30 samples (15500 us)", benchmark.CPUUS)
	}
}

func TestGenerateTurboReportOmitsFailedBenchmarks(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("UPLC turbo reports are Linux-only")
	}
	root := t.TempDir()
	epochDir := filepath.Join(root, "484")
	if err := os.MkdirAll(epochDir, 0o700); err != nil {
		t.Fatalf("create epoch directory: %v", err)
	}
	path := filepath.Join(epochDir, "invalid-v1.flat")
	if err := writeTurboTestProgram(path, []byte{0xff, 0xff}); err != nil {
		t.Fatalf("write invalid Flat program: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(epochDir, "unwrapped-v1.flat"),
		[]byte{0xff},
		0o600,
	); err != nil {
		t.Fatalf("write invalid CBOR wrapper: %v", err)
	}

	report, err := GenerateTurboReport(
		root,
		1,
		0,
		time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	)
	if err == nil {
		t.Fatal("generate turbo report succeeded for an invalid Flat program")
	}
	if len(report.Benchmarks) != 0 {
		t.Fatalf("benchmarks = %d, want failed benchmarks omitted", len(report.Benchmarks))
	}
	if !strings.Contains(err.Error(), "decode Flat program") ||
		!strings.Contains(err.Error(), "decode CBOR byte string") ||
		strings.Contains(err.Error(), root) {
		t.Fatalf("generation error does not preserve safe diagnostics: %v", err)
	}
	reportPath := filepath.Join(t.TempDir(), "report.json")
	if writeErr := WriteTurboReport(reportPath, report); writeErr != nil {
		t.Fatalf("write partial turbo report: %v", writeErr)
	}
	reportJSON, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read partial turbo report: %v", readErr)
	}
	var decoded struct {
		Benchmarks []map[string]json.RawMessage `json:"benchmarks"`
	}
	if unmarshalErr := json.Unmarshal(reportJSON, &decoded); unmarshalErr != nil {
		t.Fatalf("decode partial turbo report: %v", unmarshalErr)
	}
	if len(decoded.Benchmarks) != 0 {
		t.Fatalf("failed benchmark JSON = %s", reportJSON)
	}
}

func TestDiscoverTurboCasesRejectsDuplicateBenchmarkIDs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, epoch := range []string{"484", "485"} {
		path := filepath.Join(root, epoch, "duplicate-v1.flat")
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create epoch directory: %v", err)
		}
		if err := os.WriteFile(path, []byte{0x80}, 0o600); err != nil {
			t.Fatalf("write benchmark: %v", err)
		}
	}

	if _, err := discoverTurboCases(root); err == nil ||
		!strings.Contains(err.Error(), "duplicate benchmark ID") {
		t.Fatalf("duplicate benchmark IDs error = %v", err)
	}
}

func writeTurboTestProgram(path string, program []byte) error {
	encoded, err := cbor.Marshal(program)
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o600)
}
