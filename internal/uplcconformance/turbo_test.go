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
	"crypto/sha256"
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
	if report.Scenario != "uplc-turbo" || report.Setup.SampleCount != 2 {
		t.Fatalf("report metadata = %+v / %+v", report.Scenario, report.Setup)
	}
	if len(report.Benchmarks) != 1 ||
		report.Benchmarks[0].SizeBytes != len(encoded) {
		t.Fatalf("benchmarks = %+v", report.Benchmarks)
	}
	if report.Benchmarks[0].ProtocolVersion != "8.0" {
		t.Fatalf("protocol version = %q, want 8.0 for epoch 484", report.Benchmarks[0].ProtocolVersion)
	}
	if report.Benchmarks[0].BudgetCPU == nil ||
		report.Benchmarks[0].BudgetMem == nil {
		t.Fatalf("consumed budget is missing: %+v", report.Benchmarks[0])
	}
	if len(report.Summary) != 1 || report.Summary[0].Group != "V1" ||
		report.Summary[0].Samples != 2 {
		t.Fatalf("summary = %+v", report.Summary)
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
		"schema_version",
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
	assertTurboJSONFields(t, implementation, "name", "language")
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
		"flat_us",
		"mem_bytes",
		"size_bytes",
	)
	for _, field := range []string{"cpu_us", "flat_us", "mem_bytes"} {
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

func TestGenerateTurboReportKeepsFailedBenchmarks(t *testing.T) {
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
	if len(report.Benchmarks) != 2 {
		t.Fatalf("benchmarks = %d, want both failed benchmarks in report", len(report.Benchmarks))
	}
	for i, benchmark := range report.Benchmarks {
		if benchmark.BudgetCPU != nil || benchmark.BudgetMem != nil ||
			benchmark.FlatUS != nil || benchmark.MemBytes != nil || benchmark.CPUUS != nil ||
			benchmark.Error == "" || strings.Contains(benchmark.Error, root) {
			t.Fatalf("failed benchmark %d contains measurements: %+v", i, benchmark)
		}
	}
	if !strings.Contains(report.Benchmarks[0].Error, "decode Flat program") ||
		!strings.Contains(report.Benchmarks[1].Error, "decode CBOR byte string") {
		t.Fatalf("benchmark errors = %q / %q", report.Benchmarks[0].Error, report.Benchmarks[1].Error)
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
	if len(decoded.Benchmarks) != 2 {
		t.Fatalf("failed benchmark JSON = %s", reportJSON)
	}
	for i, benchmark := range decoded.Benchmarks {
		if string(benchmark["cpu_us"]) != "null" ||
			string(benchmark["flat_us"]) != "null" ||
			string(benchmark["mem_bytes"]) != "null" ||
			string(benchmark["budget_cpu"]) != "null" {
			t.Fatalf("failed benchmark JSON = %s", reportJSON)
		}
		var failureReason string
		if err := json.Unmarshal(benchmark["error"], &failureReason); err != nil ||
			failureReason == "" || strings.Contains(failureReason, root) {
			t.Fatalf("failed benchmark %d error = %q, err = %v", i, failureReason, err)
		}
	}
}

func TestTurboRepresentativeIndexGroupsNearDuplicatePrograms(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.flat")
	secondPath := filepath.Join(root, "second.flat")
	first := make([]byte, 512)
	for i := range first {
		first[i] = byte(i)
	}
	second := append([]byte(nil), first...)
	second[256] ^= 1
	if err := writeTurboTestProgram(firstPath, first); err != nil {
		t.Fatalf("write first Flat program: %v", err)
	}
	if err := writeTurboTestProgram(secondPath, second); err != nil {
		t.Fatalf("write second Flat program: %v", err)
	}

	index := turboRepresentativeIndex{
		bands:        make(map[turboBandKey][]int),
		groupIndexes: make(map[string][]int),
		unindexed:    make(map[string][]int),
	}
	firstHashes, firstIndexed := turboMinHashes(first)
	firstCandidate := turboRepresentativeCandidate{
		benchmark: TurboBenchmark{Bench: "first", Group: "V1"},
		path:      firstPath,
		size:      len(first),
		hash:      sha256.Sum256(first),
		minHashes: firstHashes,
		indexed:   firstIndexed,
		medianCPU: 100,
		medianMem: 200,
	}
	if err := index.add(firstCandidate, first); err != nil {
		t.Fatalf("add first representative: %v", err)
	}

	secondHashes, secondIndexed := turboMinHashes(second)
	secondCandidate := turboRepresentativeCandidate{
		benchmark: TurboBenchmark{Bench: "second", Group: "V1"},
		path:      secondPath,
		size:      len(second),
		hash:      sha256.Sum256(second),
		minHashes: secondHashes,
		indexed:   secondIndexed,
		medianCPU: 105,
		medianMem: 190,
	}
	if err := index.add(secondCandidate, second); err != nil {
		t.Fatalf("add second representative: %v", err)
	}
	if got := len(index.representatives); got != 1 {
		t.Fatalf("representatives = %d, want 1 for 99%% byte similarity", got)
	}
}

func TestTurboRepresentativeIndexKeepsDissimilarProgramsSeparate(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	first := make([]byte, 512)
	second := make([]byte, len(first))
	for i := range first {
		first[i] = byte(i)
		second[i] = byte(255 - i)
	}
	firstPath := filepath.Join(root, "first.flat")
	secondPath := filepath.Join(root, "second.flat")
	for path, program := range map[string][]byte{firstPath: first, secondPath: second} {
		if err := writeTurboTestProgram(path, program); err != nil {
			t.Fatalf("write Flat program %s: %v", path, err)
		}
	}

	index := turboRepresentativeIndex{
		bands:        make(map[turboBandKey][]int),
		groupIndexes: make(map[string][]int),
		unindexed:    make(map[string][]int),
	}
	for name, program := range map[string][]byte{"first": first, "second": second} {
		path := filepath.Join(root, name+".flat")
		hashes, indexed := turboMinHashes(program)
		candidate := turboRepresentativeCandidate{
			benchmark: TurboBenchmark{Bench: name, Group: "V1"},
			path:      path,
			size:      len(program),
			hash:      sha256.Sum256(program),
			minHashes: hashes,
			indexed:   indexed,
			medianCPU: 100,
			medianMem: 200,
		}
		if err := index.add(candidate, program); err != nil {
			t.Fatalf("add %s representative: %v", name, err)
		}
	}
	if got := len(index.representatives); got != 2 {
		t.Fatalf("representatives = %d, want 2 for dissimilar programs", got)
	}
}

func writeTurboTestProgram(path string, program []byte) error {
	encoded, err := cbor.Marshal(program)
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o600)
}
