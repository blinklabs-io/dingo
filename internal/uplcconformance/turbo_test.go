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
	"testing"
	"time"

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
	if err := os.WriteFile(filepath.Join(root, "addInteger-v1.flat"), wrapped, 0o600); err != nil {
		t.Fatalf("write Flat program: %v", err)
	}

	report, err := GenerateTurboReport(
		root,
		"test/turbo",
		"",
		"v0.0.0-test",
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
	if bytes.Contains(reportJSON, []byte(`"passed"`)) ||
		bytes.Contains(reportJSON, []byte(`"failed"`)) {
		t.Fatalf(
			"performance report exposes conformance outcomes: %s",
			reportJSON,
		)
	}
}

func TestGenerateTurboReportSkipsUndecodablePrograms(t *testing.T) {
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
	if err := writeTurboTestProgram(
		filepath.Join(root, "good-v1.flat"),
		encoded,
	); err != nil {
		t.Fatalf("write Flat program: %v", err)
	}
	if err := writeTurboTestProgram(
		filepath.Join(root, "bad-v1.flat"),
		[]byte{0xff, 0xff, 0xff},
	); err != nil {
		t.Fatalf("write invalid Flat program: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(root, "unwrapped-v1.flat"),
		[]byte{0xff},
		0o600,
	); err != nil {
		t.Fatalf("write invalid CBOR wrapper: %v", err)
	}
	generatedAt := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	report, err := GenerateTurboReport(root, "", "", "", 1, 0, generatedAt)
	if err != nil {
		t.Fatalf("generate turbo report: %v", err)
	}
	if len(report.Benchmarks) != 1 || report.Benchmarks[0].Bench != "good-v1.flat" {
		t.Fatalf("benchmarks = %+v", report.Benchmarks)
	}
	if len(report.Skipped) != 2 ||
		report.Skipped[0].Bench != "bad-v1.flat" ||
		report.Skipped[1].Bench != "unwrapped-v1.flat" ||
		report.Skipped[0].Reason == "" || report.Skipped[1].Reason == "" {
		t.Fatalf("skipped = %+v", report.Skipped)
	}

	if err := os.Remove(filepath.Join(root, "good-v1.flat")); err != nil {
		t.Fatalf("remove valid program: %v", err)
	}
	if _, err := GenerateTurboReport(
		root, "", "", "", 1, 0, generatedAt,
	); err == nil {
		t.Fatal("report with no measurable program must be an error")
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
