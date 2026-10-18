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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestGenerateReportOmitsFailedProgramFromPartialReport(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("UPLC turbo reports are Linux-only")
	}
	root := t.TempDir()
	epochDir := filepath.Join(root, "484")
	if err := os.MkdirAll(epochDir, 0o700); err != nil {
		t.Fatalf("create epoch directory: %v", err)
	}
	encoded, err := cbor.Marshal([]byte{0xff, 0xff})
	if err != nil {
		t.Fatalf("encode invalid Flat program: %v", err)
	}
	if err := os.WriteFile(filepath.Join(epochDir, "invalid-v1.flat"), encoded, 0o600); err != nil {
		t.Fatalf("write invalid Flat program: %v", err)
	}

	reportPath := filepath.Join(t.TempDir(), "report.json")
	generateErr := generateReport(root, reportPath, 1, 0)
	if generateErr == nil {
		t.Fatal("generate report succeeded for an invalid Flat program")
	}
	if !strings.Contains(generateErr.Error(), "benchmark invalid-v1.flat") {
		t.Fatalf("generation error = %v", generateErr)
	}
	reportJSON, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read partial report: %v", err)
	}
	var report struct {
		Scenario   string `json:"scenario"`
		Benchmarks []struct {
			BudgetCPU *int64   `json:"budget_cpu"`
			BudgetMem *int64   `json:"budget_mem"`
			MemBytes  *float64 `json:"mem_bytes"`
			CPUUS     *float64 `json:"cpu_us"`
		} `json:"benchmarks"`
	}
	if err := json.Unmarshal(reportJSON, &report); err != nil {
		t.Fatalf("decode partial report: %v", err)
	}
	if report.Scenario != "uplc-turbo" || len(report.Benchmarks) != 0 {
		t.Fatalf("partial report = %+v", report)
	}
	if strings.Contains(string(reportJSON), root) {
		t.Fatalf("partial report leaks corpus path: %s", reportJSON)
	}
}
