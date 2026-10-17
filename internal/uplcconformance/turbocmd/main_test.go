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

func TestGenerateReportWritesPartialReportOnProgramFailure(t *testing.T) {
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
	if err := generateReport(root, reportPath, 1, 0); err == nil {
		t.Fatal("generate report succeeded for an invalid Flat program")
	}
	reportJSON, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read partial report: %v", err)
	}
	var report struct {
		Scenario   string `json:"scenario"`
		Benchmarks []struct {
			BudgetCPU *int64   `json:"budget_cpu"`
			FlatUS    *float64 `json:"flat_us"`
			MemBytes  *float64 `json:"mem_bytes"`
			CPUUS     *float64 `json:"cpu_us"`
			Error     string   `json:"error"`
		} `json:"benchmarks"`
	}
	if err := json.Unmarshal(reportJSON, &report); err != nil {
		t.Fatalf("decode partial report: %v", err)
	}
	if report.Scenario != "uplc-turbo" || len(report.Benchmarks) != 1 {
		t.Fatalf("partial report = %+v", report)
	}
	benchmark := report.Benchmarks[0]
	if benchmark.BudgetCPU != nil || benchmark.FlatUS != nil || benchmark.MemBytes != nil ||
		benchmark.CPUUS != nil || benchmark.Error == "" ||
		strings.Contains(benchmark.Error, root) {
		t.Fatalf("failed benchmark has measurements: %+v", benchmark)
	}
}
