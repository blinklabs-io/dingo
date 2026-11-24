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

package benchci

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The benchmark workflows run TestCheckFiles and TestGateFiles with `go test`
// instead of building a binary. Both skip unless their input variables are set.

// TestNoBenchciBinaries keeps the benchmark checks out of separate main
// packages: the workflows invoke them through go test.
func TestNoBenchciBinaries(t *testing.T) {
	t.Parallel()
	if _, err := os.Stat("cmd"); err == nil {
		t.Fatal("internal/benchci/cmd exists; run the checks through go test, not a main package")
	}
	out, err := exec.Command("go", "list", "-f", "{{.Name}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	if got := string(out); got != "benchci\n" {
		t.Fatalf("internal/benchci tree holds packages other than benchci: %q", got)
	}
}

// TestCheckFiles is the weekly regression check. It compares
// BENCHCI_OLD_FILE to BENCHCI_NEW_FILE over TrackedBenchmarks, writes the
// report to BENCHCI_REPORT, and appends "regression=<bool>" to BENCHCI_OUTPUT.
// A regression does not fail the test; the workflow reads the output line.
func TestCheckFiles(t *testing.T) {
	t.Parallel()
	oldFile := os.Getenv("BENCHCI_OLD_FILE")
	newFile := os.Getenv("BENCHCI_NEW_FILE")
	reportPath := os.Getenv("BENCHCI_REPORT")
	outputPath := os.Getenv("BENCHCI_OUTPUT")
	if oldFile == "" || newFile == "" || reportPath == "" || outputPath == "" {
		t.Skip("BENCHCI_OLD_FILE, BENCHCI_NEW_FILE, BENCHCI_REPORT and BENCHCI_OUTPUT not all set")
	}
	report, regressed, err := Compare(oldFile, newFile, TrackedBenchmarks)
	if err != nil {
		t.Fatalf("comparing %s to %s: %v", oldFile, newFile, err)
	}
	if err := os.WriteFile(reportPath, []byte(report), 0o600); err != nil {
		t.Fatalf("writing report: %v", err)
	}
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("opening output: %v", err)
	}
	if _, err := fmt.Fprintf(f, "regression=%t\n", regressed); err != nil {
		_ = f.Close()
		t.Fatalf("writing output: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("closing output: %v", err)
	}
}

// TestGateFiles is the per-pull-request gate. It compares BENCHCI_BASE_FILE to
// BENCHCI_HEAD_FILE, writes the report to BENCHCI_REPORT, and fails when a
// gate benchmark regressed or is missing from head.
func TestGateFiles(t *testing.T) {
	t.Parallel()
	baseFile := os.Getenv("BENCHCI_BASE_FILE")
	headFile := os.Getenv("BENCHCI_HEAD_FILE")
	reportPath := os.Getenv("BENCHCI_REPORT")
	if baseFile == "" || headFile == "" || reportPath == "" {
		t.Skip("BENCHCI_BASE_FILE, BENCHCI_HEAD_FILE and BENCHCI_REPORT not all set")
	}
	report, regressed, err := CompareGate(baseFile, headFile)
	if err != nil {
		t.Fatalf("comparing %s to %s: %v", baseFile, headFile, err)
	}
	if err := os.WriteFile(filepath.Clean(reportPath), []byte(report), 0o600); err != nil {
		t.Fatalf("writing report: %v", err)
	}
	if regressed {
		t.Fatalf(
			"a gate benchmark is significantly slower by more than %.0f%%, or missing from head",
			GateRegressionThresholdPercent,
		)
	}
}
