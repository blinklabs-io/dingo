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

// pathsFromEnv returns the named variables, or skips the test when any is
// unset. go test runs the test binary in the package directory, not where it
// was invoked, so a relative path would resolve under internal/benchci; it
// fails the test instead.
func pathsFromEnv(t *testing.T, names ...string) []string {
	t.Helper()
	paths := make([]string, len(names))
	for i, name := range names {
		paths[i] = os.Getenv(name)
		if paths[i] == "" {
			t.Skipf("%s not set", name)
		}
	}
	for i, name := range names {
		if !filepath.IsAbs(paths[i]) {
			t.Fatalf("%s=%q must be an absolute path", name, paths[i])
		}
	}
	return paths
}

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
	p := pathsFromEnv(
		t,
		"BENCHCI_OLD_FILE",
		"BENCHCI_NEW_FILE",
		"BENCHCI_REPORT",
		"BENCHCI_OUTPUT",
	)
	oldFile, newFile, reportPath, outputPath := p[0], p[1], p[2], p[3]
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
	p := pathsFromEnv(
		t,
		"BENCHCI_BASE_FILE",
		"BENCHCI_HEAD_FILE",
		"BENCHCI_REPORT",
	)
	baseFile, headFile, reportPath := p[0], p[1], p[2]
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
